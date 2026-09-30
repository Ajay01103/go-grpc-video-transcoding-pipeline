package main

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/Ajay01103/go-mux/pkg/events"
	pkglogger "github.com/Ajay01103/go-mux/pkg/logger"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/Ajay01103/go-mux/pkg/storage"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var errPermanent = errors.New("permanent error")

// ──────────────────────────────────────────────────────────────────
// Adaptive ladder
// ──────────────────────────────────────────────────────────────────

type rendition struct {
	name    string
	width   int
	height  int
	bitrate string
}

var ladder = []rendition{
	{name: "1080p", width: 1920, height: 1080, bitrate: "5000k"},
	{name: "720p", width: 1280, height: 720, bitrate: "2800k"},
	{name: "480p", width: 854, height: 480, bitrate: "1400k"},
	{name: "360p", width: 640, height: 360, bitrate: "800k"},
}

var transcodePreset = envOr("TRANSCODE_PRESET", "veryfast")
var heartbeatInterval = 20 * time.Second

func envOr(name, fallback string) string {
	if v := strings.TrimSpace(os.Getenv(name)); v != "" {
		return v
	}
	return fallback
}

func sourceHeight(ctx context.Context, path string) (int, error) {
	output, err := exec.CommandContext(ctx, "ffprobe",
		"-v", "error", "-select_streams", "v:0",
		"-show_entries", "stream=height", "-of", "csv=p=0", path,
	).Output()
	if err != nil {
		return 0, fmt.Errorf("%w: ffprobe height: %s", errPermanent, err)
	}
	var height int
	if _, err := fmt.Sscanf(strings.TrimSpace(string(output)), "%d", &height); err != nil {
		return 0, fmt.Errorf("%w: parse ffprobe height %q: %s", errPermanent, strings.TrimSpace(string(output)), err)
	}
	return height, nil
}

func ladderFor(sh int) []rendition {
	filtered := make([]rendition, 0, len(ladder))
	for _, r := range ladder {
		if r.height <= sh {
			filtered = append(filtered, r)
		}
	}
	if len(filtered) == 0 {
		filtered = append(filtered, ladder[len(ladder)-1])
	}
	return filtered
}

func ffmpegArgs(source, output string, rungs []rendition) []string {
	args := []string{"-y", "-i", source, "-preset", transcodePreset}
	filterParts := []string{fmt.Sprintf("[0:v]split=%d", len(rungs))}
	for i, r := range rungs {
		filterParts[0] += fmt.Sprintf("[vsrc%d]", i)
		filterParts = append(filterParts, fmt.Sprintf("[vsrc%d]scale=w=%d:h=%d[v%d]", i, r.width, r.height, i))
	}
	args = append(args, "-filter_complex", strings.Join(filterParts, ";"))
	for i := range rungs {
		args = append(args, "-map", fmt.Sprintf("[v%d]", i), "-map", "0:a:0")
	}
	streamMap := make([]string, 0, len(rungs))
	for i, r := range rungs {
		streamMap = append(streamMap, fmt.Sprintf("v:%d,a:%d", i, i))
		args = append(args,
			fmt.Sprintf("-c:v:%d", i), "libx264",
			fmt.Sprintf("-c:a:%d", i), "aac",
			fmt.Sprintf("-b:v:%d", i), r.bitrate,
			fmt.Sprintf("-maxrate:v:%d", i), r.bitrate,
			fmt.Sprintf("-bufsize:v:%d", i), r.bitrate,
		)
	}
	// Use forward slashes for the ffmpeg path templates so the %v and %05d
	// placeholders are not mangled by Windows backslash path joining.
	// Pass the segment filename via -hls_segment_filename (the correct flag
	// for multi-variant HLS); the positional output arg only carries the
	// per-stream playlist path.
	outputFwd := filepath.ToSlash(output)
	segmentTemplate := outputFwd + "/stream_%v/segment_%05d.m4s"
	playlistTemplate := outputFwd + "/stream_%v/index.m3u8"
	args = append(args,
		"-var_stream_map", strings.Join(streamMap, " "),
		"-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4", "-master_pl_name", "master.m3u8",
		"-hls_segment_filename", segmentTemplate,
		playlistTemplate,
	)
	return args
}

// ──────────────────────────────────────────────────────────────────
// cappedBuffer — bounded stderr capture for classifyFFmpegError
// ──────────────────────────────────────────────────────────────────

const stderrCapBytes = 64 * 1024

type cappedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len()+len(p) > stderrCapBytes {
		return len(p), nil // silently drop once cap is hit
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// permanentFFmpegPatterns are substrings in ffmpeg stderr that indicate the
// source is permanently unusable.  Retrying will never help.
var permanentFFmpegPatterns = []string{
	"Invalid data found when processing input",
	"No such file or directory",
	"moov atom not found",
	"Invalid NAL unit size",
	"could not find codec parameters",
	"Unknown encoder",
}

func classifyFFmpegError(err error, stderr string) error {
	for _, pattern := range permanentFFmpegPatterns {
		if strings.Contains(stderr, pattern) {
			return fmt.Errorf("%w: %s", errPermanent, stderr)
		}
	}
	return fmt.Errorf("transcode failed: %w", err)
}

// ──────────────────────────────────────────────────────────────────
// transcodeWorker
// ──────────────────────────────────────────────────────────────────

type transcodeWorker struct {
	store  *storage.Client
	bucket string
}

func (w *transcodeWorker) transcode(ctx context.Context, msg events.MsgAcker,
	request *pipelinepb.TranscodeRequested, renditionCount *int) error {

	bucket, key, err := storage.ParseSourceURI(request.GetSourceUri())
	if err != nil {
		return fmt.Errorf("%w: parse source URI: %s", errPermanent, err)
	}
	outputPrefix := request.GetOutputPrefix()
	if outputPrefix == "" {
		return fmt.Errorf("%w: output prefix is required", errPermanent)
	}

	stagingDir, err := os.MkdirTemp("", "mux-transcode-stage-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stagingDir)

	// Heartbeat before staging so slow RustFS downloads don't expire AckWait.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		ticker := time.NewTicker(heartbeatInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = msg.InProgress()
			case <-hbCtx.Done():
				return
			}
		}
	}()

	localSource := filepath.Join(stagingDir, "source")
	if err := w.store.GetToFile(ctx, bucket, key, localSource); err != nil {
		return fmt.Errorf("stage source: %w", err)
	}

	height, err := sourceHeight(ctx, localSource)
	if err != nil {
		return err // already wrapped with errPermanent inside sourceHeight
	}
	rungs := ladderFor(height)

	localOut := filepath.Join(stagingDir, "out")
	if err := os.MkdirAll(localOut, 0o755); err != nil {
		return err
	}

	var stderrBuf cappedBuffer
	command := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs(localSource, localOut, rungs)...)
	command.Stdout = os.Stdout
	command.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			// Shutdown: ffmpeg killed by context cancellation.
			// Staging dir cleaned by defer.  Return ctx.Err() so the caller NAKs.
			return ctx.Err()
		}
		return classifyFFmpegError(err, stderrBuf.String())
	}

	// Upload artifacts to RustFS before emitting the completed event (durable publish guarantee).
	if err := w.store.UploadDir(ctx, w.bucket, outputPrefix+"/hls", localOut); err != nil {
		return fmt.Errorf("upload hls artifacts: %w", err)
	}
	*renditionCount = len(rungs)
	return nil
}

func handle(ctx context.Context, publisher *events.Publisher, logger *zap.Logger,
	msg events.MsgAcker, worker *transcodeWorker) error {

	request := new(pipelinepb.TranscodeRequested)
	if err := proto.Unmarshal(msg.Data(), request); err != nil {
		return err
	}
	renditionCount := 0
	if err := worker.transcode(ctx, msg, request, &renditionCount); err != nil {
		isPermanent := errors.Is(err, errPermanent)
		cfg := events.WorkerFailureConfig{ConsumerName: events.ConsumerTranscodeWorkers, Logger: logger}
		return events.HandleFinalAttempt(ctx, msg, cfg, isPermanent, func(pubCtx context.Context) error {
			failed := &pipelinepb.TranscodeFailed{
				RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
				ErrorCode: "TRANSCODE_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(),
				TimestampUnix: time.Now().UTC().Unix(),
			}
			msgID := events.StepTerminalMessageID(request.GetRunId(), "transcode-failed", request.GetAttempt())
			return publisher.Publish(pubCtx, events.SubjectTranscodeFailed, msgID, failed)
		})
	}
	completed := &pipelinepb.TranscodeCompleted{
		RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
		OutputPrefix:           request.GetOutputPrefix(),
		MasterPlaylistLocation: request.GetOutputPrefix() + "/hls/master.m3u8",
		RenditionCount:         int32(renditionCount),
		Attempt:                request.GetAttempt(),
		TimestampUnix:          time.Now().UTC().Unix(),
	}
	msgID := events.StepTerminalMessageID(request.GetRunId(), "transcode-completed", request.GetAttempt())
	return publisher.Publish(ctx, events.SubjectTranscodeCompleted, msgID, completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "transcode service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := pkglogger.New()
	defer logger.Sync()

	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	js, nc, err := events.Connect(context.Background(), natsURL)
	if err != nil {
		logger.Error("cannot connect to nats", zap.Error(err))
		return err
	}
	defer nc.Drain()
	if err := events.EnsureStreams(context.Background(), js); err != nil {
		return err
	}
	if err := events.EnsureWorkerConsumers(context.Background(), js); err != nil {
		return err
	}
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerTranscodeWorkers)
	if err != nil {
		return err
	}
	publisher := events.NewPublisher(js)
	cfg := storage.ConfigFromEnv()
	worker := &transcodeWorker{store: storage.New(cfg), bucket: cfg.Bucket}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("TRANSCODE WORKER started", zap.String("nats", natsURL))

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for ctx.Err() == nil {
			batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if fetchErr != nil {
				continue
			}
			for msg := range batch.Messages() {
				handleErr := handle(ctx, publisher, logger, msg, worker)
				switch {
				case errors.Is(handleErr, events.ErrMessageTerminated):
					// Term() already called inside HandleFinalAttempt.
				case handleErr != nil && ctx.Err() == nil:
					logger.Warn("transcode attempt failed, will retry", zap.Error(handleErr))
					_ = msg.Nak()
				case handleErr != nil:
					_ = msg.Nak()
				default:
					logger.Info("transcode completed, HLS artifacts uploaded")
					_ = msg.Ack()
				}
			}
		}
	}()

	server := &http.Server{
		Addr:    ":50082",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	go server.ListenAndServe()

	<-ctx.Done()
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
