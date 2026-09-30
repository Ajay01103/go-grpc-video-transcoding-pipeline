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

const (
	tileWidthPx  = 160
	tileHeightPx = 90
	columns      = 8
	stderrCapBytes = 64 * 1024
)

var permanentFFmpegPatterns = []string{
	"Invalid data found when processing input",
	"No such file or directory",
	"moov atom not found",
	"could not find codec parameters",
}

type cappedBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *cappedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.buf.Len()+len(p) > stderrCapBytes {
		return len(p), nil
	}
	return b.buf.Write(p)
}

func (b *cappedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

func vttTime(seconds float64) string {
	whole := int(seconds)
	h := whole / 3600
	m := (whole % 3600) / 60
	s := whole % 60
	ms := int((seconds - float64(whole)) * 1000)
	return fmt.Sprintf("%02d:%02d:%02d.%03d", h, m, s, ms)
}

func storyboardVTT(duration float64, interval int) string {
	var sb strings.Builder
	sb.WriteString("WEBVTT\n\n")
	for i, ts := 0, 0.0; ts < duration; i, ts = i+1, ts+float64(interval) {
		end := ts + float64(interval)
		row := i / columns
		col := i % columns
		sb.WriteString(fmt.Sprintf("%s --> %s\nstoryboard.jpg#xywh=%d,%d,%d,%d\n\n",
			vttTime(ts), vttTime(end),
			col*tileWidthPx, row*tileHeightPx, tileWidthPx, tileHeightPx))
	}
	return sb.String()
}

type storyboardWorker struct {
	store  *storage.Client
	bucket string
}

func (w *storyboardWorker) handle(ctx context.Context, publisher *events.Publisher, logger *zap.Logger, msg events.MsgAcker) error {
	request := new(pipelinepb.StoryboardRequested)
	if err := proto.Unmarshal(msg.Data(), request); err != nil {
		return err
	}
	if request.GetOutputPrefix() == "" {
		return fmt.Errorf("%w: output prefix is required", errPermanent)
	}

	stagingDir, err := os.MkdirTemp("", "mux-storyboard-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(stagingDir)

	// Heartbeat before staging so slow downloads don't expire AckWait (2 min).
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		ticker := time.NewTicker(20 * time.Second)
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

	bucket, key, err := storage.ParseSourceURI(request.GetSourceUri())
	if err != nil {
		return fmt.Errorf("%w: parse source URI: %s", errPermanent, err)
	}
	localSource := filepath.Join(stagingDir, "source")
	if err := w.store.GetToFile(ctx, bucket, key, localSource); err != nil {
		return fmt.Errorf("stage source: %w", err)
	}

	// Determine the number of tile rows from the video duration so we can
	// pass an explicit "8xN" layout to ffmpeg's tile filter.
	// The "8x*" wildcard syntax is not supported by all ffmpeg builds.
	interval := 5 // one frame every 5 seconds
	duration := request.GetDurationSeconds()
	if duration <= 0 {
		duration = 3600
	}
	totalFrames := int(duration)/interval + 1
	rows := (totalFrames + columns - 1) / columns // ceil division
	if rows < 1 {
		rows = 1
	}
	tileLayout := fmt.Sprintf("%dx%d", columns, rows)

	var stderrBuf cappedBuffer
	command := exec.CommandContext(ctx, "ffmpeg", "-y", "-i", localSource,
		"-vf", fmt.Sprintf("fps=1/%d,scale=%d:%d,tile=%s", interval, tileWidthPx, tileHeightPx, tileLayout),
		"-frames:v", "1",
		"-q:v", "3",
		filepath.Join(stagingDir, "storyboard.jpg"),
	)
	command.Stderr = io.MultiWriter(os.Stderr, &stderrBuf)

	if err := command.Run(); err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		classifiedErr := err
		stderr := stderrBuf.String()
		for _, pattern := range permanentFFmpegPatterns {
			if strings.Contains(stderr, pattern) {
				classifiedErr = fmt.Errorf("%w: %s", errPermanent, stderr)
				break
			}
		}
		isPermanent := errors.Is(classifiedErr, errPermanent)
		cfg := events.WorkerFailureConfig{ConsumerName: events.ConsumerStoryboardWorkers, Logger: logger}
		return events.HandleFinalAttempt(ctx, msg, cfg, isPermanent, func(pubCtx context.Context) error {
			failed := &pipelinepb.StoryboardFailed{
				RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
				ErrorCode: "STORYBOARD_FAILED", ErrorMessage: classifiedErr.Error(), Attempt: request.GetAttempt(),
				TimestampUnix: time.Now().UTC().Unix(),
			}
			msgID := events.StepTerminalMessageID(request.GetRunId(), "storyboard-failed", request.GetAttempt())
			return publisher.Publish(pubCtx, events.SubjectStoryboardFailed, msgID, failed)
		})
	}

	vttPath := filepath.Join(stagingDir, "storyboard.vtt")
	if err := os.WriteFile(vttPath, []byte(storyboardVTT(duration, interval)), 0644); err != nil {
		return err
	}

	spriteLocation := request.GetOutputPrefix() + "/storyboard/storyboard.jpg"
	vttLocation := request.GetOutputPrefix() + "/storyboard/storyboard.vtt"
	if err := w.store.PutFile(ctx, w.bucket, spriteLocation, filepath.Join(stagingDir, "storyboard.jpg")); err != nil {
		return fmt.Errorf("upload storyboard sprite: %w", err)
	}
	if err := w.store.PutFile(ctx, w.bucket, vttLocation, vttPath); err != nil {
		return fmt.Errorf("upload storyboard vtt: %w", err)
	}
	completed := &pipelinepb.StoryboardCompleted{
		RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
		SpriteLocation: spriteLocation, VttLocation: vttLocation,
		Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.StepTerminalMessageID(request.GetRunId(), "storyboard-completed", request.GetAttempt())
	return publisher.Publish(ctx, events.SubjectStoryboardCompleted, msgID, completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "storyboard service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerStoryboardWorkers)
	if err != nil {
		return err
	}
	publisher := events.NewPublisher(js)
	cfg := storage.ConfigFromEnv()
	worker := &storyboardWorker{store: storage.New(cfg), bucket: cfg.Bucket}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("STORYBOARD WORKER started", zap.String("nats", natsURL))

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
				handleErr := worker.handle(ctx, publisher, logger, msg)
				switch {
				case errors.Is(handleErr, events.ErrMessageTerminated):
				case handleErr != nil && ctx.Err() == nil:
					logger.Warn("storyboard attempt failed, will retry", zap.Error(handleErr))
					_ = msg.Nak()
				case handleErr != nil:
					_ = msg.Nak()
				default:
					logger.Info("storyboard completed, sprite + vtt uploaded")
					_ = msg.Ack()
				}
			}
		}
	}()

	server := &http.Server{
		Addr:    ":50084",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	go server.ListenAndServe()

	<-ctx.Done()
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
