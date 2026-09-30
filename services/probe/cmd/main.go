package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
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

// errPermanent wraps errors that cannot be fixed by retrying (e.g. corrupt /
// missing source file).  Workers return fmt.Errorf("%w: …", errPermanent).
var errPermanent = errors.New("permanent error")

type probeStream struct {
	CodecType string `json:"codec_type"`
	CodecName string `json:"codec_name"`
	Duration  string `json:"duration"`
	Width     int32  `json:"width"`
	Height    int32  `json:"height"`
	BitRate   int64  `json:"bit_rate,string"`
}

type probeWorker struct {
	ffprobe string
	store   *storage.Client
	bucket  string
}

// probe stages the source locally, runs ffprobe, and returns the result.
func (w *probeWorker) probe(ctx context.Context, sourceURI string) (*pipelinepb.ProbeResult, error) {
	bucket, key, err := storage.ParseSourceURI(sourceURI)
	if err != nil {
		// Unparseable URI will never succeed — permanent.
		return nil, fmt.Errorf("%w: parse source URI: %s", errPermanent, err)
	}
	stagingDir, err := os.MkdirTemp("", "mux-probe-stage-")
	if err != nil {
		return nil, err
	}
	defer os.RemoveAll(stagingDir)

	localPath := filepath.Join(stagingDir, "source")
	if err := w.store.GetToFile(ctx, bucket, key, localPath); err != nil {
		return nil, fmt.Errorf("stage source: %w", err)
	}

	cmd := exec.CommandContext(ctx, w.ffprobe,
		"-v", "error",
		"-show_entries", "stream=codec_type,codec_name,duration,width,height,bit_rate",
		"-of", "json", localPath,
	)
	output, err := cmd.Output()
	if err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			// Non-zero exit from ffprobe means the file is unreadable — permanent.
			return nil, fmt.Errorf("%w: ffprobe failed (exit %d): %s",
				errPermanent, exitErr.ExitCode(), string(exitErr.Stderr))
		}
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}

	var response struct {
		Streams []probeStream `json:"streams"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, fmt.Errorf("%w: decode ffprobe response: %s", errPermanent, err)
	}
	result := &pipelinepb.ProbeResult{}
	for _, stream := range response.Streams {
		switch stream.CodecType {
		case "video":
			result.VideoCodec = stream.CodecName
			result.Width = stream.Width
			result.Height = stream.Height
			result.BitRate = stream.BitRate
			if stream.Duration != "" {
				if _, scanErr := fmt.Sscanf(stream.Duration, "%f", &result.Duration); scanErr != nil {
					return nil, fmt.Errorf("%w: parse video duration: %s", errPermanent, scanErr)
				}
			}
		case "audio":
			result.AudioCodec = stream.CodecName
		}
	}
	return result, nil
}

func (w *probeWorker) handle(ctx context.Context, publisher *events.Publisher, logger *zap.Logger, msg events.MsgAcker) error {
	request := new(pipelinepb.ProbeRequested)
	if err := proto.Unmarshal(msg.Data(), request); err != nil {
		return fmt.Errorf("decode probe request: %w", err)
	}

	// Heartbeat before staging so a slow RustFS download doesn't expire AckWait.
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

	result, err := w.probe(ctx, request.GetSourceUri())
	if err != nil {
		isPermanent := errors.Is(err, errPermanent)
		cfg := events.WorkerFailureConfig{ConsumerName: events.ConsumerProbeWorkers, Logger: logger}
		return events.HandleFinalAttempt(ctx, msg, cfg, isPermanent, func(pubCtx context.Context) error {
			failed := &pipelinepb.ProbeFailed{
				RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
				ErrorCode: "PROBE_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(),
				TimestampUnix: time.Now().UTC().Unix(),
			}
			msgID := events.StepTerminalMessageID(request.GetRunId(), "probe-failed", request.GetAttempt())
			return publisher.Publish(pubCtx, events.SubjectProbeFailed, msgID, failed)
		})
	}

	completed := &pipelinepb.ProbeCompleted{
		RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
		SourceUri: request.GetSourceUri(), Result: result, Attempt: request.GetAttempt(),
		TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.StepTerminalMessageID(request.GetRunId(), "probe-completed", request.GetAttempt())
	return publisher.Publish(ctx, events.SubjectProbeCompleted, msgID, completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "probe service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerProbeWorkers)
	if err != nil {
		return err
	}
	publisher := events.NewPublisher(js)
	cfg := storage.ConfigFromEnv()
	worker := &probeWorker{ffprobe: "ffprobe", store: storage.New(cfg), bucket: cfg.Bucket}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("PROBE WORKER started", zap.String("nats", natsURL))

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
					// Term() already called inside HandleFinalAttempt.
				case handleErr != nil && ctx.Err() == nil:
					logger.Warn("probe attempt failed, will retry", zap.Error(handleErr))
					_ = msg.Nak()
				case handleErr != nil:
					// Shutdown path: ctx cancelled, NAK so another replica picks it up.
					_ = msg.Nak()
				default:
					logger.Info("probe completed")
					_ = msg.Ack()
				}
			}
		}
	}()

	server := &http.Server{
		Addr:    ":50081",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	go server.ListenAndServe()

	<-ctx.Done()
	wg.Wait() // wait for any in-flight NAK before Drain()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
