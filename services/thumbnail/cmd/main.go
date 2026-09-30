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

var permanentFFmpegPatterns = []string{
	"Invalid data found when processing input",
	"No such file or directory",
	"moov atom not found",
	"could not find codec parameters",
}

const stderrCapBytes = 64 * 1024

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

type thumbnailWorker struct {
	store  *storage.Client
	bucket string
}

func (w *thumbnailWorker) handle(ctx context.Context, publisher *events.Publisher, logger *zap.Logger, msg events.MsgAcker) error {
	request := new(pipelinepb.ThumbnailRequested)
	if err := proto.Unmarshal(msg.Data(), request); err != nil {
		return err
	}
	outputPrefix := request.GetOutputPrefix()
	if outputPrefix == "" {
		return fmt.Errorf("%w: output prefix is required", errPermanent)
	}

	stagingDir, err := os.MkdirTemp("", "mux-thumbnail-")
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

	temporaryFile := filepath.Join(stagingDir, "thumbnail.webp")
	if err := os.MkdirAll(filepath.Dir(temporaryFile), 0755); err != nil {
		return err
	}

	var stderrBuf cappedBuffer
	command := exec.CommandContext(ctx, "ffmpeg",
		"-y", "-i", localSource, "-ss", "00:00:01",
		"-frames:v", "1", "-update", "1", "-vf", "scale=1280:-1", temporaryFile,
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
		cfg := events.WorkerFailureConfig{ConsumerName: events.ConsumerThumbnailWorkers, Logger: logger}
		return events.HandleFinalAttempt(ctx, msg, cfg, isPermanent, func(pubCtx context.Context) error {
			failed := &pipelinepb.ThumbnailFailed{
				RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
				ErrorCode: "THUMBNAIL_FAILED", ErrorMessage: classifiedErr.Error(), Attempt: request.GetAttempt(),
				TimestampUnix: time.Now().UTC().Unix(),
			}
			msgID := events.StepTerminalMessageID(request.GetRunId(), "thumbnail-failed", request.GetAttempt())
			return publisher.Publish(pubCtx, events.SubjectThumbnailFailed, msgID, failed)
		})
	}

	// Durable publish: upload to RustFS before emitting the completed event.
	location := outputPrefix + "/thumbnails/thumbnail.webp"
	if err := w.store.PutFile(ctx, w.bucket, location, temporaryFile); err != nil {
		return fmt.Errorf("upload thumbnail: %w", err)
	}
	completed := &pipelinepb.ThumbnailCompleted{
		RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
		Location: location, Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.StepTerminalMessageID(request.GetRunId(), "thumbnail-completed", request.GetAttempt())
	return publisher.Publish(ctx, events.SubjectThumbnailCompleted, msgID, completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "thumbnail service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerThumbnailWorkers)
	if err != nil {
		return err
	}
	publisher := events.NewPublisher(js)
	cfg := storage.ConfigFromEnv()
	worker := &thumbnailWorker{store: storage.New(cfg), bucket: cfg.Bucket}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	logger.Info("THUMBNAIL WORKER started", zap.String("nats", natsURL))

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
					logger.Warn("thumbnail attempt failed, will retry", zap.Error(handleErr))
					_ = msg.Nak()
				case handleErr != nil:
					_ = msg.Nak()
				default:
					logger.Info("thumbnail completed, artifact uploaded")
					_ = msg.Ack()
				}
			}
		}
	}()

	server := &http.Server{
		Addr:    ":50083",
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) }),
	}
	go server.ListenAndServe()

	<-ctx.Done()
	wg.Wait()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
