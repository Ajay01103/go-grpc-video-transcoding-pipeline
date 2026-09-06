package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

var errMessageTerminated = errors.New("message terminated")

func generateThumbnail(ctx context.Context, source, output string) error {
	if err := os.MkdirAll(filepath.Dir(output), 0755); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "ffmpeg", "-y", "-i", source, "-ss", "00:00:01", "-vframes", "1", "-vf", "scale=1280:-1", "-f", "image2", output)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("thumbnail ffmpeg failed: %w: %s", err, output)
	}
	return nil
}

func handle(ctx context.Context, publisher *events.Publisher, message jetstream.Msg) error {
	request := new(pipelinepb.ThumbnailRequested)
	if err := proto.Unmarshal(message.Data(), request); err != nil {
		return err
	}
	metadata, err := message.Metadata()
	if err != nil {
		return err
	}
	outputPrefix := request.GetOutputPrefix()
	if outputPrefix == "" {
		return fmt.Errorf("output prefix is required")
	}
	temporaryDir, err := os.MkdirTemp("", "mux-thumbnail-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryDir)
	temporaryFile := filepath.Join(temporaryDir, "thumbnail.webp")
	if err := generateThumbnail(ctx, request.GetSourceUri(), temporaryFile); err != nil {
		if metadata.NumDelivered < 5 {
			return err
		}
		failed := &pipelinepb.ThumbnailFailed{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), ErrorCode: "THUMBNAIL_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
		if publishErr := publisher.Publish(ctx, events.SubjectThumbnailFailed, events.TerminalMessageID(request.GetRunId(), "thumbnail-failed"), failed); publishErr != nil {
			return publishErr
		}
		if termErr := message.Term(); termErr != nil {
			return termErr
		}
		return errMessageTerminated
	}
	finalDir := filepath.Join(outputPrefix, "thumbnail")
	if err := os.MkdirAll(filepath.Dir(finalDir), 0755); err != nil {
		return err
	}
	if err := os.RemoveAll(finalDir); err != nil {
		return err
	}
	if err := os.Rename(temporaryDir, finalDir); err != nil {
		return fmt.Errorf("commit thumbnail output: %w", err)
	}
	completed := &pipelinepb.ThumbnailCompleted{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), Location: filepath.Join(finalDir, "thumbnail.webp"), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
	return publisher.Publish(ctx, events.SubjectThumbnailCompleted, events.TerminalMessageID(request.GetRunId(), "thumbnail-completed"), completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "thumbnail service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	natsURL := os.Getenv("NATS_URL")
	if natsURL == "" {
		natsURL = "nats://localhost:4222"
	}
	js, nc, err := events.Connect(context.Background(), natsURL)
	if err != nil {
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
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		for ctx.Err() == nil {
			batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if fetchErr != nil {
				continue
			}
			for message := range batch.Messages() {
				handleErr := handle(ctx, publisher, message)
				if errors.Is(handleErr, errMessageTerminated) {
					continue
				}
				if handleErr != nil && ctx.Err() == nil {
					_ = message.Nak()
				}
				if handleErr == nil {
					_ = message.Ack()
				}
			}
		}
	}()
	server := &http.Server{Addr: ":50083", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
	go server.ListenAndServe()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
