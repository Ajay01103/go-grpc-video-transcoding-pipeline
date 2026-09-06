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
	"strings"
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

var errMessageTerminated = errors.New("message terminated")

func storyboardVTT(duration float64, interval, tileWidth, tileHeight int) string {
	const tileWidthPx = 160
	const tileHeightPx = 90
	const columns = 8
	var builder strings.Builder
	builder.WriteString("WEBVTT\n\n")
	index := 0
	for timestamp := 0.0; timestamp < duration; timestamp += float64(interval) {
		end := timestamp + float64(interval)
		row := index / columns
		column := index % columns
		builder.WriteString(fmt.Sprintf("%s --> %s\nstoryboard.jpg#xywh=%d,%d,%d,%d\n\n", vttTime(timestamp), vttTime(end), column*tileWidthPx, row*tileHeightPx, tileWidthPx, tileHeightPx))
		index++
	}
	return builder.String()
}

func vttTime(seconds float64) string {
	whole := int(seconds)
	hours := whole / 3600
	minutes := (whole % 3600) / 60
	remaining := whole % 60
	millis := int((seconds - float64(whole)) * 1000)
	return fmt.Sprintf("%02d:%02d:%02d.%03d", hours, minutes, remaining, millis)
}

func generateStoryboard(ctx context.Context, source, outputDir string) error {
	if err := os.MkdirAll(outputDir, 0755); err != nil {
		return err
	}
	command := exec.CommandContext(ctx, "ffmpeg", "-y", "-i", source, "-vf", "fps=1/5,scale=160:90,tile=8x?", filepath.Join(outputDir, "storyboard.jpg"))
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("storyboard ffmpeg failed: %w: %s", err, output)
	}
	return nil
}

func handle(ctx context.Context, publisher *events.Publisher, message jetstream.Msg) error {
	request := new(pipelinepb.StoryboardRequested)
	if err := proto.Unmarshal(message.Data(), request); err != nil {
		return err
	}
	metadata, err := message.Metadata()
	if err != nil {
		return err
	}
	if request.GetOutputPrefix() == "" {
		return fmt.Errorf("output prefix is required")
	}
	temporaryDir, err := os.MkdirTemp("", "mux-storyboard-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryDir)
	if err := generateStoryboard(ctx, request.GetSourceUri(), temporaryDir); err != nil {
		if metadata.NumDelivered < 5 {
			return err
		}
		failed := &pipelinepb.StoryboardFailed{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), ErrorCode: "STORYBOARD_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
		if publishErr := publisher.Publish(ctx, events.SubjectStoryboardFailed, events.TerminalMessageID(request.GetRunId(), "storyboard-failed"), failed); publishErr != nil {
			return publishErr
		}
		if termErr := message.Term(); termErr != nil {
			return termErr
		}
		return errMessageTerminated
	}
	vttPath := filepath.Join(temporaryDir, "storyboard.vtt")
	if err := os.WriteFile(vttPath, []byte(storyboardVTT(3600, 5, 1280, 680)), 0644); err != nil {
		return err
	}
	finalDir := filepath.Join(request.GetOutputPrefix(), "storyboard")
	if err := os.MkdirAll(filepath.Dir(finalDir), 0755); err != nil {
		return err
	}
	if err := os.RemoveAll(finalDir); err != nil {
		return err
	}
	if err := os.Rename(temporaryDir, finalDir); err != nil {
		return fmt.Errorf("commit storyboard output: %w", err)
	}
	completed := &pipelinepb.StoryboardCompleted{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), SpriteLocation: filepath.Join(finalDir, "storyboard.jpg"), VttLocation: filepath.Join(finalDir, "storyboard.vtt"), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
	return publisher.Publish(ctx, events.SubjectStoryboardCompleted, events.TerminalMessageID(request.GetRunId(), "storyboard-completed"), completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "storyboard service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerStoryboardWorkers)
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
	server := &http.Server{Addr: ":50084", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
	go server.ListenAndServe()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
