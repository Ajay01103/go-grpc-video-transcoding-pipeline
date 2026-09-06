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
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

var errMessageTerminated = errors.New("message terminated")

type probeWorker struct {
	ffprobe string
}

type probeStream struct {
	CodecType string `json:"codec_type"`
	CodecName string `json:"codec_name"`
	Duration  string `json:"duration"`
	Width     int32  `json:"width"`
	Height    int32  `json:"height"`
	BitRate   int64  `json:"bit_rate,string"`
}

func (w *probeWorker) probe(ctx context.Context, sourceURI string) (*pipelinepb.ProbeResult, error) {
	cmd := exec.CommandContext(ctx, w.ffprobe,
		"-v", "error",
		"-show_entries", "stream=codec_type,codec_name,duration,width,height,bit_rate",
		"-of", "json", sourceURI,
	)
	output, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("ffprobe failed: %w", err)
	}
	var response struct {
		Streams []probeStream `json:"streams"`
	}
	if err := json.Unmarshal(output, &response); err != nil {
		return nil, fmt.Errorf("decode ffprobe response: %w", err)
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
				if _, err := fmt.Sscanf(stream.Duration, "%f", &result.Duration); err != nil {
					return nil, fmt.Errorf("parse video duration: %w", err)
				}
			}
		case "audio":
			result.AudioCodec = stream.CodecName
		}
	}
	return result, nil
}

func (w *probeWorker) handle(ctx context.Context, publisher *events.Publisher, message jetstream.Msg) error {
	request := new(pipelinepb.ProbeRequested)
	if err := proto.Unmarshal(message.Data(), request); err != nil {
		return fmt.Errorf("decode probe request: %w", err)
	}
	result, err := w.probe(ctx, request.GetSourceUri())
	if err != nil {
		metadata, metadataErr := message.Metadata()
		if metadataErr != nil || metadata.NumDelivered < 5 {
			return fmt.Errorf("probe attempt failed: %w", err)
		}
		failed := &pipelinepb.ProbeFailed{
			RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
			ErrorCode: "PROBE_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(),
			TimestampUnix: time.Now().UTC().Unix(),
		}
		if publishErr := publisher.Publish(ctx, events.SubjectProbeFailed, events.TerminalMessageID(request.GetRunId(), "probe-failed"), failed); publishErr != nil {
			return publishErr
		}
		if termErr := message.Term(); termErr != nil {
			return termErr
		}
		return errMessageTerminated
	}
	completed := &pipelinepb.ProbeCompleted{
		RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(),
		SourceUri: request.GetSourceUri(), Result: result, Attempt: request.GetAttempt(),
		TimestampUnix: time.Now().UTC().Unix(),
	}
	return publisher.Publish(ctx, events.SubjectProbeCompleted, events.TerminalMessageID(request.GetRunId(), "probe-completed"), completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "probe service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerProbeWorkers)
	if err != nil {
		return err
	}
	publisher := events.NewPublisher(js)
	worker := &probeWorker{ffprobe: "ffprobe"}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	go func() {
		for ctx.Err() == nil {
			batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if fetchErr != nil {
				continue
			}
			for message := range batch.Messages() {
				handleErr := worker.handle(ctx, publisher, message)
				if errors.Is(handleErr, errMessageTerminated) {
					continue
				}
				if handleErr != nil && ctx.Err() == nil {
					_ = message.Nak()
				}
				if handleErr == nil {
					_ = message.Ack()
				}
				if ctx.Err() != nil {
					return
				}
			}
		}
	}()
	server := &http.Server{Addr: ":50081", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
	go server.ListenAndServe()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
