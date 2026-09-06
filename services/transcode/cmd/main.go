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

func ffmpegArgs(source, output string) []string {
	args := []string{"-y", "-i", source, "-preset", "medium"}
	filterParts := []string{fmt.Sprintf("[0:v]split=%d", len(ladder))}
	for index, item := range ladder {
		filterParts[0] += fmt.Sprintf("[vsrc%d]", index)
		filterParts = append(filterParts, fmt.Sprintf("[vsrc%d]scale=w=%d:h=%d[v%d]", index, item.width, item.height, index))
	}
	args = append(args, "-filter_complex", strings.Join(filterParts, ";"))
	for index := range ladder {
		args = append(args, "-map", fmt.Sprintf("[v%d]", index), "-map", "0:a:0")
	}
	streamMap := make([]string, 0, len(ladder))
	for index, item := range ladder {
		streamMap = append(streamMap, fmt.Sprintf("v:%d,a:%d", index, index))
		args = append(args,
			fmt.Sprintf("-c:v:%d", index), "libx264",
			fmt.Sprintf("-c:a:%d", index), "aac",
			fmt.Sprintf("-b:v:%d", index), item.bitrate,
			fmt.Sprintf("-maxrate:v:%d", index), item.bitrate,
			fmt.Sprintf("-bufsize:v:%d", index), item.bitrate,
		)
	}
	args = append(args,
		"-var_stream_map", strings.Join(streamMap, " "),
		"-f", "hls", "-hls_time", "6", "-hls_playlist_type", "vod",
		"-hls_segment_type", "fmp4", "-master_pl_name", "master.m3u8",
		filepath.Join(output, "stream_%v", "segment_%05d.m4s"),
	)
	return args
}

func transcode(ctx context.Context, message jetstream.Msg, request *pipelinepb.TranscodeRequested) error {
	metadata, err := message.Metadata()
	if err != nil {
		return err
	}
	if metadata.NumDelivered > 1 {
		return fmt.Errorf("transcode delivery %d", metadata.NumDelivered)
	}
	temporaryDir, err := os.MkdirTemp("", "mux-transcode-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryDir)
	command := exec.CommandContext(ctx, "ffmpeg", ffmpegArgs(request.GetSourceUri(), temporaryDir)...)
	command.Stdout = os.Stdout
	command.Stderr = os.Stderr
	done := make(chan error, 1)
	go func() { done <- command.Run() }()
	heartbeat := time.NewTicker(20 * time.Second)
	defer heartbeat.Stop()
	for {
		select {
		case err := <-done:
			if err != nil {
				return fmt.Errorf("transcode failed: %w", err)
			}
			outputPrefix := request.GetOutputPrefix()
			if outputPrefix == "" {
				return fmt.Errorf("output prefix is required")
			}
			if err := os.MkdirAll(filepath.Dir(outputPrefix), 0755); err != nil {
				return err
			}
			if err := os.RemoveAll(outputPrefix); err != nil {
				return err
			}
			if err := os.Rename(temporaryDir, outputPrefix); err != nil {
				return fmt.Errorf("commit transcode output: %w", err)
			}
			return nil
		case <-heartbeat.C:
			if err := message.InProgress(); err != nil {
				return fmt.Errorf("transcode heartbeat: %w", err)
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func handle(ctx context.Context, publisher *events.Publisher, message jetstream.Msg) error {
	request := new(pipelinepb.TranscodeRequested)
	if err := proto.Unmarshal(message.Data(), request); err != nil {
		return err
	}
	if err := transcode(ctx, message, request); err != nil {
		metadata, metadataErr := message.Metadata()
		if metadataErr != nil || metadata.NumDelivered < 5 {
			return err
		}
		failed := &pipelinepb.TranscodeFailed{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), ErrorCode: "TRANSCODE_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
		if publishErr := publisher.Publish(ctx, events.SubjectTranscodeFailed, events.TerminalMessageID(request.GetRunId(), "transcode-failed"), failed); publishErr != nil {
			return publishErr
		}
		if termErr := message.Term(); termErr != nil {
			return termErr
		}
		return errMessageTerminated
	}
	completed := &pipelinepb.TranscodeCompleted{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), OutputPrefix: request.GetOutputPrefix(), MasterPlaylistLocation: filepath.Join(request.GetOutputPrefix(), "master.m3u8"), RenditionCount: int32(len(ladder)), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
	return publisher.Publish(ctx, events.SubjectTranscodeCompleted, events.TerminalMessageID(request.GetRunId(), "transcode-completed"), completed)
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "transcode service exited: %v\n", err)
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
	consumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerTranscodeWorkers)
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
	server := &http.Server{Addr: ":50082", Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { w.WriteHeader(http.StatusOK) })}
	go server.ListenAndServe()
	<-ctx.Done()
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	return server.Shutdown(shutdownCtx)
}
