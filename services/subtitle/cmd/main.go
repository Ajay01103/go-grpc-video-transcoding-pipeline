package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/Ajay01103/go-notion/subtitle/config"
	"github.com/Ajay01103/go-notion/subtitle/db"
	"github.com/Ajay01103/go-notion/subtitle/internal/repository"
	"github.com/Ajay01103/go-notion/subtitle/internal/transcriber"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "subtitle service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger, _ := zap.NewProduction()
	defer logger.Sync()

	cfg, err := config.Load()
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	session, err := db.Connect(ctx, db.Config{
		Hosts:             cfg.ScyllaHosts,
		Port:              cfg.ScyllaPort,
		Username:          cfg.ScyllaUsername,
		Password:          cfg.ScyllaPassword,
		Consistency:       0,
		Datacenter:        cfg.ScyllaDatacenter,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("connect to scylladb: %w", err)
	}
	defer session.Close()

	repo := repository.NewSubtitleRepo(session)
	if err := repo.EnsureSchema(); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}

	transcriber := transcriber.NewTranscriberClient(cfg.WhisperServerURL)
	js, nc, err := events.Connect(context.Background(), cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Drain()
	if err := events.EnsureStreams(context.Background(), js); err != nil {
		return fmt.Errorf("ensure event streams: %w", err)
	}
	if err := events.EnsureWorkerConsumers(context.Background(), js); err != nil {
		return fmt.Errorf("ensure worker consumers: %w", err)
	}
	subtitleConsumer, err := js.Consumer(context.Background(), events.StreamPipelineJobs, events.ConsumerSubtitleWorkers)
	if err != nil {
		return fmt.Errorf("get subtitle consumer: %w", err)
	}
	publisher := events.NewPublisher(js)
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/subtitle/transcribe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			AssetID   string `json:"asset_id"`
			AudioFile string `json:"audio_file"`
			Language  string `json:"language"`
			OutputDir string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if reqBody.Language == "" {
			reqBody.Language = "en"
		}
		if reqBody.OutputDir == "" {
			reqBody.OutputDir = filepath.Join("/tmp", reqBody.AssetID, "subtitles")
		}

		assetID, err := uuid.Parse(reqBody.AssetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}

		if err := repo.SetSubtitleStatus(r.Context(), assetID, reqBody.Language, "running"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		result, err := transcriber.Transcribe(r.Context(), reqBody.AudioFile, reqBody.Language)
		if err != nil {
			if updateErr := repo.SetSubtitleStatus(r.Context(), assetID, reqBody.Language, "failed"); updateErr != nil {
				logger.Error("failed to update subtitle status", zap.Error(updateErr))
			}
			http.Error(w, fmt.Sprintf("transcription failed: %v", err), http.StatusInternalServerError)
			return
		}

		vttContent := transcriber.SegmentsToVTT(result.Segments)
		if err := os.MkdirAll(reqBody.OutputDir, 0755); err != nil {
			http.Error(w, fmt.Sprintf("create output dir: %v", err), http.StatusInternalServerError)
			return
		}

		vttFile := filepath.Join(reqBody.OutputDir, fmt.Sprintf("%s.vtt", reqBody.Language))
		if err := os.WriteFile(vttFile, []byte(vttContent), 0644); err != nil {
			http.Error(w, fmt.Sprintf("write vtt: %v", err), http.StatusInternalServerError)
			return
		}

		if err := repo.SetSubtitleStatus(r.Context(), assetID, reqBody.Language, "done"); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]interface{}{
			"vtt_file": vttFile,
			"segments": result.Segments,
		})
	})

	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	consumerCtx, stopConsumer := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopConsumer()
	go consumeSubtitleEvents(consumerCtx, subtitleConsumer, publisher, repo, transcriber)
	go func() {
		logger.Info("subtitle service started", zap.String("addr", addr))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("listen and serve", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-consumerCtx.Done()
	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	return srv.Shutdown(ctxShutdown)
}

func consumeSubtitleEvents(ctx context.Context, consumer jetstream.Consumer, publisher *events.Publisher, repo *repository.SubtitleRepo, client *transcriber.TranscriberClient) {
	for ctx.Err() == nil {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			continue
		}
		for message := range batch.Messages() {
			handleErr := handleSubtitleEvent(ctx, message, publisher, repo, client)
			if errors.Is(handleErr, errSubtitleTerminated) {
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
}

var errSubtitleTerminated = errors.New("subtitle message terminated")

func handleSubtitleEvent(ctx context.Context, message jetstream.Msg, publisher *events.Publisher, repo *repository.SubtitleRepo, client *transcriber.TranscriberClient) error {
	request := new(pipelinepb.SubtitleRequested)
	if err := proto.Unmarshal(message.Data(), request); err != nil {
		return err
	}
	assetID, err := uuid.Parse(request.GetAssetId())
	if err != nil {
		return err
	}
	if err := repo.SetSubtitleStatus(ctx, assetID, request.GetLanguage(), "running"); err != nil {
		return err
	}
	result, err := client.Transcribe(ctx, request.GetSourceUri(), request.GetLanguage())
	if err != nil {
		metadata, metadataErr := message.Metadata()
		if metadataErr != nil || metadata.NumDelivered < 5 {
			return err
		}
		_ = repo.SetSubtitleStatus(ctx, assetID, request.GetLanguage(), "failed")
		failed := &pipelinepb.SubtitleFailed{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), Language: request.GetLanguage(), ErrorCode: "SUBTITLE_FAILED", ErrorMessage: err.Error(), Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
		if publishErr := publisher.Publish(ctx, events.SubjectSubtitleFailed, events.TerminalMessageID(request.GetRunId(), "subtitle-failed:"+request.GetLanguage()), failed); publishErr != nil {
			return publishErr
		}
		if termErr := message.Term(); termErr != nil {
			return termErr
		}
		return errSubtitleTerminated
	}

	outputPrefix := request.GetOutputPrefix()
	if outputPrefix == "" {
		return fmt.Errorf("subtitle output prefix is required")
	}
	temporaryDir, err := os.MkdirTemp("", "mux-subtitle-")
	if err != nil {
		return err
	}
	defer os.RemoveAll(temporaryDir)
	temporaryFile := filepath.Join(temporaryDir, request.GetLanguage()+".vtt")
	if err := os.WriteFile(temporaryFile, []byte(client.SegmentsToVTT(result.Segments)), 0644); err != nil {
		return err
	}
	finalDir := filepath.Join(outputPrefix, "subtitles")
	if err := os.MkdirAll(finalDir, 0755); err != nil {
		return err
	}
	finalFile := filepath.Join(finalDir, request.GetLanguage()+".vtt")
	if err := os.Rename(temporaryFile, finalFile); err != nil {
		return fmt.Errorf("commit subtitle output: %w", err)
	}
	if err := repo.SetSubtitleStatus(ctx, assetID, request.GetLanguage(), "done"); err != nil {
		return err
	}
	completed := &pipelinepb.SubtitleCompleted{RunId: request.GetRunId(), AssetId: request.GetAssetId(), OrgId: request.GetOrgId(), Language: request.GetLanguage(), VttLocation: finalFile, Attempt: request.GetAttempt(), TimestampUnix: time.Now().UTC().Unix()}
	return publisher.Publish(ctx, events.SubjectSubtitleCompleted, events.TerminalMessageID(request.GetRunId(), "subtitle-completed:"+request.GetLanguage()), completed)
}
