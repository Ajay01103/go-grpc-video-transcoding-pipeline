package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/Ajay01103/go-notion/job/config"
	"github.com/Ajay01103/go-notion/job/db"
	"github.com/Ajay01103/go-notion/job/internal/activity"
	"github.com/Ajay01103/go-notion/job/internal/orchestrator"
	"github.com/Ajay01103/go-notion/job/internal/repository"
	"github.com/Ajay01103/go-notion/pkg/events"
	playbackconnect "github.com/Ajay01103/go-notion/playback/gen/pb/pbconnect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "job service exited: %v\n", err)
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

	js, nc, err := events.Connect(context.Background(), cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Drain()
	repo := repository.NewJobRepo(session)
	if err := repo.EnsureSchema(); err != nil {
		return fmt.Errorf("ensure schema: %w", err)
	}
	if err := events.EnsureStreams(context.Background(), js); err != nil {
		return fmt.Errorf("ensure event streams: %w", err)
	}
	if err := events.EnsureJobConsumers(context.Background(), js); err != nil {
		return fmt.Errorf("ensure job consumers: %w", err)
	}
	jobOrchestrator := orchestrator.New(repo, events.NewPublisher(js))
	playbackClient := playbackconnect.NewPlaybackServiceClient(http.DefaultClient, cfg.PlaybackURL)
	jobOrchestrator.SetPlaybackCreator(orchestrator.NewPlaybackCreator(playbackClient))
	consumerCtx, stopConsumer := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stopConsumer()
	go consumeUploadEvents(consumerCtx, js, jobOrchestrator)
	go consumeProbeResults(consumerCtx, js, jobOrchestrator)
	go consumeStepResults(consumerCtx, js, jobOrchestrator)

	pipelineActivity := activity.NewPipelineActivity(repo)
	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/pipeline/status", func(w http.ResponseWriter, r *http.Request) {
		assetID := r.URL.Query().Get("asset_id")
		if assetID == "" {
			http.Error(w, "asset_id is required", http.StatusBadRequest)
			return
		}
		parsed, err := uuid.Parse(assetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		rows, err := repo.GetPipelineStatus(r.Context(), parsed)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(rows)
	})

	mux.HandleFunc("/pipeline/probe", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			AssetID    string `json:"asset_id"`
			SourceFile string `json:"source_file"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		assetID, err := uuid.Parse(reqBody.AssetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		result, err := pipelineActivity.ProbeAsset(r.Context(), activity.ProbeAssetInput{
			AssetID:    assetID,
			SourceFile: reqBody.SourceFile,
		})
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(result)
	})

	mux.HandleFunc("/pipeline/transcode", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			AssetID   string `json:"asset_id"`
			SourceURI string `json:"source_uri"`
			OutputDir string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if reqBody.OutputDir == "" {
			reqBody.OutputDir = filepath.Join("/tmp", reqBody.AssetID)
		}
		assetID, err := uuid.Parse(reqBody.AssetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		if err := pipelineActivity.TranscodeLadder(r.Context(), activity.TranscodeLadderInput{
			AssetID:   assetID,
			SourceURI: reqBody.SourceURI,
			OutputDir: reqBody.OutputDir,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/pipeline/thumbnail", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			AssetID   string `json:"asset_id"`
			SourceURI string `json:"source_uri"`
			OutputDir string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if reqBody.OutputDir == "" {
			reqBody.OutputDir = filepath.Join("/tmp", reqBody.AssetID)
		}
		assetID, err := uuid.Parse(reqBody.AssetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		if err := pipelineActivity.GenerateThumbnail(r.Context(), activity.GenerateThumbnailInput{
			AssetID:   assetID,
			SourceURI: reqBody.SourceURI,
			OutputDir: reqBody.OutputDir,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	mux.HandleFunc("/pipeline/storyboard", func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			http.Error(w, "POST required", http.StatusMethodNotAllowed)
			return
		}
		var reqBody struct {
			AssetID   string `json:"asset_id"`
			SourceURI string `json:"source_uri"`
			OutputDir string `json:"output_dir"`
		}
		if err := json.NewDecoder(r.Body).Decode(&reqBody); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		if reqBody.OutputDir == "" {
			reqBody.OutputDir = filepath.Join("/tmp", reqBody.AssetID)
		}
		assetID, err := uuid.Parse(reqBody.AssetID)
		if err != nil {
			http.Error(w, "invalid asset_id", http.StatusBadRequest)
			return
		}
		if err := pipelineActivity.GenerateStoryboard(r.Context(), activity.GenerateStoryboardInput{
			AssetID:   assetID,
			SourceURI: reqBody.SourceURI,
			OutputDir: reqBody.OutputDir,
		}); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("job service started", zap.String("addr", addr))
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

func consumeUploadEvents(ctx context.Context, js jetstream.JetStream, jobOrchestrator *orchestrator.Orchestrator) {
	consumer, err := js.Consumer(ctx, events.StreamAssetEvents, events.ConsumerJobOrchestrator)
	if err != nil {
		return
	}
	for {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for message := range batch.Messages() {
			if err := jobOrchestrator.HandleUploadCompleted(ctx, message.Data()); err != nil {
				_ = message.Nak()
				continue
			}
			_ = message.Ack()
		}
	}
}

func consumeProbeResults(ctx context.Context, js jetstream.JetStream, jobOrchestrator *orchestrator.Orchestrator) {
	consumer, err := js.Consumer(ctx, events.StreamPipelineJobs, events.ConsumerJobProbeResults)
	if err != nil {
		return
	}
	for {
		batch, err := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return
			}
			continue
		}
		for message := range batch.Messages() {
			if err := jobOrchestrator.HandleProbeCompleted(ctx, message.Data()); err != nil {
				_ = message.Nak()
				continue
			}
			_ = message.Ack()
		}
	}
}

func consumeStepResults(ctx context.Context, js jetstream.JetStream, jobOrchestrator *orchestrator.Orchestrator) {
	consumer, err := js.Consumer(ctx, events.StreamPipelineJobs, events.ConsumerJobStepResults)
	if err != nil {
		return
	}
	for ctx.Err() == nil {
		batch, fetchErr := consumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
		if fetchErr != nil {
			continue
		}
		for message := range batch.Messages() {
			var handleErr error
			if message.Subject() == events.SubjectSubtitleFailed {
				handleErr = jobOrchestrator.HandleStepFailed(ctx, message.Subject(), message.Data())
			} else {
				handleErr = jobOrchestrator.HandleStepCompleted(ctx, message.Subject(), message.Data())
			}
			if handleErr != nil {
				_ = message.Nak()
				continue
			}
			_ = message.Ack()
		}
	}
}
