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
	"sync"
	"syscall"
	"time"

	"github.com/Ajay01103/go-mux/pkg/events"
	pkglogger "github.com/Ajay01103/go-mux/pkg/logger"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/Ajay01103/go-mux/pkg/storage"
	"github.com/Ajay01103/go-mux/subtitle/config"
	"github.com/Ajay01103/go-mux/subtitle/db"
	"github.com/Ajay01103/go-mux/subtitle/internal/repository"
	"github.com/Ajay01103/go-mux/subtitle/internal/transcriber"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var errPermanent = errors.New("permanent error")

func classifyWhisperError(err error) error {
	var httpErr *transcriber.HTTPError
	if errors.As(err, &httpErr) {
		// HTTP 4xx (except 408 Request Timeout, 429 Too Many Requests) from Whisper is permanent.
		if httpErr.StatusCode >= 400 && httpErr.StatusCode < 500 && httpErr.StatusCode != 408 && httpErr.StatusCode != 429 {
			return fmt.Errorf("%w: %w", errPermanent, err)
		}
	}
	return err
}

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "subtitle service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := pkglogger.New()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	logger.Info("SUBTITLE SERVICE starting")

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
		Datacenter:        cfg.ScyllaDatacenter,
		ReplicationFactor: cfg.ReplicationFactor,
	})
	cancel()
	if err != nil {
		return fmt.Errorf("connect to scylladb: %w", err)
	}
	defer session.Close()

	repo := repository.NewSubtitleRepo(session)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := db.Migrate(migrateCtx, session); err != nil {
		migrateCancel()
		return fmt.Errorf("run migrations: %w", err)
	}
	migrateCancel()

	transcriberClient := transcriber.NewTranscriberClient(cfg.WhisperServerURL)
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
	storageConfig := storage.ConfigFromEnv()
	store := storage.New(storageConfig)
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

		result, err := transcriberClient.Transcribe(r.Context(), reqBody.AudioFile, reqBody.Language)
		if err != nil {
			if updateErr := repo.SetSubtitleStatus(r.Context(), assetID, reqBody.Language, "failed"); updateErr != nil {
				logger.Error("failed to update subtitle status", zap.Error(updateErr))
			}
			http.Error(w, fmt.Sprintf("transcription failed: %v", err), http.StatusInternalServerError)
			return
		}

		vttContent := transcriberClient.SegmentsToVTT(result.Segments)
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

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for consumerCtx.Err() == nil {
			batch, fetchErr := subtitleConsumer.Fetch(1, jetstream.FetchMaxWait(5*time.Second))
			if fetchErr != nil {
				continue
			}
			for message := range batch.Messages() {
				handleErr := handleSubtitleEvent(consumerCtx, publisher, logger, message, repo, transcriberClient, store, storageConfig.Bucket)
				switch {
				case errors.Is(handleErr, events.ErrMessageTerminated):
					// Term() already called inside HandleFinalAttempt.
				case handleErr != nil && consumerCtx.Err() == nil:
					logger.Warn("subtitle attempt failed, will retry", zap.Error(handleErr))
					_ = message.Nak()
				case handleErr != nil:
					_ = message.Nak()
				default:
					logger.Info("subtitle completed")
					_ = message.Ack()
				}
			}
		}
	}()

	go func() {
		logger.Info("SUBTITLE SERVICE started", zap.String("addr", addr), zap.String("natsConsumer", events.ConsumerSubtitleWorkers))
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("listen and serve", zap.Error(err))
			os.Exit(1)
		}
	}()

	<-consumerCtx.Done()
	wg.Wait()

	ctxShutdown, cancelShutdown := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancelShutdown()
	return srv.Shutdown(ctxShutdown)
}

func handleSubtitleEvent(ctx context.Context, publisher *events.Publisher, logger *zap.Logger, message events.MsgAcker, repo *repository.SubtitleRepo, client *transcriber.TranscriberClient, store *storage.Client, bucket string) error {
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

	// Heartbeat before staging/transcribing so slow Whisper calls don't expire AckWait.
	hbCtx, hbCancel := context.WithCancel(ctx)
	defer hbCancel()
	go func() {
		ticker := time.NewTicker(20 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				_ = message.InProgress()
			case <-hbCtx.Done():
				return
			}
		}
	}()

	result, err := client.Transcribe(ctx, request.GetSourceUri(), request.GetLanguage())
	if err != nil {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		classifiedErr := classifyWhisperError(err)
		isPermanent := errors.Is(classifiedErr, errPermanent)
		cfg := events.WorkerFailureConfig{
			ConsumerName: events.ConsumerSubtitleWorkers,
			Logger:       logger,
		}
		return events.HandleFinalAttempt(ctx, message, cfg, isPermanent, func(pubCtx context.Context) error {
			_ = repo.SetSubtitleResult(pubCtx, assetID, request.GetLanguage(), "failed", "", classifiedErr.Error())
			failed := &pipelinepb.SubtitleFailed{
				RunId:         request.GetRunId(),
				AssetId:       request.GetAssetId(),
				OrgId:         request.GetOrgId(),
				Language:      request.GetLanguage(),
				ErrorCode:     "SUBTITLE_FAILED",
				ErrorMessage:  classifiedErr.Error(),
				Attempt:       request.GetAttempt(),
				TimestampUnix: time.Now().UTC().Unix(),
			}
			msgID := events.StepTerminalMessageID(request.GetRunId(), "subtitle-failed:"+request.GetLanguage(), request.GetAttempt())
			return publisher.Publish(pubCtx, events.SubjectSubtitleFailed, msgID, failed)
		})
	}

	outputPrefix := request.GetOutputPrefix()
	if outputPrefix == "" {
		return fmt.Errorf("%w: subtitle output prefix is required", errPermanent)
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
	// Durable publish: VTT goes to RustFS before the completed event fires.
	vttLocation := outputPrefix + "/subtitles/" + request.GetLanguage() + ".vtt"
	if err := store.PutFile(ctx, bucket, vttLocation, temporaryFile); err != nil {
		return fmt.Errorf("upload subtitle vtt: %w", err)
	}
	if err := repo.SetSubtitleResult(ctx, assetID, request.GetLanguage(), "done", vttLocation, ""); err != nil {
		return err
	}
	completed := &pipelinepb.SubtitleCompleted{
		RunId:         request.GetRunId(),
		AssetId:       request.GetAssetId(),
		OrgId:         request.GetOrgId(),
		Language:      request.GetLanguage(),
		VttLocation:   vttLocation,
		Attempt:       request.GetAttempt(),
		TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.StepTerminalMessageID(request.GetRunId(), "subtitle-completed:"+request.GetLanguage(), request.GetAttempt())
	return publisher.Publish(ctx, events.SubjectSubtitleCompleted, msgID, completed)
}
