package main

import (
	"context"
	"fmt"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"connectrpc.com/connect"
	"github.com/Ajay01103/go-mux/pkg/interceptor"
	"github.com/Ajay01103/go-mux/job/config"
	"github.com/Ajay01103/go-mux/job/db"
	"github.com/Ajay01103/go-mux/job/gen/pb"
	jobconnect "github.com/Ajay01103/go-mux/job/gen/pb/pbconnect"
	"github.com/Ajay01103/go-mux/job/internal/orchestrator"
	"github.com/Ajay01103/go-mux/job/internal/repository"
	"github.com/Ajay01103/go-mux/pkg/events"
	pkglogger "github.com/Ajay01103/go-mux/pkg/logger"
	playbackconnect "github.com/Ajay01103/go-mux/playback/gen/pb/pbconnect"
	"github.com/google/uuid"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/types/known/timestamppb"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintf(os.Stderr, "job service exited: %v\n", err)
		os.Exit(1)
	}
}

func run() error {
	logger := pkglogger.New()
	defer logger.Sync()

	undo := zap.ReplaceGlobals(logger)
	defer undo()

	logger.Info("JOB SERVICE starting")

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

	js, nc, err := events.Connect(context.Background(), cfg.NATSURL)
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Drain()
	repo := repository.NewJobRepo(session)
	migrateCtx, migrateCancel := context.WithTimeout(context.Background(), 60*time.Second)
	if err := db.Migrate(migrateCtx, session); err != nil {
		migrateCancel()
		return fmt.Errorf("run migrations: %w", err)
	}
	migrateCancel()
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

	// WaitGroup ensures in-flight NAKs complete before nc.Drain().
	var wg sync.WaitGroup
	wg.Add(3)
	go func() { defer wg.Done(); consumeUploadEvents(consumerCtx, js, jobOrchestrator) }()
	go func() { defer wg.Done(); consumeProbeResults(consumerCtx, js, jobOrchestrator) }()
	go func() { defer wg.Done(); consumeStepResults(consumerCtx, js, jobOrchestrator) }()

	mux := http.NewServeMux()

	mux.HandleFunc("/health", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok"))
	})

	// ConnectRPC surface for pipeline status/cancel/retry.
	jobHandler := &jobRPCHandler{repo: repo, orchestrator: jobOrchestrator}
	rpcPath, rpcHandler := jobconnect.NewJobServiceHandler(jobHandler, connect.WithInterceptors(interceptor.NewLoggingInterceptor(logger)))
	mux.Handle(rpcPath, rpcHandler)

	addr := fmt.Sprintf(":%s", cfg.HTTPPort)
	srv := &http.Server{Addr: addr, Handler: mux}
	go func() {
		logger.Info("JOB SERVICE started at ConnectRPC server", zap.String("addr", addr))
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
			var handleErr error
			if message.Subject() == events.SubjectProbeFailed {
				handleErr = jobOrchestrator.HandleStepFailed(ctx, message.Subject(), message.Data())
			} else {
				handleErr = jobOrchestrator.HandleProbeCompleted(ctx, message.Data())
			}
			if handleErr != nil {
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
			switch message.Subject() {
			case events.SubjectTranscodeFailed, events.SubjectThumbnailFailed, events.SubjectStoryboardFailed, events.SubjectSubtitleFailed:
				handleErr = jobOrchestrator.HandleStepFailed(ctx, message.Subject(), message.Data())
			default:
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

type jobRPCHandler struct {
	jobconnect.UnimplementedJobServiceHandler
	repo         *repository.JobRepo
	orchestrator *orchestrator.Orchestrator
}

func (h *jobRPCHandler) GetPipelineStatus(ctx context.Context, req *connect.Request[pb.GetPipelineStatusRequest]) (*connect.Response[pb.GetPipelineStatusResponse], error) {
	var run *repository.PipelineRun
	var err error
	if req.Msg.GetRunId() != "" {
		runID, parseErr := uuid.Parse(req.Msg.GetRunId())
		if parseErr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
		}
		run, err = h.repo.GetRunByRunID(ctx, runID)
	} else {
		assetID, parseErr := uuid.Parse(req.Msg.GetAssetId())
		if parseErr != nil {
			return nil, connect.NewError(connect.CodeInvalidArgument, parseErr)
		}
		run, err = h.repo.GetRun(ctx, assetID)
	}
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if run == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("pipeline run not found"))
	}
	steps, err := h.repo.GetStepsForRun(ctx, run.RunID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	response := &pb.GetPipelineStatusResponse{
		Run: &pb.PipelineRunSummary{
			RunId:      run.RunID.String(),
			AssetId:    run.AssetID.String(),
			OrgId:      run.OrgID.String(),
			Status:     run.Status,
			PlaybackId: run.PlaybackID.String(),
		},
		Steps: make([]*pb.PipelineStepStatus, 0, len(steps)),
	}
	for _, step := range steps {
		response.Steps = append(response.Steps, &pb.PipelineStepStatus{
			RunId:     step.RunID.String(),
			AssetId:   step.AssetID.String(),
			Step:      step.Step,
			Status:    step.Status,
			UpdatedAt: timestamppb.New(step.UpdatedAt).AsTime().Format(time.RFC3339Nano),
		})
	}
	return connect.NewResponse(response), nil
}

func (h *jobRPCHandler) CancelPipeline(ctx context.Context, req *connect.Request[pb.CancelPipelineRequest]) (*connect.Response[pb.CancelPipelineResponse], error) {
	runID, err := uuid.Parse(req.Msg.GetRunId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	if err := h.orchestrator.CancelPipeline(ctx, runID); err != nil {
		return nil, connect.NewError(connect.CodeFailedPrecondition, err)
	}
	return connect.NewResponse(&pb.CancelPipelineResponse{Ok: true}), nil
}

func (h *jobRPCHandler) RetryPipeline(ctx context.Context, req *connect.Request[pb.RetryPipelineRequest]) (*connect.Response[pb.RetryPipelineResponse], error) {
	runID, err := uuid.Parse(req.Msg.GetRunId())
	if err != nil {
		return nil, connect.NewError(connect.CodeInvalidArgument, err)
	}
	run, err := h.repo.GetRunByRunID(ctx, runID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	if run == nil {
		return nil, connect.NewError(connect.CodeNotFound, fmt.Errorf("pipeline run not found"))
	}
	steps, err := h.repo.GetRunSteps(ctx, runID)
	if err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	failed := 0
	for _, step := range steps {
		if step.Status == "FAILED" {
			failed++
		}
	}
	if failed == 0 {
		return nil, connect.NewError(connect.CodeFailedPrecondition, fmt.Errorf("no failed steps to retry"))
	}
	if err := h.orchestrator.RetryPipeline(ctx, runID); err != nil {
		return nil, connect.NewError(connect.CodeInternal, err)
	}
	return connect.NewResponse(&pb.RetryPipelineResponse{Ok: true, StepsRequeued: int32(failed)}), nil
}
