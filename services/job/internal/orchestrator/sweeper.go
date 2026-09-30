package orchestrator

import (
	"context"
	"strings"
	"time"

	"github.com/Ajay01103/go-mux/job/internal/repository"
	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

var stepStaleThreshold = map[string]time.Duration{
	"probe":      5 * time.Minute,
	"thumbnail":  10 * time.Minute,
	"storyboard": 15 * time.Minute,
	"transcode":  90 * time.Minute, // floor; scaled by 3×duration
}

func staleThresholdFor(stepName string, probeDuration float64) time.Duration {
	base, ok := stepStaleThreshold[stepName]
	if !ok {
		// subtitle:* or unknown — use probe floor
		base = 5 * time.Minute
	}
	if stepName == "transcode" && probeDuration > 0 {
		computed := time.Duration(probeDuration*3) * time.Second
		if computed > base {
			base = computed
		}
	}
	return base
}

type SweeperRepo interface {
	GetActiveRuns(ctx context.Context) ([]repository.PipelineRun, error)
	GetRunSteps(ctx context.Context, runID uuid.UUID) ([]repository.StepSnapshot, error)
}

type StepFailureHandler interface {
	HandleStepFailed(ctx context.Context, subject string, payload []byte) error
}

type Sweeper struct {
	repo         SweeperRepo
	orchestrator StepFailureHandler
	interval     time.Duration
	logger       *zap.Logger
}

func NewSweeper(repo SweeperRepo, orch StepFailureHandler, interval time.Duration, logger *zap.Logger) *Sweeper {
	if interval <= 0 {
		interval = 5 * time.Minute
	}
	if logger == nil {
		logger = zap.NewNop()
	}
	return &Sweeper{
		repo:         repo,
		orchestrator: orch,
		interval:     interval,
		logger:       logger,
	}
}

func (s *Sweeper) Start(ctx context.Context) {
	ticker := time.NewTicker(s.interval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			s.Sweep(ctx)
		case <-ctx.Done():
			return
		}
	}
}

func (s *Sweeper) Sweep(ctx context.Context) {
	runs, err := s.repo.GetActiveRuns(ctx)
	if err != nil {
		s.logger.Error("sweeper: failed to load active runs", zap.Error(err))
		return
	}

	for _, run := range runs {
		steps, err := s.repo.GetRunSteps(ctx, run.RunID)
		if err != nil {
			s.logger.Error("sweeper: failed to get run steps", zap.String("run_id", run.RunID.String()), zap.Error(err))
			continue
		}

		for _, step := range steps {
			threshold := staleThresholdFor(step.Step, run.ProbeDurationSeconds)
			isStale := step.Status == "RUNNING" && time.Since(step.UpdatedAt) > threshold

			if isStale {
				s.logger.Warn("sweeper: swept stale step",
					zap.String("run_id", run.RunID.String()),
					zap.String("step", step.Step),
					zap.Duration("age", time.Since(step.UpdatedAt)),
					zap.Duration("threshold", threshold),
				)
				s.failStaleStep(ctx, run, step)
			} else if run.Status == "RUNNING" && step.Status == "FAILED" && isRequiredStep(step.Step) {
				// recoverOrphanedFailedStep: run is still marked RUNNING but a required step failed
				s.failStaleStep(ctx, run, step)
			}
		}
	}
}

func isRequiredStep(stepName string) bool {
	return stepName == "probe" || stepName == "transcode" || stepName == "thumbnail" || stepName == "storyboard"
}

func (s *Sweeper) failStaleStep(ctx context.Context, run repository.PipelineRun, step repository.StepSnapshot) {
	subject, payload := syntheticFailurePayload(run, step, "SWEEP_TIMEOUT", "step exceeded stale threshold")
	if err := s.orchestrator.HandleStepFailed(ctx, subject, payload); err != nil {
		s.logger.Error("sweeper: HandleStepFailed failed",
			zap.String("run_id", run.RunID.String()),
			zap.String("step", step.Step),
			zap.Error(err),
		)
	}
}

func syntheticFailurePayload(run repository.PipelineRun, step repository.StepSnapshot, errorCode, errorMessage string) (string, []byte) {
	runIDStr := run.RunID.String()
	assetIDStr := run.AssetID.String()
	orgIDStr := run.OrgID.String()
	attempt := int32(step.Attempt)
	now := time.Now().UTC().Unix()

	switch {
	case step.Step == "probe":
		evt := &pipelinepb.ProbeFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectProbeFailed, data

	case step.Step == "transcode":
		evt := &pipelinepb.TranscodeFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectTranscodeFailed, data

	case step.Step == "thumbnail":
		evt := &pipelinepb.ThumbnailFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectThumbnailFailed, data

	case step.Step == "storyboard":
		evt := &pipelinepb.StoryboardFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectStoryboardFailed, data

	case strings.HasPrefix(step.Step, "subtitle:"):
		lang := strings.TrimPrefix(step.Step, "subtitle:")
		evt := &pipelinepb.SubtitleFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			Language:      lang,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectSubtitleFailed, data

	default:
		// Fallback as transcode failed
		evt := &pipelinepb.TranscodeFailed{
			RunId:         runIDStr,
			AssetId:       assetIDStr,
			OrgId:         orgIDStr,
			ErrorCode:     errorCode,
			ErrorMessage:  errorMessage,
			Attempt:       attempt,
			TimestampUnix: now,
		}
		data, _ := proto.Marshal(evt)
		return events.SubjectTranscodeFailed, data
	}
}
