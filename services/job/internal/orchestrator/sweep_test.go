package orchestrator

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/Ajay01103/go-mux/job/internal/repository"
	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/google/uuid"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

type mockSweeperRepo struct {
	runs  []repository.PipelineRun
	steps map[uuid.UUID][]repository.StepSnapshot
}

func (m *mockSweeperRepo) GetActiveRuns(ctx context.Context) ([]repository.PipelineRun, error) {
	return m.runs, nil
}

func (m *mockSweeperRepo) GetRunSteps(ctx context.Context, runID uuid.UUID) ([]repository.StepSnapshot, error) {
	return m.steps[runID], nil
}

type mockStepFailureHandler struct {
	mu       sync.Mutex
	calls    []failureCall
	onFailed func(subject string, payload []byte) error
}

type failureCall struct {
	subject string
	payload []byte
}

func (m *mockStepFailureHandler) HandleStepFailed(ctx context.Context, subject string, payload []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.calls = append(m.calls, failureCall{subject: subject, payload: payload})
	if m.onFailed != nil {
		return m.onFailed(subject, payload)
	}
	return nil
}

func TestSweeperFailsStaleTranscode(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	assetID := uuid.New()
	orgID := uuid.New()

	repo := &mockSweeperRepo{
		runs: []repository.PipelineRun{
			{
				RunID:                runID,
				AssetID:              assetID,
				OrgID:                orgID,
				Status:               "RUNNING",
				ProbeDurationSeconds: 0,
			},
		},
		steps: map[uuid.UUID][]repository.StepSnapshot{
			runID: {
				{
					Step:      "transcode",
					Status:    "RUNNING",
					Attempt:   2,
					UpdatedAt: time.Now().Add(-120 * time.Minute),
				},
			},
		},
	}

	handler := &mockStepFailureHandler{}
	sweeper := NewSweeper(repo, handler, 5*time.Minute, zap.NewNop())
	sweeper.Sweep(ctx)

	if len(handler.calls) != 1 {
		t.Fatalf("expected 1 call to HandleStepFailed, got %d", len(handler.calls))
	}
	call := handler.calls[0]
	if call.subject != events.SubjectTranscodeFailed {
		t.Fatalf("expected subject %s, got %s", events.SubjectTranscodeFailed, call.subject)
	}

	evt := new(pipelinepb.TranscodeFailed)
	if err := proto.Unmarshal(call.payload, evt); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if evt.GetErrorCode() != "SWEEP_TIMEOUT" {
		t.Fatalf("expected error code SWEEP_TIMEOUT, got %s", evt.GetErrorCode())
	}
	if evt.GetAttempt() != 2 {
		t.Fatalf("expected attempt 2, got %d", evt.GetAttempt())
	}
}

func TestSweeperRespectsLongVideo(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	assetID := uuid.New()
	orgID := uuid.New()

	// 3600 seconds duration => 3 * 3600 = 10800s = 3 hours threshold.
	// Step has been running for 100 minutes (< 180 min). Should NOT be swept.
	repo := &mockSweeperRepo{
		runs: []repository.PipelineRun{
			{
				RunID:                runID,
				AssetID:              assetID,
				OrgID:                orgID,
				Status:               "RUNNING",
				ProbeDurationSeconds: 3600,
			},
		},
		steps: map[uuid.UUID][]repository.StepSnapshot{
			runID: {
				{
					Step:      "transcode",
					Status:    "RUNNING",
					Attempt:   1,
					UpdatedAt: time.Now().Add(-100 * time.Minute),
				},
			},
		},
	}

	handler := &mockStepFailureHandler{}
	sweeper := NewSweeper(repo, handler, 5*time.Minute, zap.NewNop())
	sweeper.Sweep(ctx)

	if len(handler.calls) != 0 {
		t.Fatalf("expected 0 calls for long video within threshold, got %d", len(handler.calls))
	}
}

func TestSweeperFailsStalProbe(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	assetID := uuid.New()
	orgID := uuid.New()

	// Probe floor is 5 minutes. Running for 6 minutes => should be swept.
	repo := &mockSweeperRepo{
		runs: []repository.PipelineRun{
			{
				RunID:   runID,
				AssetID: assetID,
				OrgID:   orgID,
				Status:  "RUNNING",
			},
		},
		steps: map[uuid.UUID][]repository.StepSnapshot{
			runID: {
				{
					Step:      "probe",
					Status:    "RUNNING",
					Attempt:   1,
					UpdatedAt: time.Now().Add(-6 * time.Minute),
				},
			},
		},
	}

	handler := &mockStepFailureHandler{}
	sweeper := NewSweeper(repo, handler, 5*time.Minute, zap.NewNop())
	sweeper.Sweep(ctx)

	if len(handler.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(handler.calls))
	}
	if handler.calls[0].subject != events.SubjectProbeFailed {
		t.Fatalf("expected subject %s, got %s", events.SubjectProbeFailed, handler.calls[0].subject)
	}
}

func TestSweeperSubtitleBestEffort(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	assetID := uuid.New()
	orgID := uuid.New()

	// Subtitle running for 6 minutes (> 5 min floor).
	run := repository.PipelineRun{
		RunID:   runID,
		AssetID: assetID,
		OrgID:   orgID,
		Status:  "RUNNING",
	}

	repo := &mockSweeperRepo{
		runs: []repository.PipelineRun{run},
		steps: map[uuid.UUID][]repository.StepSnapshot{
			runID: {
				{
					Step:      "subtitle:es",
					Status:    "RUNNING",
					Attempt:   1,
					UpdatedAt: time.Now().Add(-6 * time.Minute),
				},
			},
		},
	}

	handler := &mockStepFailureHandler{}
	sweeper := NewSweeper(repo, handler, 5*time.Minute, zap.NewNop())
	sweeper.Sweep(ctx)

	if len(handler.calls) != 1 {
		t.Fatalf("expected 1 call, got %d", len(handler.calls))
	}
	if handler.calls[0].subject != events.SubjectSubtitleFailed {
		t.Fatalf("expected %s, got %s", events.SubjectSubtitleFailed, handler.calls[0].subject)
	}

	// Verify run status stays RUNNING (subtitle best-effort)
	if run.Status != "RUNNING" {
		t.Fatalf("run status changed to %s, want RUNNING", run.Status)
	}
}

func TestSweeperRecoverOrphanedFailed(t *testing.T) {
	ctx := context.Background()
	runID := uuid.New()
	assetID := uuid.New()
	orgID := uuid.New()

	repo := &mockSweeperRepo{
		runs: []repository.PipelineRun{
			{
				RunID:   runID,
				AssetID: assetID,
				OrgID:   orgID,
				Status:  "RUNNING", // still RUNNING, but transcode already FAILED
			},
		},
		steps: map[uuid.UUID][]repository.StepSnapshot{
			runID: {
				{
					Step:      "transcode",
					Status:    "FAILED",
					Attempt:   1,
					UpdatedAt: time.Now().Add(-10 * time.Minute),
				},
			},
		},
	}

	handler := &mockStepFailureHandler{}
	sweeper := NewSweeper(repo, handler, 5*time.Minute, zap.NewNop())

	// First sweep: should call HandleStepFailed to recover the orphaned failure
	sweeper.Sweep(ctx)
	if len(handler.calls) != 1 {
		t.Fatalf("first sweep: expected 1 call, got %d", len(handler.calls))
	}

	// On second sweep, it should still behave idempotently
	sweeper.Sweep(ctx)
	if len(handler.calls) != 2 {
		t.Fatalf("second sweep: expected 2 calls total, got %d", len(handler.calls))
	}
}
