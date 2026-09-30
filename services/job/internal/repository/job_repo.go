package repository

import (
	"context"
	"fmt"
	"hash/crc32"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

// ──────────────────────────────────────────────────────────────────
// Domain types
// ──────────────────────────────────────────────────────────────────

type PipelineStep struct {
	RunID     uuid.UUID `json:"run_id"`
	AssetID   uuid.UUID `json:"asset_id"`
	Step      string    `json:"step"`
	Status    string    `json:"status"`
	Attempt   int       `json:"attempt"`
	UpdatedAt time.Time `json:"updated_at"`
}

// StepSnapshot is the lightweight version used by the orchestrator and sweeper.
// UpdatedAt is needed by the sweeper to detect stale steps.
type StepSnapshot struct {
	Step      string
	Status    string
	Attempt   int
	UpdatedAt time.Time
}

type PipelineRun struct {
	RunID                      uuid.UUID `json:"run_id"`
	AssetID                    uuid.UUID `json:"asset_id"`
	OrgID                      uuid.UUID `json:"org_id"`
	SourceURI                  string    `json:"source_uri"`
	RequestedSubtitleLanguages []string  `json:"requested_subtitle_languages,omitempty"`
	Status                     string    `json:"status"`
	RunGeneration              int       `json:"run_generation"`
	ProbeDurationSeconds       float64   `json:"probe_duration_seconds,omitempty"`
	FailedErrorCode            string    `json:"failed_error_code,omitempty"`
	FailedErrorMessage         string    `json:"failed_error_message,omitempty"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
	PlaybackID                 uuid.UUID `json:"playback_id,omitempty"`
}

// activeRunShards is the fixed shard count for the active_runs table.
// Keep this small (power of 2 ≤ 16) so the sweeper reads only a handful
// of partitions.
const activeRunShards = 8

func shardFor(assetID uuid.UUID) int {
	return int(crc32.ChecksumIEEE(assetID[:]) % activeRunShards)
}

// ──────────────────────────────────────────────────────────────────
// Repository
// ──────────────────────────────────────────────────────────────────

type JobRepo struct {
	session *gocql.Session
}

func NewJobRepo(session *gocql.Session) *JobRepo {
	return &JobRepo{session: session}
}

// ── Run lifecycle ─────────────────────────────────────────────────

func (r *JobRepo) CreateOrGetRun(ctx context.Context, assetID, orgID uuid.UUID, sourceURI string, languages []string) (*PipelineRun, bool, error) {
	run, err := r.GetRun(ctx, assetID)
	if err != nil {
		return nil, false, err
	}
	if run != nil {
		return run, false, nil
	}

	now := time.Now().UTC()
	candidate := &PipelineRun{
		RunID:                      uuid.New(),
		AssetID:                    assetID,
		OrgID:                      orgID,
		SourceURI:                  sourceURI,
		RequestedSubtitleLanguages: languages,
		Status:                     "PENDING",
		RunGeneration:              0,
		CreatedAt:                  now,
		UpdatedAt:                  now,
	}
	var applied bool
	if err := r.session.Query(
		`INSERT INTO pipeline_runs
		 (asset_id, run_id, org_id, source_uri, requested_subtitle_languages,
		  status, playback_id, run_generation, probe_duration_seconds,
		  failed_error_code, failed_error_message, created_at, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		candidate.AssetID, candidate.RunID, candidate.OrgID, candidate.SourceURI,
		candidate.RequestedSubtitleLanguages, candidate.Status, uuid.Nil,
		candidate.RunGeneration, 0.0, "", "", candidate.CreatedAt, candidate.UpdatedAt,
	).WithContext(ctx).Scan(
		&applied, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil, nil,
	); err != nil {
		return nil, false, fmt.Errorf("create pipeline run: %w", err)
	}
	if !applied {
		run, err = r.GetRun(ctx, assetID)
		if err != nil {
			return nil, false, err
		}
		return run, false, nil
	}

	// Reverse lookup row keyed by run_id so event consumers can resolve
	// asset_id from the run_id they receive in payloads.
	if err := r.session.Query(
		`INSERT INTO pipeline_runs_by_run
		 (run_id, asset_id, org_id, status, created_at)
		 VALUES (?, ?, ?, ?, ?)`,
		candidate.RunID, candidate.AssetID, candidate.OrgID,
		candidate.Status, candidate.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return nil, false, fmt.Errorf("create pipeline run reverse index: %w", err)
	}

	// Register in the active_runs index so the sweeper can find this run
	// without a full table scan.
	if err := r.session.Query(
		`INSERT INTO active_runs (shard, asset_id, run_id, created_at)
		 VALUES (?, ?, ?, ?)`,
		shardFor(candidate.AssetID), candidate.AssetID, candidate.RunID, candidate.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return nil, false, fmt.Errorf("register active run: %w", err)
	}

	return candidate, true, nil
}

func (r *JobRepo) GetRun(ctx context.Context, assetID uuid.UUID) (*PipelineRun, error) {
	var run PipelineRun
	err := r.session.Query(
		`SELECT asset_id, run_id, org_id, source_uri, requested_subtitle_languages,
		        status, playback_id, run_generation, probe_duration_seconds,
		        failed_error_code, failed_error_message, created_at, updated_at
		 FROM pipeline_runs WHERE asset_id = ?`,
		assetID,
	).WithContext(ctx).Scan(
		&run.AssetID, &run.RunID, &run.OrgID, &run.SourceURI,
		&run.RequestedSubtitleLanguages, &run.Status, &run.PlaybackID,
		&run.RunGeneration, &run.ProbeDurationSeconds,
		&run.FailedErrorCode, &run.FailedErrorMessage,
		&run.CreatedAt, &run.UpdatedAt,
	)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline run: %w", err)
	}
	return &run, nil
}

// GetRunByRunID resolves the reverse index (run_id → asset_id) and then
// loads the full run.
func (r *JobRepo) GetRunByRunID(ctx context.Context, runID uuid.UUID) (*PipelineRun, error) {
	var assetID uuid.UUID
	err := r.session.Query(
		`SELECT asset_id FROM pipeline_runs_by_run WHERE run_id = ?`,
		runID,
	).WithContext(ctx).Scan(&assetID)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup run by run_id: %w", err)
	}
	return r.GetRun(ctx, assetID)
}

func (r *JobRepo) SetRunStatus(ctx context.Context, assetID uuid.UUID, status string) error {
	if err := r.session.Query(
		`UPDATE pipeline_runs SET status = ?, updated_at = ? WHERE asset_id = ?`,
		status, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set pipeline run status: %w", err)
	}
	// Remove from active_runs on terminal transition.
	if status == "COMPLETED" || status == "FAILED" || status == "CANCELLED" {
		if err := r.removeActiveRun(ctx, assetID); err != nil {
			// Log-worthy but not fatal: the sweeper will simply try to sweep
			// this run again and find it already terminal.
			_ = err
		}
	}
	return nil
}

// SetRunFailedWithError atomically persists the FAILED status together with
// the error fields so that republishRunFailed can reconstruct the original
// terminal event after a crash between the DB write and the NATS publish.
func (r *JobRepo) SetRunFailedWithError(ctx context.Context, assetID uuid.UUID, errorCode, errorMessage string) error {
	if err := r.session.Query(
		`UPDATE pipeline_runs
		 SET status = 'FAILED', failed_error_code = ?, failed_error_message = ?, updated_at = ?
		 WHERE asset_id = ?`,
		errorCode, errorMessage, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set run failed with error: %w", err)
	}
	if err := r.removeActiveRun(ctx, assetID); err != nil {
		_ = err
	}
	return nil
}

// SetRunStatusByRunID updates run status via the reverse index.
// Returns false when the run does not exist.
func (r *JobRepo) SetRunStatusByRunID(ctx context.Context, runID uuid.UUID, status string) (bool, error) {
	run, err := r.GetRunByRunID(ctx, runID)
	if err != nil || run == nil {
		return false, err
	}
	return true, r.SetRunStatus(ctx, run.AssetID, status)
}

func (r *JobRepo) SetRunPlaybackID(ctx context.Context, assetID, playbackID uuid.UUID) error {
	if err := r.session.Query(
		`UPDATE pipeline_runs SET playback_id = ?, updated_at = ? WHERE asset_id = ?`,
		playbackID, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set pipeline run playback id: %w", err)
	}
	return nil
}

// SetRunProbeDuration persists the probed video duration so the sweeper can
// compute max(floor, 3 × duration) as the transcode stale threshold.
func (r *JobRepo) SetRunProbeDuration(ctx context.Context, assetID uuid.UUID, durationSeconds float64) error {
	if err := r.session.Query(
		`UPDATE pipeline_runs SET probe_duration_seconds = ?, updated_at = ? WHERE asset_id = ?`,
		durationSeconds, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set probe duration: %w", err)
	}
	return nil
}

// IncrementRunGeneration increments run_generation and returns the new value.
// Called by RetryPipeline so that each retry cycle produces distinct terminal
// event message IDs, preventing JetStream dedup from dropping a second failure.
func (r *JobRepo) IncrementRunGeneration(ctx context.Context, assetID uuid.UUID) (int, error) {
	run, err := r.GetRun(ctx, assetID)
	if err != nil {
		return 0, err
	}
	if run == nil {
		return 0, fmt.Errorf("pipeline run for asset %s not found", assetID)
	}
	newGen := run.RunGeneration + 1
	if err := r.session.Query(
		`UPDATE pipeline_runs SET run_generation = ?, updated_at = ? WHERE asset_id = ?`,
		newGen, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return 0, fmt.Errorf("increment run generation: %w", err)
	}
	return newGen, nil
}

// ── Step lifecycle ────────────────────────────────────────────────

// SetStepStatusForRun writes a step row.  updated_at is always refreshed so
// the sweeper can detect steps that have been stuck longer than the threshold.
// attempt is included so HandleStepFailed can ignore stale deliveries from
// a prior attempt.
func (r *JobRepo) SetStepStatusForRun(ctx context.Context, runID, assetID uuid.UUID, step, status string, attempt int) error {
	if err := r.session.Query(
		`INSERT INTO pipeline_steps (run_id, step, asset_id, status, attempt, updated_at)
		 VALUES (?, ?, ?, ?, ?, ?)`,
		runID, step, assetID, status, attempt, time.Now().UTC(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set step status: %w", err)
	}
	return nil
}

func (r *JobRepo) InitializeRunSteps(ctx context.Context, runID, assetID uuid.UUID, languages []string) error {
	steps := []string{"probe", "transcode", "thumbnail", "storyboard"}
	for _, language := range languages {
		steps = append(steps, "subtitle:"+language)
	}
	for _, step := range steps {
		if err := r.session.Query(
			`INSERT INTO pipeline_steps (run_id, step, asset_id, status, attempt, updated_at)
			 VALUES (?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
			runID, step, assetID, "PENDING", 1, time.Now().UTC(),
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("initialize pipeline step %s: %w", step, err)
		}
	}
	return nil
}

// GetRunSteps returns lightweight snapshots (used by orchestrator logic and
// the sweeper).  UpdatedAt is included so the sweeper can check step age.
func (r *JobRepo) GetRunSteps(ctx context.Context, runID uuid.UUID) ([]StepSnapshot, error) {
	var rows []StepSnapshot
	iter := r.session.Query(
		`SELECT step, status, attempt, updated_at FROM pipeline_steps WHERE run_id = ?`,
		runID,
	).WithContext(ctx).Iter()
	var step, status string
	var attempt int
	var updatedAt time.Time
	for iter.Scan(&step, &status, &attempt, &updatedAt) {
		rows = append(rows, StepSnapshot{Step: step, Status: status, Attempt: attempt, UpdatedAt: updatedAt})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get pipeline run steps: %w", err)
	}
	return rows, nil
}

// GetPipelineStatus returns full step rows for the API (GetPipelineStatus RPC).
func (r *JobRepo) GetPipelineStatus(ctx context.Context, assetID uuid.UUID) ([]PipelineStep, error) {
	run, err := r.GetRun(ctx, assetID)
	if err != nil {
		return nil, err
	}
	if run == nil {
		return nil, nil
	}
	return r.GetStepsForRun(ctx, run.RunID)
}

func (r *JobRepo) GetStepsForRun(ctx context.Context, runID uuid.UUID) ([]PipelineStep, error) {
	var rows []PipelineStep
	iter := r.session.Query(
		`SELECT run_id, asset_id, step, status, attempt, updated_at
		 FROM pipeline_steps WHERE run_id = ?`,
		runID,
	).WithContext(ctx).Iter()
	var stepRunID, stepAssetID uuid.UUID
	var stepName, stepStatus string
	var stepAttempt int
	var updatedAt time.Time
	for iter.Scan(&stepRunID, &stepAssetID, &stepName, &stepStatus, &stepAttempt, &updatedAt) {
		rows = append(rows, PipelineStep{
			RunID:     stepRunID,
			AssetID:   stepAssetID,
			Step:      stepName,
			Status:    stepStatus,
			Attempt:   stepAttempt,
			UpdatedAt: updatedAt,
		})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate pipeline steps: %w", err)
	}
	return rows, nil
}

// ── Artifact tracking ─────────────────────────────────────────────

// RecordArtifact persists an artifact location per run+step.
func (r *JobRepo) RecordArtifact(ctx context.Context, runID uuid.UUID, step, location string) error {
	if err := r.session.Query(
		`INSERT INTO pipeline_artifacts (run_id, step, location, updated_at)
		 VALUES (?, ?, ?, ?)`,
		runID, step, location, time.Now().UTC(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("record artifact: %w", err)
	}
	return nil
}

// GetArtifacts loads all artifact locations for a run.
func (r *JobRepo) GetArtifacts(ctx context.Context, runID uuid.UUID) (map[string]string, error) {
	locations := map[string]string{}
	iter := r.session.Query(
		`SELECT step, location FROM pipeline_artifacts WHERE run_id = ?`,
		runID,
	).WithContext(ctx).Iter()
	var step, location string
	for iter.Scan(&step, &location) {
		locations[step] = location
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get artifacts: %w", err)
	}
	return locations, nil
}

// ── Active-runs index ─────────────────────────────────────────────

// GetActiveRuns returns all runs that have not yet reached a terminal state.
// It reads all shards of the active_runs table (O(active runs), not O(all runs))
// so it stays cheap even as pipeline_runs_by_run grows indefinitely.
func (r *JobRepo) GetActiveRuns(ctx context.Context) ([]PipelineRun, error) {
	var runs []PipelineRun
	for shard := 0; shard < activeRunShards; shard++ {
		iter := r.session.Query(
			`SELECT asset_id, run_id FROM active_runs WHERE shard = ?`,
			shard,
		).WithContext(ctx).Iter()
		var assetID, runID uuid.UUID
		for iter.Scan(&assetID, &runID) {
			run, err := r.GetRun(ctx, assetID)
			if err != nil {
				_ = iter.Close()
				return nil, err
			}
			if run != nil {
				runs = append(runs, *run)
			}
		}
		if err := iter.Close(); err != nil {
			return nil, fmt.Errorf("iterate active_runs shard %d: %w", shard, err)
		}
	}
	return runs, nil
}

func (r *JobRepo) removeActiveRun(ctx context.Context, assetID uuid.UUID) error {
	// We need the created_at to hit the exact row (it is part of the primary key).
	run, err := r.GetRun(ctx, assetID)
	if err != nil || run == nil {
		return err
	}
	return r.session.Query(
		`DELETE FROM active_runs WHERE shard = ? AND created_at = ? AND asset_id = ?`,
		shardFor(assetID), run.CreatedAt, assetID,
	).WithContext(ctx).Exec()
}
