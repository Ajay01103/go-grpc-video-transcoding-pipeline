package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type PipelineStep struct {
	RunID     uuid.UUID `json:"run_id"`
	AssetID   uuid.UUID `json:"asset_id"`
	Step      string    `json:"step"`
	Status    string    `json:"status"`
	UpdatedAt time.Time `json:"updated_at"`
}

type StepSnapshot struct {
	Step   string
	Status string
}

type PipelineRun struct {
	RunID                      uuid.UUID `json:"run_id"`
	AssetID                    uuid.UUID `json:"asset_id"`
	OrgID                      uuid.UUID `json:"org_id"`
	SourceURI                  string    `json:"source_uri"`
	RequestedSubtitleLanguages []string  `json:"requested_subtitle_languages,omitempty"`
	Status                     string    `json:"status"`
	CreatedAt                  time.Time `json:"created_at"`
	UpdatedAt                  time.Time `json:"updated_at"`
	PlaybackID                 uuid.UUID `json:"playback_id,omitempty"`
}

type JobRepo struct {
	session *gocql.Session
}

func NewJobRepo(session *gocql.Session) *JobRepo {
	return &JobRepo{session: session}
}

func (r *JobRepo) EnsureSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS pipeline_runs (
			asset_id uuid PRIMARY KEY,
			run_id uuid,
			org_id uuid,
			source_uri text,
			requested_subtitle_languages list<text>,
			status text,
			created_at timestamp,
			updated_at timestamp
			playback_id uuid
		)`,
		`CREATE TABLE IF NOT EXISTS pipeline_steps (
			run_id uuid,
			asset_id uuid,
			step text,
			status text,
			updated_at timestamp,
			PRIMARY KEY (run_id, step)
		)`,
	}
	for _, q := range queries {
		if err := r.session.Query(q).WithContext(context.Background()).Exec(); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

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
		CreatedAt:                  now,
		UpdatedAt:                  now,
	}
	var applied bool
	if err := r.session.Query(
		`INSERT INTO pipeline_runs (asset_id, run_id, org_id, source_uri, requested_subtitle_languages, status, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?) IF NOT EXISTS`,
		candidate.AssetID, candidate.RunID, candidate.OrgID, candidate.SourceURI, candidate.RequestedSubtitleLanguages, candidate.Status, candidate.CreatedAt, candidate.UpdatedAt,
	).WithContext(ctx).Scan(&applied); err != nil {
		return nil, false, fmt.Errorf("create pipeline run: %w", err)
	}
	if !applied {
		run, err = r.GetRun(ctx, assetID)
		if err != nil {
			return nil, false, err
		}
		return run, false, nil
	}
	return candidate, true, nil
}

func (r *JobRepo) GetRun(ctx context.Context, assetID uuid.UUID) (*PipelineRun, error) {
	var run PipelineRun
	err := r.session.Query(
		`SELECT asset_id, run_id, org_id, source_uri, requested_subtitle_languages, status, created_at, updated_at, playback_id FROM pipeline_runs WHERE asset_id = ?`,
		assetID,
	).WithContext(ctx).Scan(
		&run.AssetID, &run.RunID, &run.OrgID, &run.SourceURI, &run.RequestedSubtitleLanguages, &run.Status, &run.CreatedAt, &run.UpdatedAt, &run.PlaybackID,
	)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get pipeline run: %w", err)
	}
	return &run, nil
}

func (r *JobRepo) SetRunStatus(ctx context.Context, assetID uuid.UUID, status string) error {
	if err := r.session.Query(
		`UPDATE pipeline_runs SET status = ?, updated_at = ? WHERE asset_id = ?`,
		status, time.Now().UTC(), assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set pipeline run status: %w", err)
	}
	return nil
}

func (r *JobRepo) SetStepStatus(ctx context.Context, assetID uuid.UUID, step, status string) error {
	run, err := r.GetRun(ctx, assetID)
	if err != nil {
		return err
	}
	if run == nil {
		return fmt.Errorf("pipeline run for asset %s not found", assetID)
	}
	return r.SetStepStatusForRun(ctx, run.RunID, step, status)
}

func (r *JobRepo) SetStepStatusForRun(ctx context.Context, runID uuid.UUID, step, status string) error {
	if err := r.session.Query(
		`UPDATE pipeline_steps SET status = ?, updated_at = ? WHERE run_id = ? AND step = ?`,
		status, time.Now().UTC(), runID, step,
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
			`INSERT INTO pipeline_steps (run_id, asset_id, step, status, updated_at) VALUES (?, ?, ?, ?, ?) IF NOT EXISTS`,
			runID, assetID, step, "PENDING", time.Now().UTC(),
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("initialize pipeline step %s: %w", step, err)
		}
	}
	return nil
}

func (r *JobRepo) GetPipelineStatus(ctx context.Context, assetID uuid.UUID) ([]PipelineStep, error) {
	var rows []PipelineStep
	iter := r.session.Query(
		`SELECT run_id, asset_id, step, status, updated_at FROM pipeline_steps WHERE asset_id = ? ALLOW FILTERING`,
		assetID,
	).WithContext(ctx).Iter()
	var stepAssetID uuid.UUID
	var runID uuid.UUID
	var stepName string
	var stepStatus string
	var updatedAt time.Time
	for iter.Scan(&runID, &stepAssetID, &stepName, &stepStatus, &updatedAt) {
		rows = append(rows, PipelineStep{AssetID: stepAssetID, Step: stepName, Status: stepStatus, UpdatedAt: updatedAt})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate pipeline steps: %w", err)
	}
	return rows, nil
}

func (r *JobRepo) GetRunSteps(ctx context.Context, runID uuid.UUID) ([]StepSnapshot, error) {
	var rows []StepSnapshot
	iter := r.session.Query(
		`SELECT step, status FROM pipeline_steps WHERE run_id = ?`,
		runID,
	).WithContext(ctx).Iter()
	var step, status string
	for iter.Scan(&step, &status) {
		rows = append(rows, StepSnapshot{Step: step, Status: status})
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("get pipeline run steps: %w", err)
	}
	return rows, nil
}
