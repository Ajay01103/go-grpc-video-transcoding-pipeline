package observability

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type DLQEntry struct {
	EntryID       uuid.UUID
	PipelineID    uuid.UUID
	AssetID       uuid.UUID
	StepType      string // probe, transcode, thumbnail, storyboard, subtitle
	Error         string
	Retries       int
	LastAttemptAt time.Time
	CreatedAt     time.Time
	ExpiresAt     time.Time
}

type DLQ struct {
	session *gocql.Session
}

func NewDLQ(session *gocql.Session) *DLQ {
	return &DLQ{session: session}
}

func (d *DLQ) EnsureSchema() error {
	q := `CREATE TABLE IF NOT EXISTS pipeline_dlq (
		asset_id uuid,
		entry_id uuid,
		pipeline_id uuid,
		step_type text,
		error text,
		retries int,
		last_attempt_at timestamp,
		created_at timestamp,
		expires_at timestamp,
		PRIMARY KEY (asset_id, created_at, entry_id)
	) WITH CLUSTERING ORDER BY (created_at DESC)
	AND default_time_to_live = 2592000`
	return d.session.Query(q).WithContext(context.Background()).Exec()
}

func (d *DLQ) RecordFailure(ctx context.Context, assetID, pipelineID uuid.UUID, stepType, errMsg string) error {
	entry := &DLQEntry{
		EntryID:       uuid.New(),
		PipelineID:    pipelineID,
		AssetID:       assetID,
		StepType:      stepType,
		Error:         errMsg,
		Retries:       0,
		LastAttemptAt: time.Now().UTC(),
		CreatedAt:     time.Now().UTC(),
		ExpiresAt:     time.Now().UTC().Add(30 * 24 * time.Hour),
	}

	if err := d.session.Query(
		`INSERT INTO pipeline_dlq (asset_id, entry_id, pipeline_id, step_type, error, retries, last_attempt_at, created_at, expires_at)
		 VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		entry.AssetID, entry.EntryID, entry.PipelineID, entry.StepType, entry.Error, entry.Retries, entry.LastAttemptAt, entry.CreatedAt, entry.ExpiresAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("record dlq entry: %w", err)
	}

	return nil
}

func (d *DLQ) ListFailures(ctx context.Context, limit int) ([]DLQEntry, error) {
	var results []DLQEntry
	iter := d.session.Query(
		`SELECT entry_id, pipeline_id, asset_id, step_type, error, retries, last_attempt_at, created_at, expires_at
		 FROM pipeline_dlq LIMIT ?`,
		limit,
	).WithContext(ctx).Iter()

	var entry DLQEntry
	for iter.Scan(&entry.EntryID, &entry.PipelineID, &entry.AssetID, &entry.StepType, &entry.Error, &entry.Retries, &entry.LastAttemptAt, &entry.CreatedAt, &entry.ExpiresAt) {
		results = append(results, entry)
	}

	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("list failures: %w", err)
	}

	return results, nil
}
