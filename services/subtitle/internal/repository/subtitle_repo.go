package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/gocql/gocql"
)

type SubtitleSegment struct {
	StartMS  int64
	EndMS    int64
	Text     string
	Language string
}

type SubtitleRecord struct {
	AssetID   uuid.UUID
	Language  string
	Status    string
	Segments  []SubtitleSegment
	CreatedAt time.Time
	UpdatedAt time.Time
}

type SubtitleRepo struct {
	session *gocql.Session
}

func NewSubtitleRepo(session *gocql.Session) *SubtitleRepo {
	return &SubtitleRepo{session: session}
}

func (r *SubtitleRepo) SetSubtitleStatus(ctx context.Context, assetID uuid.UUID, language, status string) error {
	return r.SetSubtitleResult(ctx, assetID, language, status, "", "")
}

// SetSubtitleResult records status plus the VTT storage location or an error
// message in one write (matches the migration-1 schema columns).
func (r *SubtitleRepo) SetSubtitleResult(ctx context.Context, assetID uuid.UUID, language, status, vttLocation, errorMessage string) error {
	if err := r.session.Query(
		`INSERT INTO subtitles (asset_id, language, status, vtt_location, error_message, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		assetID, language, status, vttLocation, errorMessage, time.Now().UTC(), time.Now().UTC(),
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("set subtitle result: %w", err)
	}
	return nil
}

func (r *SubtitleRepo) GetSubtitle(ctx context.Context, assetID uuid.UUID, language string) (*SubtitleRecord, error) {
	var rec SubtitleRecord
	var segmentsJSON string
	err := r.session.Query(
		`SELECT asset_id, language, status, segments, created_at, updated_at FROM subtitles WHERE asset_id = ? AND language = ? LIMIT 1`,
		assetID, language,
	).WithContext(ctx).Scan(&rec.AssetID, &rec.Language, &rec.Status, &segmentsJSON, &rec.CreatedAt, &rec.UpdatedAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get subtitle: %w", err)
	}
	return &rec, nil
}
