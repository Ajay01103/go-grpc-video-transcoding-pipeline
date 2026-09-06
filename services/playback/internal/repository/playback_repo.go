package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type PlaybackRecord struct {
	PlaybackID   uuid.UUID `json:"playback_id"`
	AssetID      uuid.UUID `json:"asset_id"`
	Policy       string    `json:"policy"`
	SigningKeyID string    `json:"signing_key_id"`
	Revoked      bool      `json:"revoked"`
	CreatedAt    time.Time `json:"created_at"`
}

type PlaybackRepo struct {
	session *gocql.Session
}

func NewPlaybackRepo(session *gocql.Session) *PlaybackRepo {
	return &PlaybackRepo{session: session}
}

func (r *PlaybackRepo) EnsureSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS playback_ids (
			playback_id uuid PRIMARY KEY,
			asset_id uuid,
			policy text,
			signing_key_id text,
			revoked boolean,
			created_at timestamp
		)`,
		`CREATE TABLE IF NOT EXISTS playback_ids_by_asset (
			asset_id uuid,
			playback_id uuid,
			PRIMARY KEY (asset_id, playback_id)
		)`,
	}
	for _, q := range queries {
		if err := r.session.Query(q).WithContext(context.Background()).Exec(); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
}

func (r *PlaybackRepo) CreatePlayback(ctx context.Context, assetID uuid.UUID, policy string) (*PlaybackRecord, error) {
	existing, err := r.GetActivePlaybackByAssetPolicy(ctx, assetID, policy)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		return existing, nil
	}
	playbackID := uuid.New()
	now := time.Now().UTC()
	rec := &PlaybackRecord{
		PlaybackID: playbackID,
		AssetID:    assetID,
		Policy:     policy,
		CreatedAt:  now,
	}
	if err := r.session.Query(
		`INSERT INTO playback_ids (playback_id, asset_id, policy, signing_key_id, revoked, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		rec.PlaybackID, rec.AssetID, rec.Policy, rec.SigningKeyID, rec.Revoked, rec.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert playback record: %w", err)
	}
	if err := r.session.Query(
		`INSERT INTO playback_ids_by_asset (asset_id, playback_id) VALUES (?, ?)`,
		rec.AssetID, rec.PlaybackID,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert playback-by-asset: %w", err)
	}
	return rec, nil
}

func (r *PlaybackRepo) GetActivePlaybackByAssetPolicy(ctx context.Context, assetID uuid.UUID, policy string) (*PlaybackRecord, error) {
	var rec PlaybackRecord
	err := r.session.Query(
		`SELECT playback_id, asset_id, policy, signing_key_id, revoked, created_at FROM playback_ids WHERE asset_id = ? AND policy = ? ALLOW FILTERING`,
		assetID, policy,
	).WithContext(ctx).Scan(&rec.PlaybackID, &rec.AssetID, &rec.Policy, &rec.SigningKeyID, &rec.Revoked, &rec.CreatedAt)
	if err == gocql.ErrNotFound || rec.Revoked {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find playback by asset and policy: %w", err)
	}
	return &rec, nil
}

func (r *PlaybackRepo) ResolvePlayback(ctx context.Context, playbackID uuid.UUID) (*PlaybackRecord, error) {
	var rec PlaybackRecord
	err := r.session.Query(
		`SELECT playback_id, asset_id, policy, signing_key_id, revoked, created_at FROM playback_ids WHERE playback_id = ? LIMIT 1`,
		playbackID,
	).WithContext(ctx).Scan(&rec.PlaybackID, &rec.AssetID, &rec.Policy, &rec.SigningKeyID, &rec.Revoked, &rec.CreatedAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve playback id: %w", err)
	}
	return &rec, nil
}

func (r *PlaybackRepo) RevokePlayback(ctx context.Context, playbackID uuid.UUID) error {
	if err := r.session.Query(
		`UPDATE playback_ids SET revoked = true WHERE playback_id = ?`,
		playbackID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("revoke playback: %w", err)
	}
	return nil
}
