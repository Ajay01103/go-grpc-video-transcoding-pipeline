package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type PlaybackRecord struct {
	PlaybackID     uuid.UUID `json:"playback_id"`
	AssetID        uuid.UUID `json:"asset_id"`
	Policy         string    `json:"policy"`
	ArtifactPrefix string    `json:"artifact_prefix"`
	SigningKeyID   string    `json:"signing_key_id"`
	Revoked        bool      `json:"revoked"`
	CreatedAt      time.Time `json:"created_at"`
}

type PlaybackRepo struct {
	session *gocql.Session
}

func NewPlaybackRepo(session *gocql.Session) *PlaybackRepo {
	return &PlaybackRepo{session: session}
}

func (r *PlaybackRepo) CreatePlayback(ctx context.Context, assetID uuid.UUID, policy, artifactPrefix string) (*PlaybackRecord, error) {
	existing, err := r.GetActivePlaybackByAssetPolicy(ctx, assetID, policy)
	if err != nil {
		return nil, err
	}
	if existing != nil {
		// Idempotent: update the prefix (artifacts may have moved) and return.
		if artifactPrefix != "" && artifactPrefix != existing.ArtifactPrefix {
			if err := r.session.Query(
				`UPDATE playback_ids SET signing_key_id = ? WHERE playback_id = ?`,
				artifactPrefix, existing.PlaybackID,
			).WithContext(ctx).Exec(); err != nil {
				return nil, fmt.Errorf("update playback artifact prefix: %w", err)
			}
			existing.ArtifactPrefix = artifactPrefix
		}
		return existing, nil
	}
	playbackID := uuid.New()
	now := time.Now().UTC()
	rec := &PlaybackRecord{
		PlaybackID:     playbackID,
		AssetID:        assetID,
		Policy:         policy,
		ArtifactPrefix: artifactPrefix,
		CreatedAt:      now,
	}
	// Single-partition batch keeps both rows consistent.
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO playback_ids (playback_id, asset_id, policy, artifact_prefix, signing_key_id, revoked, created_at) VALUES (?, ?, ?, ?, ?, ?, ?)`,
		rec.PlaybackID, rec.AssetID, rec.Policy, rec.ArtifactPrefix, rec.SigningKeyID, rec.Revoked, rec.CreatedAt,
	)
	batch.Query(
		`INSERT INTO playback_ids_by_asset (asset_id, playback_id, policy) VALUES (?, ?, ?)`,
		rec.AssetID, rec.PlaybackID, rec.Policy,
	)
	batch.Query(
		`INSERT INTO playback_ids_by_asset_policy (asset_id, policy, playback_id) VALUES (?, ?, ?)`,
		rec.AssetID, rec.Policy, rec.PlaybackID,
	)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return nil, fmt.Errorf("insert playback record: %w", err)
	}
	return rec, nil
}

func (r *PlaybackRepo) GetActivePlaybackByAssetPolicy(ctx context.Context, assetID uuid.UUID, policy string) (*PlaybackRecord, error) {
	var playbackID uuid.UUID
	err := r.session.Query(
		`SELECT playback_id FROM playback_ids_by_asset_policy WHERE asset_id = ? AND policy = ? LIMIT 1`,
		assetID, policy,
	).WithContext(ctx).Scan(&playbackID)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("find playback by asset and policy: %w", err)
	}
	rec, err := r.ResolvePlayback(ctx, playbackID)
	if err != nil || rec == nil {
		return nil, err
	}
	if rec.Revoked {
		return nil, nil
	}
	return rec, nil
}

func (r *PlaybackRepo) ResolvePlayback(ctx context.Context, playbackID uuid.UUID) (*PlaybackRecord, error) {
	var rec PlaybackRecord
	err := r.session.Query(
		`SELECT playback_id, asset_id, policy, artifact_prefix, signing_key_id, revoked, created_at FROM playback_ids WHERE playback_id = ? LIMIT 1`,
		playbackID,
	).WithContext(ctx).Scan(&rec.PlaybackID, &rec.AssetID, &rec.Policy, &rec.ArtifactPrefix, &rec.SigningKeyID, &rec.Revoked, &rec.CreatedAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("resolve playback id: %w", err)
	}
	return &rec, nil
}

func (r *PlaybackRepo) RevokePlayback(ctx context.Context, playbackID uuid.UUID) error {
	rec, err := r.ResolvePlayback(ctx, playbackID)
	if err != nil {
		return err
	}
	if rec == nil {
		return fmt.Errorf("playback %s not found", playbackID)
	}
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(`UPDATE playback_ids SET revoked = true WHERE playback_id = ?`, playbackID)
	batch.Query(`DELETE FROM playback_ids_by_asset_policy WHERE asset_id = ? AND policy = ? AND playback_id = ?`, rec.AssetID, rec.Policy, playbackID)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("revoke playback: %w", err)
	}
	return nil
}
