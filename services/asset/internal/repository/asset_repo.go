package repository

import (
	"context"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type Asset struct {
	ID          uuid.UUID `json:"id"`
	OrgID       uuid.UUID `json:"org_id"`
	Title       string    `json:"title"`
	Status      string    `json:"status"`
	SourceURI   string    `json:"source_uri,omitempty"`
	DurationMs  int64     `json:"duration_ms,omitempty"`
	ErrorString string    `json:"error_string,omitempty"`
	CreatedAt   time.Time `json:"created_at"`
	UpdatedAt   time.Time `json:"updated_at"`
}

type AssetRepo struct {
	session *gocql.Session
}

func (r *AssetRepo) ListStaleProcessingAssets(ctx context.Context, before time.Time, limit int) ([]Asset, error) {
	if limit <= 0 {
		limit = 100
	}
	var assets []Asset
	iter := r.session.Query(
		`SELECT asset_id FROM assets_by_status WHERE status = ? AND updated_at < ? LIMIT ?`,
		"PROCESSING", before, limit,
	).WithContext(ctx).Iter()
	var assetID uuid.UUID
	for iter.Scan(&assetID) {
		asset, err := r.GetAsset(ctx, assetID)
		if err != nil {
			return nil, err
		}
		if asset != nil {
			assets = append(assets, *asset)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("list stale processing assets: %w", err)
	}
	return assets, nil
}

func NewAssetRepo(session *gocql.Session) *AssetRepo {
	return &AssetRepo{session: session}
}

func (r *AssetRepo) CreateAsset(ctx context.Context, orgID uuid.UUID, title string) (*Asset, error) {
	assetID := uuid.New()
	now := time.Now().UTC()
	asset := &Asset{
		ID:        assetID,
		OrgID:     orgID,
		Title:     title,
		Status:    "PREPARING",
		CreatedAt: now,
		UpdatedAt: now,
	}
	if err := r.session.Query(
		`INSERT INTO assets_by_id (asset_id, org_id, title, status, source_uri, duration_ms, error_string, created_at, updated_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
		asset.ID, asset.OrgID, asset.Title, asset.Status, asset.SourceURI, asset.DurationMs, asset.ErrorString, asset.CreatedAt, asset.UpdatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert asset_by_id: %w", err)
	}
	if err := r.session.Query(
		`INSERT INTO assets_by_org (org_id, created_at, asset_id, status) VALUES (?, ?, ?, ?)`,
		asset.OrgID, asset.CreatedAt, asset.ID, asset.Status,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert asset_by_org: %w", err)
	}
	if err := r.session.Query(
		`INSERT INTO assets_by_status (status, updated_at, asset_id) VALUES (?, ?, ?)`,
		asset.Status, asset.UpdatedAt, asset.ID,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert asset_by_status: %w", err)
	}
	return asset, nil
}

func (r *AssetRepo) GetAsset(ctx context.Context, assetID uuid.UUID) (*Asset, error) {
	var asset Asset
	err := r.session.Query(
		`SELECT asset_id, org_id, title, status, source_uri, duration_ms, error_string, created_at, updated_at FROM assets_by_id WHERE asset_id = ? LIMIT 1`,
		assetID,
	).WithContext(ctx).Scan(
		&asset.ID, &asset.OrgID, &asset.Title, &asset.Status, &asset.SourceURI, &asset.DurationMs, &asset.ErrorString, &asset.CreatedAt, &asset.UpdatedAt,
	)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("select asset: %w", err)
	}
	return &asset, nil
}

func (r *AssetRepo) ListAssets(ctx context.Context, orgID uuid.UUID, limit int) ([]Asset, error) {
	if limit <= 0 {
		limit = 20
	}

	var rows []Asset
	iter := r.session.Query(
		`SELECT asset_id, status, created_at FROM assets_by_org WHERE org_id = ? LIMIT ?`,
		orgID, limit,
	).WithContext(ctx).Iter()
	var assetID uuid.UUID
	var status string
	var createdAt time.Time
	for iter.Scan(&assetID, &status, &createdAt) {
		asset, err := r.GetAsset(ctx, assetID)
		if err != nil {
			return nil, err
		}
		if asset != nil {
			asset.Status = status
			rows = append(rows, *asset)
		}
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate org assets: %w", err)
	}
	return rows, nil
}

func (r *AssetRepo) SetSourceURI(ctx context.Context, assetID uuid.UUID, sourceURI string) error {
	now := time.Now().UTC()
	if err := r.session.Query(
		`UPDATE assets_by_id SET source_uri = ?, updated_at = ? WHERE asset_id = ?`,
		sourceURI, now, assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("update source uri: %w", err)
	}
	return nil
}

func (r *AssetRepo) UpdateStatus(ctx context.Context, assetID uuid.UUID, status string) error {
	now := time.Now().UTC()
	// Read the old row so the status index can be re-keyed correctly.
	asset, err := r.GetAsset(ctx, assetID)
	if err != nil {
		return err
	}
	if asset == nil {
		return fmt.Errorf("asset %s not found", assetID)
	}
	if err := r.session.Query(
		`UPDATE assets_by_id SET status = ?, updated_at = ? WHERE asset_id = ?`,
		status, now, assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("update status: %w", err)
	}
	// Keep indexes consistent: new status row keyed on the new timestamp,
	// old status row tombstoned via the same (status, updated_at, asset_id) key.
	if err := r.session.Query(
		`INSERT INTO assets_by_status (status, updated_at, asset_id) VALUES (?, ?, ?)`,
		status, now, assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("update asset_by_status: %w", err)
	}
	if asset.Status != status {
		if err := r.session.Query(
			`DELETE FROM assets_by_status WHERE status = ? AND updated_at = ? AND asset_id = ?`,
			asset.Status, asset.UpdatedAt, assetID,
		).WithContext(ctx).Exec(); err != nil {
			return fmt.Errorf("remove stale asset_by_status row: %w", err)
		}
	}
	if err := r.session.Query(
		`UPDATE assets_by_org SET status = ? WHERE org_id = ? AND created_at = ? AND asset_id = ?`,
		status, asset.OrgID, asset.CreatedAt, assetID,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("update asset_by_org status: %w", err)
	}
	return nil
}
