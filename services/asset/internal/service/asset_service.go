package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Ajay01103/go-mux/asset/internal/repository"
	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/Ajay01103/go-mux/pkg/storage"
	"github.com/google/uuid"
)

type AssetService struct {
	repo      *repository.AssetRepo
	publisher *events.Publisher
	storage   *storage.Client
	bucket    string
}

func NewAssetService(repo *repository.AssetRepo, publisher *events.Publisher, store *storage.Client, bucket string) *AssetService {
	return &AssetService{repo: repo, publisher: publisher, storage: store, bucket: bucket}
}

// DefaultOrgID is a fixed dev org applied until multi-org support lands.
// The frontend no longer sends org_id; every asset created without one
// lands here.
const DefaultOrgID = "00000000-0000-0000-0000-000000000001"

type CreateAssetRequest struct {
	OrgID uuid.UUID `json:"org_id"`
	Title string    `json:"title"`
}

type AssetUploadURL struct {
	AssetID   uuid.UUID `json:"asset_id"`
	UploadURL string    `json:"upload_url"`
	ExpiresAt time.Time `json:"expires_at"`
	Bucket    string    `json:"bucket"`
	ObjectKey string    `json:"object_key"`
}

func (s *AssetService) CreateAsset(ctx context.Context, orgID uuid.UUID, title string) (*repository.Asset, error) {
	if orgID == uuid.Nil {
		orgID = uuid.MustParse(DefaultOrgID)
	}
	if title == "" {
		return nil, fmt.Errorf("title is required")
	}
	asset, err := s.repo.CreateAsset(ctx, orgID, title)
	if err != nil {
		return nil, err
	}
	return asset, nil
}

func (s *AssetService) GetAsset(ctx context.Context, assetID uuid.UUID) (*repository.Asset, error) {
	if assetID == uuid.Nil {
		return nil, fmt.Errorf("asset_id is required")
	}
	return s.repo.GetAsset(ctx, assetID)
}

func (s *AssetService) ListAssets(ctx context.Context, orgID uuid.UUID, limit int) ([]repository.Asset, error) {
	if orgID == uuid.Nil {
		orgID = uuid.MustParse(DefaultOrgID)
	}
	return s.repo.ListAssets(ctx, orgID, limit)
}

func (s *AssetService) CreateUploadURL(ctx context.Context, assetID uuid.UUID, bucket string, ttl time.Duration) (*AssetUploadURL, error) {
	if assetID == uuid.Nil {
		return nil, fmt.Errorf("asset_id is required")
	}
	if bucket == "" {
		bucket = s.bucket
	}
	if ttl <= 0 {
		ttl = 30 * time.Minute
	}
	objectKey := fmt.Sprintf("raw/%s/source.mp4", assetID.String())
	// Real SigV4 presigned PUT against RustFS — the client uploads bytes
	// directly to object storage; no bytes flow through this service.
	uploadURL, err := s.storage.PresignPut(objectKey, ttl)
	if err != nil {
		return nil, fmt.Errorf("presign upload url: %w", err)
	}
	return &AssetUploadURL{
		AssetID:   assetID,
		UploadURL: uploadURL,
		ExpiresAt: time.Now().Add(ttl).UTC(),
		Bucket:    bucket,
		ObjectKey: objectKey,
	}, nil
}

func (s *AssetService) CompleteUpload(ctx context.Context, assetID uuid.UUID, sourceURI string) error {
	if assetID == uuid.Nil {
		return fmt.Errorf("asset_id is required")
	}
	// Production contract: the source must live in RustFS. Normalize to the
	// canonical s3:// form so workers can always parse it.
	if _, _, err := storage.ParseSourceURI(sourceURI); err != nil {
		// Allow the bare object key and rewrite it into the canonical URI.
		if sourceURI == fmt.Sprintf("raw/%s/source.mp4", assetID.String()) {
			sourceURI = fmt.Sprintf("s3://%s/%s", s.bucket, sourceURI)
		} else {
			return fmt.Errorf("source_uri must be s3://bucket/key (got %q)", sourceURI)
		}
	}
	if err := s.repo.SetSourceURI(ctx, assetID, sourceURI); err != nil {
		return err
	}
	if err := s.repo.UpdateStatus(ctx, assetID, "PROCESSING"); err != nil {
		return err
	}
	if s.publisher == nil {
		return fmt.Errorf("event publisher is required")
	}
	asset, err := s.repo.GetAsset(ctx, assetID)
	if err != nil {
		return err
	}
	if asset == nil {
		return fmt.Errorf("asset %s not found", assetID)
	}
	event := &pipelinepb.AssetUploadCompleted{
		AssetId:        asset.ID.String(),
		OrgId:          asset.OrgID.String(),
		SourceUri:      sourceURI,
		IdempotencyKey: events.UploadMessageID(assetID.String()),
		TimestampUnix:  time.Now().UTC().Unix(),
	}
	return s.publisher.PublishUploadCompleted(ctx, event)
}

func (s *AssetService) MarkErrored(ctx context.Context, assetID uuid.UUID, message string) error {
	if err := s.repo.UpdateStatus(ctx, assetID, "ERRORED"); err != nil {
		return err
	}
	return nil
}

func (s *AssetService) ReconcileProcessingAssets(ctx context.Context, olderThan time.Duration) error {
	if s.publisher == nil {
		return fmt.Errorf("event publisher is required")
	}
	if olderThan <= 0 {
		return fmt.Errorf("reconciliation age must be positive")
	}
	assets, err := s.repo.ListStaleProcessingAssets(ctx, time.Now().UTC().Add(-olderThan), 100)
	if err != nil {
		return err
	}
	for _, asset := range assets {
		if asset.SourceURI == "" {
			continue
		}
		event := &pipelinepb.AssetUploadCompleted{
			AssetId: asset.ID.String(), OrgId: asset.OrgID.String(), SourceUri: asset.SourceURI,
			IdempotencyKey: events.UploadMessageID(asset.ID.String()), TimestampUnix: time.Now().UTC().Unix(),
		}
		if err := s.publisher.PublishUploadCompleted(ctx, event); err != nil {
			return fmt.Errorf("reconcile asset %s: %w", asset.ID, err)
		}
	}
	return nil
}
