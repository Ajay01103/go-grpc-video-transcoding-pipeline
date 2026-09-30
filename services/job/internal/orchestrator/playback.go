package orchestrator

import (
	"context"
	"fmt"
	"time"

	"connectrpc.com/connect"
	playbackpb "github.com/Ajay01103/go-mux/playback/gen/pb"
	playbackconnect "github.com/Ajay01103/go-mux/playback/gen/pb/pbconnect"
	"github.com/google/uuid"
)

type RetryingPlaybackCreator struct {
	client playbackconnect.PlaybackServiceClient
}

func NewPlaybackCreator(client playbackconnect.PlaybackServiceClient) *RetryingPlaybackCreator {
	return &RetryingPlaybackCreator{client: client}
}

func (c *RetryingPlaybackCreator) CreateWithRetry(ctx context.Context, assetID uuid.UUID, policy, artifactPrefix string) (*playbackpb.PlaybackRecord, error) {
	if c.client == nil {
		return nil, fmt.Errorf("playback client is required")
	}
	var lastErr error
	for attempt := 0; attempt < 3; attempt++ {
		response, err := c.client.CreatePlayback(ctx, connect.NewRequest(&playbackpb.CreatePlaybackRequest{AssetId: assetID.String(), Policy: policy, ArtifactPrefix: artifactPrefix}))
		if err == nil {
			return response.Msg.GetPlayback(), nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-time.After(time.Duration(attempt+1) * time.Second):
		}
	}
	return nil, fmt.Errorf("create playback after retries: %w", lastErr)
}