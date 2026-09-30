package playback

import (
	"context"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/Ajay01103/go-mux/playback/internal/repository"
	"github.com/google/uuid"
)

type PlaybackService struct {
	repo          *repository.PlaybackRepo
	signer        *TokenSigner
	tokenTTL      time.Duration
	cdnURL        string
	rustfsBaseURL string
	subtitleLangs []string
}

func NewPlaybackService(repo *repository.PlaybackRepo, signer *TokenSigner, tokenTTL time.Duration) *PlaybackService {
	if tokenTTL <= 0 {
		tokenTTL = 5 * time.Minute
	}
	endpoint := strings.TrimRight(strings.TrimSpace(os.Getenv("RUSTFS_URL")), "/")
	if endpoint == "" {
		endpoint = "http://localhost:9000"
	}
	bucket := strings.TrimSpace(os.Getenv("RUSTFS_BUCKET"))
	if bucket == "" {
		bucket = "uploads"
	}
	return &PlaybackService{
		repo:          repo,
		signer:        signer,
		tokenTTL:      tokenTTL,
		cdnURL:        strings.TrimSpace(os.Getenv("CDN_BASE_URL")),
		rustfsBaseURL: endpoint + "/" + bucket,
		subtitleLangs: []string{"en"},
	}
}

func (s *PlaybackService) CreatePlayback(ctx context.Context, assetID uuid.UUID, policy, artifactPrefix string) (*repository.PlaybackRecord, error) {
	if assetID == uuid.Nil {
		return nil, fmt.Errorf("asset_id is required")
	}
	if policy == "" {
		policy = "public"
	}
	return s.repo.CreatePlayback(ctx, assetID, policy, artifactPrefix)
}

func (s *PlaybackService) ResolvePlayback(ctx context.Context, playbackID uuid.UUID) (*repository.PlaybackRecord, error) {
	if playbackID == uuid.Nil {
		return nil, fmt.Errorf("playback_id is required")
	}
	return s.repo.ResolvePlayback(ctx, playbackID)
}

func (s *PlaybackService) RevokePlayback(ctx context.Context, playbackID uuid.UUID) error {
	if playbackID == uuid.Nil {
		return fmt.Errorf("playback_id is required")
	}
	return s.repo.RevokePlayback(ctx, playbackID)
}

// IssueToken mints a verifiable signed playback JWT. The CDN edge verifies it
// against this service's JWKS; it is no longer a placeholder string.
func (s *PlaybackService) IssueToken(ctx context.Context, playbackID uuid.UUID) (string, error) {
	if playbackID == uuid.Nil {
		return "", fmt.Errorf("playback_id is required")
	}
	record, err := s.repo.ResolvePlayback(ctx, playbackID)
	if err != nil {
		return "", err
	}
	if record == nil {
		return "", fmt.Errorf("playback not found")
	}
	if record.Revoked {
		return "", fmt.Errorf("playback revoked")
	}
	return s.signer.IssueToken(ctx, record.PlaybackID.String(), "playback", s.tokenTTL)
}

type PlayerConfig struct {
	Src           string          `json:"src"`
	Poster        string          `json:"poster"`
	StoryboardSrc string          `json:"storyboard_src"`
	Subtitles     []SubtitleTrack `json:"subtitles"`
	AssetID       string          `json:"asset_id"`
	PlaybackID    string          `json:"playback_id"`
}

type SubtitleTrack struct {
	URL   string `json:"url"`
	Lang  string `json:"lang"`
	Label string `json:"label"`
	Auto  bool   `json:"auto"`
}

// GetPlayerConfig returns a complete player configuration for a playback ID.
//
// URL assembly depends on configuration:
//
//   - Default (no CDN_BASE_URL / cdn_url): direct public RustFS URLs. The
//     uploads bucket carries a bucket policy granting anonymous s3:GetObject
//     on assets/* plus a CORS rule for GET/HEAD, so browsers fetch manifests,
//     segments, posters, storyboards, and captions straight from object
//     storage. No proxy hop, no per-URL tokens.
//   - CDN_BASE_URL set (production): URLs point at the CDN edge (e.g. the
//     Cloudflare Worker in services/cdn/) and each URL gets a short-lived
//     signed playback JWT; the edge verifies them against this service's JWKS.
func (s *PlaybackService) GetPlayerConfig(ctx context.Context, playbackID uuid.UUID, cdnURL string, subtitleLangs []string) (*PlayerConfig, error) {
	if playbackID == uuid.Nil {
		return nil, fmt.Errorf("playback_id is required")
	}

	record, err := s.repo.ResolvePlayback(ctx, playbackID)
	if err != nil {
		return nil, err
	}
	if record == nil {
		return nil, fmt.Errorf("playback not found")
	}
	if record.Revoked {
		return nil, fmt.Errorf("playback revoked")
	}

	signTokens := false
	if cdnURL == "" {
		cdnURL = s.cdnURL
	}
	if cdnURL == "" {
		// Direct mode: public bucket policy, no tokens.
		cdnURL = s.rustfsBaseURL
	} else {
		signTokens = true
	}

	base := fmt.Sprintf("%s/%s", strings.TrimRight(cdnURL, "/"), record.ArtifactPrefix)

	var srcToken, storyboardToken string
	if signTokens {
		// Signed-edge mode: every URL class carries its own short-lived JWT.
		srcToken, err = s.signer.IssueToken(ctx, record.PlaybackID.String(), "playback", s.tokenTTL)
		if err != nil {
			return nil, err
		}
		storyboardToken, err = s.signer.IssueToken(ctx, record.PlaybackID.String(), "storyboard", s.tokenTTL)
		if err != nil {
			return nil, err
		}
	}

	config := &PlayerConfig{
		AssetID:       record.AssetID.String(),
		PlaybackID:    record.PlaybackID.String(),
		Src:           withToken(fmt.Sprintf("%s/hls/master.m3u8", base), srcToken),
		Poster:        withToken(fmt.Sprintf("%s/thumbnails/thumbnail.webp", base), srcToken),
		StoryboardSrc: withToken(fmt.Sprintf("%s/storyboard/storyboard.vtt", base), storyboardToken),
		Subtitles:     []SubtitleTrack{},
	}

	for _, lang := range subtitleLangs {
		config.Subtitles = append(config.Subtitles, SubtitleTrack{
			URL:   withToken(fmt.Sprintf("%s/subtitles/%s.vtt", base, lang), srcToken),
			Lang:  lang,
			Label: s.languageLabel(lang),
			Auto:  true,
		})
	}

	return config, nil
}

// withToken appends ?token=… when a token is present (direct-public mode has
// none — the bucket policy makes assets/* readable anonymously).
func withToken(url, token string) string {
	if token == "" {
		return url
	}
	return url + "?token=" + token
}

func (s *PlaybackService) languageLabel(lang string) string {
	labels := map[string]string{
		"en": "English",
		"es": "Spanish",
		"fr": "French",
		"de": "German",
		"ja": "Japanese",
		"zh": "Chinese",
	}
	if label, ok := labels[lang]; ok {
		return label
	}
	return lang
}
