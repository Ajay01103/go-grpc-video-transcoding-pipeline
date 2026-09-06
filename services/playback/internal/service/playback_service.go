package service

import (
	"context"
	"fmt"
	"time"

	"github.com/Ajay01103/go-notion/playback/internal/repository"
	"github.com/google/uuid"
)

type PlaybackService struct {
	repo *repository.PlaybackRepo
	tokenTTL time.Duration
}

func NewPlaybackService(repo *repository.PlaybackRepo, tokenTTL time.Duration) *PlaybackService {
	if tokenTTL <= 0 {
		tokenTTL = 5 * time.Minute
	}
	return &PlaybackService{repo: repo, tokenTTL: tokenTTL}
}

func (s *PlaybackService) CreatePlayback(ctx context.Context, assetID uuid.UUID, policy string) (*repository.PlaybackRecord, error) {
	if assetID == uuid.Nil {
		return nil, fmt.Errorf("asset_id is required")
	}
	if policy == "" {
		policy = "public"
	}
	return s.repo.CreatePlayback(ctx, assetID, policy)
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
	return fmt.Sprintf("playback-token-%s-%d", record.PlaybackID.String(), time.Now().Add(s.tokenTTL).Unix()), nil
}

type PlayerConfig struct {
	Src           string        `json:"src"`
	Poster        string        `json:"poster"`
	StoryboardSrc string        `json:"storyboard_src"`
	Subtitles     []SubtitleTrack `json:"subtitles"`
	AssetID       string        `json:"asset_id"`
	PlaybackID    string        `json:"playback_id"`
}

type SubtitleTrack struct {
	URL   string `json:"url"`
	Lang  string `json:"lang"`
	Label string `json:"label"`
	Auto  bool   `json:"auto"`
}

// GetPlayerConfig returns a complete player configuration for a playback ID
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

	if cdnURL == "" {
		cdnURL = "https://cdn.you.com"
	}

	token, err := s.IssueToken(ctx, playbackID)
	if err != nil {
		token = ""
	}

	config := &PlayerConfig{
		AssetID:       record.AssetID.String(),
		PlaybackID:    record.PlaybackID.String(),
		Src:           fmt.Sprintf("%s/%s/hls/master.m3u8?token=%s", cdnURL, playbackID.String(), token),
		Poster:        fmt.Sprintf("%s/%s/thumbnails/thumbnail.webp?token=%s", cdnURL, playbackID.String(), token),
		StoryboardSrc: fmt.Sprintf("%s/%s/storyboard/storyboard.vtt?token=%s", cdnURL, playbackID.String(), token),
		Subtitles:     []SubtitleTrack{},
	}

	for _, lang := range subtitleLangs {
		config.Subtitles = append(config.Subtitles, SubtitleTrack{
			URL:   fmt.Sprintf("%s/%s/subtitles/%s.vtt?token=%s", cdnURL, playbackID.String(), lang, token),
			Lang:  lang,
			Label: s.languageLabel(lang),
			Auto:  false,
		})
	}

	return config, nil
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
