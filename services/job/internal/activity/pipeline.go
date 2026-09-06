package activity

import (
	"context"
	"fmt"

	"github.com/Ajay01103/go-notion/job/internal/repository"
	"github.com/Ajay01103/go-notion/job/internal/worker"
	"github.com/google/uuid"
)

type PipelineActivity struct {
	repo   *repository.JobRepo
	worker *worker.MediaWorker
}

func NewPipelineActivity(repo *repository.JobRepo) *PipelineActivity {
	return &PipelineActivity{
		repo:   repo,
		worker: worker.NewMediaWorker(),
	}
}

type ProbeAssetInput struct {
	AssetID    uuid.UUID
	SourceFile string
}

type ProbeAssetOutput struct {
	Duration   float64
	Width      int
	Height     int
	VideoCodec string
	AudioCodec string
}

func (a *PipelineActivity) ProbeAsset(ctx context.Context, input ProbeAssetInput) (*ProbeAssetOutput, error) {
	if err := a.repo.SetStepStatus(ctx, input.AssetID, "probe", "running"); err != nil {
		return nil, err
	}

	result, err := a.worker.ProbeFile(ctx, input.SourceFile)
	if err != nil {
		if updateErr := a.repo.SetStepStatus(ctx, input.AssetID, "probe", "failed"); updateErr != nil {
			fmt.Printf("failed to update probe step: %v\n", updateErr)
		}
		return nil, fmt.Errorf("probe failed: %w", err)
	}

	if err := a.repo.SetStepStatus(ctx, input.AssetID, "probe", "done"); err != nil {
		return nil, err
	}

	return &ProbeAssetOutput{
		Duration:   result.Duration,
		Width:      result.Width,
		Height:     result.Height,
		VideoCodec: result.VideoCodec,
		AudioCodec: result.AudioCodec,
	}, nil
}

type TranscodeLadderInput struct {
	AssetID   uuid.UUID
	SourceURI string
	OutputDir string
}

func (a *PipelineActivity) TranscodeLadder(ctx context.Context, input TranscodeLadderInput) error {
	if err := a.repo.SetStepStatus(ctx, input.AssetID, "transcode", "running"); err != nil {
		return err
	}

	renditions := []worker.Rendition{
		{Name: "1080p", Width: 1920, Height: 1080, BitRate: "5000k", FrameRate: "30"},
		{Name: "720p", Width: 1280, Height: 720, BitRate: "2800k", FrameRate: "30"},
		{Name: "480p", Width: 854, Height: 480, BitRate: "1400k", FrameRate: "30"},
		{Name: "360p", Width: 640, Height: 360, BitRate: "800k", FrameRate: "30"},
	}

	job := worker.TranscodeJob{
		AssetID:    input.AssetID.String(),
		SourceFile: input.SourceURI,
		OutputDir:  input.OutputDir,
		Renditions: renditions,
	}

	if err := a.worker.TranscodeLadder(ctx, job); err != nil {
		if updateErr := a.repo.SetStepStatus(ctx, input.AssetID, "transcode", "failed"); updateErr != nil {
			fmt.Printf("failed to update transcode step: %v\n", updateErr)
		}
		return fmt.Errorf("transcode ladder failed: %w", err)
	}

	if err := a.repo.SetStepStatus(ctx, input.AssetID, "transcode", "done"); err != nil {
		return err
	}
	return nil
}

type GenerateThumbnailInput struct {
	AssetID   uuid.UUID
	SourceURI string
	OutputDir string
}

func (a *PipelineActivity) GenerateThumbnail(ctx context.Context, input GenerateThumbnailInput) error {
	if err := a.repo.SetStepStatus(ctx, input.AssetID, "thumbnail", "running"); err != nil {
		return err
	}

	outputFile := fmt.Sprintf("%s/thumbnail.webp", input.OutputDir)
	if err := a.worker.GenerateThumbnail(ctx, input.SourceURI, outputFile, "00:00:01"); err != nil {
		if updateErr := a.repo.SetStepStatus(ctx, input.AssetID, "thumbnail", "failed"); updateErr != nil {
			fmt.Printf("failed to update thumbnail step: %v\n", updateErr)
		}
		return fmt.Errorf("generate thumbnail failed: %w", err)
	}

	if err := a.repo.SetStepStatus(ctx, input.AssetID, "thumbnail", "done"); err != nil {
		return err
	}
	return nil
}

type GenerateStoryboardInput struct {
	AssetID   uuid.UUID
	SourceURI string
	OutputDir string
}

func (a *PipelineActivity) GenerateStoryboard(ctx context.Context, input GenerateStoryboardInput) error {
	if err := a.repo.SetStepStatus(ctx, input.AssetID, "storyboard", "running"); err != nil {
		return err
	}

	if err := a.worker.GenerateStoryboard(ctx, input.SourceURI, input.OutputDir, 5, 1200, 680); err != nil {
		if updateErr := a.repo.SetStepStatus(ctx, input.AssetID, "storyboard", "failed"); updateErr != nil {
			fmt.Printf("failed to update storyboard step: %v\n", updateErr)
		}
		return fmt.Errorf("generate storyboard failed: %w", err)
	}

	if err := a.repo.SetStepStatus(ctx, input.AssetID, "storyboard", "done"); err != nil {
		return err
	}
	return nil
}
