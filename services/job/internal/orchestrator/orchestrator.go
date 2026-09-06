package orchestrator

import (
	"context"
	"fmt"
	"path"
	"time"

	"github.com/Ajay01103/go-notion/job/internal/repository"
	"github.com/Ajay01103/go-notion/pkg/events"
	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	playbackpb "github.com/Ajay01103/go-notion/playback/gen/pb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type PlaybackCreator interface {
	CreateWithRetry(context.Context, uuid.UUID, string) (*playbackpb.PlaybackRecord, error)
}

type Orchestrator struct {
	repo      *repository.JobRepo
	publisher *events.Publisher
	playback  PlaybackCreator
}

func New(repo *repository.JobRepo, publisher *events.Publisher) *Orchestrator {
	return &Orchestrator{repo: repo, publisher: publisher}
}

func (o *Orchestrator) SetPlaybackCreator(playback PlaybackCreator) {
	o.playback = playback
}

func (o *Orchestrator) HandleUploadCompleted(ctx context.Context, payload []byte) error {
	event := new(pipelinepb.AssetUploadCompleted)
	if err := proto.Unmarshal(payload, event); err != nil {
		return fmt.Errorf("decode upload event: %w", err)
	}
	assetID, err := uuid.Parse(event.GetAssetId())
	if err != nil {
		return fmt.Errorf("parse asset id: %w", err)
	}
	orgID, err := uuid.Parse(event.GetOrgId())
	if err != nil {
		return fmt.Errorf("parse org id: %w", err)
	}
	run, _, err := o.repo.CreateOrGetRun(ctx, assetID, orgID, event.GetSourceUri(), event.GetRequestedSubtitleLanguages())
	if err != nil {
		return err
	}
	if err := o.repo.InitializeRunSteps(ctx, run.RunID, assetID, event.GetRequestedSubtitleLanguages()); err != nil {
		return err
	}
	request := &pipelinepb.ProbeRequested{
		RunId:          run.RunID.String(),
		AssetId:        assetID.String(),
		SourceUri:      event.GetSourceUri(),
		OrgId:          orgID.String(),
		Attempt:        1,
		IdempotencyKey: events.RequestedMessageID(run.RunID.String(), "probe", 1),
	}
	return o.publisher.Publish(ctx, events.SubjectProbeRequested, request.GetIdempotencyKey(), request)
}

func (o *Orchestrator) HandleProbeCompleted(ctx context.Context, payload []byte) error {
	event := new(pipelinepb.ProbeCompleted)
	if err := proto.Unmarshal(payload, event); err != nil {
		return fmt.Errorf("decode probe completion: %w", err)
	}
	runID, err := uuid.Parse(event.GetRunId())
	if err != nil {
		return fmt.Errorf("parse run id: %w", err)
	}
	assetID, err := uuid.Parse(event.GetAssetId())
	if err != nil {
		return fmt.Errorf("parse asset id: %w", err)
	}
	if err := o.repo.SetStepStatusForRun(ctx, runID, "probe", "DONE"); err != nil {
		return err
	}
	run, err := o.repo.GetRun(ctx, assetID)
	if err != nil || run == nil {
		if err == nil {
			err = fmt.Errorf("pipeline run not found")
		}
		return err
	}
	prefix := OutputPrefix(assetID, runID)
	requests := []proto.Message{
		&pipelinepb.TranscodeRequested{RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(), OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1, IdempotencyKey: events.RequestedMessageID(runID.String(), "transcode", 1)},
		&pipelinepb.ThumbnailRequested{RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(), OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1, IdempotencyKey: events.RequestedMessageID(runID.String(), "thumbnail", 1)},
		&pipelinepb.StoryboardRequested{RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(), OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1, IdempotencyKey: events.RequestedMessageID(runID.String(), "storyboard", 1)},
	}
	for _, language := range run.RequestedSubtitleLanguages {
		requests = append(requests, &pipelinepb.SubtitleRequested{RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(), Language: language, OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1, IdempotencyKey: events.RequestedMessageID(runID.String(), "subtitle:"+language, 1)})
	}
	for _, request := range requests {
		var subject, idempotencyKey string
		switch typed := request.(type) {
		case *pipelinepb.TranscodeRequested:
			subject, idempotencyKey = events.SubjectTranscodeRequested, typed.GetIdempotencyKey()
		case *pipelinepb.ThumbnailRequested:
			subject, idempotencyKey = events.SubjectThumbnailRequested, typed.GetIdempotencyKey()
		case *pipelinepb.StoryboardRequested:
			subject, idempotencyKey = events.SubjectStoryboardRequested, typed.GetIdempotencyKey()
		case *pipelinepb.SubtitleRequested:
			subject, idempotencyKey = events.SubjectSubtitleRequested, typed.GetIdempotencyKey()
		}
		if err := o.publisher.Publish(ctx, subject, idempotencyKey, request); err != nil {
			return err
		}
	}
	return nil
}

func (o *Orchestrator) HandleStepCompleted(ctx context.Context, subject string, payload []byte) error {
	var runID, assetID, step string
	var location string
	switch subject {
	case events.SubjectTranscodeCompleted:
		event := new(pipelinepb.TranscodeCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "transcode", event.GetMasterPlaylistLocation()
	case events.SubjectThumbnailCompleted:
		event := new(pipelinepb.ThumbnailCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "thumbnail", event.GetLocation()
	case events.SubjectStoryboardCompleted:
		event := new(pipelinepb.StoryboardCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "storyboard", event.GetVttLocation()
	case events.SubjectSubtitleCompleted:
		event := new(pipelinepb.SubtitleCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "subtitle:"+event.GetLanguage(), event.GetVttLocation()
	default:
		return fmt.Errorf("unsupported completion subject %s", subject)
	}
	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return err
	}
	assetUUID, err := uuid.Parse(assetID)
	if err != nil {
		return err
	}
	if err := o.repo.SetStepStatusForRun(ctx, runUUID, step, "DONE"); err != nil {
		return err
	}
	return o.tryComplete(ctx, runUUID, assetUUID, map[string]string{step: location})
}

func (o *Orchestrator) HandleStepFailed(ctx context.Context, subject string, payload []byte) error {
	if subject == events.SubjectSubtitleFailed {
		failed := new(pipelinepb.SubtitleFailed)
		if err := proto.Unmarshal(payload, failed); err != nil {
			return err
		}
		runID, err := uuid.Parse(failed.GetRunId())
		if err != nil {
			return err
		}
		return o.repo.SetStepStatusForRun(ctx, runID, "subtitle:"+failed.GetLanguage(), "FAILED")
	} else {
		return nil
	}
}

func (o *Orchestrator) tryComplete(ctx context.Context, runID, assetID uuid.UUID, locations map[string]string) error {
	run, err := o.repo.GetRun(ctx, assetID)
	if err != nil {
		return err
	}
	if run == nil || run.PlaybackID != uuid.Nil || run.Status == "COMPLETED" {
		return nil
	}
	steps, err := o.repo.GetRunSteps(ctx, runID)
	if err != nil {
		return err
	}
	required := map[string]bool{"probe": true, "transcode": true, "thumbnail": true, "storyboard": true}
	for _, step := range steps {
		if required[step.Step] && step.Status != "DONE" {
			return nil
		}
	}
	if o.playback == nil {
		return fmt.Errorf("playback creator is required for completion")
	}
	playback, err := o.playback.CreateWithRetry(ctx, assetID, "signed")
	if err != nil {
		return err
	}
	if playback == nil {
		return fmt.Errorf("playback creator returned no record")
	}
	if err := o.repo.SetRunStatus(ctx, assetID, "COMPLETED"); err != nil {
		return err
	}
	completed := &pipelinepb.PipelineRunCompleted{RunId: runID.String(), AssetId: assetID.String(), OrgId: run.OrgID.String(), PlaybackId: playback.GetPlaybackId(), ArtifactLocations: locations, TimestampUnix: time.Now().UTC().Unix()}
	return o.publisher.Publish(ctx, events.SubjectPipelineRunCompleted, events.TerminalMessageID(runID.String(), "completed"), completed)
}

func OutputPrefix(assetID, runID uuid.UUID) string {
	return path.Join("assets", assetID.String(), "runs", runID.String())
}
