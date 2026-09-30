package orchestrator

import (
	"context"
	"fmt"
	"path"
	"strings"
	"time"

	"github.com/Ajay01103/go-mux/job/internal/repository"
	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	playbackpb "github.com/Ajay01103/go-mux/playback/gen/pb"
	"github.com/google/uuid"
	"google.golang.org/protobuf/proto"
)

type PlaybackCreator interface {
	CreateWithRetry(context.Context, uuid.UUID, string, string) (*playbackpb.PlaybackRecord, error)
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

// ──────────────────────────────────────────────────────────────────
// Upload → probe dispatch
// ──────────────────────────────────────────────────────────────────

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
	run, created, err := o.repo.CreateOrGetRun(ctx, assetID, orgID, event.GetSourceUri(), event.GetRequestedSubtitleLanguages())
	if err != nil {
		return err
	}
	if !created {
		// Idempotency: never create a second run for an asset that already has
		// one in a non-terminal state.
		switch run.Status {
		case "COMPLETED", "FAILED", "CANCELLED":
			return nil
		}
	}
	if err := o.repo.InitializeRunSteps(ctx, run.RunID, assetID, event.GetRequestedSubtitleLanguages()); err != nil {
		return err
	}
	if err := o.repo.SetRunStatus(ctx, assetID, "RUNNING"); err != nil {
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

// ──────────────────────────────────────────────────────────────────
// Probe → post-probe step fan-out
// ──────────────────────────────────────────────────────────────────

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
	run, err := o.repo.GetRunByRunID(ctx, runID)
	if err != nil {
		return err
	}
	if run == nil {
		return fmt.Errorf("pipeline run %s not found", runID)
	}
	if run.Status == "CANCELLED" || run.Status == "COMPLETED" {
		return nil
	}

	// Attempt-aware guard: ignore if a newer probe attempt has already been recorded.
	steps, err := o.repo.GetRunSteps(ctx, runID)
	if err != nil {
		return err
	}
	incomingAttempt := event.GetAttempt()
	for _, s := range steps {
		if s.Step == "probe" {
			if s.Status == "DONE" {
				return nil // already succeeded
			}
			if int32(s.Attempt) > incomingAttempt {
				return nil // stale delivery
			}
			break
		}
	}

	if err := o.repo.SetStepStatusForRun(ctx, runID, assetID, "probe", "DONE", int(incomingAttempt)); err != nil {
		return err
	}

	// Persist probe duration so the sweeper can scale transcode thresholds.
	probeDuration := 0.0
	if event.GetResult() != nil {
		probeDuration = event.GetResult().GetDuration()
	}
	if probeDuration > 0 {
		if err := o.repo.SetRunProbeDuration(ctx, assetID, probeDuration); err != nil {
			return err
		}
	}

	prefix := OutputPrefix(assetID, runID)
	requests := []proto.Message{
		&pipelinepb.TranscodeRequested{
			RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(),
			OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1,
			IdempotencyKey: events.RequestedMessageID(runID.String(), "transcode", 1),
		},
		&pipelinepb.ThumbnailRequested{
			RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(),
			OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1,
			IdempotencyKey: events.RequestedMessageID(runID.String(), "thumbnail", 1),
		},
		&pipelinepb.StoryboardRequested{
			RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(),
			OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1,
			DurationSeconds: probeDuration,
			IdempotencyKey:  events.RequestedMessageID(runID.String(), "storyboard", 1),
		},
	}
	for _, language := range run.RequestedSubtitleLanguages {
		requests = append(requests, &pipelinepb.SubtitleRequested{
			RunId: runID.String(), AssetId: assetID.String(), SourceUri: event.GetSourceUri(),
			Language: language, OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: 1,
			IdempotencyKey: events.RequestedMessageID(runID.String(), "subtitle:"+language, 1),
		})
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

// ──────────────────────────────────────────────────────────────────
// Step completion
// ──────────────────────────────────────────────────────────────────

func (o *Orchestrator) HandleStepCompleted(ctx context.Context, subject string, payload []byte) error {
	var runID, assetID, step, location string
	var incomingAttempt int32
	switch subject {
	case events.SubjectTranscodeCompleted:
		event := new(pipelinepb.TranscodeCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "transcode", event.GetMasterPlaylistLocation()
		incomingAttempt = event.GetAttempt()
	case events.SubjectThumbnailCompleted:
		event := new(pipelinepb.ThumbnailCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "thumbnail", event.GetLocation()
		incomingAttempt = event.GetAttempt()
	case events.SubjectStoryboardCompleted:
		event := new(pipelinepb.StoryboardCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "storyboard", event.GetVttLocation()
		incomingAttempt = event.GetAttempt()
	case events.SubjectSubtitleCompleted:
		event := new(pipelinepb.SubtitleCompleted)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, step, location = event.GetRunId(), event.GetAssetId(), "subtitle:"+event.GetLanguage(), event.GetVttLocation()
		incomingAttempt = event.GetAttempt()
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
	run, err := o.repo.GetRunByRunID(ctx, runUUID)
	if err != nil {
		return err
	}
	if run == nil || run.Status == "CANCELLED" || run.Status == "COMPLETED" {
		return nil
	}

	// Attempt-aware guard: ignore completions from stale attempts.
	steps, err := o.repo.GetRunSteps(ctx, runUUID)
	if err != nil {
		return err
	}
	for _, s := range steps {
		if s.Step == step {
			if s.Status == "DONE" {
				return nil // already recorded on a parallel path
			}
			if int32(s.Attempt) > incomingAttempt {
				return nil // stale delivery from a prior attempt
			}
			break
		}
	}

	if err := o.repo.SetStepStatusForRun(ctx, runUUID, assetUUID, step, "DONE", int(incomingAttempt)); err != nil {
		return err
	}
	if location != "" {
		if err := o.repo.RecordArtifact(ctx, runUUID, step, location); err != nil {
			return err
		}
	}
	return o.tryComplete(ctx, runUUID, assetUUID)
}

// ──────────────────────────────────────────────────────────────────
// Step failure
// ──────────────────────────────────────────────────────────────────

// HandleStepFailed processes a *Failed event from any worker (or a synthetic
// one from the sweeper).  It is idempotent and attempt-aware:
//
//   - CANCELLED / COMPLETED run → no-op
//   - FAILED run               → re-publish pipeline.run.failed (crash-recovery)
//   - step already DONE        → no-op (concurrent success)
//   - incomingAttempt < stored → no-op (stale delivery from prior attempt)
//   - subtitle:* subject       → step FAILED, run stays RUNNING, tryComplete
//   - required-step failure    → step FAILED, run FAILED, pipeline.run.failed
func (o *Orchestrator) HandleStepFailed(ctx context.Context, subject string, payload []byte) error {
	var runID, assetID, orgID, step, errorCode, errorMessage string
	var incomingAttempt int32

	switch subject {
	case events.SubjectProbeFailed:
		event := new(pipelinepb.ProbeFailed)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, orgID, step, errorCode, errorMessage, incomingAttempt =
			event.GetRunId(), event.GetAssetId(), event.GetOrgId(), "probe",
			event.GetErrorCode(), event.GetErrorMessage(), event.GetAttempt()
	case events.SubjectTranscodeFailed:
		event := new(pipelinepb.TranscodeFailed)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, orgID, step, errorCode, errorMessage, incomingAttempt =
			event.GetRunId(), event.GetAssetId(), event.GetOrgId(), "transcode",
			event.GetErrorCode(), event.GetErrorMessage(), event.GetAttempt()
	case events.SubjectThumbnailFailed:
		event := new(pipelinepb.ThumbnailFailed)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, orgID, step, errorCode, errorMessage, incomingAttempt =
			event.GetRunId(), event.GetAssetId(), event.GetOrgId(), "thumbnail",
			event.GetErrorCode(), event.GetErrorMessage(), event.GetAttempt()
	case events.SubjectStoryboardFailed:
		event := new(pipelinepb.StoryboardFailed)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, orgID, step, errorCode, errorMessage, incomingAttempt =
			event.GetRunId(), event.GetAssetId(), event.GetOrgId(), "storyboard",
			event.GetErrorCode(), event.GetErrorMessage(), event.GetAttempt()
	case events.SubjectSubtitleFailed:
		event := new(pipelinepb.SubtitleFailed)
		if err := proto.Unmarshal(payload, event); err != nil {
			return err
		}
		runID, assetID, orgID, step, errorCode, errorMessage, incomingAttempt =
			event.GetRunId(), event.GetAssetId(), event.GetOrgId(), "subtitle:"+event.GetLanguage(),
			event.GetErrorCode(), event.GetErrorMessage(), event.GetAttempt()
	default:
		return fmt.Errorf("unsupported failure subject %s", subject)
	}

	_ = orgID // used below when publishing pipeline.run.failed

	runUUID, err := uuid.Parse(runID)
	if err != nil {
		return err
	}
	assetUUID, err := uuid.Parse(assetID)
	if err != nil {
		return err
	}
	run, err := o.repo.GetRunByRunID(ctx, runUUID)
	if err != nil {
		return err
	}
	if run == nil {
		return nil // run not found, nothing to do
	}

	// ── Guard 1: terminal states ──────────────────────────────────
	if run.Status == "CANCELLED" || run.Status == "COMPLETED" {
		return nil
	}
	if run.Status == "FAILED" {
		// Crash-recovery path: the DB write succeeded but the NATS publish
		// crashed.  Re-publish using the persisted error fields.
		return o.republishRunFailed(ctx, run)
	}

	// ── Guard 2: step-level attempt ───────────────────────────────
	steps, err := o.repo.GetRunSteps(ctx, runUUID)
	if err != nil {
		return err
	}
	for _, s := range steps {
		if s.Step == step {
			if s.Status == "DONE" {
				return nil // step already succeeded on a concurrent path
			}
			if int32(s.Attempt) > incomingAttempt {
				return nil // stale delivery from a prior attempt
			}
			break
		}
	}

	// ── Record the step failure ───────────────────────────────────
	if err := o.repo.SetStepStatusForRun(ctx, runUUID, assetUUID, step, "FAILED", int(incomingAttempt)); err != nil {
		return err
	}

	// ── Subtitle best-effort: keyed on step name prefix, not subject ──
	// This means synthetic failures from the sweeper also take this branch.
	if strings.HasPrefix(step, "subtitle:") {
		// Subtitles are best-effort: mark the step failed but do not fail
		// the run.  Trigger tryComplete so a completed run is not blocked
		// waiting for a subtitle step that will never succeed.
		return o.tryComplete(ctx, runUUID, assetUUID)
	}

	// ── Required-step failure: fail the run ───────────────────────
	if err := o.repo.SetRunFailedWithError(ctx, assetUUID, errorCode, errorMessage); err != nil {
		return err
	}

	failed := &pipelinepb.PipelineRunFailed{
		RunId:         runID,
		AssetId:       assetID,
		OrgId:         orgID,
		ErrorCode:     errorCode,
		ErrorMessage:  errorMessage,
		TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.TerminalMessageID(runID, "failed", run.RunGeneration)
	return o.publisher.Publish(ctx, events.SubjectPipelineRunFailed, msgID, failed)
}

// republishRunFailed reconstructs and re-publishes pipeline.run.failed from
// the error fields persisted atomically with the FAILED status.  This covers
// the crash-between-DB-write-and-NATS-publish window.
// It uses the same TerminalMessageID so JetStream dedup absorbs it if the
// process crashed and recovered quickly (within the ~2-min dedup window).
// If recovery was slower, this produces a genuine new message — which is
// correct: Asset and Webhook need to see it.
func (o *Orchestrator) republishRunFailed(ctx context.Context, run *repository.PipelineRun) error {
	failed := &pipelinepb.PipelineRunFailed{
		RunId:         run.RunID.String(),
		AssetId:       run.AssetID.String(),
		OrgId:         run.OrgID.String(),
		ErrorCode:     run.FailedErrorCode,
		ErrorMessage:  run.FailedErrorMessage,
		TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.TerminalMessageID(run.RunID.String(), "failed", run.RunGeneration)
	return o.publisher.Publish(ctx, events.SubjectPipelineRunFailed, msgID, failed)
}

// ──────────────────────────────────────────────────────────────────
// Completion join
// ──────────────────────────────────────────────────────────────────

func (o *Orchestrator) tryComplete(ctx context.Context, runID, assetID uuid.UUID) error {
	run, err := o.repo.GetRun(ctx, assetID)
	if err != nil {
		return err
	}
	if run == nil || run.PlaybackID != uuid.Nil || run.Status == "COMPLETED" || run.Status == "CANCELLED" {
		return nil
	}
	steps, err := o.repo.GetRunSteps(ctx, runID)
	if err != nil {
		return err
	}
	// Subtitle steps are best-effort: they are NOT in the required set and
	// never block completion regardless of their status.
	required := map[string]bool{"probe": true, "transcode": true, "thumbnail": true, "storyboard": true}
	for _, step := range steps {
		if required[step.Step] && step.Status != "DONE" {
			return nil // a required step is not yet done
		}
	}
	artifactLocations, err := o.repo.GetArtifacts(ctx, runID)
	if err != nil {
		return err
	}
	if o.playback == nil {
		return fmt.Errorf("playback creator is required for completion")
	}
	artifactPrefix := OutputPrefix(assetID, runID)
	playback, err := o.playback.CreateWithRetry(ctx, assetID, "signed", artifactPrefix)
	if err != nil {
		return err
	}
	if playback == nil || playback.GetPlaybackId() == "" {
		return fmt.Errorf("playback creator returned no record")
	}
	if err := o.repo.SetRunPlaybackID(ctx, assetID, uuid.MustParse(playback.GetPlaybackId())); err != nil {
		return err
	}
	if err := o.repo.SetRunStatus(ctx, assetID, "COMPLETED"); err != nil {
		return err
	}
	completed := &pipelinepb.PipelineRunCompleted{
		RunId: runID.String(), AssetId: assetID.String(), OrgId: run.OrgID.String(),
		PlaybackId: playback.GetPlaybackId(), ArtifactLocations: artifactLocations,
		TimestampUnix: time.Now().UTC().Unix(),
	}
	msgID := events.TerminalMessageID(runID.String(), "completed", run.RunGeneration)
	return o.publisher.Publish(ctx, events.SubjectPipelineRunCompleted, msgID, completed)
}

// ──────────────────────────────────────────────────────────────────
// Cancel / Retry
// ──────────────────────────────────────────────────────────────────

// CancelPipeline marks a run CANCELLED.  New requested events stop being
// published because handlers no-op on CANCELLED runs.
func (o *Orchestrator) CancelPipeline(ctx context.Context, runID uuid.UUID) error {
	ok, err := o.repo.SetRunStatusByRunID(ctx, runID, "CANCELLED")
	if err != nil {
		return err
	}
	if !ok {
		return fmt.Errorf("pipeline run %s not found", runID)
	}
	return nil
}

// RetryPipeline re-publishes *Requested events for every step currently in
// FAILED, with an incremented attempt number and an incremented run_generation
// so that subsequent terminal events have distinct NATS dedup keys.
// Done steps are never redone.
func (o *Orchestrator) RetryPipeline(ctx context.Context, runID uuid.UUID) error {
	run, err := o.repo.GetRunByRunID(ctx, runID)
	if err != nil {
		return err
	}
	if run == nil {
		return fmt.Errorf("pipeline run %s not found", runID)
	}
	steps, err := o.repo.GetRunSteps(ctx, runID)
	if err != nil {
		return err
	}

	// Increment the run generation BEFORE dispatching so that terminal events
	// on this retry cycle carry a generation that differs from the previous one.
	newGen, err := o.repo.IncrementRunGeneration(ctx, run.AssetID)
	if err != nil {
		return err
	}
	_ = newGen // generation is now stored; workers read it via GetRun

	if err := o.repo.SetRunStatus(ctx, run.AssetID, "RUNNING"); err != nil {
		return err
	}

	prefix := OutputPrefix(run.AssetID, runID)
	published := 0
	for _, step := range steps {
		if step.Status != "FAILED" {
			continue
		}
		nextAttempt := int32(step.Attempt + 1)
		name := step.Step
		language := ""
		if lang, found := strings.CutPrefix(name, "subtitle:"); found {
			language = lang
		}

		// Reset the step to PENDING with the new attempt number so that
		// stale-attempt guards in HandleStepFailed / HandleStepCompleted
		// will let the new delivery through.
		if err := o.repo.SetStepStatusForRun(ctx, runID, run.AssetID, name, "PENDING", int(nextAttempt)); err != nil {
			return err
		}

		var subject string
		var request proto.Message
		switch {
		case language != "":
			request = &pipelinepb.SubtitleRequested{
				RunId: runID.String(), AssetId: run.AssetID.String(), SourceUri: run.SourceURI,
				Language: language, OutputPrefix: prefix, OrgId: run.OrgID.String(),
				Attempt: nextAttempt, IdempotencyKey: events.RequestedMessageID(runID.String(), name, nextAttempt),
			}
			subject = events.SubjectSubtitleRequested
		case name == "probe":
			request = &pipelinepb.ProbeRequested{
				RunId: runID.String(), AssetId: run.AssetID.String(), SourceUri: run.SourceURI,
				OrgId: run.OrgID.String(), Attempt: nextAttempt,
				IdempotencyKey: events.RequestedMessageID(runID.String(), name, nextAttempt),
			}
			subject = events.SubjectProbeRequested
		case name == "transcode":
			request = &pipelinepb.TranscodeRequested{
				RunId: runID.String(), AssetId: run.AssetID.String(), SourceUri: run.SourceURI,
				OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: nextAttempt,
				IdempotencyKey: events.RequestedMessageID(runID.String(), name, nextAttempt),
			}
			subject = events.SubjectTranscodeRequested
		case name == "thumbnail":
			request = &pipelinepb.ThumbnailRequested{
				RunId: runID.String(), AssetId: run.AssetID.String(), SourceUri: run.SourceURI,
				OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: nextAttempt,
				IdempotencyKey: events.RequestedMessageID(runID.String(), name, nextAttempt),
			}
			subject = events.SubjectThumbnailRequested
		case name == "storyboard":
			request = &pipelinepb.StoryboardRequested{
				RunId: runID.String(), AssetId: run.AssetID.String(), SourceUri: run.SourceURI,
				OutputPrefix: prefix, OrgId: run.OrgID.String(), Attempt: nextAttempt,
				IdempotencyKey: events.RequestedMessageID(runID.String(), name, nextAttempt),
			}
			subject = events.SubjectStoryboardRequested
		default:
			continue
		}
		if err := o.publisher.Publish(ctx, subject,
			request.(interface{ GetIdempotencyKey() string }).GetIdempotencyKey(), request); err != nil {
			return err
		}
		published++
	}
	if published == 0 {
		return fmt.Errorf("no failed steps to retry for run %s", runID)
	}
	return nil
}

// ──────────────────────────────────────────────────────────────────
// Helpers
// ──────────────────────────────────────────────────────────────────

func OutputPrefix(assetID, runID uuid.UUID) string {
	return path.Join("assets", assetID.String(), "runs", runID.String())
}
