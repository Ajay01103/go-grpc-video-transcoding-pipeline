package events

const (
	StreamAssetEvents     = "ASSET_EVENTS"
	StreamPipelineJobs    = "PIPELINE_JOBS"
	StreamPipelineEvents  = "PIPELINE_EVENTS"
	StreamWebhookDelivery = "WEBHOOK_DELIVERY"

	SubjectAssetUploadCompleted = "assets.upload.completed"

	SubjectProbeRequested      = "jobs.probe.requested"
	SubjectProbeCompleted      = "jobs.probe.completed"
	SubjectProbeFailed         = "jobs.probe.failed"
	SubjectTranscodeRequested  = "jobs.transcode.requested"
	SubjectTranscodeCompleted  = "jobs.transcode.completed"
	SubjectTranscodeFailed     = "jobs.transcode.failed"
	SubjectThumbnailRequested  = "jobs.thumbnail.requested"
	SubjectThumbnailCompleted  = "jobs.thumbnail.completed"
	SubjectThumbnailFailed     = "jobs.thumbnail.failed"
	SubjectStoryboardRequested = "jobs.storyboard.requested"
	SubjectStoryboardCompleted = "jobs.storyboard.completed"
	SubjectStoryboardFailed    = "jobs.storyboard.failed"
	SubjectSubtitleRequested   = "jobs.subtitle.requested"
	SubjectSubtitleCompleted   = "jobs.subtitle.completed"
	SubjectSubtitleFailed      = "jobs.subtitle.failed"

	SubjectPipelineRunCompleted = "pipeline.run.completed"
	SubjectPipelineRunFailed    = "pipeline.run.failed"

	SubjectWebhookDeliveryAttempt = "webhooks.delivery.attempt"
	SubjectWebhookDeliveryRetry   = "webhooks.delivery.retry"

	ConsumerAssetStatusUpdater = "asset-status-updater"
	ConsumerWebhookDelivery    = "webhook-delivery"
	ConsumerWebhookAttempts    = "webhook-delivery-attempts"
	ConsumerJobOrchestrator    = "job-orchestrator"
	ConsumerJobProbeResults    = "job-probe-results"
	ConsumerJobStepResults     = "job-step-results"
	ConsumerProbeWorkers       = "probe-workers"
	ConsumerTranscodeWorkers   = "transcode-workers"
	ConsumerThumbnailWorkers   = "thumbnail-workers"
	ConsumerStoryboardWorkers  = "storyboard-workers"
	ConsumerSubtitleWorkers    = "subtitle-workers"
)
