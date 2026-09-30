package events

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// ──────────────────────────────────────────────────────────────────
// MaxDeliver constants
// ──────────────────────────────────────────────────────────────────

// MaxDeliverUnlimited tells JetStream to retry indefinitely.
// Use for orchestrator and terminal consumers whose handlers are DB-gated
// and idempotent: permanently dropping a result event because a Scylla
// outage exhausted the delivery budget is far worse than retrying forever.
const MaxDeliverUnlimited = -1

// workerBackOff is the shared retry schedule for worker consumers.
// WorkerMaxDeliver must be > len(workerBackOff) (4) for every finite entry.
var workerBackOff = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// orchestratorBackOff is the retry schedule for orchestrator / terminal
// consumers. Unlimited MaxDeliver means JetStream never stops, so the
// schedule just controls the spacing between retries.
var orchestratorBackOff = []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute}

// WorkerMaxDeliver is the per-worker-consumer MaxDeliver value.
// Workers import this to decide when to terminate a message instead of
// retrying. Values must be MaxDeliverUnlimited (-1) or > len(workerBackOff).
// Transcode gets extra headroom because SIGTERM-triggered NAKs count as
// deliveries and a pod restart can consume several before a clean encode.
var WorkerMaxDeliver = map[string]int{
	ConsumerProbeWorkers:      5,
	ConsumerTranscodeWorkers:  8,
	ConsumerThumbnailWorkers:  5,
	ConsumerStoryboardWorkers: 5,
	ConsumerSubtitleWorkers:   5,
}

// OrchestratorMaxDeliver is the MaxDeliver for orchestrator and terminal
// consumers. Always -1: a Scylla outage must never exhaust the budget.
var OrchestratorMaxDeliver = map[string]int{
	ConsumerJobOrchestrator:    MaxDeliverUnlimited,
	ConsumerJobProbeResults:    MaxDeliverUnlimited,
	ConsumerJobStepResults:     MaxDeliverUnlimited,
	ConsumerAssetStatusUpdater: MaxDeliverUnlimited,
	ConsumerWebhookDelivery:    MaxDeliverUnlimited,
}

// ──────────────────────────────────────────────────────────────────
// Connection helpers
// ──────────────────────────────────────────────────────────────────

func Connect(ctx context.Context, url string) (jetstream.JetStream, *nats.Conn, error) {
	if err := ctx.Err(); err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}
	nc, err := nats.Connect(url)
	if err != nil {
		return nil, nil, fmt.Errorf("connect to nats: %w", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		nc.Close()
		return nil, nil, fmt.Errorf("create jetstream client: %w", err)
	}
	return js, nc, nil
}

// ──────────────────────────────────────────────────────────────────
// Stream setup
// ──────────────────────────────────────────────────────────────────

func EnsureStreams(ctx context.Context, js jetstream.JetStream) error {
	for _, stream := range streamConfigs() {
		if _, err := js.CreateOrUpdateStream(ctx, stream); err != nil {
			return fmt.Errorf("ensure stream %s: %w", stream.Name, err)
		}
	}
	return nil
}

func streamConfigs() []jetstream.StreamConfig {
	return []jetstream.StreamConfig{
		{
			Name:      StreamAssetEvents,
			Subjects:  []string{SubjectAssetUploadCompleted},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.WorkQueuePolicy,
		},
		{
			Name:      StreamPipelineJobs,
			Subjects:  []string{"jobs.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.WorkQueuePolicy,
		},
		{
			Name:      StreamPipelineEvents,
			Subjects:  []string{"pipeline.run.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.InterestPolicy,
			MaxAge:    7 * 24 * time.Hour,
			MaxBytes:  1 << 30,
		},
		{
			Name:      StreamWebhookDelivery,
			Subjects:  []string{"webhooks.delivery.>"},
			Storage:   jetstream.FileStorage,
			Retention: jetstream.WorkQueuePolicy,
		},
	}
}

// ──────────────────────────────────────────────────────────────────
// Terminal consumers (asset-status-updater, webhook-delivery)
// ──────────────────────────────────────────────────────────────────

func TerminalConsumerConfigs() []jetstream.ConsumerConfig {
	return []jetstream.ConsumerConfig{
		{
			Name:          ConsumerAssetStatusUpdater,
			Durable:       ConsumerAssetStatusUpdater,
			FilterSubject: "pipeline.run.*",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    OrchestratorMaxDeliver[ConsumerAssetStatusUpdater],
			BackOff:       orchestratorBackOff,
		},
		{
			Name:          ConsumerWebhookDelivery,
			Durable:       ConsumerWebhookDelivery,
			FilterSubject: "pipeline.run.*",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    OrchestratorMaxDeliver[ConsumerWebhookDelivery],
			BackOff:       orchestratorBackOff,
		},
	}
}

func EnsureTerminalConsumers(ctx context.Context, js jetstream.JetStream) error {
	return ensureConsumers(ctx, js, TerminalConsumerConfigs(), StreamPipelineEvents)
}

func EnsureWebhookDeliveryConsumer(ctx context.Context, js jetstream.JetStream) error {
	return ensureConsumers(ctx, js, []jetstream.ConsumerConfig{
		{
			Name:           ConsumerWebhookAttempts,
			Durable:        ConsumerWebhookAttempts,
			FilterSubjects: []string{SubjectWebhookDeliveryAttempt, SubjectWebhookDeliveryRetry},
			AckPolicy:      jetstream.AckExplicitPolicy,
			AckWait:        30 * time.Second,
			MaxDeliver:     5,
			BackOff:        workerBackOff,
		},
	}, StreamWebhookDelivery)
}

// ──────────────────────────────────────────────────────────────────
// Job / orchestrator consumers
// ──────────────────────────────────────────────────────────────────

// JobConsumerConfigs returns the consumer configs for the job orchestrator.
// Exported so tests can assert full filter coverage without making live
// JetStream calls (see TestConsumerFilterCoverage).
func JobConsumerConfigs() []jetstream.ConsumerConfig {
	return []jetstream.ConsumerConfig{
		{
			Name:          ConsumerJobOrchestrator,
			Durable:       ConsumerJobOrchestrator,
			FilterSubject: SubjectAssetUploadCompleted,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    OrchestratorMaxDeliver[ConsumerJobOrchestrator],
			BackOff:       orchestratorBackOff,
		},
		// --- PIPELINE_JOBS stream consumers ---
		{
			// Gap 1 fix: include ProbeFailed so the orchestrator drives
			// failed probes to FAILED instead of hanging in RUNNING forever.
			Name:           ConsumerJobProbeResults,
			Durable:        ConsumerJobProbeResults,
			FilterSubjects: []string{SubjectProbeCompleted, SubjectProbeFailed},
			AckPolicy:      jetstream.AckExplicitPolicy,
			AckWait:        30 * time.Second,
			MaxDeliver:     OrchestratorMaxDeliver[ConsumerJobProbeResults],
			BackOff:        orchestratorBackOff,
		},
		{
			// Gap 2 fix: add the three missing *Failed subjects so every
			// required-step failure is delivered to the orchestrator.
			Name:    ConsumerJobStepResults,
			Durable: ConsumerJobStepResults,
			FilterSubjects: []string{
				SubjectTranscodeCompleted,  SubjectTranscodeFailed,
				SubjectThumbnailCompleted,  SubjectThumbnailFailed,
				SubjectStoryboardCompleted, SubjectStoryboardFailed,
				SubjectSubtitleCompleted,   SubjectSubtitleFailed,
			},
			AckPolicy:  jetstream.AckExplicitPolicy,
			AckWait:    30 * time.Second,
			MaxDeliver: OrchestratorMaxDeliver[ConsumerJobStepResults],
			BackOff:    orchestratorBackOff,
		},
	}
}

func EnsureJobConsumers(ctx context.Context, js jetstream.JetStream) error {
	configs := JobConsumerConfigs()

	// ConsumerJobOrchestrator lives on ASSET_EVENTS.
	orchestratorCfg := configs[0]
	if err := ensureConsumers(ctx, js, []jetstream.ConsumerConfig{orchestratorCfg}, StreamAssetEvents); err != nil {
		return err
	}
	// Probe-results and step-results live on PIPELINE_JOBS.
	return ensureConsumers(ctx, js, configs[1:], StreamPipelineJobs)
}

// ──────────────────────────────────────────────────────────────────
// Worker consumers
// ──────────────────────────────────────────────────────────────────

func workerConsumerConfigs() []jetstream.ConsumerConfig {
	type entry struct {
		name    string
		subject string
		ackWait time.Duration
	}
	workers := []entry{
		{ConsumerProbeWorkers, SubjectProbeRequested, 30 * time.Second},
		{ConsumerTranscodeWorkers, SubjectTranscodeRequested, 30 * time.Minute},
		{ConsumerThumbnailWorkers, SubjectThumbnailRequested, 2 * time.Minute},
		{ConsumerStoryboardWorkers, SubjectStoryboardRequested, 2 * time.Minute},
		{ConsumerSubtitleWorkers, SubjectSubtitleRequested, 10 * time.Minute},
	}
	cfgs := make([]jetstream.ConsumerConfig, 0, len(workers))
	for _, w := range workers {
		cfgs = append(cfgs, jetstream.ConsumerConfig{
			Name:          w.name,
			Durable:       w.name,
			FilterSubject: w.subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       w.ackWait,
			MaxDeliver:    WorkerMaxDeliver[w.name],
			BackOff:       workerBackOff,
		})
	}
	return cfgs
}

func EnsureWorkerConsumers(ctx context.Context, js jetstream.JetStream) error {
	return ensureConsumers(ctx, js, workerConsumerConfigs(), StreamPipelineJobs)
}

// ──────────────────────────────────────────────────────────────────
// Internal helper
// ──────────────────────────────────────────────────────────────────

func ensureConsumers(ctx context.Context, js jetstream.JetStream, configs []jetstream.ConsumerConfig, streamName string) error {
	for _, config := range configs {
		if _, err := js.CreateOrUpdateConsumer(ctx, streamName, config); err != nil {
			return fmt.Errorf("ensure consumer %s: %w", config.Durable, err)
		}
	}
	return nil
}
