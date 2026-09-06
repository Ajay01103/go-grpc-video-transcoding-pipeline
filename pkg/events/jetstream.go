package events

import (
	"context"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

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

func EnsureTerminalConsumers(ctx context.Context, js jetstream.JetStream) error {
	return ensureConsumers(ctx, js, []jetstream.ConsumerConfig{
		{
			Name:          ConsumerAssetStatusUpdater,
			Durable:       ConsumerAssetStatusUpdater,
			FilterSubject: "pipeline.run.*",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
		{
			Name:          ConsumerWebhookDelivery,
			Durable:       ConsumerWebhookDelivery,
			FilterSubject: "pipeline.run.*",
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
	}, StreamPipelineEvents)
}

func EnsureWebhookDeliveryConsumer(ctx context.Context, js jetstream.JetStream) error {
	return ensureConsumers(ctx, js, []jetstream.ConsumerConfig{
		{
			Name:          ConsumerWebhookAttempts,
			Durable:       ConsumerWebhookAttempts,
			FilterSubjects: []string{SubjectWebhookDeliveryAttempt, SubjectWebhookDeliveryRetry},
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
	}, StreamWebhookDelivery)
}

func EnsureJobConsumers(ctx context.Context, js jetstream.JetStream) error {
	if err := ensureConsumers(ctx, js, []jetstream.ConsumerConfig{
		{
			Name:          ConsumerJobOrchestrator,
			Durable:       ConsumerJobOrchestrator,
			FilterSubject: SubjectAssetUploadCompleted,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
	}, StreamAssetEvents); err != nil {
		return err
	}
	return ensureConsumers(ctx, js, []jetstream.ConsumerConfig{
		{
			Name:          ConsumerJobProbeResults,
			Durable:       ConsumerJobProbeResults,
			FilterSubject: SubjectProbeCompleted,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
		{
			Name:          ConsumerJobStepResults,
			Durable:       ConsumerJobStepResults,
			FilterSubjects: []string{SubjectTranscodeCompleted, SubjectThumbnailCompleted, SubjectStoryboardCompleted, SubjectSubtitleCompleted, SubjectSubtitleFailed},
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       30 * time.Second,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		},
	}, StreamPipelineJobs)
}

func EnsureWorkerConsumers(ctx context.Context, js jetstream.JetStream) error {
	configs := []struct {
		name    string
		subject string
		ackWait time.Duration
	}{
		{name: ConsumerProbeWorkers, subject: SubjectProbeRequested, ackWait: 30 * time.Second},
		{name: ConsumerTranscodeWorkers, subject: SubjectTranscodeRequested, ackWait: 30 * time.Minute},
		{name: ConsumerThumbnailWorkers, subject: SubjectThumbnailRequested, ackWait: 2 * time.Minute},
		{name: ConsumerStoryboardWorkers, subject: SubjectStoryboardRequested, ackWait: 2 * time.Minute},
		{name: ConsumerSubtitleWorkers, subject: SubjectSubtitleRequested, ackWait: 10 * time.Minute},
	}
	consumerConfigs := make([]jetstream.ConsumerConfig, 0, len(configs))
	for _, worker := range configs {
		consumerConfigs = append(consumerConfigs, jetstream.ConsumerConfig{
			Name:          worker.name,
			Durable:       worker.name,
			FilterSubject: worker.subject,
			AckPolicy:     jetstream.AckExplicitPolicy,
			AckWait:       worker.ackWait,
			MaxDeliver:    5,
			BackOff:       []time.Duration{5 * time.Second, 30 * time.Second, 2 * time.Minute, 10 * time.Minute},
		})
	}
	return ensureConsumers(ctx, js, consumerConfigs, StreamPipelineJobs)
}

func ensureConsumers(ctx context.Context, js jetstream.JetStream, configs []jetstream.ConsumerConfig, streamName string) error {
	for _, config := range configs {
		if _, err := js.CreateOrUpdateConsumer(ctx, streamName, config); err != nil {
			return fmt.Errorf("ensure consumer %s: %w", config.Durable, err)
		}
	}
	return nil
}
