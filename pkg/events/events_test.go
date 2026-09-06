package events

import (
	"context"
	"testing"

	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

func TestPipelineEventRoundTrip(t *testing.T) {
	original := &pipelinepb.TranscodeRequested{
		RunId:        "run-1",
		AssetId:      "asset-1",
		SourceUri:    "s3://uploads/asset-1/source.mp4",
		OutputPrefix: "asset-1/run-1",
		Attempt:      2,
	}

	payload, err := proto.Marshal(original)
	if err != nil {
		t.Fatalf("marshal event: %v", err)
	}
	decoded := new(pipelinepb.TranscodeRequested)
	if err := proto.Unmarshal(payload, decoded); err != nil {
		t.Fatalf("unmarshal event: %v", err)
	}
	if !proto.Equal(original, decoded) {
		t.Fatalf("round trip changed event: got %v, want %v", decoded, original)
	}
}

func TestMessageIDs(t *testing.T) {
	if got, want := RequestedMessageID("run-1", "transcode", 3), "run:run-1:step:transcode:attempt:3"; got != want {
		t.Fatalf("requested message ID = %q, want %q", got, want)
	}
	if got, want := UploadMessageID("asset-1"), "asset:asset-1:upload"; got != want {
		t.Fatalf("upload message ID = %q, want %q", got, want)
	}
	if got, want := TerminalMessageID("run-1", "completed"), "run:run-1:completed"; got != want {
		t.Fatalf("terminal message ID = %q, want %q", got, want)
	}
}

func TestDurableConsumerNames(t *testing.T) {
	consumers := []string{
		ConsumerAssetStatusUpdater,
		ConsumerWebhookDelivery,
		ConsumerJobOrchestrator,
		ConsumerProbeWorkers,
		ConsumerTranscodeWorkers,
		ConsumerThumbnailWorkers,
		ConsumerStoryboardWorkers,
		ConsumerSubtitleWorkers,
	}
	seen := make(map[string]struct{}, len(consumers))
	for _, consumer := range consumers {
		if consumer == "" {
			t.Fatal("durable consumer name must not be empty")
		}
		if _, ok := seen[consumer]; ok {
			t.Fatalf("duplicate durable consumer %q", consumer)
		}
		seen[consumer] = struct{}{}
	}
}

func TestStreamPolicies(t *testing.T) {
	configs := streamConfigs()
	if len(configs) != 4 {
		t.Fatalf("stream config count = %d, want 4", len(configs))
	}
	for _, config := range configs {
		switch config.Name {
		case StreamAssetEvents, StreamPipelineJobs, StreamWebhookDelivery:
			if config.Retention != jetstream.WorkQueuePolicy {
				t.Fatalf("stream %s must use WorkQueue retention", config.Name)
			}
		case StreamPipelineEvents:
			if config.Retention != jetstream.InterestPolicy {
				t.Fatalf("stream %s must use Interest retention", config.Name)
			}
			if config.MaxAge <= 0 || config.MaxBytes <= 0 {
				t.Fatalf("stream %s must have retention bounds", config.Name)
			}
		default:
			t.Fatalf("unexpected stream %q", config.Name)
		}
	}
	_ = context.Background()
}

func TestSubjectContract(t *testing.T) {
	subjects := []string{
		SubjectAssetUploadCompleted,
		SubjectProbeRequested, SubjectProbeCompleted, SubjectProbeFailed,
		SubjectTranscodeRequested, SubjectTranscodeCompleted, SubjectTranscodeFailed,
		SubjectThumbnailRequested, SubjectThumbnailCompleted, SubjectThumbnailFailed,
		SubjectStoryboardRequested, SubjectStoryboardCompleted, SubjectStoryboardFailed,
		SubjectSubtitleRequested, SubjectSubtitleCompleted, SubjectSubtitleFailed,
		SubjectPipelineRunCompleted, SubjectPipelineRunFailed,
		SubjectWebhookDeliveryAttempt, SubjectWebhookDeliveryRetry,
	}
	seen := make(map[string]struct{}, len(subjects))
	for _, subject := range subjects {
		if subject == "" {
			t.Fatal("event subject must not be empty")
		}
		if _, ok := seen[subject]; ok {
			t.Fatalf("duplicate event subject %q", subject)
		}
		seen[subject] = struct{}{}
	}
}
