package events

import (
	"context"
	"fmt"

	"github.com/Ajay01103/go-mux/pkg/pipelinepb"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"google.golang.org/protobuf/proto"
)

type Publisher struct {
	js jetstream.JetStream
}

func NewPublisher(js jetstream.JetStream) *Publisher {
	return &Publisher{js: js}
}

func (p *Publisher) PublishUploadCompleted(ctx context.Context, event *pipelinepb.AssetUploadCompleted) error {
	if event == nil {
		return fmt.Errorf("upload completed event is required")
	}
	return p.publish(ctx, SubjectAssetUploadCompleted, event, event.GetIdempotencyKey())
}

func RequestedMessageID(runID, step string, attempt int32) string {
	return fmt.Sprintf("run:%s:step:%s:attempt:%d", runID, step, attempt)
}

func UploadMessageID(assetID string) string {
	return fmt.Sprintf("asset:%s:upload", assetID)
}

// TerminalMessageID returns the NATS dedup key for a run-level terminal event
// (pipeline.run.completed / pipeline.run.failed).  generation is the
// run_generation value at the time of the transition; each RetryPipeline call
// increments it so that a second failure on a retried run produces a distinct
// ID and is not silently dropped by JetStream's dedup window.
func TerminalMessageID(runID, outcome string, generation int) string {
	return fmt.Sprintf("run:%s:%s:gen:%d", runID, outcome, generation)
}

// StepTerminalMessageID returns the NATS dedup key for a step-level terminal
// event (*Completed / *Failed published by workers).  attempt is the proto
// Attempt field echoed from the *Requested message, so a retry produces a
// distinct key that is not collapsed with the first attempt.
func StepTerminalMessageID(runID, outcome string, attempt int32) string {
	return fmt.Sprintf("run:%s:%s:attempt:%d", runID, outcome, attempt)
}

func (p *Publisher) Publish(ctx context.Context, subject, idempotencyKey string, event proto.Message) error {
	return p.publish(ctx, subject, event, idempotencyKey)
}

func (p *Publisher) PublishBytes(ctx context.Context, subject, idempotencyKey string, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("payload for %s is required", subject)
	}
	message := nats.NewMsg(subject)
	message.Data = payload
	if idempotencyKey != "" {
		message.Header.Set(nats.MsgIdHdr, idempotencyKey)
	}
	if _, err := p.js.PublishMsg(ctx, message); err != nil {
		return fmt.Errorf("publish %s payload: %w", subject, err)
	}
	return nil
}

func (p *Publisher) publish(ctx context.Context, subject string, event proto.Message, idempotencyKey string) error {
	if event == nil {
		return fmt.Errorf("event for %s is required", subject)
	}
	payload, err := proto.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal %s event: %w", subject, err)
	}
	message := nats.NewMsg(subject)
	message.Data = payload
	if idempotencyKey != "" {
		message.Header.Set(nats.MsgIdHdr, idempotencyKey)
	}
	if _, err := p.js.PublishMsg(ctx, message); err != nil {
		return fmt.Errorf("publish %s event: %w", subject, err)
	}
	return nil
}
