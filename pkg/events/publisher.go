package events

import (
	"context"
	"fmt"
	"strings"

	"github.com/Ajay01103/go-notion/pkg/pipelinepb"
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

func TerminalMessageID(runID, outcome string) string {
	return strings.Join([]string{"run", runID, outcome}, ":")
}

func (p *Publisher) Publish(ctx context.Context, subject, idempotencyKey string, event proto.Message) error {
	return p.publish(ctx, subject, event, idempotencyKey)
}

func (p *Publisher) PublishBytes(ctx context.Context, subject, idempotencyKey string, payload []byte) error {
	if len(payload) == 0 {
		return fmt.Errorf("payload for %s is required", subject)
	}
	message := &nats.Msg{Subject: subject, Data: payload}
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
	message := &nats.Msg{Subject: subject, Data: payload}
	if idempotencyKey != "" {
		message.Header.Set(nats.MsgIdHdr, idempotencyKey)
	}
	if _, err := p.js.PublishMsg(ctx, message); err != nil {
		return fmt.Errorf("publish %s event: %w", subject, err)
	}
	return nil
}
