package events

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
)

type mockMsgAcker struct {
	metadata *jetstream.MsgMetadata
	metaErr  error
	nakCalls int
	termCalls int
	inProgressCalls int
	ackCalls int
	data []byte
	subject string
	headers nats.Header
}

func (m *mockMsgAcker) Metadata() (*jetstream.MsgMetadata, error) {
	return m.metadata, m.metaErr
}
func (m *mockMsgAcker) Nak() error {
	m.nakCalls++
	return nil
}
func (m *mockMsgAcker) Term() error {
	m.termCalls++
	return nil
}
func (m *mockMsgAcker) InProgress() error {
	m.inProgressCalls++
	return nil
}
func (m *mockMsgAcker) Ack() error {
	m.ackCalls++
	return nil
}
func (m *mockMsgAcker) Data() []byte {
	return m.data
}
func (m *mockMsgAcker) Subject() string {
	return m.subject
}
func (m *mockMsgAcker) Headers() nats.Header {
	return m.headers
}

func TestHandleFinalAttempt_RetryableMetadataOK(t *testing.T) {
	ctx := context.Background()
	msg := &mockMsgAcker{
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 2,
		},
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers, // maxDeliver = 5
		Logger:       zap.NewNop(),
	}
	published := false
	err := HandleFinalAttempt(ctx, msg, cfg, false, func(pubCtx context.Context) error {
		published = true
		return nil
	})
	if errors.Is(err, ErrMessageTerminated) {
		t.Fatalf("expected retryable error, got ErrMessageTerminated")
	}
	if published {
		t.Fatalf("published should not be called for retryable failure")
	}
	if msg.termCalls != 0 {
		t.Fatalf("term should not be called, got %d", msg.termCalls)
	}
}

func TestHandleFinalAttempt_MetadataError(t *testing.T) {
	ctx := context.Background()
	msg := &mockMsgAcker{
		metaErr: errors.New("cannot read metadata"),
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers,
		Logger:       zap.NewNop(),
	}
	published := false
	err := HandleFinalAttempt(ctx, msg, cfg, false, func(pubCtx context.Context) error {
		published = true
		return nil
	})
	if !errors.Is(err, ErrMessageTerminated) {
		t.Fatalf("expected ErrMessageTerminated, got %v", err)
	}
	if !published {
		t.Fatalf("expected publishFailed to be called")
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected Term() to be called once, got %d", msg.termCalls)
	}
}

func TestHandleFinalAttempt_FinalDelivery(t *testing.T) {
	ctx := context.Background()
	msg := &mockMsgAcker{
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 5,
		},
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers, // maxDeliver = 5
		Logger:       zap.NewNop(),
	}
	published := false
	err := HandleFinalAttempt(ctx, msg, cfg, false, func(pubCtx context.Context) error {
		published = true
		return nil
	})
	if !errors.Is(err, ErrMessageTerminated) {
		t.Fatalf("expected ErrMessageTerminated, got %v", err)
	}
	if !published {
		t.Fatalf("expected publishFailed to be called")
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected Term() to be called once, got %d", msg.termCalls)
	}
}

func TestHandleFinalAttempt_PermanentError(t *testing.T) {
	ctx := context.Background()
	msg := &mockMsgAcker{
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 1,
		},
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers,
		Logger:       zap.NewNop(),
	}
	published := false
	err := HandleFinalAttempt(ctx, msg, cfg, true, func(pubCtx context.Context) error {
		published = true
		return nil
	})
	if !errors.Is(err, ErrMessageTerminated) {
		t.Fatalf("expected ErrMessageTerminated, got %v", err)
	}
	if !published {
		t.Fatalf("expected publishFailed to be called")
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected Term() to be called once, got %d", msg.termCalls)
	}
}

func TestHandleFinalAttempt_ShutdownOnFinalDelivery(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	msg := &mockMsgAcker{
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 5,
		},
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers,
		Logger:       zap.NewNop(),
	}
	published := false
	err := HandleFinalAttempt(ctx, msg, cfg, false, func(pubCtx context.Context) error {
		published = true
		return nil
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("expected ctx.Err() (context.Canceled), got %v", err)
	}
	if published {
		t.Fatalf("publishFailed should not be called on shutdown")
	}
	if msg.termCalls != 0 {
		t.Fatalf("Term() should not be called on shutdown, got %d", msg.termCalls)
	}
}

func TestHandleFinalAttempt_PublishFailsAfterRetries(t *testing.T) {
	ctx := context.Background()
	msg := &mockMsgAcker{
		metadata: &jetstream.MsgMetadata{
			NumDelivered: 5,
		},
	}
	cfg := WorkerFailureConfig{
		ConsumerName: ConsumerProbeWorkers,
		Logger:       zap.NewNop(),
	}
	publishCalls := 0
	start := time.Now()
	err := HandleFinalAttempt(ctx, msg, cfg, false, func(pubCtx context.Context) error {
		publishCalls++
		return errors.New("nats down")
	})
	_ = start
	if !errors.Is(err, ErrMessageTerminated) {
		t.Fatalf("expected ErrMessageTerminated even after publish retries fail, got %v", err)
	}
	if publishCalls != 3 {
		t.Fatalf("expected 3 publish attempts, got %d", publishCalls)
	}
	if msg.termCalls != 1 {
		t.Fatalf("expected Term() to be called anyway, got %d", msg.termCalls)
	}
}
