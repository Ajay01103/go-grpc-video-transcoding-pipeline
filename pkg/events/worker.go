package events

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
	"go.uber.org/zap"
	"google.golang.org/protobuf/proto"
)

// ──────────────────────────────────────────────────────────────────
// MsgAcker — thin interface over jetstream.Msg
// ──────────────────────────────────────────────────────────────────

// MsgAcker is a minimal interface over jetstream.Msg that lets workers be
// tested without a real NATS connection.  jetstream.Msg satisfies this
// interface directly; tests inject a mock.
type MsgAcker interface {
	Metadata() (*jetstream.MsgMetadata, error)
	Nak() error
	Term() error
	InProgress() error
	Ack() error
	Data() []byte
	Subject() string
	Headers() nats.Header
}

// ──────────────────────────────────────────────────────────────────
// ErrMessageTerminated
// ──────────────────────────────────────────────────────────────────

// ErrMessageTerminated is returned by HandleFinalAttempt when the message has
// been Term()-ed.  Worker loops must not call Nak() after receiving this error.
var ErrMessageTerminated = errors.New("message terminated")

// ──────────────────────────────────────────────────────────────────
// WorkerFailureConfig
// ──────────────────────────────────────────────────────────────────

// WorkerFailureConfig carries per-worker parameters used by HandleFinalAttempt.
type WorkerFailureConfig struct {
	// ConsumerName is used to look up the MaxDeliver threshold from
	// WorkerMaxDeliver.  Must be one of the ConsumerXxx constants.
	ConsumerName string
	Logger       *zap.Logger
}

// ──────────────────────────────────────────────────────────────────
// HandleFinalAttempt
// ──────────────────────────────────────────────────────────────────

// HandleFinalAttempt decides whether a worker failure is retryable or terminal.
//
// Decision logic:
//
//	ctx cancelled       → shutdown path: return ctx.Err() so the caller Nak()-s
//	                       without entering the terminal path.  The sweeper will
//	                       recover any run left hanging by a shutdown on the last
//	                       delivery.
//	isPermanent=true    → terminal immediately (corrupt source, etc.)
//	metadata unreadable → treated as the final attempt (can't know count)
//	NumDelivered >= max → terminal
//	otherwise           → retryable: return the original err, caller Nak()-s
//
// Terminal path:
//  1. Call publishFailed under context.WithoutCancel + 10 s timeout so a
//     cancelled process context does not kill the publish.
//  2. Retry the publish up to 3 times with 500 ms / 1 s / 2 s back-off.
//  3. If the publish still fails, log loudly and Term() the message anyway
//     (leaving it in the stream would block all subsequent messages on a
//     WorkQueue stream).  The sweeper will detect and fail the run.
//  4. If the publish succeeds, Term() the message.
//  5. Return ErrMessageTerminated in both cases so the caller skips Nak().
func HandleFinalAttempt(
	ctx context.Context,
	msg MsgAcker,
	cfg WorkerFailureConfig,
	isPermanent bool,
	publishFailed func(pubCtx context.Context) error,
) error {
	// Shutdown guard: ctx is already done, just NAK and return.
	if ctx.Err() != nil {
		return ctx.Err()
	}

	metadata, metadataErr := msg.Metadata()
	maxDeliver := WorkerMaxDeliver[cfg.ConsumerName]
	isFinalAttempt := isPermanent ||
		metadataErr != nil ||
		metadata.NumDelivered >= uint64(maxDeliver)

	if !isFinalAttempt {
		// Retryable: caller wraps this with the original error and Nak()-s.
		return fmt.Errorf("retryable: not yet at max deliveries")
	}

	// Publish under a fresh context so a cancelled worker context does not
	// kill the publish mid-flight.
	pubCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
	defer cancel()

	publishErr := publishWithRetry(pubCtx, publishFailed, 3,
		[]time.Duration{500 * time.Millisecond, time.Second, 2 * time.Second})

	if publishErr != nil {
		// Publish failed after retries.  Log loudly and Term() anyway so the
		// message does not block the WorkQueue.  The sweeper will fail the run.
		if cfg.Logger != nil {
			cfg.Logger.Error("worker: failed to publish *Failed after retries; terminating message anyway",
				zap.String("consumer", cfg.ConsumerName),
				zap.Error(publishErr))
		}
		_ = msg.Term()
		return ErrMessageTerminated
	}

	if err := msg.Term(); err != nil {
		return err
	}
	return ErrMessageTerminated
}

// ──────────────────────────────────────────────────────────────────
// publishWithRetry
// ──────────────────────────────────────────────────────────────────

// publishWithRetry calls fn up to maxAttempts times, waiting delays[i-1]
// between attempt i-1 and attempt i.  delays must have len == maxAttempts-1.
func publishWithRetry(ctx context.Context, fn func(context.Context) error, maxAttempts int, delays []time.Duration) error {
	var lastErr error
	for i := 0; i < maxAttempts; i++ {
		if i > 0 {
			select {
			case <-time.After(delays[i-1]):
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if lastErr = fn(ctx); lastErr == nil {
			return nil
		}
	}
	return lastErr
}

// ──────────────────────────────────────────────────────────────────
// PublishProto — convenience wrapper used by workers
// ──────────────────────────────────────────────────────────────────

// PublishProto marshals msg and publishes it to subject with the given
// idempotency key.  It is a thin wrapper so workers do not need to import
// google.golang.org/protobuf/proto directly.
func PublishProto(ctx context.Context, pub *Publisher, subject, msgID string, msg proto.Message) error {
	return pub.Publish(ctx, subject, msgID, msg)
}
