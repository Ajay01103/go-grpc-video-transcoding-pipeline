package publisher

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"time"

	"github.com/Ajay01103/go-mux/pkg/events"
	"github.com/Ajay01103/go-mux/webhook/internal/repository"
	"github.com/google/uuid"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

type WebhookPublisher struct {
	repo   *repository.WebhookRepo
	client *http.Client
	events *events.Publisher
}

type WebhookEvent struct {
	Type      string      `json:"type"`
	Timestamp time.Time   `json:"timestamp"`
	Data      interface{} `json:"data"`
}

func NewWebhookPublisher(repo *repository.WebhookRepo, eventPublisher *events.Publisher) *WebhookPublisher {
	return &WebhookPublisher{
		repo:   repo,
		events: eventPublisher,
		client: &http.Client{
			Timeout: 10 * time.Second,
		},
	}
}

type DeliveryAttempt struct {
	DeliveryID string `json:"delivery_id"`
	EndpointID string `json:"endpoint_id"`
	EventType  string `json:"event_type"`
	Payload    string `json:"payload"`
	Attempt    int    `json:"attempt"`
}

func (wp *WebhookPublisher) IngestPipelineEvent(ctx context.Context, orgID uuid.UUID, eventType string, message proto.Message, eventID string) error {
	if wp.events == nil {
		return fmt.Errorf("event publisher is required")
	}
	if eventID != "" {
		applied, err := wp.repo.RecordEventDeduplicated(ctx, orgID, eventID)
		if err != nil {
			return err
		}
		if !applied {
			return nil // already processed
		}
	}
	payload, err := protojson.Marshal(message)
	if err != nil {
		return fmt.Errorf("marshal pipeline event: %w", err)
	}
	return wp.ingest(ctx, orgID, eventType, payload)
}

func (wp *WebhookPublisher) ingest(ctx context.Context, orgID uuid.UUID, eventType string, payload []byte) error {
	endpoints, err := wp.repo.GetEndpoints(ctx, orgID)
	if err != nil {
		return err
	}
	for _, endpoint := range endpoints {
		if !containsEvent(endpoint.Events, eventType) {
			continue
		}
		intent, err := wp.repo.CreateDeliveryIntent(ctx, endpoint.EndpointID, eventType, string(payload))
		if err != nil {
			return err
		}
		attempt := DeliveryAttempt{DeliveryID: intent.DeliveryID.String(), EndpointID: endpoint.EndpointID.String(), EventType: eventType, Payload: intent.Payload, Attempt: intent.Attempt}
		encoded, err := json.Marshal(attempt)
		if err != nil {
			return err
		}
		if err := wp.events.PublishBytes(ctx, events.SubjectWebhookDeliveryAttempt, fmt.Sprintf("delivery:%s:attempt:%d", intent.DeliveryID, intent.Attempt), encoded); err != nil {
			return err
		}
	}
	return nil
}

func containsEvent(events []string, eventType string) bool {
	for _, event := range events {
		if event == eventType {
			return true
		}
	}
	return false
}

func (wp *WebhookPublisher) DeliverAttempt(ctx context.Context, attempt DeliveryAttempt) error {
	endpointID, err := uuid.Parse(attempt.EndpointID)
	if err != nil {
		return err
	}
	endpoint, err := wp.repo.GetEndpoint(ctx, endpointID)
	if err != nil || endpoint == nil {
		return fmt.Errorf("load webhook endpoint: %w", err)
	}
	signature := repository.SignWebhookPayload(attempt.Payload, endpoint.Secret)
	request, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.URL, bytes.NewBufferString(attempt.Payload))
	if err != nil {
		return err
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("X-Webhook-Signature", signature)
	request.Header.Set("X-Webhook-Event", attempt.EventType)
	response, err := wp.client.Do(request)
	if err != nil {
		_ = wp.repo.RecordDelivery(ctx, endpoint.EndpointID, attempt.EventType, 0, attempt.Attempt, err.Error())
		return err
	}
	defer response.Body.Close()
	if err := wp.repo.RecordDelivery(ctx, endpoint.EndpointID, attempt.EventType, response.StatusCode, attempt.Attempt, attempt.Payload); err != nil {
		return err
	}
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return fmt.Errorf("webhook returned status %d", response.StatusCode)
	}
	return nil
}

func (wp *WebhookPublisher) PublishEvent(ctx context.Context, orgID uuid.UUID, eventType string, data interface{}) error {
	endpoints, err := wp.repo.GetEndpoints(ctx, orgID)
	if err != nil {
		return fmt.Errorf("get endpoints: %w", err)
	}

	event := &WebhookEvent{
		Type:      eventType,
		Timestamp: time.Now().UTC(),
		Data:      data,
	}
	payload, err := json.Marshal(event)
	if err != nil {
		return fmt.Errorf("marshal event: %w", err)
	}

	for _, ep := range endpoints {
		if !containsEvent(ep.Events, eventType) {
			continue
		}
		if err := wp.deliverWithRetry(ctx, ep, eventType, payload); err != nil {
			return err
		}
	}

	return nil
}

func (wp *WebhookPublisher) deliverWithRetry(ctx context.Context, ep repository.WebhookEndpoint, eventType string, payload []byte) error {
	maxRetries := 3
	backoff := 1 * time.Second

	for attempt := 1; attempt <= maxRetries; attempt++ {
		sig := repository.SignWebhookPayload(string(payload), ep.Secret)

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, ep.URL, bytes.NewReader(payload))
		if err != nil {
			_ = wp.repo.RecordDelivery(ctx, ep.EndpointID, eventType, 0, attempt, fmt.Sprintf("request create error: %v", err))
			continue
		}

		req.Header.Set("Content-Type", "application/json")
		req.Header.Set("X-Webhook-Signature", sig)
		req.Header.Set("X-Webhook-Event", eventType)

		resp, err := wp.client.Do(req)
		if err != nil {
			_ = wp.repo.RecordDelivery(ctx, ep.EndpointID, eventType, 0, attempt, fmt.Sprintf("delivery error: %v", err))
			time.Sleep(backoff)
			backoff *= 2
			continue
		}

		statusCode := resp.StatusCode
		resp.Body.Close()

		_ = wp.repo.RecordDelivery(ctx, ep.EndpointID, eventType, statusCode, attempt, "")

		if statusCode >= 200 && statusCode < 300 {
			return nil
		}

		time.Sleep(backoff)
		backoff *= 2
	}
	return fmt.Errorf("webhook delivery failed after %d attempts", maxRetries)
}
