package repository

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/uuid"
)

type WebhookEndpoint struct {
	OrgID      uuid.UUID
	EndpointID uuid.UUID
	URL        string
	Secret     string
	Events     []string
	CreatedAt  time.Time
}

type WebhookDelivery struct {
	EndpointID  uuid.UUID
	DeliveredAt time.Time
	DeliveryID  uuid.UUID
	EventType   string
	StatusCode  int
	Attempt     int
	Payload     string
	CreatedAt   time.Time
}

type DeliveryIntent struct {
	DeliveryID uuid.UUID
	EndpointID uuid.UUID
	EventType  string
	Payload    string
	Attempt    int
}

type WebhookRepo struct {
	session *gocql.Session
}

func NewWebhookRepo(session *gocql.Session) *WebhookRepo {
	return &WebhookRepo{session: session}
}

func (r *WebhookRepo) CreateEndpoint(ctx context.Context, orgID uuid.UUID, url, secret string, events []string) (*WebhookEndpoint, error) {
	endpointID := uuid.New()
	ep := &WebhookEndpoint{
		OrgID:      orgID,
		EndpointID: endpointID,
		URL:        url,
		Secret:     secret,
		Events:     events,
		CreatedAt:  time.Now().UTC(),
	}
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO webhook_endpoints (org_id, endpoint_id, url, secret, events, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		ep.OrgID, ep.EndpointID, ep.URL, ep.Secret, ep.Events, ep.CreatedAt,
	)
	batch.Query(
		`INSERT INTO webhook_endpoints_by_id (endpoint_id, org_id, url, secret, events, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		ep.EndpointID, ep.OrgID, ep.URL, ep.Secret, ep.Events, ep.CreatedAt,
	)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return nil, fmt.Errorf("insert webhook endpoint: %w", err)
	}
	return ep, nil
}

// DeleteEndpoint removes a subscription from both lookup tables.
func (r *WebhookRepo) DeleteEndpoint(ctx context.Context, orgID, endpointID uuid.UUID) error {
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(`DELETE FROM webhook_endpoints WHERE org_id = ? AND endpoint_id = ?`, orgID, endpointID)
	batch.Query(`DELETE FROM webhook_endpoints_by_id WHERE endpoint_id = ?`, endpointID)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return fmt.Errorf("delete webhook endpoint: %w", err)
	}
	return nil
}

func (r *WebhookRepo) GetEndpoints(ctx context.Context, orgID uuid.UUID) ([]WebhookEndpoint, error) {
	var results []WebhookEndpoint
	iter := r.session.Query(
		`SELECT org_id, endpoint_id, url, secret, events, created_at FROM webhook_endpoints WHERE org_id = ?`,
		orgID,
	).WithContext(ctx).Iter()
	var ep WebhookEndpoint
	for iter.Scan(&ep.OrgID, &ep.EndpointID, &ep.URL, &ep.Secret, &ep.Events, &ep.CreatedAt) {
		results = append(results, ep)
	}
	if err := iter.Close(); err != nil {
		return nil, fmt.Errorf("iterate endpoints: %w", err)
	}
	return results, nil
}

func (r *WebhookRepo) RecordDelivery(ctx context.Context, endpointID uuid.UUID, eventType string, statusCode int, attempt int, payload string) error {
	delivery := &WebhookDelivery{
		EndpointID:  endpointID,
		DeliveredAt: time.Now().UTC(),
		DeliveryID:  uuid.New(),
		EventType:   eventType,
		StatusCode:  statusCode,
		Attempt:     attempt,
		Payload:     payload,
		CreatedAt:   time.Now().UTC(),
	}
	if err := r.session.Query(
		`INSERT INTO webhook_deliveries (endpoint_id, delivered_at, delivery_id, event_type, status_code, attempt, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		delivery.EndpointID, delivery.DeliveredAt, delivery.DeliveryID, delivery.EventType, delivery.StatusCode, delivery.Attempt, delivery.Payload, delivery.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return fmt.Errorf("record delivery: %w", err)
	}
	return nil
}

func (r *WebhookRepo) CreateDeliveryIntent(ctx context.Context, endpointID uuid.UUID, eventType string, payload string) (*DeliveryIntent, error) {
	intent := &DeliveryIntent{DeliveryID: uuid.New(), EndpointID: endpointID, EventType: eventType, Payload: payload, Attempt: 1}
	now := time.Now().UTC()
	batch := r.session.NewBatch(gocql.LoggedBatch).WithContext(ctx)
	batch.Query(
		`INSERT INTO webhook_deliveries (endpoint_id, delivered_at, delivery_id, event_type, status_code, attempt, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		intent.EndpointID, now, intent.DeliveryID, intent.EventType, 0, intent.Attempt, intent.Payload, now,
	)
	// Reverse lookup row so GetDeliveryStatus can find a delivery by ID.
	batch.Query(
		`INSERT INTO deliveries_by_id (delivery_id, endpoint_id, event_type, status_code, attempt, delivered_at) VALUES (?, ?, ?, ?, ?, ?)`,
		intent.DeliveryID, intent.EndpointID, intent.EventType, 0, intent.Attempt, now,
	)
	if err := r.session.ExecuteBatch(batch); err != nil {
		return nil, fmt.Errorf("create delivery intent: %w", err)
	}
	return intent, nil
}

// GetDeliveryByID resolves the reverse index for status queries.
func (r *WebhookRepo) GetDeliveryByID(ctx context.Context, deliveryID uuid.UUID) (*WebhookDelivery, error) {
	var endpointID uuid.UUID
	var deliveredAt time.Time
	err := r.session.Query(
		`SELECT endpoint_id, delivered_at FROM deliveries_by_id WHERE delivery_id = ?`,
		deliveryID,
	).WithContext(ctx).Scan(&endpointID, &deliveredAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("lookup delivery by id: %w", err)
	}
	var delivery WebhookDelivery
	err = r.session.Query(
		`SELECT endpoint_id, delivered_at, delivery_id, event_type, status_code, attempt, payload, created_at FROM webhook_deliveries WHERE endpoint_id = ? AND delivered_at = ? AND delivery_id = ?`,
		endpointID, deliveredAt, deliveryID,
	).WithContext(ctx).Scan(&delivery.EndpointID, &delivery.DeliveredAt, &delivery.DeliveryID, &delivery.EventType, &delivery.StatusCode, &delivery.Attempt, &delivery.Payload, &delivery.CreatedAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("load delivery: %w", err)
	}
	return &delivery, nil
}

func (r *WebhookRepo) GetEndpoint(ctx context.Context, endpointID uuid.UUID) (*WebhookEndpoint, error) {
	var endpoint WebhookEndpoint
	err := r.session.Query(`SELECT endpoint_id, org_id, url, secret, events, created_at FROM webhook_endpoints_by_id WHERE endpoint_id = ?`, endpointID).WithContext(ctx).Scan(&endpoint.EndpointID, &endpoint.OrgID, &endpoint.URL, &endpoint.Secret, &endpoint.Events, &endpoint.CreatedAt)
	if err == gocql.ErrNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("get endpoint: %w", err)
	}
	return &endpoint, nil
}

func SignWebhookPayload(payload, secret string) string {
	mac := hmac.New(sha256.New, []byte(secret))
	mac.Write([]byte(payload))
	return hex.EncodeToString(mac.Sum(nil))
}

// RecordEventDeduplicated inserts into webhook_event_dedup IF NOT EXISTS.
// Returns true if applied (new event), false if it already exists (duplicate).
func (r *WebhookRepo) RecordEventDeduplicated(ctx context.Context, orgID uuid.UUID, eventID string) (bool, error) {
	if eventID == "" {
		return true, nil
	}
	var existingOrgID uuid.UUID
	var existingEventID string
	applied, err := r.session.Query(
		`INSERT INTO webhook_event_dedup (org_id, event_id) VALUES (?, ?) IF NOT EXISTS`,
		orgID, eventID,
	).WithContext(ctx).ScanCAS(&existingOrgID, &existingEventID)
	if err != nil {
		return false, fmt.Errorf("check webhook event dedup: %w", err)
	}
	return applied, nil
}
