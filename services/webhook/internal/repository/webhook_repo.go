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

func (r *WebhookRepo) EnsureSchema() error {
	queries := []string{
		`CREATE TABLE IF NOT EXISTS webhook_endpoints (
			org_id uuid,
			endpoint_id uuid,
			url text,
			secret text,
			events set<text>,
			created_at timestamp,
			PRIMARY KEY (org_id, endpoint_id)
		)`,
		`CREATE TABLE IF NOT EXISTS webhook_deliveries (
			endpoint_id uuid,
			delivered_at timestamp,
			delivery_id uuid,
			event_type text,
			status_code int,
			attempt int,
			payload text,
			created_at timestamp,
			PRIMARY KEY (endpoint_id, delivered_at, delivery_id)
		) WITH CLUSTERING ORDER BY (delivered_at DESC)`,
	}
	for _, q := range queries {
		if err := r.session.Query(q).WithContext(context.Background()).Exec(); err != nil {
			return fmt.Errorf("ensure schema: %w", err)
		}
	}
	return nil
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
	if err := r.session.Query(
		`INSERT INTO webhook_endpoints (org_id, endpoint_id, url, secret, events, created_at) VALUES (?, ?, ?, ?, ?, ?)`,
		ep.OrgID, ep.EndpointID, ep.URL, ep.Secret, ep.Events, ep.CreatedAt,
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("insert webhook endpoint: %w", err)
	}
	return ep, nil
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
	if err := r.session.Query(
		`INSERT INTO webhook_deliveries (endpoint_id, delivered_at, delivery_id, event_type, status_code, attempt, payload, created_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?)`,
		intent.EndpointID, time.Now().UTC(), intent.DeliveryID, intent.EventType, 0, intent.Attempt, intent.Payload, time.Now().UTC(),
	).WithContext(ctx).Exec(); err != nil {
		return nil, fmt.Errorf("create delivery intent: %w", err)
	}
	return intent, nil
}

func (r *WebhookRepo) GetEndpoint(ctx context.Context, endpointID uuid.UUID) (*WebhookEndpoint, error) {
	var endpoint WebhookEndpoint
	err := r.session.Query(`SELECT org_id, endpoint_id, url, secret, events, created_at FROM webhook_endpoints WHERE endpoint_id = ? ALLOW FILTERING`, endpointID).WithContext(ctx).Scan(&endpoint.OrgID, &endpoint.EndpointID, &endpoint.URL, &endpoint.Secret, &endpoint.Events, &endpoint.CreatedAt)
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
