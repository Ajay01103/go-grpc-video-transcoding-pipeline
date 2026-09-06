package observability

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/gocql/gocql"
)

// IdempotencyKey represents a client-supplied request identifier
type IdempotencyKey struct {
	ClientID   string
	RequestID  uuid.UUID
	AssetID    uuid.UUID
	CreatedAt  time.Time
	Operation  string
}

// IdempotencyStore tracks seen requests to prevent duplicates
type IdempotencyStore struct {
	session *gocql.Session
	cache   map[string]uuid.UUID
	mu      sync.RWMutex
}

func NewIdempotencyStore(session *gocql.Session) *IdempotencyStore {
	return &IdempotencyStore{
		session: session,
		cache:   make(map[string]uuid.UUID),
	}
}

func (is *IdempotencyStore) EnsureSchema() error {
	q := `CREATE TABLE IF NOT EXISTS idempotency_keys (
		client_id text,
		request_id uuid,
		asset_id uuid,
		created_at timestamp,
		operation text,
		PRIMARY KEY (client_id, request_id)
	)`
	return is.session.Query(q).WithContext(context.Background()).Exec()
}

func (is *IdempotencyStore) RecordRequest(ctx context.Context, clientID string, assetID uuid.UUID, operation string) (uuid.UUID, error) {
	key := fmt.Sprintf("%s:%s:%s", clientID, assetID.String(), operation)

	is.mu.RLock()
	if existing, ok := is.cache[key]; ok {
		is.mu.RUnlock()
		return existing, nil
	}
	is.mu.RUnlock()

	requestID := uuid.New()
	ikey := &IdempotencyKey{
		ClientID:  clientID,
		RequestID: requestID,
		AssetID:   assetID,
		CreatedAt: time.Now().UTC(),
		Operation: operation,
	}

	if err := is.session.Query(
		`INSERT INTO idempotency_keys (client_id, request_id, asset_id, created_at, operation) VALUES (?, ?, ?, ?, ?)`,
		ikey.ClientID, ikey.RequestID, ikey.AssetID, ikey.CreatedAt, ikey.Operation,
	).WithContext(ctx).Exec(); err != nil {
		return uuid.Nil, fmt.Errorf("record idempotency key: %w", err)
	}

	is.mu.Lock()
	is.cache[key] = requestID
	is.mu.Unlock()

	return requestID, nil
}

func (is *IdempotencyStore) Get(ctx context.Context, clientID string, assetID uuid.UUID, operation string) (uuid.UUID, error) {
	key := fmt.Sprintf("%s:%s:%s", clientID, assetID.String(), operation)

	is.mu.RLock()
	if existing, ok := is.cache[key]; ok {
		is.mu.RUnlock()
		return existing, nil
	}
	is.mu.RUnlock()

	var requestID uuid.UUID
	err := is.session.Query(
		`SELECT request_id FROM idempotency_keys WHERE client_id = ? AND asset_id = ? AND operation = ? LIMIT 1`,
		clientID, assetID, operation,
	).WithContext(ctx).Scan(&requestID)

	if err == gocql.ErrNotFound {
		return uuid.Nil, nil
	}
	if err != nil {
		return uuid.Nil, fmt.Errorf("get idempotency key: %w", err)
	}

	is.mu.Lock()
	is.cache[key] = requestID
	is.mu.Unlock()

	return requestID, nil
}
