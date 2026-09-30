-- Migration 1: Webhook ownership. Webhook service owns endpoint subscriptions
-- and delivery history.
--
-- webhook_deliveries stores every scheduled delivery (including intents that
-- have not succeeded yet, status_code 0) so delivery state survives restarts.

CREATE TABLE IF NOT EXISTS webhook_endpoints (
    org_id      uuid,
    endpoint_id uuid,
    url         text,
    secret      text,
    events      set<text>,
    created_at  timestamp,
    PRIMARY KEY (org_id, endpoint_id)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS webhook_deliveries (
    endpoint_id  uuid,
    delivered_at timestamp,
    delivery_id  uuid,
    event_type   text,
    status_code  int,           -- 0 = scheduled/pending, 2xx = success
    attempt      int,
    payload      text,
    created_at   timestamp,
    PRIMARY KEY (endpoint_id, delivered_at, delivery_id)
) WITH CLUSTERING ORDER BY (delivered_at DESC)
  AND compaction = {'class': 'TimeWindowCompactionStrategy'};

-- Reverse lookup for GetDeliveryStatus-style queries without a full scan.
CREATE TABLE IF NOT EXISTS deliveries_by_id (
    delivery_id  uuid PRIMARY KEY,
    endpoint_id  uuid,
    event_type   text,
    status_code  int,
    attempt      int,
    delivered_at timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

-- Reverse lookup for endpoints so DeliverAttempt avoids
-- SELECT ... WHERE endpoint_id = ? ALLOW FILTERING (full scan).
CREATE TABLE IF NOT EXISTS webhook_endpoints_by_id (
    endpoint_id uuid PRIMARY KEY,
    org_id      uuid,
    url         text,
    secret      text,
    events      set<text>,
    created_at  timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};
