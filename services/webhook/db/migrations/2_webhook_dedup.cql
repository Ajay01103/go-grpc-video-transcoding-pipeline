-- Migration 2: Webhook event deduplication table to prevent duplicate delivery intents
-- when terminal events are re-published during crash recovery.

CREATE TABLE IF NOT EXISTS webhook_event_dedup (
    org_id   uuid,
    event_id text,
    PRIMARY KEY (org_id, event_id)
) WITH default_time_to_live = 86400  -- 24h, longer than JetStream dedup window
  AND compaction = {'class': 'LeveledCompactionStrategy'};
