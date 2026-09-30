-- Migration 1: Asset service owns assets_by_id and assets_by_org only.
-- Playback tables belong to the playback service (see services/playback/db/migrations).

CREATE TABLE IF NOT EXISTS assets_by_id (
    asset_id     uuid PRIMARY KEY,
    org_id       uuid,
    title        text,
    status       text,
    source_uri   text,
    duration_ms  bigint,
    error_string text,
    created_at   timestamp,
    updated_at   timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS assets_by_org (
    org_id     uuid,
    created_at timestamp,
    asset_id   uuid,
    status     text,
    PRIMARY KEY (org_id, created_at, asset_id)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};
