-- Migration 2: Status index for the reconciliation sweep. ListStaleProcessingAssets
-- previously ran SELECT ... WHERE status = ? ALLOW FILTERING (full cluster scan);
-- this lookup table partitions by status so the sweep is a bounded partition read.

CREATE TABLE IF NOT EXISTS assets_by_status (
    status     text,
    updated_at timestamp,
    asset_id   uuid,
    PRIMARY KEY (status, updated_at, asset_id)
) WITH CLUSTERING ORDER BY (updated_at DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};
