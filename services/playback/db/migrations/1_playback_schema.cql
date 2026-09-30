-- Migration 1: Playback identity. The playback service exclusively owns these
-- tables; the asset service previously duplicated them and that copy is removed.
--
-- playback_ids_by_asset_policy removes the ALLOW FILTERING lookup
-- (WHERE asset_id = ? AND policy = ?) that scanned the whole playback_ids table
-- on every CreatePlayback call.

CREATE TABLE IF NOT EXISTS playback_ids (
    playback_id    uuid PRIMARY KEY,
    asset_id       uuid,
    policy         text,          -- public | signed
    signing_key_id text,
    revoked        boolean,
    created_at     timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS playback_ids_by_asset (
    asset_id    uuid,
    playback_id uuid,
    policy      text,
    PRIMARY KEY (asset_id, playback_id)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS playback_ids_by_asset_policy (
    asset_id    uuid,
    policy      text,
    playback_id uuid,
    PRIMARY KEY (asset_id, policy, playback_id)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};
