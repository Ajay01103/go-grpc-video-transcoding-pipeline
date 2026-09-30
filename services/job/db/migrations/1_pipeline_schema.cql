-- Migration 1: Pipeline state. The job orchestrator is the ONLY writer of
-- pipeline_runs and pipeline_steps.
--
-- NOTE: the previous inline EnsureSchema had a missing comma before playback_id,
-- which made this table impossible to create and blocked every pipeline run.
-- The schema below is the corrected contract.
--
-- pipeline_runs is keyed by asset_id (one active run per asset). run_status_by_run
-- gives the reverse lookup keyed by run_id, which event consumers receive in payloads.

CREATE TABLE IF NOT EXISTS pipeline_runs (
    asset_id                    uuid PRIMARY KEY,
    run_id                      uuid,
    org_id                      uuid,
    source_uri                  text,
    requested_subtitle_languages list<text>,
    status                      text,
    playback_id                 uuid,
    created_at                  timestamp,
    updated_at                  timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS pipeline_runs_by_run (
    run_id     uuid PRIMARY KEY,
    asset_id   uuid,
    org_id     uuid,
    status     text,
    created_at timestamp
) WITH compaction = {'class': 'LeveledCompactionStrategy'};

CREATE TABLE IF NOT EXISTS pipeline_steps (
    run_id     uuid,
    step       text,
    asset_id   uuid,
    status     text,
    updated_at timestamp,
    PRIMARY KEY (run_id, step)
) WITH CLUSTERING ORDER BY (step ASC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};
