-- Migration 3: Coordination columns and active-runs index.
--
-- pipeline_steps.attempt
--   Tracks which attempt a step is currently on.  The orchestrator uses this
--   to ignore stale deliveries from a prior attempt when a RetryPipeline has
--   already dispatched a new one.
--
-- pipeline_runs.run_generation
--   Incremented by RetryPipeline.  Included in every run-level terminal-event
--   message ID so that pipeline.run.failed for retry-2 has a distinct NATS
--   dedup key from pipeline.run.failed for retry-1.
--
-- pipeline_runs.probe_duration_seconds
--   Written by HandleProbeCompleted.  Used by the sweeper to compute a
--   per-asset stale threshold (max(floor, 3 × duration)) so it does not
--   incorrectly sweep a long encode that is still running.
--
-- pipeline_runs.failed_error_code / failed_error_message
--   Written atomically with the FAILED status transition.  republishRunFailed
--   reads these to reconstruct the original terminal event after a crash
--   between the DB write and the NATS publish.
--
-- active_runs
--   Small lookup table (O(active runs)).  Written when a run is created,
--   deleted when it reaches a terminal state (COMPLETED/FAILED/CANCELLED).
--   The sweeper reads only this table, avoiding a full scan of
--   pipeline_runs_by_run as that table grows.

ALTER TABLE pipeline_steps ADD attempt int;

ALTER TABLE pipeline_runs ADD run_generation         int;
ALTER TABLE pipeline_runs ADD probe_duration_seconds double;
ALTER TABLE pipeline_runs ADD failed_error_code      text;
ALTER TABLE pipeline_runs ADD failed_error_message   text;

CREATE TABLE IF NOT EXISTS active_runs (
    shard      int,
    asset_id   uuid,
    run_id     uuid,
    created_at timestamp,
    PRIMARY KEY (shard, created_at, asset_id)
) WITH CLUSTERING ORDER BY (created_at DESC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};
