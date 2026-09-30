-- Migration 2: Artifact locations per run+step. Written by the orchestrator as
-- each completion event arrives, and read back to populate the
-- artifact_locations map on pipeline.run.completed.

CREATE TABLE IF NOT EXISTS pipeline_artifacts (
    run_id     uuid,
    step       text,
    location   text,
    updated_at timestamp,
    PRIMARY KEY (run_id, step)
) WITH CLUSTERING ORDER BY (step ASC)
  AND compaction = {'class': 'LeveledCompactionStrategy'};
