-- Migration 2: artifact_prefix on playback_ids. GetPlayerConfig assembles
-- player URLs from the artifact prefix the orchestrator published to, so the
-- playback record must carry it. The runner guarantees this applies once per
-- database (ScyllaDB rejects a duplicate ADD).
ALTER TABLE playback_ids ADD artifact_prefix text;
