-- Migration 1: Subtitle ownership. Subtitle service owns transcription status
-- and the storage location of generated WebVTT per asset+language.
--
-- vtt_location points into RustFS ({output-prefix}/subtitles/{language}.vtt);
-- segments are kept as JSON for debugging and re-rendering.

CREATE TABLE IF NOT EXISTS subtitles (
    asset_id    uuid,
    language    text,
    status      text,          -- pending | running | done | failed
    vtt_location text,
    segments    text,
    error_message text,
    created_at  timestamp,
    updated_at  timestamp,
    PRIMARY KEY (asset_id, language)
) WITH compaction = {'class': 'LeveledCompactionStrategy'};
