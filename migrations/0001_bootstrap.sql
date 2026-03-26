CREATE TABLE IF NOT EXISTS worker_status (
  source TEXT PRIMARY KEY,
  status TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS raw_ingest_events (
  id BIGSERIAL PRIMARY KEY,
  source TEXT NOT NULL,
  topic TEXT NOT NULL,
  payload JSONB NOT NULL,
  ingested_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS raw_ingest_events_source_topic_idx
  ON raw_ingest_events (source, topic, ingested_at DESC);

CREATE TABLE IF NOT EXISTS live_events (
  event_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  sport TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  status TEXT NOT NULL,
  payload JSONB NOT NULL,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS live_events_source_sport_idx
  ON live_events (source, sport, updated_at DESC);
