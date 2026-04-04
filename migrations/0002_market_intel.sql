CREATE TABLE IF NOT EXISTS market_intel_source_status (
  source TEXT PRIMARY KEY,
  load_mode TEXT NOT NULL,
  health_status TEXT NOT NULL,
  detail TEXT NOT NULL DEFAULT '',
  refreshed_at TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS market_intel_opportunities (
  opportunity_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  kind TEXT NOT NULL,
  sport TEXT NOT NULL DEFAULT '',
  competition_name TEXT NOT NULL DEFAULT '',
  event_id TEXT NOT NULL DEFAULT '',
  event_name TEXT NOT NULL DEFAULT '',
  market_name TEXT NOT NULL DEFAULT '',
  selection_name TEXT NOT NULL DEFAULT '',
  secondary_selection_name TEXT NOT NULL DEFAULT '',
  venue TEXT NOT NULL DEFAULT '',
  secondary_venue TEXT NOT NULL DEFAULT '',
  price DOUBLE PRECISION,
  secondary_price DOUBLE PRECISION,
  fair_price DOUBLE PRECISION,
  liquidity DOUBLE PRECISION,
  edge_percent DOUBLE PRECISION,
  arbitrage_margin DOUBLE PRECISION,
  stake_hint DOUBLE PRECISION,
  start_time TEXT NOT NULL DEFAULT '',
  observed_at TEXT NOT NULL DEFAULT '',
  event_url TEXT NOT NULL DEFAULT '',
  deep_link_url TEXT NOT NULL DEFAULT '',
  is_live BOOLEAN NOT NULL DEFAULT FALSE,
  quotes JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_intel_opportunities_source_kind_idx
  ON market_intel_opportunities (source, kind, updated_at DESC);

CREATE INDEX IF NOT EXISTS market_intel_opportunities_event_idx
  ON market_intel_opportunities (event_id, updated_at DESC);

CREATE TABLE IF NOT EXISTS market_intel_event_details (
  detail_id TEXT PRIMARY KEY,
  source TEXT NOT NULL,
  event_id TEXT NOT NULL,
  sport TEXT NOT NULL DEFAULT '',
  event_name TEXT NOT NULL DEFAULT '',
  home_team TEXT NOT NULL DEFAULT '',
  away_team TEXT NOT NULL DEFAULT '',
  start_time TEXT NOT NULL DEFAULT '',
  is_live BOOLEAN NOT NULL DEFAULT FALSE,
  quotes JSONB NOT NULL DEFAULT '[]'::jsonb,
  history JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  observed_at TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_intel_event_details_source_event_idx
  ON market_intel_event_details (source, event_id, updated_at DESC);
