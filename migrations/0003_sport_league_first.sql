-- Sport/League registry with primary source tracking
CREATE TABLE IF NOT EXISTS sport_leagues (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sport_key TEXT NOT NULL,
  sport_title TEXT NOT NULL,
  group_name TEXT NOT NULL,
  active BOOLEAN NOT NULL DEFAULT TRUE,
  primary_source TEXT NOT NULL,
  primary_refreshed_at TIMESTAMPTZ,
  fallback_source TEXT,
  fallback_refreshed_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE UNIQUE INDEX IF NOT EXISTS sport_leagues_sport_group_uidx
  ON sport_leagues (sport_key, group_name);

CREATE INDEX IF NOT EXISTS sport_leagues_primary_source_idx ON sport_leagues (primary_source, updated_at DESC);

-- Events keyed by sport_league
CREATE TABLE IF NOT EXISTS market_events (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sport_league_id UUID NOT NULL REFERENCES sport_leagues(id),
  event_id TEXT NOT NULL,
  event_name TEXT NOT NULL,
  home_team TEXT NOT NULL,
  away_team TEXT NOT NULL,
  commence_time TIMESTAMPTZ,
  is_live BOOLEAN NOT NULL DEFAULT FALSE,
  source TEXT NOT NULL,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE(sport_league_id, event_id, source)
);

CREATE INDEX IF NOT EXISTS market_events_sport_league_idx ON market_events (sport_league_id, refreshed_at DESC);
CREATE INDEX IF NOT EXISTS market_events_source_idx ON market_events (source, refreshed_at DESC);

-- Market quotes per event
CREATE TABLE IF NOT EXISTS market_quotes (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  market_event_id UUID NOT NULL REFERENCES market_events(id),
  source TEXT NOT NULL,
  market_id TEXT NOT NULL DEFAULT '',
  selection_id TEXT NOT NULL DEFAULT '',
  market_name TEXT NOT NULL,
  selection_name TEXT NOT NULL,
  venue TEXT NOT NULL,
  price DOUBLE PRECISION,
  fair_price DOUBLE PRECISION,
  liquidity DOUBLE PRECISION,
  point DOUBLE PRECISION,
  side TEXT NOT NULL,
  is_sharp BOOLEAN NOT NULL DEFAULT FALSE,
  event_url TEXT NOT NULL DEFAULT '',
  deep_link_url TEXT NOT NULL DEFAULT '',
  notes JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  refreshed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_quotes_event_idx ON market_quotes (market_event_id, venue);
CREATE INDEX IF NOT EXISTS market_quotes_market_idx ON market_quotes (market_name, selection_name);

-- Derived opportunities (arbs, +EV, etc.) computed from quotes
CREATE TABLE IF NOT EXISTS market_opportunities (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  sport_league_id UUID NOT NULL REFERENCES sport_leagues(id),
  event_id TEXT NOT NULL,
  source TEXT NOT NULL,
  kind TEXT NOT NULL,             -- "arbitrage", "positive_ev", "value"
  market_name TEXT NOT NULL,
  selection_name TEXT NOT NULL,
  secondary_selection_name TEXT,
  venue TEXT NOT NULL,
  secondary_venue TEXT,
  price DOUBLE PRECISION,
  secondary_price DOUBLE PRECISION,
  fair_price DOUBLE PRECISION,
  liquidity DOUBLE PRECISION,
  edge_percent DOUBLE PRECISION,
  arbitrage_margin DOUBLE PRECISION,
  stake_hint DOUBLE PRECISION,
  start_time TIMESTAMPTZ,
  event_url TEXT NOT NULL DEFAULT '',
  deep_link_url TEXT NOT NULL DEFAULT '',
  is_live BOOLEAN NOT NULL DEFAULT FALSE,
  notes JSONB NOT NULL DEFAULT '[]'::jsonb,
  raw_data JSONB NOT NULL DEFAULT '{}'::jsonb,
  computed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_opportunities_kind_idx ON market_opportunities (kind, computed_at DESC);
CREATE INDEX IF NOT EXISTS market_opportunities_sport_idx ON market_opportunities (sport_league_id, computed_at DESC);

-- Keep legacy tables for migration reference, will be replaced
-- DROP TABLE IF EXISTS market_intel_source_status;
-- DROP TABLE IF EXISTS market_intel_opportunities;
-- DROP TABLE IF EXISTS market_intel_event_details;
