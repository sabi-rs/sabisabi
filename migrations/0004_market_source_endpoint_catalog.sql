CREATE TABLE IF NOT EXISTS market_source_endpoints (
  provider TEXT NOT NULL,
  endpoint_key TEXT NOT NULL,
  method TEXT NOT NULL,
  path_template TEXT NOT NULL,
  transport TEXT NOT NULL,
  category TEXT NOT NULL,
  ingest_strategy TEXT NOT NULL,
  source_of_truth TEXT NOT NULL,
  notes TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  PRIMARY KEY (provider, endpoint_key)
);

CREATE INDEX IF NOT EXISTS market_source_endpoints_provider_idx
  ON market_source_endpoints (provider, category, updated_at DESC);
