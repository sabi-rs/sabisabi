CREATE TABLE IF NOT EXISTS market_source_endpoint_snapshots (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  provider TEXT NOT NULL,
  endpoint_key TEXT NOT NULL,
  requested_url TEXT NOT NULL DEFAULT '',
  capture_mode TEXT NOT NULL,
  payload JSONB NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  FOREIGN KEY (provider, endpoint_key)
    REFERENCES market_source_endpoints(provider, endpoint_key)
);

CREATE INDEX IF NOT EXISTS market_source_endpoint_snapshots_lookup_idx
  ON market_source_endpoint_snapshots (provider, endpoint_key, captured_at DESC);
