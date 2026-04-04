CREATE TABLE IF NOT EXISTS market_source_policies (
  source TEXT PRIMARY KEY,
  enabled BOOLEAN NOT NULL DEFAULT TRUE,
  selection_priority INTEGER NOT NULL,
  freshness_threshold_secs BIGINT NOT NULL,
  reserve_requests_remaining BIGINT,
  notes TEXT NOT NULL DEFAULT '',
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS market_source_policies_enabled_idx
  ON market_source_policies (enabled, selection_priority, updated_at DESC);
