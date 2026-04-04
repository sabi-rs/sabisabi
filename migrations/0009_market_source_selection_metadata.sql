ALTER TABLE sport_leagues
  ADD COLUMN IF NOT EXISTS primary_selection_reason TEXT NOT NULL DEFAULT '';

ALTER TABLE market_intel_source_status
  ADD COLUMN IF NOT EXISTS latency_ms BIGINT;

ALTER TABLE market_intel_source_status
  ADD COLUMN IF NOT EXISTS requests_remaining BIGINT;

ALTER TABLE market_intel_source_status
  ADD COLUMN IF NOT EXISTS requests_limit BIGINT;

ALTER TABLE market_intel_source_status
  ADD COLUMN IF NOT EXISTS rate_limit_reset_at TEXT;
