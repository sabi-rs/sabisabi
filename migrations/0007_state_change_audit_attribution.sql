ALTER TABLE state_change_audit
  ADD COLUMN IF NOT EXISTS actor TEXT NOT NULL DEFAULT '';

ALTER TABLE state_change_audit
  ADD COLUMN IF NOT EXISTS request_id TEXT NOT NULL DEFAULT '';

CREATE INDEX IF NOT EXISTS state_change_audit_actor_idx
  ON state_change_audit (actor, changed_at DESC);

CREATE INDEX IF NOT EXISTS state_change_audit_request_idx
  ON state_change_audit (request_id, changed_at DESC);
