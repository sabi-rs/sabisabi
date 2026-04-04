CREATE TABLE IF NOT EXISTS state_change_audit (
  id UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  batch_id UUID NOT NULL,
  entity_type TEXT NOT NULL,
  entity_id TEXT NOT NULL,
  action TEXT NOT NULL,
  change_source TEXT NOT NULL,
  before_state JSONB,
  after_state JSONB,
  metadata JSONB NOT NULL DEFAULT '{}'::jsonb,
  changed_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS state_change_audit_batch_idx
  ON state_change_audit (batch_id, changed_at ASC);

CREATE INDEX IF NOT EXISTS state_change_audit_entity_idx
  ON state_change_audit (entity_type, entity_id, changed_at DESC);

CREATE INDEX IF NOT EXISTS state_change_audit_source_idx
  ON state_change_audit (change_source, changed_at DESC);
