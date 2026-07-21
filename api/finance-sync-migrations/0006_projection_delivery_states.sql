-- Forward repair for databases that applied runtime schema before dead-letter support.
ALTER TABLE projection_outbox ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz;
ALTER TABLE projection_outbox ADD COLUMN IF NOT EXISTS dead_letter_reason text;
ALTER TABLE projection_outbox DROP CONSTRAINT IF EXISTS projection_outbox_state_check;
ALTER TABLE projection_outbox ADD CONSTRAINT projection_outbox_state_check
  CHECK (state IN ('pending', 'uncertain', 'delivered', 'dead_letter'));
CREATE INDEX IF NOT EXISTS projection_outbox_unresolved_idx
  ON projection_outbox (deployment_id, connection_id, created_at)
  WHERE state IN ('pending', 'uncertain');
