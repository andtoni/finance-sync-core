-- G8 resiliency: dead-letter state for projection outbox after retry exhaustion
ALTER TABLE projection_outbox DROP CONSTRAINT IF EXISTS projection_outbox_state_check;
ALTER TABLE projection_outbox ADD CONSTRAINT projection_outbox_state_check
  CHECK (state IN ('pending','delivered','dead_letter'));
ALTER TABLE projection_outbox ADD COLUMN IF NOT EXISTS dead_lettered_at timestamptz;
ALTER TABLE projection_outbox ADD COLUMN IF NOT EXISTS dead_letter_reason text;
