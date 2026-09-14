CREATE TABLE IF NOT EXISTS investec_terminal_revision_recovery_intents (
  intent_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  outbox_id uuid NOT NULL REFERENCES projection_outbox(id) ON DELETE RESTRICT,
  expected_payload_hash char(64) NOT NULL,
  reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2048),
  actor_subject text NOT NULL,
  mode text NOT NULL CHECK (mode IN ('dry_run','apply')),
  state text NOT NULL DEFAULT 'planned' CHECK (state IN ('planned','claimed','delivered','suppressed','failed')),
  ordinary_operation jsonb,
  bound_lifecycle_operation jsonb,
  bound_lifecycle_receipt jsonb,
  new_operation_idempotency_key text,
  new_receipt jsonb,
  original_last_error text,
  claim_owner uuid,
  claim_lease_until timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  completed_at timestamptz,
  UNIQUE (deployment_id, local_owner_id, outbox_id, expected_payload_hash, mode)
);
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS ordinary_operation jsonb;
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS bound_lifecycle_operation jsonb;
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS bound_lifecycle_receipt jsonb;
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS original_last_error text;
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS claim_owner uuid;
ALTER TABLE investec_terminal_revision_recovery_intents ADD COLUMN IF NOT EXISTS claim_lease_until timestamptz;
ALTER TABLE investec_terminal_revision_recovery_intents DROP CONSTRAINT IF EXISTS investec_terminal_revision_recovery_intents_state_check;
ALTER TABLE investec_terminal_revision_recovery_intents ADD CONSTRAINT investec_terminal_revision_recovery_intents_state_check CHECK (state IN ('planned','claimed','delivered','suppressed','failed'));
CREATE TABLE IF NOT EXISTS investec_terminal_revision_recovery_audit (
  audit_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  intent_id uuid NOT NULL REFERENCES investec_terminal_revision_recovery_intents(intent_id),
  outbox_id uuid NOT NULL REFERENCES projection_outbox(id) ON DELETE RESTRICT,
  new_operation_idempotency_key text,
  new_receipt jsonb,
  actor_subject text NOT NULL,
  reason text NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE OR REPLACE FUNCTION reject_terminal_revision_audit_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'terminal revision recovery audits are append-only'; END $$;
CREATE TRIGGER terminal_revision_recovery_audit_append_only BEFORE UPDATE OR DELETE ON investec_terminal_revision_recovery_audit FOR EACH ROW EXECUTE FUNCTION reject_terminal_revision_audit_mutation();
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT, UPDATE ON investec_terminal_revision_recovery_intents TO finance_sync_runtime;
  GRANT SELECT, INSERT ON investec_terminal_revision_recovery_audit TO finance_sync_runtime;
  REVOKE DELETE ON investec_terminal_revision_recovery_intents FROM finance_sync_runtime;
  REVOKE UPDATE, DELETE ON investec_terminal_revision_recovery_audit FROM finance_sync_runtime;
 END IF;
END $$;
