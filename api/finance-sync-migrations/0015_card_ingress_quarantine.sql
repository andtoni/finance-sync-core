-- Append-only audit for every malformed, unsupported, declined, or unresolved Card Code ingress.
CREATE TABLE IF NOT EXISTS card_ingress_quarantines (
  quarantine_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  source text NOT NULL,
  reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2048),
  envelope jsonb NOT NULL,
  created_at timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS card_ingress_quarantines_lookup_idx
  ON card_ingress_quarantines (deployment_id, created_at);
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON card_ingress_quarantines TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON card_ingress_quarantines FROM finance_sync_runtime;
 END IF;
END $$;
CREATE OR REPLACE FUNCTION reject_card_ingress_quarantine_mutation() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'card ingress quarantines are append-only'; END $$;
DROP TRIGGER IF EXISTS card_ingress_quarantines_append_only ON card_ingress_quarantines;
CREATE TRIGGER card_ingress_quarantines_append_only BEFORE UPDATE OR DELETE ON card_ingress_quarantines FOR EACH ROW EXECUTE FUNCTION reject_card_ingress_quarantine_mutation();
