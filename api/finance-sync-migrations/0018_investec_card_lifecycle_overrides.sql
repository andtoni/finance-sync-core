-- Durable user attestations for Investec card authorization -> posted settlement
-- links where the provider omitted a lifecycle ID and same-currency amounts differ.
-- Raw observations and provider identity remain immutable; this table is a
-- separate, append-only operator exception consumed by the lifecycle matcher.
CREATE TABLE IF NOT EXISTS investec_card_lifecycle_overrides (
  override_id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  source_resource_incarnation_id uuid NOT NULL,
  pending_transaction_id text NOT NULL CHECK (length(btrim(pending_transaction_id)) BETWEEN 1 AND 512),
  posted_transaction_id text NOT NULL CHECK (length(btrim(posted_transaction_id)) BETWEEN 1 AND 512),
  actor_subject text NOT NULL CHECK (length(btrim(actor_subject)) BETWEEN 1 AND 512),
  reason text NOT NULL CHECK (length(btrim(reason)) BETWEEN 1 AND 2048),
  created_at timestamptz NOT NULL DEFAULT now(),
  CHECK (pending_transaction_id <> posted_transaction_id),
  FOREIGN KEY (deployment_id, local_owner_id, source_resource_incarnation_id)
    REFERENCES resource_incarnations(deployment_id, local_owner_id, source_resource_incarnation_id),
  UNIQUE (deployment_id, local_owner_id, source_resource_incarnation_id, pending_transaction_id),
  UNIQUE (deployment_id, local_owner_id, source_resource_incarnation_id, posted_transaction_id)
);

-- A provider fact may participate in only one manual lifecycle pair, regardless
-- of whether it was submitted as the pending or posted side. Transaction-scoped
-- advisory locks make the cross-column check safe under concurrent inserts.
CREATE OR REPLACE FUNCTION reserve_investec_card_lifecycle_override_facts()
RETURNS trigger LANGUAGE plpgsql AS $$
DECLARE
  pending_lock bigint;
  posted_lock bigint;
BEGIN
  pending_lock := hashtextextended(
    NEW.deployment_id || chr(31) || NEW.local_owner_id || chr(31)
      || NEW.source_resource_incarnation_id::text || chr(31) || NEW.pending_transaction_id,
    0
  );
  posted_lock := hashtextextended(
    NEW.deployment_id || chr(31) || NEW.local_owner_id || chr(31)
      || NEW.source_resource_incarnation_id::text || chr(31) || NEW.posted_transaction_id,
    0
  );
  PERFORM pg_advisory_xact_lock(LEAST(pending_lock, posted_lock));
  IF pending_lock <> posted_lock THEN
    PERFORM pg_advisory_xact_lock(GREATEST(pending_lock, posted_lock));
  END IF;

  IF EXISTS (
    SELECT 1
    FROM investec_card_lifecycle_overrides existing
    WHERE existing.deployment_id = NEW.deployment_id
      AND existing.local_owner_id = NEW.local_owner_id
      AND existing.source_resource_incarnation_id = NEW.source_resource_incarnation_id
      AND (
        existing.pending_transaction_id = ANY (
          ARRAY[NEW.pending_transaction_id, NEW.posted_transaction_id]
        )
        OR existing.posted_transaction_id = ANY (
          ARRAY[NEW.pending_transaction_id, NEW.posted_transaction_id]
        )
      )
  ) THEN
    RETURN NULL;
  END IF;
  RETURN NEW;
END
$$;

DROP TRIGGER IF EXISTS investec_card_lifecycle_overrides_one_to_one
  ON investec_card_lifecycle_overrides;
CREATE TRIGGER investec_card_lifecycle_overrides_one_to_one
  BEFORE INSERT ON investec_card_lifecycle_overrides
  FOR EACH ROW EXECUTE FUNCTION reserve_investec_card_lifecycle_override_facts();

CREATE INDEX IF NOT EXISTS investec_card_lifecycle_overrides_lookup_idx
  ON investec_card_lifecycle_overrides (
    deployment_id, local_owner_id, source_resource_incarnation_id,
    pending_transaction_id, posted_transaction_id
  );

DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT ON investec_card_lifecycle_overrides TO finance_sync_runtime;
  REVOKE UPDATE, DELETE ON investec_card_lifecycle_overrides FROM finance_sync_runtime;
 END IF;
END $$;

CREATE OR REPLACE FUNCTION reject_investec_card_lifecycle_override_mutation()
RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
  RAISE EXCEPTION 'Investec card lifecycle overrides are append-only';
END
$$;

DROP TRIGGER IF EXISTS investec_card_lifecycle_overrides_append_only
  ON investec_card_lifecycle_overrides;
CREATE TRIGGER investec_card_lifecycle_overrides_append_only
  BEFORE UPDATE OR DELETE ON investec_card_lifecycle_overrides
  FOR EACH ROW EXECUTE FUNCTION reject_investec_card_lifecycle_override_mutation();
