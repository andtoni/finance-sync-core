-- Plaid Transactions Sync cursors belong to the Plaid Item, not an account.
CREATE TABLE IF NOT EXISTS plaid_item_checkpoints (
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  provider_connection_id text NOT NULL,
  provider_generation integer NOT NULL CHECK (provider_generation > 0),
  cursor text,
  updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, local_owner_id, provider_connection_id, provider_generation)
);
CREATE INDEX IF NOT EXISTS plaid_item_checkpoints_lookup_idx
  ON plaid_item_checkpoints (deployment_id, local_owner_id, provider_connection_id, provider_generation);
DO $$ BEGIN
 IF EXISTS (SELECT 1 FROM pg_roles WHERE rolname = 'finance_sync_runtime') THEN
  GRANT SELECT, INSERT, UPDATE ON plaid_item_checkpoints TO finance_sync_runtime;
 END IF;
END $$;
