-- A continuation reserves one exact, pre-provisioned writer slot. The slot is
-- non-secret metadata and is consumed atomically with its continuation token.
ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS selected_secret_reference text;

-- Old sessions cannot safely be completed because they did not bind a slot.
DELETE FROM provider_connect_sessions WHERE selected_secret_reference IS NULL;
ALTER TABLE provider_connect_sessions
  ALTER COLUMN selected_secret_reference SET NOT NULL;
ALTER TABLE provider_connect_sessions
  ADD CONSTRAINT provider_connect_sessions_selected_secret_reference_check
  CHECK (selected_secret_reference ~ '^[A-Za-z0-9_-]{1,128}$');
ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS released_at timestamptz;

-- Do not issue two live continuation tokens for the same finite writer slot.
-- Consumption does not release the mutex: completion retains it through the
-- write/onboarding transaction, then explicitly releases it on either result.
CREATE UNIQUE INDEX IF NOT EXISTS provider_connect_sessions_live_slot_idx
  ON provider_connect_sessions (deployment_id, provider, selected_secret_reference)
  WHERE released_at IS NULL;

-- Defense in depth: one active Plaid connection owns an exact slot deployment-wide.
-- This fails closed if legacy rows already violate the invariant.
CREATE UNIQUE INDEX IF NOT EXISTS provider_connections_active_plaid_slot_idx
  ON provider_connections (deployment_id, secret_reference)
  WHERE provider = 'plaid' AND disabled_at IS NULL;
