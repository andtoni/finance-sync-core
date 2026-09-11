-- Durable, metadata-only completion state. Credentials remain exclusively in Infisical.
ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS completion_state text NOT NULL DEFAULT 'pending',
  ADD COLUMN IF NOT EXISTS completion_item_id text,
  ADD COLUMN IF NOT EXISTS completion_lease_until timestamptz,
  ADD COLUMN IF NOT EXISTS completion_claim_version integer NOT NULL DEFAULT 0;
ALTER TABLE provider_connect_sessions
  ADD CONSTRAINT provider_connect_sessions_completion_state_check
  CHECK (completion_state IN ('pending', 'exchange_in_progress', 'credential_persisted', 'completed', 'uncertain'));
-- Completion identity is the session token, not Item ID: an Item may be relinked
-- in later sessions and must not be globally rejected by an Item index.
