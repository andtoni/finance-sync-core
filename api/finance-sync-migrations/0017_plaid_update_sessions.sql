-- Bind each Plaid Link continuation to its flow and, for update mode, the
-- already-active Item. Existing sessions are the original new-item flow.
ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS flow_kind text NOT NULL DEFAULT 'new';

ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS provider_connection_id text;

ALTER TABLE provider_connect_sessions
  ADD COLUMN IF NOT EXISTS provider_generation integer;

ALTER TABLE provider_connect_sessions
  ADD CONSTRAINT provider_connect_sessions_flow_binding_check
  CHECK (
    (flow_kind = 'new' AND provider_connection_id IS NULL AND provider_generation IS NULL)
    OR (flow_kind = 'update' AND provider_connection_id IS NOT NULL AND length(provider_connection_id) BETWEEN 1 AND 512
      AND provider_generation IS NOT NULL AND provider_generation >= 1)
  );
