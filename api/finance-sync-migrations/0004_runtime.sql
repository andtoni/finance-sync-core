CREATE TABLE IF NOT EXISTS sync_checkpoints (
  deployment_id text NOT NULL, connection_id text NOT NULL, provider text NOT NULL,
  cursor text, updated_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, connection_id)
);
CREATE TABLE IF NOT EXISTS projection_outbox (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(), deployment_id text NOT NULL, local_owner_id text NOT NULL,
  connection_id text NOT NULL, idempotency_key text NOT NULL, payload jsonb NOT NULL,
  state text NOT NULL DEFAULT 'pending' CHECK (state IN ('pending','delivered')),
  attempts integer NOT NULL DEFAULT 0, last_error text, delivered_at timestamptz, created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (deployment_id, idempotency_key)
);
CREATE INDEX IF NOT EXISTS projection_outbox_pending_idx ON projection_outbox (deployment_id, created_at) WHERE state = 'pending';
