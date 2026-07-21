CREATE TABLE IF NOT EXISTS provider_connect_sessions (
  id uuid PRIMARY KEY DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  subject text NOT NULL CHECK (position('#' in subject) > 1),
  provider text NOT NULL CHECK (provider = 'plaid'),
  token_hash text NOT NULL CHECK (length(token_hash) = 64),
  expires_at timestamptz NOT NULL,
  consumed_at timestamptz,
  created_at timestamptz NOT NULL DEFAULT now(),
  UNIQUE (deployment_id, token_hash)
);

CREATE INDEX IF NOT EXISTS provider_connect_sessions_lookup
  ON provider_connect_sessions (deployment_id, subject, provider, token_hash)
  WHERE consumed_at IS NULL;
