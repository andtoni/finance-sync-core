-- Durable native provider onboarding. Provider account identity is immutable;
-- admission, Wealthfolio mapping, and baseline eligibility are explicit gates.
CREATE TABLE IF NOT EXISTS provider_connections (
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  connection_id uuid NOT NULL DEFAULT gen_random_uuid(),
  provider_principal_id uuid NOT NULL,
  provider text NOT NULL CHECK (provider IN ('investec','mercury','plaid','snaptrade')),
  provider_connection_id text NOT NULL,
  secret_reference text NOT NULL,
  provider_generation integer NOT NULL DEFAULT 1 CHECK (provider_generation > 0),
  configuration jsonb NOT NULL DEFAULT '{}'::jsonb,
  state text NOT NULL DEFAULT 'discovered' CHECK (state IN ('discovered','active','disabled')),
  created_at timestamptz NOT NULL DEFAULT now(),
  updated_at timestamptz NOT NULL DEFAULT now(),
  disabled_at timestamptz,
  PRIMARY KEY (deployment_id, local_owner_id, connection_id),
  UNIQUE (deployment_id, local_owner_id, provider, provider_connection_id),
  UNIQUE (deployment_id, provider, provider_connection_id),
  FOREIGN KEY (deployment_id, local_owner_id, provider_principal_id)
    REFERENCES provider_principals(deployment_id, local_owner_id, provider_principal_id),
  CHECK (jsonb_typeof(configuration) = 'object'),
  CHECK (secret_reference <> '')
);
CREATE INDEX IF NOT EXISTS provider_connections_subject_idx
  ON provider_connections (deployment_id, local_owner_id, provider, state);

CREATE TABLE IF NOT EXISTS discovered_provider_accounts (
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  connection_id uuid NOT NULL,
  provider_account_id text NOT NULL,
  account_incarnation_id uuid NOT NULL,
  source_resource_incarnation_id uuid NOT NULL,
  provider_name text NOT NULL,
  provider_type text NOT NULL,
  currency text NOT NULL,
  mask text,
  assets jsonb NOT NULL DEFAULT '[]'::jsonb,
  discovery_generation integer NOT NULL CHECK (discovery_generation > 0),
  discovered_at timestamptz NOT NULL DEFAULT now(),
  PRIMARY KEY (deployment_id, local_owner_id, connection_id, provider_account_id),
  FOREIGN KEY (deployment_id, local_owner_id, connection_id)
    REFERENCES provider_connections(deployment_id, local_owner_id, connection_id),
  FOREIGN KEY (deployment_id, local_owner_id, account_incarnation_id)
    REFERENCES account_incarnations(deployment_id, local_owner_id, account_incarnation_id),
  FOREIGN KEY (deployment_id, local_owner_id, source_resource_incarnation_id)
    REFERENCES resource_incarnations(deployment_id, local_owner_id, source_resource_incarnation_id),
  CHECK (provider_account_id <> '' AND provider_name <> '' AND provider_type <> '' AND currency <> ''),
  CHECK (jsonb_typeof(assets) = 'array')
);

CREATE TABLE IF NOT EXISTS provider_account_admissions (
  deployment_id text NOT NULL,
  local_owner_id text NOT NULL,
  connection_id uuid NOT NULL,
  provider_account_id text NOT NULL,
  wealthfolio_account_id text NOT NULL,
  receiver_access_generation integer NOT NULL CHECK (receiver_access_generation > 0),
  asset_mappings jsonb NOT NULL DEFAULT '[]'::jsonb,
  asset_mapping_digest text NOT NULL,
  admitted_at timestamptz NOT NULL DEFAULT now(),
  revoked_at timestamptz,
  baseline_state text NOT NULL DEFAULT 'pending' CHECK (baseline_state IN ('pending','verified','failed')),
  baseline_verified_at timestamptz,
  baseline_fingerprint text,
  PRIMARY KEY (deployment_id, local_owner_id, connection_id, provider_account_id),
  FOREIGN KEY (deployment_id, local_owner_id, connection_id, provider_account_id)
    REFERENCES discovered_provider_accounts(deployment_id, local_owner_id, connection_id, provider_account_id),
  CHECK (wealthfolio_account_id <> ''),
  CHECK (jsonb_typeof(asset_mappings) = 'array'),
  CHECK (asset_mapping_digest ~ '^sha256:[a-f0-9]{64}$'),
  CHECK ((baseline_state = 'verified') = (baseline_verified_at IS NOT NULL)),
  CHECK (baseline_state <> 'verified' OR baseline_fingerprint IS NOT NULL)
);
CREATE UNIQUE INDEX IF NOT EXISTS provider_account_admissions_active_mapping_idx
  ON provider_account_admissions (deployment_id, local_owner_id, wealthfolio_account_id)
  WHERE revoked_at IS NULL;
CREATE INDEX IF NOT EXISTS provider_account_admissions_runtime_idx
  ON provider_account_admissions (deployment_id, connection_id)
  WHERE revoked_at IS NULL AND baseline_state = 'verified';

-- Prevent stale workers from projecting after disconnect/relink. The trigger
-- also makes provider generation changes atomic with state transitions.
CREATE OR REPLACE FUNCTION fence_provider_connection_generation() RETURNS trigger AS $$
BEGIN
  IF NEW.provider_generation < OLD.provider_generation THEN
    RAISE EXCEPTION 'provider generation cannot decrease';
  END IF;
  IF NEW.provider <> OLD.provider OR NEW.provider_connection_id <> OLD.provider_connection_id
     OR NEW.provider_principal_id <> OLD.provider_principal_id OR NEW.local_owner_id <> OLD.local_owner_id
     OR NEW.deployment_id <> OLD.deployment_id THEN
    RAISE EXCEPTION 'provider connection identity is immutable';
  END IF;
  IF NEW.secret_reference <> OLD.secret_reference AND NEW.provider_generation = OLD.provider_generation THEN
    RAISE EXCEPTION 'provider credential changes require a new provider generation';
  END IF;
  IF NEW.state = 'disabled' AND OLD.state <> 'disabled' AND NEW.provider_generation = OLD.provider_generation THEN
    RAISE EXCEPTION 'provider disable requires a new provider generation';
  END IF;
  NEW.updated_at := now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;
DROP TRIGGER IF EXISTS provider_connection_generation_fence ON provider_connections;
CREATE TRIGGER provider_connection_generation_fence
  BEFORE UPDATE ON provider_connections
  FOR EACH ROW EXECUTE FUNCTION fence_provider_connection_generation();
