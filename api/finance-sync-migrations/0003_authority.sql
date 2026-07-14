-- G2: Authority epochs for domain-specific provider publishing.
-- One active primary epoch per account+domain at any instant.
CREATE EXTENSION IF NOT EXISTS btree_gist;

CREATE TABLE IF NOT EXISTS authority_epochs (
  id uuid DEFAULT gen_random_uuid(),
  deployment_id text NOT NULL,
  canonical_account_id text NOT NULL,
  resource_domain text NOT NULL CHECK (
    resource_domain IN ('cash_ledger','balance','liability','brokerage_activity','holding','security','cost_basis','order')
  ),
  source_account_incarnation_id uuid NOT NULL,
  authority_time_selector text NOT NULL,
  authority_time_selector_version integer NOT NULL DEFAULT 1,
  role text NOT NULL CHECK (role IN ('primary','supplemental')),
  state text NOT NULL DEFAULT 'proposed' CHECK (state IN ('proposed','activating','active','frozen','closed','rolled_back')),
  predecessor_epoch_id uuid,
  late_correction_policy text NOT NULL DEFAULT 'revision_only' CHECK (late_correction_policy IN ('revision_only','full')),
  valid_range tstzrange NOT NULL,
  approved_by text,
  approved_at timestamptz,
  activated_at timestamptz,
  closed_at timestamptz,
  reason text,
  PRIMARY KEY (deployment_id, id),
  EXCLUDE USING gist (
    deployment_id WITH =,
    canonical_account_id WITH =,
    resource_domain WITH =,
    valid_range WITH &&
  ) WHERE (role = 'primary' AND state IN ('activating','active'))
);
CREATE INDEX IF NOT EXISTS idx_authority_epochs_active
  ON authority_epochs(deployment_id, canonical_account_id, resource_domain)
  WHERE state = 'active';
