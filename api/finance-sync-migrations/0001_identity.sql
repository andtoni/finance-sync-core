CREATE TABLE IF NOT EXISTS provider_principals (
 deployment_id text NOT NULL, local_owner_id text NOT NULL, provider text NOT NULL, provider_principal_id uuid NOT NULL,
 provider_subject_id text NOT NULL, created_at timestamptz NOT NULL DEFAULT now(),
 PRIMARY KEY (deployment_id, local_owner_id, provider_principal_id),
 UNIQUE (deployment_id, local_owner_id, provider, provider_principal_id),
 UNIQUE (deployment_id, local_owner_id, provider, provider_subject_id)
);
CREATE TABLE IF NOT EXISTS connection_incarnations (
 deployment_id text NOT NULL, local_owner_id text NOT NULL, connection_incarnation_id uuid NOT NULL, provider_principal_id uuid NOT NULL,
 provider_connection_id text NOT NULL, incarnation integer NOT NULL CHECK (incarnation > 0), created_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz,
 PRIMARY KEY (deployment_id, local_owner_id, connection_incarnation_id),
 UNIQUE (deployment_id, local_owner_id, provider_principal_id, provider_connection_id, incarnation),
 FOREIGN KEY (deployment_id, local_owner_id, provider_principal_id) REFERENCES provider_principals(deployment_id, local_owner_id, provider_principal_id)
);
CREATE TABLE IF NOT EXISTS account_incarnations (
 deployment_id text NOT NULL, local_owner_id text NOT NULL, account_incarnation_id uuid NOT NULL, connection_incarnation_id uuid NOT NULL,
 provider_account_id text NOT NULL, incarnation integer NOT NULL CHECK (incarnation > 0), created_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz,
 PRIMARY KEY (deployment_id, local_owner_id, account_incarnation_id),
 UNIQUE (deployment_id, local_owner_id, connection_incarnation_id, provider_account_id, incarnation),
 FOREIGN KEY (deployment_id, local_owner_id, connection_incarnation_id) REFERENCES connection_incarnations(deployment_id, local_owner_id, connection_incarnation_id)
);
CREATE TABLE IF NOT EXISTS resource_incarnations (
 deployment_id text NOT NULL, local_owner_id text NOT NULL, source_resource_incarnation_id uuid NOT NULL, account_incarnation_id uuid NOT NULL,
 resource_kind text NOT NULL, provider_resource_id text NOT NULL, incarnation integer NOT NULL CHECK (incarnation > 0), created_at timestamptz NOT NULL DEFAULT now(), ended_at timestamptz,
 PRIMARY KEY (deployment_id, local_owner_id, source_resource_incarnation_id),
 UNIQUE (deployment_id, local_owner_id, account_incarnation_id, resource_kind, provider_resource_id, incarnation),
 FOREIGN KEY (deployment_id, local_owner_id, account_incarnation_id) REFERENCES account_incarnations(deployment_id, local_owner_id, account_incarnation_id)
);
