-- Runtime outbox connection identities append the provider account id to the
-- durable UUID (`<connection-uuid>:<provider-account-id>`). Migration 0011
-- handled the UUID form but therefore left these active runtime rows fenced at
-- stale Wealthfolio access generations.
WITH latest_generation AS (
  SELECT paa.deployment_id, paa.local_owner_id,
    max(paa.receiver_access_generation) AS receiver_access_generation
  FROM provider_account_admissions paa
  JOIN provider_connections pc
    ON pc.deployment_id = paa.deployment_id
    AND pc.local_owner_id = paa.local_owner_id
    AND pc.connection_id = paa.connection_id
  WHERE paa.revoked_at IS NULL
    AND pc.disabled_at IS NULL
    AND pc.provider_generation = paa.provider_generation
  GROUP BY paa.deployment_id, paa.local_owner_id
)
UPDATE projection_outbox po
SET payload = jsonb_set(
      po.payload,
      '{accessGeneration}',
      to_jsonb(latest.receiver_access_generation),
      false
    ),
    state = 'pending',
    attempts = 0,
    last_error = NULL,
    dead_lettered_at = NULL,
    dead_letter_reason = NULL
FROM latest_generation latest
WHERE po.deployment_id = latest.deployment_id
  AND po.local_owner_id = latest.local_owner_id
  AND po.state IN ('pending', 'uncertain', 'dead_letter')
  AND (po.payload->>'accessGeneration')::bigint < latest.receiver_access_generation
  AND (po.state <> 'dead_letter' OR po.dead_letter_reason = 'BRIDGE_HTTP_REJECTED')
  AND EXISTS (
    SELECT 1
    FROM provider_account_admissions active
    JOIN provider_connections pc
      ON pc.deployment_id = active.deployment_id
      AND pc.local_owner_id = active.local_owner_id
      AND pc.connection_id = active.connection_id
    WHERE active.deployment_id = po.deployment_id
      AND active.local_owner_id = po.local_owner_id
      AND po.connection_id LIKE active.connection_id::text || ':%'
      AND active.wealthfolio_account_id = po.payload->>'accountId'
      AND active.revoked_at IS NULL
      AND pc.disabled_at IS NULL
      AND active.provider_generation = po.provider_generation
      AND pc.provider_generation = active.provider_generation
  );
