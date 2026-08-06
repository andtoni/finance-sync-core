-- Preserve the exact provider object separately from the normalized canonical payload.
-- `payload` remains the deterministic fact used for identity/projection; `raw_payload`
-- is immutable evidence for reconciliation, replay, and operator repair.
ALTER TABLE raw_observations
  ADD COLUMN IF NOT EXISTS raw_payload jsonb;
