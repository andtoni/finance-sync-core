import { assertIssuerQualifiedSubject } from './projection/bridge-client'
import type { FinanceSyncSql } from './storage/pg'

export interface ObservationQuarantineRequest {
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly reason: string
}

export interface ObservationQuarantineResult {
  readonly quarantineId: string
  readonly observationId: string
  readonly resourceId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly alreadyQuarantined: boolean
  readonly suppressedOutboxRows: number
  readonly deliveredOutboxRows: number
  readonly uncertainOutboxRows: number
  readonly createdAt: string
}

export class ObservationQuarantineError extends Error {
  constructor(readonly status: number, message: string) { super(message) }
}

interface RawObservationIdentity {
  readonly observation_id: string
  readonly transaction_id: string | null
}

interface QuarantineRow {
  readonly quarantine_id: string
  readonly created_at: string
}

interface OutboxStateCount {
  readonly state: 'pending' | 'uncertain' | 'delivered' | 'dead_letter' | 'suppressed'
  readonly count: number
}

export function createObservationQuarantineControl(sql: FinanceSyncSql, deploymentId: string) {
  return {
    async create(rawSubject: string, request: ObservationQuarantineRequest): Promise<ObservationQuarantineResult> {
      let subject: string
      try { subject = assertIssuerQualifiedSubject(rawSubject) }
      catch { throw new ObservationQuarantineError(400, 'invalid subject') }

      return sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        const observations = await tx<RawObservationIdentity[]>`
          SELECT observation_id::text, payload->>'transactionId' AS transaction_id
          FROM raw_observations
          WHERE deployment_id = ${deploymentId}
            AND local_owner_id = ${subject}
            AND source_resource_incarnation_id = ${request.sourceResourceIncarnationId}::uuid
            AND provider_version = ${request.providerVersion}
            AND semantic_hash = ${request.semanticHash}
            AND hash_specification_version = ${request.hashSpecificationVersion}
        `
        if (observations.length !== 1) {
          throw new ObservationQuarantineError(404, 'exact observation was not found')
        }

        const observation = observations[0]
        const transactionId = observation.transaction_id?.trim() || request.semanticHash
        const resourceId = `${deploymentId}:${request.sourceResourceIncarnationId}:${transactionId}`
        const inserted = await tx<QuarantineRow[]>`
          INSERT INTO observation_quarantines (
            deployment_id, local_owner_id, source_resource_incarnation_id,
            observation_id, provider_version, semantic_hash,
            hash_specification_version, reason, actor_subject
          ) VALUES (
            ${deploymentId}, ${subject}, ${request.sourceResourceIncarnationId}::uuid,
            ${observation.observation_id}::uuid, ${request.providerVersion}, ${request.semanticHash},
            ${request.hashSpecificationVersion}, ${request.reason}, ${subject}
          )
          ON CONFLICT (
            deployment_id, local_owner_id, source_resource_incarnation_id,
            provider_version, semantic_hash, hash_specification_version
          ) DO NOTHING
          RETURNING quarantine_id::text, created_at::text
        `
        const existing = inserted.length === 0
          ? await tx<QuarantineRow[]>`
            SELECT quarantine_id::text, created_at::text
            FROM observation_quarantines
            WHERE deployment_id = ${deploymentId}
              AND local_owner_id = ${subject}
              AND source_resource_incarnation_id = ${request.sourceResourceIncarnationId}::uuid
              AND provider_version = ${request.providerVersion}
              AND semantic_hash = ${request.semanticHash}
              AND hash_specification_version = ${request.hashSpecificationVersion}
          `
          : inserted
        if (existing.length !== 1) throw new ObservationQuarantineError(409, 'quarantine state is ambiguous')

        const states = await tx<OutboxStateCount[]>`
          SELECT state, count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND local_owner_id = ${subject}
            AND payload->>'resourceId' = ${resourceId}
          GROUP BY state
        `
        const stateCount = (state: OutboxStateCount['state']): number => states.find(row => row.state === state)?.count ?? 0
        const suppressed = await tx<{ count: number }[]>`
          WITH changed AS (
            UPDATE projection_outbox
            SET state = 'suppressed', last_error = 'OBSERVATION_QUARANTINED'
            WHERE deployment_id = ${deploymentId}
              AND local_owner_id = ${subject}
              AND payload->>'resourceId' = ${resourceId}
              AND state IN ('pending', 'uncertain')
            RETURNING id
          )
          SELECT count(*)::integer AS count FROM changed
        `
        return {
          quarantineId: existing[0].quarantine_id,
          observationId: observation.observation_id,
          resourceId,
          providerVersion: request.providerVersion,
          semanticHash: request.semanticHash,
          alreadyQuarantined: inserted.length === 0,
          suppressedOutboxRows: suppressed[0]?.count ?? 0,
          deliveredOutboxRows: stateCount('delivered'),
          uncertainOutboxRows: stateCount('uncertain'),
          createdAt: existing[0].created_at,
        }
      })
    },
  }
}
