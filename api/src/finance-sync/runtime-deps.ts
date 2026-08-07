import { createHash } from 'node:crypto'
import type { FinanceSyncSql } from './storage/pg'
import type { ObservationOutput } from './probes/plaid/plaid-adapter'
import { investecPrimeSaverSupersessionKey } from './probes/investec/investec-adapter'
import {
  createProviderTransportFactories,
  parseRuntimeConnections,
  runRuntimeProvider,
  type ProviderRuntimeEnv,
  type ProviderTransportFactories,
} from './provider-runtime'
import {
  assertIssuerQualifiedSubject,
  bridgeOperationToWire,
  currentStateToBridge,
  providerObservationToBridge,
  validateBridgeReceipt,
  type BridgeDisclosureOperation,
} from './projection/bridge-client'
import type { PersistEnqueueOptions, RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import { readExclusiveInput } from './secret-input'
import { loadDurableRuntimeConnections, type ProviderSecretResolver } from './durable-connections'
import { unavailableProviderSecretStore } from './connect-control'
import { assembleCurrentState } from './projection/current-state'
import {
  createFixtureRuntime,
  fixtureAuthorityRows,
  fixtureModeEnabled,
  FIXTURE_OWNER_SUBJECT,
} from './fixture-runtime'

const MAX_PROJECTION_DELIVERY_ATTEMPTS = 3
const MAX_PROJECTION_BATCH = 25
const PLAID_HOSTS = new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com'])

async function ensureFixtureAuthority(sql: FinanceSyncSql, deploymentId: string): Promise<void> {
  for (const row of fixtureAuthorityRows) {
    await sql`
      INSERT INTO provider_principals (
        deployment_id, local_owner_id, provider, provider_principal_id, provider_subject_id
      ) VALUES (
        ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.provider}, ${row.providerPrincipalId}::uuid, ${row.providerConnectionId}
      ) ON CONFLICT DO NOTHING
    `
    await sql`
      INSERT INTO connection_incarnations (
        deployment_id, local_owner_id, connection_incarnation_id, provider_principal_id,
        provider_connection_id, incarnation
      ) VALUES (
        ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.connectionIncarnationId}::uuid,
        ${row.providerPrincipalId}::uuid, ${row.providerConnectionId}, 1
      ) ON CONFLICT DO NOTHING
    `
    await sql`
      INSERT INTO account_incarnations (
        deployment_id, local_owner_id, account_incarnation_id, connection_incarnation_id,
        provider_account_id, incarnation
      ) VALUES (
        ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.accountIncarnationId}::uuid,
        ${row.connectionIncarnationId}::uuid, ${row.accountId}, 1
      ) ON CONFLICT DO NOTHING
    `
    await sql`
      INSERT INTO resource_incarnations (
        deployment_id, local_owner_id, source_resource_incarnation_id, account_incarnation_id,
        resource_kind, provider_resource_id, incarnation
      ) VALUES (
        ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.sourceResourceIncarnationId}::uuid,
        ${row.accountIncarnationId}::uuid, 'fixture', ${row.accountId}, 1
      ) ON CONFLICT DO NOTHING
    `
  }
}

export interface SyncCycleAuditEntry {
  readonly deploymentId: string
  readonly connectionId: string
  readonly outcome: 'delivered' | 'retry' | 'dead_letter'
  readonly count: number
}

interface RuntimeEnv extends ProviderRuntimeEnv {
  readonly FINANCE_SYNC_BRIDGE_URL?: string
  readonly FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS?: string
  readonly FINANCE_SYNC_BRIDGE_RECEIPT_URL?: string
  readonly FINANCE_SYNC_BRIDGE_TOKEN?: string
  readonly FINANCE_SYNC_BRIDGE_TOKEN_FILE?: string
}

function requiredEndpoint(raw: string | undefined, name: string, allowedHosts: ReadonlySet<string>, allowFixtureHttp = false): URL {
  if (!raw) throw new Error(`${name} is required for configured connections`)
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`${name} must be a valid URL`)
  }
  const fixtureLoopback = allowFixtureHttp && ['127.0.0.1', 'localhost'].includes(url.hostname.toLowerCase())
  const fixtureHttp = fixtureLoopback && url.protocol === 'http:'
  if (allowFixtureHttp && !fixtureLoopback) throw new Error(`${name} must target a loopback host in fixture mode`)
  if ((!fixtureHttp && url.protocol !== 'https:') || url.username || url.password) throw new Error(`${name} must be an HTTPS URL without credentials`)
  if (!allowedHosts.has(url.hostname.toLowerCase())) throw new Error(`${name} host is not allowed`)
  return url
}

function bridgeHosts(env: RuntimeEnv): ReadonlySet<string> {
  const hosts = env.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS?.split(',').map(value => value.trim().toLowerCase()).filter(Boolean) ?? []
  if (hosts.length === 0) throw new Error('FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS is required for configured connections')
  return new Set(hosts)
}

function observationPayload(value: unknown): Readonly<Record<string, unknown>> | null {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Readonly<Record<string, unknown>>
}

interface PrimeSaverDuplicateRow {
  readonly payload: unknown
  readonly raw_payload: unknown
  readonly provider_version: string
}

function primeSaverProviderReference(payload: Readonly<Record<string, unknown>>): string | undefined {
  const explicitReference = normalizedText(payload.reference)
  if (explicitReference) return explicitReference
  const description = normalizedText(payload.description)
  return description.match(/\b(?:MBP|UBP)\d{6,}\b/)?.[0]
}

function hasPrimeSaverSyntheticIdentity(payload: Readonly<Record<string, unknown>>): boolean {
  return typeof payload.primeSaverIdentityKey === 'string' && payload.primeSaverIdentityKey.trim().length > 0
}

function decimalMagnitude(value: unknown): string | undefined {
  const text = typeof value === 'number' && Number.isFinite(value) ? String(value)
    : typeof value === 'string' && /^[+-]?\d+(?:\.\d+)?$/.test(value.trim()) ? value.trim()
      : undefined
  if (!text) return undefined
  const unsigned = text.replace(/^[+-]/, '')
  const [whole, fraction = ''] = unsigned.split('.')
  const normalizedWhole = whole.replace(/^0+(?=\d)/, '') || '0'
  const normalizedFraction = fraction.replace(/0+$/, '')
  return normalizedFraction ? `${normalizedWhole}.${normalizedFraction}` : normalizedWhole
}

function primeSaverTransactionDate(payload: Readonly<Record<string, unknown>>): string | undefined {
  for (const value of [payload.transactionDate, payload.date]) {
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}

function primeSaverDirection(payload: Readonly<Record<string, unknown>>): string | undefined {
  const direct = normalizedText(payload.type)
  if (direct === 'CREDIT' || direct === 'DEBIT') return direct
  const transactionType = normalizedText(payload.transactionType)
  return transactionType === 'CREDIT' || transactionType === 'DEBIT' ? transactionType : undefined
}

function primeSaverDuplicatePair(
  weak: Readonly<Record<string, unknown>>,
  strong: Readonly<Record<string, unknown>>,
): boolean {
  if (!hasPrimeSaverSyntheticIdentity(weak) || !hasPrimeSaverSyntheticIdentity(strong)
    || primeSaverProviderReference(weak) || !primeSaverProviderReference(strong)) return false
  const weakDirection = primeSaverDirection(weak)
  const strongDirection = primeSaverDirection(strong)
  if (!weakDirection || !strongDirection
    || weak.accountId !== strong.accountId
    || primeSaverTransactionDate(weak) !== primeSaverTransactionDate(strong)
    || weakDirection !== strongDirection
    || decimalMagnitude(weak.amount) !== decimalMagnitude(strong.amount)) return false
  const weakDescription = normalizedText(weak.description)
  const strongDescription = normalizedText(strong.description)
  return weakDescription.length >= 8
    && strongDescription.length > weakDescription.length
    && strongDescription.endsWith(weakDescription)
}

function primeSaverWeakRawPayload(value: unknown): boolean {
  const raw = recordPayload(value)
  // Observations written before migration 0013 have no raw_payload. Treat
  // those legacy synthetic rows as weak only when the surrounding exact
  // PrimeSaver identity checks also prove a single strong counterpart.
  return raw === null || /^[-+]?0(?:\.0+)?$/.test(String(raw.runningBalance ?? '').trim())
}

async function primeSaverDuplicateRows(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  payload: Readonly<Record<string, unknown>>,
): Promise<readonly PrimeSaverDuplicateRow[]> {
  const accountId = typeof payload.accountId === 'string' ? payload.accountId : undefined
  const transactionId = typeof payload.transactionId === 'string' ? payload.transactionId : undefined
  if (!accountId || !transactionId) return []
  const rows = await sql<PrimeSaverDuplicateRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (payload->>'transactionId') payload, raw_payload, provider_version
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${connection.localOwnerId}
        AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
        AND payload ? 'transactionId'
        AND provider_version LIKE 'investec:txn:%'
      ORDER BY payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, raw_payload, provider_version
    FROM latest
    WHERE payload->>'accountId' = ${accountId}
      AND payload->>'transactionId' <> ${transactionId}
  `
  return rows
}

function primeSaverTransactionId(payload: Readonly<Record<string, unknown>>): string | undefined {
  return typeof payload.transactionId === 'string' && payload.transactionId.trim()
    ? payload.transactionId.trim()
    : undefined
}

function distinctPrimeSaverRows(rows: readonly PrimeSaverDuplicateRow[]): readonly PrimeSaverDuplicateRow[] {
  const seen = new Set<string>()
  return rows.filter(row => {
    const payload = observationPayload(row.payload)
    const transactionId = payload ? primeSaverTransactionId(payload) : undefined
    if (!transactionId || seen.has(transactionId)) return false
    seen.add(transactionId)
    return true
  })
}

function primeSaverStrongIdentityIds(
  weak: Readonly<Record<string, unknown>>,
  currentStrong: Readonly<Record<string, unknown>>,
  rows: readonly PrimeSaverDuplicateRow[],
): ReadonlySet<string> {
  const strongIdentityIds = new Set<string>()
  const candidates = [currentStrong, ...rows.map(row => observationPayload(row.payload))]
  for (const candidate of candidates) {
    if (!candidate || !primeSaverDuplicatePair(weak, candidate)) continue
    const transactionId = primeSaverTransactionId(candidate)
    if (transactionId) strongIdentityIds.add(transactionId)
  }
  return strongIdentityIds
}

async function primeSaverWeakObservationAlreadyCanonical(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  observation: ObservationOutput,
): Promise<boolean> {
  const payload = observationPayload(observation.payload)
  if (!payload || !hasPrimeSaverSyntheticIdentity(payload) || !primeSaverWeakRawPayload(observation.rawPayload)) return false
  const rows = distinctPrimeSaverRows(await primeSaverDuplicateRows(sql, deploymentId, connection, payload))
  const matches = rows.filter(row => {
    const candidate = observationPayload(row.payload)
    return candidate !== null && primeSaverDuplicatePair(payload, candidate)
  })
  // Fail closed if more than one provider-referenced identity could explain
  // the weak row, or if another distinct weak identity also explains the same
  // strong row. This keeps arrival order from silently dropping one weak row.
  if (matches.length !== 1) return false
  const strong = observationPayload(matches[0].payload)
  if (!strong) return false
  const otherWeakMatches = rows.filter(row => {
    const candidate = observationPayload(row.payload)
    return candidate !== null
      && candidate.transactionId !== payload.transactionId
      && primeSaverDuplicatePair(candidate, strong)
      && primeSaverWeakRawPayload(row.raw_payload)
  })
  return otherWeakMatches.length === 0
}

async function investecSupersededResourceIds(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  observation: ObservationOutput,
): Promise<readonly string[]> {
  if (connection.provider !== 'investec') return []
  const payload = observationPayload(observation.payload)
  if (!payload) return []
  const identityKey = typeof payload.primeSaverIdentityKey === 'string' ? payload.primeSaverIdentityKey : undefined
  const accountId = typeof payload?.accountId === 'string' ? payload.accountId : undefined
  const transactionId = typeof payload?.transactionId === 'string' ? payload.transactionId : undefined
  const supersessionKey = investecPrimeSaverSupersessionKey(payload ?? {})
  if (!identityKey || !accountId || !transactionId || !supersessionKey) return []

  const prior = await primeSaverDuplicateRows(sql, deploymentId, connection, payload)
  const resourceIds = new Set<string>()
  for (const row of prior) {
    const candidate = observationPayload(row.payload)
    if (!candidate || typeof candidate.transactionId !== 'string') continue
    const candidateKey = investecPrimeSaverSupersessionKey(candidate)
    if (candidateKey !== supersessionKey) continue
    resourceIds.add(`${deploymentId}:${connection.sourceResourceIncarnationId}:${candidate.transactionId}`)
  }

  // PrimeSaver sometimes emits a second UUID-less row with only the account
  // holder's short narrative and a zero running balance. When a single
  // provider-referenced row has the same date, direction, amount, and narrative
  // suffix, treat the weak row as a provider correction. Raw payload remains
  // append-only; only the native projection converges to the strong identity.
  if (primeSaverProviderReference(payload)) {
    const duplicateRows = await primeSaverDuplicateRows(sql, deploymentId, connection, payload)
    const weakMatches = distinctPrimeSaverRows(duplicateRows.filter(row => {
      const candidate = observationPayload(row.payload)
      return candidate !== null
        && primeSaverDuplicatePair(candidate, payload)
        && primeSaverWeakRawPayload(row.raw_payload)
    }))
    if (weakMatches.length === 1) {
      const candidate = observationPayload(weakMatches[0].payload)
      const strongIdentityIds = candidate
        ? primeSaverStrongIdentityIds(candidate, payload, duplicateRows)
        : new Set<string>()
      if (candidate && strongIdentityIds.size === 1 && strongIdentityIds.has(transactionId)) {
        const candidateTransactionId = primeSaverTransactionId(candidate)
        if (candidateTransactionId) {
          resourceIds.add(`${deploymentId}:${connection.sourceResourceIncarnationId}:${candidateTransactionId}`)
        }
      }
    }
  }
  if (resourceIds.size > 64) throw new Error('Investec PrimeSaver identity supersession candidate overflow')
  return [...resourceIds].sort()
}

function recordPayload(value: unknown): Readonly<Record<string, unknown>> | null {
  if (value === null || typeof value !== 'object' || Array.isArray(value)) return null
  return value as Readonly<Record<string, unknown>>
}

function canonicalizeJson(value: unknown): unknown {
  if (Array.isArray(value)) return value.map(canonicalizeJson)
  if (value !== null && typeof value === 'object') {
    const record = value as Record<string, unknown>
    return Object.fromEntries(Object.keys(record).sort().map(key => [key, canonicalizeJson(record[key])]))
  }
  return value
}

function canonicalJson(value: unknown): string {
  return JSON.stringify(canonicalizeJson(value))
}

function transactionId(payload: Readonly<Record<string, unknown>>): string | undefined {
  const value = payload.transactionId ?? payload.transaction_id ?? payload.sourceId
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

const INVESTEC_SAME_CURRENCY_TOLERANCE = 0.01

interface InvestecCounterpart {
  readonly payload: Readonly<Record<string, unknown>>
  readonly providerVersion: string
  readonly semanticHash: string
  readonly observedAt: string
}

interface InvestecAmbiguousCounterpart {
  readonly ambiguous: true
}

type InvestecCounterpartMatch = InvestecCounterpart | InvestecAmbiguousCounterpart

interface InvestecCounterpartRow {
  readonly payload: unknown
  readonly provider_version: string
  readonly semantic_hash: string
  readonly observed_at: string
}

function normalizedText(value: unknown): string {
  return typeof value === 'string' ? value.trim().replace(/\s+/g, ' ').toUpperCase() : ''
}

function numericMagnitude(value: unknown): number | undefined {
  const text = typeof value === 'number' && Number.isFinite(value) ? String(value)
    : typeof value === 'string' && /^[+-]?\d+(?:\.\d+)?$/.test(value.trim()) ? value.trim()
      : undefined
  if (!text) return undefined
  const number = Math.abs(Number(text))
  return Number.isFinite(number) ? number : undefined
}

function sameCurrencyAmountWithinTolerance(
  left: Readonly<Record<string, unknown>>,
  right: Readonly<Record<string, unknown>>,
): boolean {
  const leftCurrency = normalizedText(left.currency)
  const rightCurrency = normalizedText(right.currency)
  const leftAmount = numericMagnitude(left.amount)
  const rightAmount = numericMagnitude(right.amount)
  return Boolean(leftCurrency && leftCurrency === rightCurrency && leftAmount !== undefined && rightAmount !== undefined
    && Math.abs(leftAmount - rightAmount) <= INVESTEC_SAME_CURRENCY_TOLERANCE)
}

function counterpartFromRow(row: InvestecCounterpartRow): InvestecCounterpart | undefined {
  const payload = recordPayload(row.payload)
  if (!payload || !transactionId(payload) || !row.semantic_hash || !row.observed_at) return undefined
  return { payload, providerVersion: row.provider_version, semanticHash: row.semantic_hash, observedAt: row.observed_at }
}

function providerLifecycleTransactionId(payload: Readonly<Record<string, unknown>>): string | undefined {
  for (const key of [
    'pendingTransactionId', 'postedTransactionId', 'settlementTransactionId',
    'successorTransactionId', 'cardTransactionId', 'linkedTransactionId',
  ]) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  const normalized = payload.providerLifecycleTransactionId
  return typeof normalized === 'string' && normalized.trim() ? normalized.trim() : undefined
}

async function findInvestecCounterpart(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  observation: ObservationOutput,
): Promise<InvestecCounterpartMatch | undefined> {
  if (connection.provider !== 'investec' || observation.payload.isRemoved === true) return undefined
  const payload = recordPayload(observation.payload)
  const accountId = typeof payload?.accountId === 'string' ? payload.accountId : undefined
  const linkedTransactionId = payload ? providerLifecycleTransactionId(payload) : undefined
  const currentTransactionId = payload ? transactionId(payload) : undefined
  const isCard = observation.providerVersion.startsWith('investec:card:')
  const counterpartPrefix = isCard ? 'investec:txn:%' : 'investec:card:%'
  // Investec must provide the counterpart transaction ID explicitly. Shared
  // references, amounts, merchants, dates, and FX ratios are not lifecycle
  // linkage and are intentionally never used to mutate identity. A one-way
  // provider link is sufficient: search the stored counterpart for a backlink
  // when the arriving row does not repeat the link.
  if (!accountId || (!linkedTransactionId && !currentTransactionId)) return undefined
  const currentPrefix = isCard ? 'investec:card:%' : 'investec:txn:%'
  const rows = await sql<InvestecCounterpartRow[]>`
    SELECT DISTINCT ON (payload->>'transactionId') payload, provider_version, semantic_hash, observed_at
    FROM raw_observations
    WHERE deployment_id = ${deploymentId}
      AND local_owner_id = ${connection.localOwnerId}
      AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
      AND (provider_version LIKE ${counterpartPrefix} OR provider_version LIKE ${currentPrefix})
      AND payload->>'accountId' = ${accountId}
      AND (
        payload->>'transactionId' = ${linkedTransactionId ?? ''}
        OR payload->>'transactionId' = ${currentTransactionId ?? ''}
        OR payload->>'providerLifecycleTransactionId' = ${linkedTransactionId ?? ''}
        OR payload->>'providerLifecycleTransactionId' = ${currentTransactionId ?? ''}
      )
    ORDER BY payload->>'transactionId', observed_at DESC, recorded_at DESC
  `
  const parsedRows = rows.map(counterpartFromRow).filter((row): row is InvestecCounterpart => row !== undefined)
  const candidates = parsedRows.filter(candidate => {
    if (!candidate.providerVersion.startsWith(isCard ? 'investec:txn:' : 'investec:card:')) return false
    const candidateId = transactionId(candidate.payload)
    const candidateLink = providerLifecycleTransactionId(candidate.payload)
    return Boolean(candidateId && (
      (linkedTransactionId !== undefined && candidateId === linkedTransactionId)
      || (currentTransactionId !== undefined && candidateLink === currentTransactionId)
    ))
  })
  if (candidates.length !== 1) return candidates.length > 1 ? { ambiguous: true } : undefined

  const candidate = candidates[0]
  const candidateId = transactionId(candidate.payload)
  if (!candidateId || (providerLifecycleTransactionId(candidate.payload) !== undefined
    && providerLifecycleTransactionId(candidate.payload) !== currentTransactionId)) {
    return { ambiguous: true }
  }
  const currentLinks = parsedRows.filter(row => row.providerVersion.startsWith(isCard ? 'investec:card:' : 'investec:txn:')
    && transactionId(row.payload) !== currentTransactionId
    && providerLifecycleTransactionId(row.payload) === candidateId)
  const counterpartLinks = parsedRows.filter(row => row.providerVersion.startsWith(isCard ? 'investec:txn:' : 'investec:card:')
    && providerLifecycleTransactionId(row.payload) === currentTransactionId)
  const currentLinkCount = currentLinks.length + (linkedTransactionId === candidateId ? 1 : 0)
  if ((linkedTransactionId !== undefined ? currentLinkCount !== 1 : currentLinkCount !== 0)
    || (providerLifecycleTransactionId(candidate.payload) !== undefined
      ? counterpartLinks.length !== 1
      : counterpartLinks.length !== 0)) {
    return { ambiguous: true }
  }
  return candidate
}

function markInvestecReconciliationReview(
  payload: Readonly<Record<string, unknown>>,
  reason: string,
): Readonly<Record<string, unknown>> {
  const existingMetadata = recordPayload(payload.metadata)
  const existingProviderOwned = recordPayload(existingMetadata?.providerOwned)
  return Object.freeze({
    ...payload,
    needsReview: true,
    metadata: {
      ...(existingMetadata ?? {}),
      providerOwned: {
        ...(existingProviderOwned ?? {}),
        investecReconciliation: { reason },
      },
    },
  })
}

function correlatedInvestecPayload(
  card: Readonly<Record<string, unknown>>,
  settled: Readonly<Record<string, unknown>>,
): Readonly<Record<string, unknown>> {
  const cardId = transactionId(card)
  const settledId = transactionId(settled)
  if (!cardId || !settledId) throw new Error('Investec correlation requires both transaction identities')
  const settlementAmount = settled.settlementAmount ?? settled.amount
  const settlementCurrency = settled.settlementCurrency ?? settled.currency
  const cardMetadata = recordPayload(card.metadata)
  const settledMetadata = recordPayload(settled.metadata)
  const cardProviderOwned = recordPayload(cardMetadata?.providerOwned)
  const settledProviderOwned = recordPayload(settledMetadata?.providerOwned)
  const cardInvestecMetadata = recordPayload(cardProviderOwned?.investec)
  const settledInvestecMetadata = recordPayload(settledProviderOwned?.investec)
  const originalCurrency = typeof card.originalCurrency === 'string' ? card.originalCurrency : undefined
  const originalAmount = originalCurrency !== undefined && typeof card.originalAmount === 'string'
    ? card.originalAmount
    : undefined
  const settlementDate = settled.settlementDate ?? settled.postingDate ?? settled.valueDate ?? settled.date
  return Object.freeze({
    ...settled,
    transactionId: settledId,
    pendingTransactionId: cardId,
    // The posted ledger fact must use the settlement/account leg for native
    // cash economics. Preserve the card leg as original evidence instead of
    // making a USD amount look like ZAR account cash (or vice versa).
    amount: settlementAmount,
    currency: settlementCurrency,
    ...(originalAmount === undefined ? {} : { originalAmount }),
    ...(originalCurrency === undefined ? {} : { originalCurrency }),
    settlementAmount,
    settlementCurrency,
    date: card.date ?? settled.date,
    settlementDate,
    sourceType: card.sourceType ?? 'CARD',
    subtype: card.subtype ?? 'CARD_PURCHASE',
    ...(card.authorizationDateTime === undefined ? {} : { authorizationDateTime: card.authorizationDateTime }),
    sourceRecordId: settledId,
    description: card.description ?? settled.description,
    reference: settled.reference ?? card.reference,
    merchantContext: card.merchantContext ?? settled.merchantContext,
    metadata: {
      providerOwned: {
        ...(cardProviderOwned ?? {}),
        ...(settledProviderOwned ?? {}),
        investec: {
          ...(cardInvestecMetadata ?? {}),
          ...(settledInvestecMetadata ?? {}),
        },
        investecCorrelation: {
          cardTransactionId: cardId,
          settlementTransactionId: settledId,
          confidence: 'provider-explicit',
          originalCurrency: originalCurrency ?? null,
          settlementCurrency: settlementCurrency ?? null,
          sameCurrencyRoundingMatch: sameCurrencyAmountWithinTolerance(card, settled),
        },
      },
    },
    status: 'posted',
  })
}

async function investecProjectionOperations(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  observation: ObservationOutput,
  accessGeneration: number,
  accountId: string,
  connectionId: string,
): Promise<BridgeDisclosureOperation[]> {
  if (await primeSaverWeakObservationAlreadyCanonical(sql, deploymentId, connection, observation)) return []
  const counterpart = await findInvestecCounterpart(sql, deploymentId, connection, observation)
  const current = recordPayload(observation.payload)
  if (!current || !transactionId(current) || !counterpart) {
    const operation = providerObservationToBridge(connection.provider, observation, accessGeneration, accountId, connectionId)
    return operation ? [operation] : []
  }
  if ('ambiguous' in counterpart) {
    const reviewSemanticHash = createHash('sha256').update([
      'investec-reconciliation-v1', observation.semanticHash, 'AMBIGUOUS_CARD_SETTLEMENT',
    ].join('\\x00'), 'utf8').digest('hex')
    const operation = providerObservationToBridge(
      connection.provider,
      {
        ...observation,
        semanticHash: reviewSemanticHash,
        payload: markInvestecReconciliationReview(current, 'AMBIGUOUS_CARD_SETTLEMENT'),
      },
      accessGeneration,
      accountId,
      connectionId,
    )
    return operation ? [operation] : []
  }
  const currentIsCard = observation.providerVersion.startsWith('investec:card:')
  const card = currentIsCard ? current : counterpart.payload
  const settled = currentIsCard ? counterpart.payload : current
  const correlated = correlatedInvestecPayload(card, settled)
  const lifecycleSemanticHash = createHash('sha256').update([
    'investec-card-settlement-v2',
    transactionId(card)!, transactionId(settled)!,
    String(card.amount ?? ''), String(card.currency ?? ''),
    String(settled.settlementAmount ?? settled.amount ?? ''), String(settled.settlementCurrency ?? settled.currency ?? ''),
    String(settled.settlementDate ?? settled.postingDate ?? settled.date ?? ''),
    canonicalJson(correlated),
  ].join('\\x00'), 'utf8').digest('hex')
  const lifecycleObservation: ObservationOutput = {
    ...observation,
    semanticHash: lifecycleSemanticHash,
    payload: correlated,
  }
  const lifecycle = providerObservationToBridge(connection.provider, lifecycleObservation, accessGeneration, accountId, connectionId)
  if (!lifecycle) return []

  // Always materialize the pending card resource before applying the lifecycle
  // migration. This makes card-first and poll-first arrival order equivalent.
  const pendingSemanticHash = currentIsCard
    ? observation.semanticHash
    : createHash('sha256').update([
      'investec-card-pending-revision-v1', counterpart.semanticHash, lifecycleSemanticHash,
    ].join('\\x00'), 'utf8').digest('hex')
  const pendingObservation: ObservationOutput = currentIsCard
    ? observation
    : {
      ...observation,
      providerVersion: counterpart.providerVersion,
      semanticHash: pendingSemanticHash,
      payload: counterpart.payload,
      observedAt: counterpart.observedAt,
    }
  const pending = providerObservationToBridge(connection.provider, pendingObservation, accessGeneration, accountId, connectionId)
  return pending ? [pending, lifecycle] : [lifecycle]
}

export function assertBridgeConfiguration(env: RuntimeEnv, fixture = false): void {
  const allowedBridgeHosts = bridgeHosts(env)
  requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', allowedBridgeHosts, fixture)
  requiredEndpoint(env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, 'FINANCE_SYNC_BRIDGE_RECEIPT_URL', allowedBridgeHosts, fixture)
  readExclusiveInput(env as Readonly<Record<string, string | undefined>>, 'FINANCE_SYNC_BRIDGE_TOKEN', true)
}

const UUID_PREFIX = /^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}:/i

/**
 * Durable connections use `<provider-connection-uuid>:<provider-account-id>`.
 * Non-matching IDs are retained only for isolated legacy unit paths; they can
 * never be returned by the PostgreSQL loader.
 */
async function assertDurableAuthority(sql: FinanceSyncSql, deploymentId: string, connection: RuntimeConnection): Promise<void> {
  if (!UUID_PREFIX.test(connection.connectionId)) return
  const separator = connection.connectionId.indexOf(':')
  const connectionId = connection.connectionId.slice(0, separator)
  const providerAccountId = connection.connectionId.slice(separator + 1)
  if (!providerAccountId || connection.accountIds.length !== 1 || connection.accountIds[0] !== providerAccountId) {
    throw new Error('durable connection identity is invalid')
  }
  const authorized = await sql<{ authorized: number }[]>`
    SELECT 1 AS authorized
    FROM provider_connections pc
    JOIN discovered_provider_accounts dpa
      ON dpa.deployment_id = pc.deployment_id AND dpa.local_owner_id = pc.local_owner_id
      AND dpa.connection_id = pc.connection_id
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id AND paa.provider_account_id = dpa.provider_account_id
    WHERE pc.deployment_id = ${deploymentId} AND pc.local_owner_id = ${connection.localOwnerId}
      AND pc.connection_id = ${connectionId}::uuid AND pc.provider = ${connection.provider}
      AND pc.state = 'active' AND pc.disabled_at IS NULL
      AND pc.provider_generation = ${connection.providerGeneration}
      AND dpa.provider_account_id = ${providerAccountId}
      AND dpa.source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
      AND dpa.discovery_generation = pc.provider_generation
      AND paa.provider_generation = pc.provider_generation
      AND paa.revoked_at IS NULL AND paa.baseline_state = 'verified'
      AND paa.receiver_access_generation = ${connection.receiverAccessGeneration}
      AND paa.wealthfolio_account_id = ${connection.wealthfolioAccountIds[providerAccountId]}
    FOR SHARE OF pc, dpa, paa
  `
  if (authorized.length !== 1) throw new Error('durable connection authority is stale')
}

export type BridgeReceiptLookup = (operation: BridgeDisclosureOperation) => Promise<unknown | null>

export function createRuntimeDependencies(
  sql: FinanceSyncSql,
  deploymentId: string,
  env: RuntimeEnv = process.env,
  transportFactories?: ProviderTransportFactories,
  receiptLookup?: BridgeReceiptLookup,
  secretResolver: ProviderSecretResolver = unavailableProviderSecretStore,
): SyncRuntimeDependencies {
  const fixture = fixtureModeEnabled(env as NodeJS.ProcessEnv)
  const fixtureRuntime = fixture ? createFixtureRuntime(deploymentId, env as NodeJS.ProcessEnv) : undefined

  // Legacy metadata is parsed only as a startup validation aid for existing
  // deployments/tests. It is never returned by loadConnections; PostgreSQL is
  // the sole runtime admission authority.
  const legacyValidationOnly = parseRuntimeConnections(env, deploymentId)
  const factories = transportFactories ?? fixtureRuntime?.factories ?? createProviderTransportFactories(env)
  for (const connection of legacyValidationOnly) {
    if (connection.provider === 'plaid') factories.plaid(connection)
    else if (connection.provider === 'investec') factories.investec(connection)
    else if (connection.provider === 'mercury') factories.mercury(connection)
    else factories.snaptrade(connection)
  }
  let bridgeUrl: URL | undefined
  let bridgeReceiptUrl: URL | undefined
  let bridgeToken: string | undefined
  const bridgeInputs = [env.FINANCE_SYNC_BRIDGE_URL, env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, env.FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS,
    env.FINANCE_SYNC_BRIDGE_TOKEN, env.FINANCE_SYNC_BRIDGE_TOKEN_FILE]
  if (bridgeInputs.some(Boolean)) {
    const allowedBridgeHosts = bridgeHosts(env)
    bridgeUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', allowedBridgeHosts, fixture)
    bridgeReceiptUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, 'FINANCE_SYNC_BRIDGE_RECEIPT_URL', allowedBridgeHosts, fixture)
    bridgeToken = readExclusiveInput(env as Readonly<Record<string, string | undefined>>, 'FINANCE_SYNC_BRIDGE_TOKEN', true)
  }

  return {
    loadConnections: async () => {
      if (fixtureRuntime) {
        await ensureFixtureAuthority(sql, deploymentId)
        if (!bridgeUrl || !bridgeReceiptUrl || !bridgeToken) throw new Error('bridge configuration is required for fixture connections')
        return fixtureRuntime.connections
      }
      const connections = await loadDurableRuntimeConnections(sql, deploymentId, secretResolver)
      if (connections.length > 0 && (!bridgeUrl || !bridgeReceiptUrl || !bridgeToken)) throw new Error('bridge configuration is required for admitted connections')
      return connections
    },
    loadCheckpoint: async connection => {
      await assertDurableAuthority(sql, deploymentId, connection)
      return (await sql<{ cursor: string | null }[]>`
        SELECT cursor FROM sync_checkpoints
        WHERE deployment_id = ${deploymentId} AND connection_id = ${connection.connectionId}
          AND provider_generation = ${connection.providerGeneration}
      `)[0]?.cursor ?? null
    },
    runProvider: async (connection, cursor) => runRuntimeProvider(connection, cursor, factories),
    persistAndEnqueue: async (connection, observations, options?: PersistEnqueueOptions) => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      await assertDurableAuthority(tx, deploymentId, connection)
      const currentStates = options?.skipCurrentState ? [] : assembleCurrentState(connection.provider, observations, connection.accountIds.map(providerAccountId => ({
        providerAccountId,
        accountId: connection.wealthfolioAccountIds[providerAccountId],
      })), {
        providerConnectionId: connection.connectionId,
        providerIncarnationId: connection.sourceResourceIncarnationId,
        providerGeneration: connection.providerGeneration,
      })
      let insertedCount = 0
      const enrichedObservations: Array<{ observation: ObservationOutput; wealthfolioAccountId: string }> = []
      for (const observation of observations) {
        if (observation.deploymentId !== deploymentId
          || observation.localOwnerId !== connection.localOwnerId
          || observation.sourceResourceIncarnationId !== connection.sourceResourceIncarnationId) {
          throw new Error('observation authority mismatch')
        }
        let accountId = typeof observation.payload.accountId === 'string' ? observation.payload.accountId : undefined
        if (observation.payload.isRemoved === true && !accountId) {
          const transactionId = observation.payload.transactionId
          if (typeof transactionId !== 'string' || !transactionId) throw new Error('removed transaction identity is required')
          const prior = await tx<{ account_id: string }[]>`
            SELECT payload->>'accountId' AS account_id
            FROM raw_observations
            WHERE deployment_id = ${deploymentId}
              AND local_owner_id = ${connection.localOwnerId}
              AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
              AND provider_version = ${`${connection.provider}:txn:${transactionId}`}
              AND payload ? 'accountId'
            ORDER BY recorded_at DESC
            LIMIT 1
          `
          accountId = prior[0]?.account_id
        }
        if (!accountId || !connection.accountIds.includes(accountId)) throw new Error('provider account is outside the admitted connection scope')
        const wealthfolioAccountId = connection.wealthfolioAccountIds[accountId]
        if (!wealthfolioAccountId) throw new Error('provider account has no Wealthfolio mapping')
        const enriched: ObservationOutput = observation.payload.accountId === accountId
          ? observation
          : { ...observation, payload: Object.freeze({ ...observation.payload, accountId }) }
        const inserted = await tx`
          INSERT INTO raw_observations (
            deployment_id, local_owner_id, observation_id, source_resource_incarnation_id,
            provider_version, semantic_hash, hash_specification_version, payload, raw_payload, observed_at
          ) VALUES (
            ${enriched.deploymentId}, ${enriched.localOwnerId}, gen_random_uuid(),
            ${enriched.sourceResourceIncarnationId}::uuid, ${enriched.providerVersion},
            ${enriched.semanticHash}, ${enriched.hashSpecificationVersion},
            ${tx.json(enriched.payload as never)},
            ${enriched.rawPayload === undefined ? null : tx.json(enriched.rawPayload as never)},
            ${enriched.observedAt}
          )
          ON CONFLICT DO NOTHING
          RETURNING observation_id
        `
        insertedCount += inserted.length
        enrichedObservations.push({ observation: enriched, wealthfolioAccountId })
      }

      // Derive projections only after the whole batch is visible in the raw
      // evidence table. This lets a weak PrimeSaver correction observe its
      // strong counterpart regardless of provider arrival order and suppress
      // the weak upsert before it reaches the outbox.
      for (const { observation: enriched, wealthfolioAccountId } of enrichedObservations) {
        const supersededResourceIds = await investecSupersededResourceIds(tx, deploymentId, connection, enriched)
        const projectedObservation = supersededResourceIds.length === 0
          ? enriched
          : { ...enriched, supersededResourceIds }
        const operations = connection.provider === 'investec'
          ? await investecProjectionOperations(
            tx,
            deploymentId,
            connection,
            projectedObservation,
            connection.receiverAccessGeneration,
            wealthfolioAccountId,
            connection.connectionId,
          )
          : (() => {
            const operation = providerObservationToBridge(
              connection.provider,
              projectedObservation,
              connection.receiverAccessGeneration,
              wealthfolioAccountId,
              connection.connectionId,
            )
            return operation ? [operation] : []
          })()
        for (const operation of operations) await tx`
          INSERT INTO projection_outbox (
            deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload
          ) VALUES (
            ${deploymentId}, ${connection.localOwnerId}, ${connection.connectionId}, ${connection.providerGeneration},
            ${operation.idempotencyKey}, ${tx.json(operation as never)}
          )
          ON CONFLICT (deployment_id, connection_id, provider_generation, idempotency_key) DO NOTHING
        `
      }
      for (const current of currentStates) {
        const operation = currentStateToBridge({
          deploymentId,
          localOwnerId: connection.localOwnerId,
          sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
        }, current.payload, current.semanticHash, connection.receiverAccessGeneration, connection.connectionId)
        await tx`
          INSERT INTO projection_outbox (
            deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload
          ) VALUES (
            ${deploymentId}, ${connection.localOwnerId}, ${connection.connectionId}, ${connection.providerGeneration},
            ${operation.idempotencyKey}, ${tx.json(operation as never)}
          )
          ON CONFLICT (deployment_id, connection_id, provider_generation, idempotency_key) DO NOTHING
        `
      }
      return insertedCount
    }),
    projectPending: async connection => {
      if (!bridgeUrl || !bridgeToken) return { delivered: 0, failed: 0 }
      return sql.begin(async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        await assertDurableAuthority(tx, deploymentId, connection)
        const unresolvedBefore = await tx<{ count: number }[]>`
          SELECT count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND provider_generation = ${connection.providerGeneration}
            AND state = 'dead_letter'
        `
        if ((unresolvedBefore[0]?.count ?? 0) > 0) return { delivered: 0, failed: unresolvedBefore[0].count }

        let delivered = 0
        let deliveryFailures = 0
        const rows = await tx<{ id: string; payload: BridgeDisclosureOperation; attempts: number; state: 'pending' | 'uncertain' }[]>`
          SELECT id, payload, attempts, state
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND provider_generation = ${connection.providerGeneration}
            AND state IN ('pending', 'uncertain')
          ORDER BY created_at, (payload ? 'lifecycleSuccessorId'), id
          LIMIT ${MAX_PROJECTION_BATCH}
          FOR UPDATE SKIP LOCKED
        `
        for (const row of rows) {
          const markDelivered = async () => {
            await tx`
              UPDATE projection_outbox
              SET state = 'delivered', delivered_at = now(), attempts = attempts + 1,
                  last_error = NULL, dead_lettered_at = NULL, dead_letter_reason = NULL
              WHERE id = ${row.id}::uuid
                AND provider_generation = ${connection.providerGeneration}
            `
            delivered++
          }
          const recoverReceipt = async (): Promise<boolean> => {
            try {
              const lookup = receiptLookup ?? (async (operation: BridgeDisclosureOperation): Promise<unknown | null> => {
                if (!bridgeReceiptUrl || !bridgeToken) return null
                const base = bridgeReceiptUrl.toString().replace(/\/+$/, '')
                const response = await fetch(`${base}/${encodeURIComponent(operation.subject)}/${encodeURIComponent(operation.idempotencyKey)}`, {
                  method: 'GET', redirect: 'error', headers: { authorization: `Bearer ${bridgeToken}` },
                })
                if (response.status === 404) return null
                if (!response.ok) throw new Error('bridge receipt lookup failed')
                return response.json()
              })
              const recovered = await lookup(row.payload)
              if (recovered === null) return false
              validateBridgeReceipt(row.payload, recovered)
              await markDelivered()
              return true
            } catch {
              return false
            }
          }

          // Never replay an uncertain commit before attempting receipt recovery.
          if (row.state === 'uncertain' && await recoverReceipt()) continue

          let response: Response
          try {
            response = await fetch(bridgeUrl, {
              method: 'POST',
              redirect: 'error',
              headers: {
                'content-type': 'application/json',
                authorization: `Bearer ${bridgeToken}`,
                'idempotency-key': row.payload.idempotencyKey,
              },
              body: JSON.stringify(bridgeOperationToWire(row.payload)),
            })
          } catch {
            if (await recoverReceipt()) continue
            await tx`
              UPDATE projection_outbox
              SET attempts = attempts + 1, last_error = 'BRIDGE_RESPONSE_UNCERTAIN', state = 'uncertain'
              WHERE id = ${row.id}::uuid
                AND provider_generation = ${connection.providerGeneration}
            `
            deliveryFailures++
            continue
          }

          if (!response.ok) {
            const nextAttempts = row.attempts + 1
            if (nextAttempts >= MAX_PROJECTION_DELIVERY_ATTEMPTS) {
              await tx`
                UPDATE projection_outbox
                SET attempts = ${nextAttempts}, last_error = 'BRIDGE_HTTP_REJECTED', state = 'dead_letter',
                    dead_lettered_at = now(), dead_letter_reason = 'BRIDGE_HTTP_REJECTED'
                WHERE id = ${row.id}::uuid
                  AND provider_generation = ${connection.providerGeneration}
              `
            } else {
              await tx`
                UPDATE projection_outbox SET attempts = ${nextAttempts}, last_error = 'BRIDGE_HTTP_REJECTED'
                WHERE id = ${row.id}::uuid
                  AND provider_generation = ${connection.providerGeneration}
              `
            }
            deliveryFailures++
            continue
          }

          try {
            validateBridgeReceipt(row.payload, await response.json())
            await markDelivered()
          } catch {
            if (await recoverReceipt()) continue
            // A successful HTTP commit with a lost/invalid receipt is uncertain,
            // not terminal. It can only be resolved by a bound receipt lookup.
            await tx`
              UPDATE projection_outbox
              SET attempts = attempts + 1, last_error = 'BRIDGE_RECEIPT_UNCERTAIN', state = 'uncertain'
              WHERE id = ${row.id}::uuid
                AND provider_generation = ${connection.providerGeneration}
            `
            deliveryFailures++
          }
        }
        const unresolvedAfter = await tx<{ count: number }[]>`
          SELECT count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND provider_generation = ${connection.providerGeneration}
            AND state IN ('pending', 'uncertain', 'dead_letter')
        `
        return { delivered, failed: Math.max(deliveryFailures, unresolvedAfter[0]?.count ?? 0) }
      })
    },
    saveCheckpoint: async (connection, cursor) => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      await assertDurableAuthority(tx, deploymentId, connection)
      await tx`
        INSERT INTO sync_checkpoints (deployment_id, connection_id, provider_generation, provider, cursor)
        VALUES (${deploymentId}, ${connection.connectionId}, ${connection.providerGeneration}, ${connection.provider}, ${cursor})
        ON CONFLICT (deployment_id, connection_id, provider_generation) DO UPDATE
        SET cursor = EXCLUDED.cursor, provider = EXCLUDED.provider, updated_at = now()
      `
    }),
  }
}
