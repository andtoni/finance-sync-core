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

interface InvestecCounterpart {
  readonly payload: Readonly<Record<string, unknown>>
  readonly providerVersion: string
  readonly semanticHash: string
  readonly observedAt: string
  readonly provenance: 'provider-explicit' | 'provider-facts-exact'
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

interface CashDecimalMagnitude {
  readonly units: bigint
  readonly scale: number
}

function cashDecimalMagnitude(value: unknown): CashDecimalMagnitude | undefined {
  const text = typeof value === 'number' && Number.isFinite(value) ? String(value)
    : typeof value === 'string' && /^[+-]?\d+(?:\.\d+)?$/.test(value.trim()) ? value.trim()
      : undefined
  if (!text) return undefined
  const match = /^([+-]?)(\d+)(?:\.(\d+))?$/.exec(text)
  if (!match || match[2].length > 30 || (match[3]?.length ?? 0) > 12) return undefined
  const fraction = match[3] ?? ''
  const units = BigInt(`${match[2]}${fraction}`)
  return { units: units < 0n ? -units : units, scale: fraction.length }
}

function sameCurrencyAmountWithinTolerance(
  left: Readonly<Record<string, unknown>>,
  right: Readonly<Record<string, unknown>>,
): boolean {
  const leftCurrency = normalizedText(left.currency)
  const rightCurrency = normalizedText(right.currency)
  const leftAmount = cashDecimalMagnitude(left.amount)
  const rightAmount = cashDecimalMagnitude(right.amount)
  if (!leftCurrency || leftCurrency !== rightCurrency || leftAmount === undefined || rightAmount === undefined) return false
  // Card authorization and same-currency settlement are the same economic leg;
  // a one-cent tolerance can silently pair unrelated provider facts.
  const scale = Math.max(leftAmount.scale, rightAmount.scale)
  const factor = (value: CashDecimalMagnitude) => value.units * 10n ** BigInt(scale - value.scale)
  return factor(leftAmount) === factor(rightAmount)
}

function terminalFour(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const digits = value.replace(/\D/g, '')
  return digits.length >= 4 ? digits.slice(-4) : undefined
}

function currencyCode(value: unknown): string | undefined {
  const normalized = normalizedText(value)
  return /^[A-Z]{3}$/.test(normalized) ? normalized : undefined
}

function providerCardAccountCurrency(payload: Readonly<Record<string, unknown>>): string | undefined {
  const metadata = recordPayload(payload.metadata)
  const owned = recordPayload(metadata?.providerOwned)
  const investec = recordPayload(owned?.investec)
  return currencyCode(investec?.accountCurrency)
}

function providerTransactionType(payload: Readonly<Record<string, unknown>>): string | undefined {
  for (const value of [payload.transactionType, payload.sourceType]) {
    const direct = normalizedText(value)
    if (direct === 'CARDPURCHASES') return direct
  }
  const metadata = recordPayload(payload.metadata)
  const owned = recordPayload(metadata?.providerOwned)
  const investec = recordPayload(owned?.investec)
  return normalizedText(investec?.providerTransactionType) || undefined
}

function providerDirection(payload: Readonly<Record<string, unknown>>): string | undefined {
  const value = normalizedText(payload.direction ?? payload.type)
  return value === 'DEBIT' || value === 'CREDIT' ? value : undefined
}

function merchantMatches(card: Readonly<Record<string, unknown>>, settled: Readonly<Record<string, unknown>>): boolean {
  const cardMerchant = normalizedText(card.description).normalize('NFKC')
  const postedMerchant = normalizedText(settled.description).normalize('NFKC')
  if (!cardMerchant || cardMerchant.length < 3 || !postedMerchant) return false
  const escaped = cardMerchant.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return postedMerchant === cardMerchant || new RegExp(`^${escaped}(?:\\s|$)`).test(postedMerchant)
}

function exactProviderFactPair(card: Readonly<Record<string, unknown>>, settled: Readonly<Record<string, unknown>>): boolean {
  if (hasConflictingLifecycleIds(card) || hasConflictingLifecycleIds(settled)
    || providerLifecycleTransactionId(card) || providerLifecycleTransactionId(settled)
    || normalizedText(card.status) !== 'PENDING' || normalizedText(settled.status) !== 'POSTED'
    || providerTransactionType(settled) !== 'CARDPURCHASES') return false
  const cardFour = terminalFour(card.cardNumber)
  const settledFour = terminalFour(settled.cardNumber)
  if (!cardFour || !settledFour || cardFour !== settledFour) return false
  const cardDate = typeof card.transactionDate === 'string' ? card.transactionDate : undefined
  const settledDate = typeof settled.transactionDate === 'string' ? settled.transactionDate : undefined
  if (!cardDate || !settledDate || cardDate !== settledDate) return false
  const cardDirection = providerDirection(card)
  if (!cardDirection || cardDirection !== providerDirection(settled)) return false
  if (!merchantMatches(card, settled)) return false
  const originalAmount = typeof card.originalAmount === 'string' ? card.originalAmount : undefined
  const originalCurrency = currencyCode(card.originalCurrency)
  const cardCurrency = currencyCode(card.currency)
  const settlementAmount = settled.settlementAmount ?? settled.amount
  const settlementCurrency = currencyCode(settled.settlementCurrency ?? settled.currency)
  const accountCurrency = providerCardAccountCurrency(card)
  const originalMagnitude = cashDecimalMagnitude(originalAmount)
  const settlementMagnitude = cashDecimalMagnitude(settlementAmount)
  if (!originalAmount || !originalCurrency || !cardCurrency || cardCurrency !== originalCurrency
    || !settlementAmount || !settlementCurrency || !accountCurrency || settlementCurrency !== accountCurrency
    || originalMagnitude === undefined || originalMagnitude.units <= 0n
    || settlementMagnitude === undefined || settlementMagnitude.units <= 0n) return false
  // Foreign authorization and account settlement magnitudes are intentionally
  // incomparable; only equal currencies receive the strict tolerance check.
  return originalCurrency !== settlementCurrency || sameCurrencyAmountWithinTolerance(
    { amount: originalAmount, currency: originalCurrency },
    { amount: settlementAmount, currency: settlementCurrency },
  )
}

function counterpartFromRow(row: InvestecCounterpartRow): InvestecCounterpart | undefined {
  const payload = recordPayload(row.payload)
  if (!payload || !transactionId(payload) || !row.semantic_hash || !row.observed_at) return undefined
  return { payload, providerVersion: row.provider_version, semanticHash: row.semantic_hash, observedAt: row.observed_at, provenance: 'provider-explicit' }
}

function providerLifecycleTransactionIds(payload: Readonly<Record<string, unknown>>): readonly string[] {
  const ids = new Set<string>()
  for (const key of [
    'pendingTransactionId', 'postedTransactionId', 'settlementTransactionId',
    'successorTransactionId', 'cardTransactionId', 'linkedTransactionId', 'providerLifecycleTransactionId',
  ]) {
    const value = payload[key]
    if (typeof value === 'string' && value.trim()) ids.add(value.trim())
  }
  return [...ids]
}

function providerLifecycleTransactionId(payload: Readonly<Record<string, unknown>>): string | undefined {
  const ids = providerLifecycleTransactionIds(payload)
  return ids.length === 1 ? ids[0] : undefined
}

function hasConflictingLifecycleIds(payload: Readonly<Record<string, unknown>>): boolean {
  return payload.providerLifecycleIdConflict === true || providerLifecycleTransactionIds(payload).length > 1
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
  if (!payload || !accountId || (!linkedTransactionId && !currentTransactionId)) return undefined
  if (hasConflictingLifecycleIds(payload)) return { ambiguous: true }
  const currentPrefix = isCard ? 'investec:card:%' : 'investec:txn:%'
  const counterpartPrefix = isCard ? 'investec:txn:%' : 'investec:card:%'
  if (!isCard && (normalizedText(payload.status) !== 'POSTED' || providerTransactionType(payload) !== 'CARDPURCHASES')) return undefined
  // Explicit lifecycle IDs remain authoritative and are resolved before the
  // deliberately narrower provider-fact matcher below.
  if (linkedTransactionId || currentTransactionId) {
    const rows = await sql<InvestecCounterpartRow[]>`
      WITH latest AS (
        SELECT DISTINCT ON (payload->>'transactionId')
          payload, provider_version, semantic_hash, hash_specification_version, observed_at,
          deployment_id, local_owner_id, source_resource_incarnation_id
        FROM raw_observations
        WHERE deployment_id = ${deploymentId} AND local_owner_id = ${connection.localOwnerId}
          AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
          AND (provider_version LIKE ${counterpartPrefix} OR provider_version LIKE ${currentPrefix})
          AND payload->>'accountId' = ${accountId}
        ORDER BY payload->>'transactionId', recorded_at DESC, observed_at DESC, observation_id DESC
      )
      SELECT payload, provider_version, semantic_hash, observed_at
      FROM latest
      WHERE payload->>'isRemoved' IS DISTINCT FROM 'true'
        AND (payload->>'transactionId' = ${linkedTransactionId ?? ''}
          OR payload->>'transactionId' = ${currentTransactionId ?? ''}
          OR payload->>'providerLifecycleTransactionId' = ${linkedTransactionId ?? ''}
          OR payload->>'providerLifecycleTransactionId' = ${currentTransactionId ?? ''})
        AND NOT EXISTS (
          SELECT 1 FROM observation_quarantines q
          WHERE q.deployment_id = latest.deployment_id AND q.local_owner_id = latest.local_owner_id
            AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
            AND q.provider_version = latest.provider_version AND q.semantic_hash = latest.semantic_hash
            AND q.hash_specification_version = latest.hash_specification_version
        )
    `
    const parsed = rows.map(counterpartFromRow).filter((row): row is InvestecCounterpart => row !== undefined)
    const candidates = parsed.filter(candidate => candidate.providerVersion.startsWith(isCard ? 'investec:txn:' : 'investec:card:')
      && (isCard
        ? normalizedText(candidate.payload.status) === 'POSTED' && providerTransactionType(candidate.payload) === 'CARDPURCHASES'
        : normalizedText(candidate.payload.status) === 'PENDING')
      && ((linkedTransactionId !== undefined && transactionId(candidate.payload) === linkedTransactionId)
        || (currentTransactionId !== undefined && providerLifecycleTransactionId(candidate.payload) === currentTransactionId)))
    if (candidates.length === 1) {
      if (hasConflictingLifecycleIds(candidates[0].payload)) return { ambiguous: true }
      const candidateLink = providerLifecycleTransactionId(candidates[0].payload)
      if (candidateLink !== undefined && candidateLink !== currentTransactionId) return { ambiguous: true }
      return candidates[0]
    }
    if (candidates.length > 1 || linkedTransactionId) return candidates.length > 1 ? { ambiguous: true } : undefined
  }

  const providerDate = typeof payload.transactionDate === 'string' ? payload.transactionDate : undefined
  if (!providerDate) return undefined
  const rows = await sql<InvestecCounterpartRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (payload->>'transactionId')
        payload, provider_version, semantic_hash, hash_specification_version, observed_at
      FROM raw_observations
      WHERE deployment_id = ${deploymentId} AND local_owner_id = ${connection.localOwnerId}
        AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
        AND payload->>'accountId' = ${accountId}
        AND (provider_version LIKE 'investec:card:%' OR provider_version LIKE 'investec:txn:%')
      ORDER BY payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, provider_version, semantic_hash, observed_at
    FROM latest
    WHERE payload->>'transactionDate' = ${providerDate}
      AND payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND (
        (provider_version LIKE 'investec:card:%' AND payload->>'status' = 'pending')
        OR (provider_version LIKE 'investec:txn:%' AND payload->>'status' = 'posted'
          AND (
            payload->>'transactionType' = 'CARDPURCHASES'
            OR payload->'metadata'->'providerOwned'->'investec'->>'providerTransactionType' = 'CARDPURCHASES'
          ))
      )
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = ${deploymentId} AND q.local_owner_id = ${connection.localOwnerId}
          AND q.source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
          AND q.provider_version = latest.provider_version AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  const all = rows.map(counterpartFromRow).filter((row): row is InvestecCounterpart => row !== undefined)
  const cards = all.filter(row => row.providerVersion.startsWith('investec:card:'))
  const settled = all.filter(row => row.providerVersion.startsWith('investec:txn:') && providerTransactionType(row.payload) === 'CARDPURCHASES')
  const currentCandidates = isCard
    ? settled.filter(row => exactProviderFactPair(payload, row.payload))
    : cards.filter(row => exactProviderFactPair(row.payload, payload))
  if (currentCandidates.length !== 1) return currentCandidates.length > 1 ? { ambiguous: true } : undefined
  const candidate = currentCandidates[0]
  const currentId = currentTransactionId
  const cardRows = isCard
    ? [{ payload, providerVersion: observation.providerVersion, semanticHash: observation.semanticHash, observedAt: observation.observedAt, provenance: 'provider-facts-exact' as const }, ...cards.filter(row => transactionId(row.payload) !== currentId)]
    : cards
  const settledRows = isCard
    ? settled
    : [{ payload, providerVersion: observation.providerVersion, semanticHash: observation.semanticHash, observedAt: observation.observedAt, provenance: 'provider-facts-exact' as const }, ...settled.filter(row => transactionId(row.payload) !== currentId)]
  const reverseCount = isCard
    ? cardRows.filter(card => exactProviderFactPair(card.payload, candidate.payload)).length
    : settledRows.filter(posted => exactProviderFactPair(candidate.payload, posted.payload)).length
  if (reverseCount !== 1) return { ambiguous: true }
  return { ...candidate, provenance: 'provider-facts-exact' }
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
  provenance: 'provider-explicit' | 'provider-facts-exact',
): Readonly<Record<string, unknown>> {
  const cardId = transactionId(card)
  const settledId = transactionId(settled)
  if (!cardId || !settledId) throw new Error('Investec correlation requires both transaction identities')
  const settlementAmount = typeof settled.settlementAmount === 'string' ? settled.settlementAmount : undefined
  const settlementCurrency = typeof settled.settlementCurrency === 'string' ? settled.settlementCurrency : undefined
  const cardMetadata = recordPayload(card.metadata)
  const settledMetadata = recordPayload(settled.metadata)
  const cardProviderOwned = recordPayload(cardMetadata?.providerOwned)
  const settledProviderOwned = recordPayload(settledMetadata?.providerOwned)
  const cardInvestecMetadata = recordPayload(cardProviderOwned?.investec)
  const settledInvestecMetadata = recordPayload(settledProviderOwned?.investec)
  const cardInvestecMetadataWithoutProvisional = Object.fromEntries(
    Object.entries(cardInvestecMetadata ?? {}).filter(
      ([key]) => key !== 'provisionalSpending' && key !== 'provisionalSpendingReason',
    ),
  )
  const originalCurrency = typeof card.originalCurrency === 'string' ? card.originalCurrency : undefined
  const originalAmount = originalCurrency !== undefined && typeof card.originalAmount === 'string'
    ? card.originalAmount
    : undefined
  const settledWithoutOriginalEvidence: Record<string, unknown> = { ...settled }
  delete settledWithoutOriginalEvidence.originalAmount
  delete settledWithoutOriginalEvidence.originalCurrency
  const settlementDate = typeof settled.settlementDate === 'string' ? settled.settlementDate : undefined
  return Object.freeze({
    ...settledWithoutOriginalEvidence,
    transactionId: settledId,
    pendingTransactionId: cardId,
    // The posted ledger fact must use the settlement/account leg for native
    // cash economics. Preserve the card leg as original evidence instead of
    // making a USD amount look like ZAR account cash (or vice versa).
    amount: settled.amount,
    currency: settled.currency,
    ...(originalAmount === undefined ? {} : { originalAmount }),
    ...(originalCurrency === undefined ? {} : { originalCurrency }),
    ...(settlementAmount === undefined ? {} : { settlementAmount }),
    ...(settlementCurrency === undefined ? {} : { settlementCurrency }),
    date: card.date ?? settled.date,
    ...(settlementDate === undefined ? {} : { settlementDate }),
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
          ...(settledInvestecMetadata ?? {}),
          ...cardInvestecMetadataWithoutProvisional,
        },
        investecCorrelation: {
          cardTransactionId: cardId,
          settlementTransactionId: settledId,
          confidence: provenance,
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
  const correlated = correlatedInvestecPayload(card, settled, counterpart.provenance)
  const lifecycleSemanticHash = createHash('sha256').update([
    'investec-card-settlement-v3',
    counterpart.provenance,
    transactionId(card)!, transactionId(settled)!,
    String(card.amount ?? ''), String(card.currency ?? ''),
    String(settled.settlementAmount ?? ''), String(settled.settlementCurrency ?? ''),
    String(settled.settlementDate ?? ''),
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

async function observationIsQuarantined(
  tx: FinanceSyncSql,
  deploymentId: string,
  localOwnerId: string,
  observation: ObservationOutput,
): Promise<boolean> {
  const rows = await tx<{ quarantine_id: string }[]>`
    SELECT quarantine_id
    FROM observation_quarantines
    WHERE deployment_id = ${deploymentId}
      AND local_owner_id = ${localOwnerId}
      AND source_resource_incarnation_id = ${observation.sourceResourceIncarnationId}::uuid
      AND provider_version = ${observation.providerVersion}
      AND semantic_hash = ${observation.semanticHash}
      AND hash_specification_version = ${observation.hashSpecificationVersion}
    LIMIT 1
  `
  return rows.length > 0
}

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

  const persistAndEnqueueTx = async (
    tx: FinanceSyncSql,
    connection: RuntimeConnection,
    observations: readonly ObservationOutput[],
    options?: PersistEnqueueOptions,
  ): Promise<number> => {
    await assertDurableAuthority(tx, deploymentId, connection)
    let insertedCount = 0
    const enrichedObservations: Array<{ observation: ObservationOutput; wealthfolioAccountId: string }> = []
    for (const observation of observations) {
      if (observation.deploymentId !== deploymentId
        || observation.localOwnerId !== connection.localOwnerId
        || observation.sourceResourceIncarnationId !== connection.sourceResourceIncarnationId) {
        throw new Error('observation authority mismatch')
      }
      if (observation.domain === 'provider_audit') {
        await tx`
          INSERT INTO raw_observations (
            deployment_id, local_owner_id, observation_id, source_resource_incarnation_id,
            provider_version, semantic_hash, hash_specification_version, payload, raw_payload, observed_at
          ) VALUES (
            ${observation.deploymentId}, ${observation.localOwnerId}, gen_random_uuid(),
            ${observation.sourceResourceIncarnationId}::uuid, ${observation.providerVersion},
            ${observation.semanticHash}, ${observation.hashSpecificationVersion},
            ${tx.json(observation.payload as never)},
            ${observation.rawPayload === undefined ? null : tx.json(observation.rawPayload as never)},
            ${observation.observedAt}
          ) ON CONFLICT DO NOTHING
        `
        continue
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
        if (!accountId) {
          // No history for this source resource; safely ignore the tombstone only for Plaid
          if (connection.provider === 'plaid') {
            continue
          }
        }
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
    // the weak upsert before it reaches the outbox. A durable quarantine is
    // checked by the exact immutable observation identity before projection.
    const projectableObservations: typeof enrichedObservations = []
    for (const candidate of enrichedObservations) {
      if (!(await observationIsQuarantined(tx, deploymentId, connection.localOwnerId, candidate.observation))) {
        projectableObservations.push(candidate)
      }
    }
    const currentStates = options?.skipCurrentState ? [] : assembleCurrentState(
      connection.provider,
      projectableObservations.map(candidate => candidate.observation),
      connection.accountIds.map(providerAccountId => ({
        providerAccountId,
        accountId: connection.wealthfolioAccountIds[providerAccountId],
      })),
      {
        providerConnectionId: connection.connectionId,
        providerIncarnationId: connection.sourceResourceIncarnationId,
        providerGeneration: connection.providerGeneration,
      },
    )
    for (const { observation: enriched, wealthfolioAccountId } of projectableObservations) {
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
    loadPlaidItemCheckpoint: async connection => {
      const providerConnectionId = connection.providerConnectionId
      if (!providerConnectionId) throw new Error('Plaid item provider connection identity is required')
      const authority = await sql<{ authorized: number }[]>`
        SELECT 1 AS authorized
        FROM provider_connections
        WHERE deployment_id = ${deploymentId}
          AND local_owner_id = ${connection.localOwnerId}
          AND provider = 'plaid'
          AND provider_connection_id = ${providerConnectionId}
          AND provider_generation = ${connection.providerGeneration}
          AND state = 'active'
          AND disabled_at IS NULL
        LIMIT 1
      `
      if (authority.length !== 1) throw new Error('plaid item authority is stale')
      const current = await sql<{ cursor: string | null }[]>`
        SELECT cursor FROM plaid_item_checkpoints
        WHERE deployment_id = ${deploymentId}
          AND local_owner_id = ${connection.localOwnerId}
          AND provider_connection_id = ${providerConnectionId}
          AND provider_generation = ${connection.providerGeneration}
      `
      if (current.length > 0) return current[0].cursor

      // Existing per-account checkpoints are usable only when every admitted
      // account has the same non-null item cursor. Otherwise restart from the
      // provider's item origin and let idempotent observations converge.
      const legacy = await sql<{ cursor: string | null }[]>`
        SELECT cursor FROM sync_checkpoints
        WHERE deployment_id = ${deploymentId}
          AND provider = 'plaid'
          AND connection_id LIKE ${`${connection.connectionId}:%`}
          AND provider_generation = ${connection.providerGeneration}
        ORDER BY connection_id
      `
      const cursors = legacy.map(row => row.cursor)
      if (cursors.length === connection.accountIds.length
        && cursors.every(cursor => cursor !== null)
        && new Set(cursors).size === 1) return cursors[0]
      return null
    },
    runProvider: async (connection, cursor) => runRuntimeProvider(connection, cursor, factories),
    persistAndEnqueue: async (connection, observations, options?: PersistEnqueueOptions) =>
      sql.begin(async transaction => persistAndEnqueueTx(
        transaction as unknown as FinanceSyncSql,
        connection,
        observations,
        options,
      )),
    persistAndEnqueueBatch: async batches => sql.begin(async transaction => {
      const tx = transaction as unknown as FinanceSyncSql
      let inserted = 0
      for (const batch of batches) {
        inserted += await persistAndEnqueueTx(tx, batch.connection, batch.observations, batch.options)
      }
      return inserted
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
    savePlaidItemCheckpoint: async (connection, cursor) => {
      const providerConnectionId = connection.providerConnectionId
      if (!providerConnectionId) throw new Error('Plaid item provider connection identity is required')
      const authority = await sql<{ authorized: number }[]>`
        SELECT 1 AS authorized
        FROM provider_connections
        WHERE deployment_id = ${deploymentId}
          AND local_owner_id = ${connection.localOwnerId}
          AND provider = 'plaid'
          AND provider_connection_id = ${providerConnectionId}
          AND provider_generation = ${connection.providerGeneration}
          AND state = 'active'
          AND disabled_at IS NULL
        LIMIT 1
      `
      if (authority.length !== 1) throw new Error('plaid item authority is stale')
      await sql`
        INSERT INTO plaid_item_checkpoints (
          deployment_id, local_owner_id, provider_connection_id, provider_generation, cursor
        ) VALUES (
          ${deploymentId}, ${connection.localOwnerId}, ${providerConnectionId}, ${connection.providerGeneration}, ${cursor}
        )
        ON CONFLICT (deployment_id, local_owner_id, provider_connection_id, provider_generation)
        DO UPDATE SET cursor = EXCLUDED.cursor, updated_at = now()
      `
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
