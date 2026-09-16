import { createHash } from 'node:crypto'
import { observationRevisionNumber, type FinanceSyncSql } from './storage/pg'
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

type ProjectionConnection = Pick<RuntimeConnection, 'provider' | 'localOwnerId' | 'sourceResourceIncarnationId' | 'accountIds' | 'connectionId'>

const MAX_PROJECTION_DELIVERY_ATTEMPTS = 3
const MAX_PROJECTION_BATCH = 25
const BRIDGE_REQUEST_TIMEOUT_MS = 30_000
const INVESTEC_CARD_SETTLEMENT_WINDOW_DAYS = 3
const DAY_MS = 86_400_000
const PLAID_HOSTS = new Set(['production.plaid.com', 'sandbox.plaid.com', 'development.plaid.com'])

interface FixtureAdmissionRow {
  readonly provider_account_id: string
  readonly source_resource_incarnation_id: string
  readonly wealthfolio_account_id: string
  readonly receiver_access_generation: number
}

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
    if (row.provider === 'investec') {
      await sql`
        INSERT INTO provider_connections (
          deployment_id, local_owner_id, connection_id, provider_principal_id,
          provider, provider_connection_id, secret_reference, provider_generation,
          configuration, state
        ) VALUES (
          ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.connectionId}::uuid,
          ${row.providerPrincipalId}::uuid, 'investec', ${row.providerConnectionId},
          'FIXTURE_ONLY', 1, '{}'::jsonb, 'discovered'
        ) ON CONFLICT DO NOTHING
      `
      await sql`
        INSERT INTO discovered_provider_accounts (
          deployment_id, local_owner_id, connection_id, provider_account_id,
          account_incarnation_id, source_resource_incarnation_id, provider_name,
          provider_type, currency, mask, assets, discovery_generation
        ) VALUES (
          ${deploymentId}, ${FIXTURE_OWNER_SUBJECT}, ${row.connectionId}::uuid,
          ${row.accountId}, ${row.accountIncarnationId}::uuid,
          ${row.sourceResourceIncarnationId}::uuid, 'Fixture Investec Card',
          'Private Bank Account', 'ZAR', '0014', '[]'::jsonb, 1
        ) ON CONFLICT DO NOTHING
      `
    }
  }
}

async function admittedFixtureConnections(
  sql: FinanceSyncSql,
  deploymentId: string,
  connections: readonly RuntimeConnection[],
): Promise<readonly RuntimeConnection[]> {
  const admissions = await sql<FixtureAdmissionRow[]>`
    SELECT dpa.provider_account_id, dpa.source_resource_incarnation_id,
      paa.wealthfolio_account_id, paa.receiver_access_generation
    FROM discovered_provider_accounts dpa
    JOIN provider_account_admissions paa
      ON paa.deployment_id = dpa.deployment_id
      AND paa.local_owner_id = dpa.local_owner_id
      AND paa.connection_id = dpa.connection_id
      AND paa.provider_account_id = dpa.provider_account_id
    JOIN provider_connections pc
      ON pc.deployment_id = dpa.deployment_id
      AND pc.local_owner_id = dpa.local_owner_id
      AND pc.connection_id = dpa.connection_id
    WHERE dpa.deployment_id = ${deploymentId}
      AND dpa.local_owner_id = ${FIXTURE_OWNER_SUBJECT}
      AND pc.state = 'active'
      AND pc.disabled_at IS NULL
      AND dpa.discovery_generation = pc.provider_generation
      AND paa.provider_generation = pc.provider_generation
      AND paa.revoked_at IS NULL
      AND paa.baseline_state = 'verified'
  `
  return connections.flatMap(connection => {
    const rows = connection.accountIds.map(providerAccountId => admissions.find(row =>
      row.provider_account_id === providerAccountId
      && row.source_resource_incarnation_id === connection.sourceResourceIncarnationId))
    if (rows.some(row => row === undefined)) return []
    const admitted = rows as FixtureAdmissionRow[]
    const generations = new Set(admitted.map(row => row.receiver_access_generation))
    if (generations.size !== 1) throw new Error('fixture receiver access generation is ambiguous')
    return [{
      ...connection,
      receiverAccessGeneration: admitted[0].receiver_access_generation,
      wealthfolioAccountIds: Object.freeze(Object.fromEntries(admitted.map(row => [
        row.provider_account_id,
        row.wealthfolio_account_id,
      ]))),
    }]
  })
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
  readonly FINANCE_SYNC_FIXTURE_BRIDGE_HOSTS?: string
}

function fixtureBridgeHosts(env: RuntimeEnv): ReadonlySet<string> {
  return new Set(env.FINANCE_SYNC_FIXTURE_BRIDGE_HOSTS?.split(',').map(value => value.trim().toLowerCase()).filter(Boolean) ?? [])
}

function requiredEndpoint(
  raw: string | undefined,
  name: string,
  allowedHosts: ReadonlySet<string>,
  allowFixtureHttp = false,
  allowedFixtureHosts: ReadonlySet<string> = new Set(),
): URL {
  if (!raw) throw new Error(`${name} is required for configured connections`)
  let url: URL
  try {
    url = new URL(raw)
  } catch {
    throw new Error(`${name} must be a valid URL`)
  }
  const hostname = url.hostname.toLowerCase()
  const fixtureLoopback = allowFixtureHttp && ['127.0.0.1', 'localhost'].includes(hostname)
  const fixtureHost = allowFixtureHttp && allowedFixtureHosts.has(hostname)
  const fixtureHttp = (fixtureLoopback || fixtureHost) && url.protocol === 'http:'
  if (allowFixtureHttp && !fixtureLoopback && !fixtureHost) throw new Error(`${name} must target a loopback or explicitly allowed fixture host`)
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
  connection: ProjectionConnection,
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
  connection: ProjectionConnection,
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
  connection: ProjectionConnection,
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
  readonly hashSpecificationVersion: number
  readonly observedAt: string
  readonly provenance: 'provider-explicit' | 'provider-facts-exact' | 'operator-authorized'
}

interface InvestecAmbiguousCounterpart {
  readonly ambiguous: true
}

type InvestecCounterpartMatch = InvestecCounterpart | InvestecAmbiguousCounterpart

interface InvestecCounterpartRow {
  readonly payload: unknown
  readonly provider_version: string
  readonly semantic_hash: string
  readonly hash_specification_version: number
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

function providerReference(payload: Readonly<Record<string, unknown>>): string | undefined {
  const value = normalizedText(payload.reference)
  return value || undefined
}

function providerMerchantIdentity(payload: Readonly<Record<string, unknown>>): string | undefined {
  const metadata = recordPayload(payload.metadata)
  const owned = recordPayload(metadata?.providerOwned)
  const investec = recordPayload(owned?.investec)
  for (const value of [
    payload.merchantId, payload.merchantCode, payload.merchantReference,
    investec?.merchantIdentity, investec?.merchantId, investec?.merchantCode,
    investec?.merchantReference,
  ]) {
    const identity = normalizedText(value).normalize('NFKC')
    if (identity) return identity
  }
  return undefined
}

function merchantMatches(card: Readonly<Record<string, unknown>>, settled: Readonly<Record<string, unknown>>): boolean {
  const cardIdentity = providerMerchantIdentity(card)
  const settledIdentity = providerMerchantIdentity(settled)
  // A provider identity is authoritative only when both feeds carry the same
  // non-empty identity. If one feed lacks it, retain the exact description
  // boundary below rather than treating the missing value as a mismatch.
  if (cardIdentity && settledIdentity) return cardIdentity === settledIdentity

  const cardMerchant = normalizedText(card.description).normalize('NFKC')
  const postedMerchant = normalizedText(settled.description).normalize('NFKC')
  if (!cardMerchant || cardMerchant.length < 3 || !postedMerchant) return false
  const escaped = cardMerchant.replace(/[.*+?^${}()|[\]\\]/g, '\\$&')
  return postedMerchant === cardMerchant || new RegExp(`^${escaped}(?:\\s|$)`).test(postedMerchant)
}

function cardSettlementDateMatches(cardDate: string, settledDate: string): boolean {
  const cardMs = Date.parse(`${cardDate}T00:00:00Z`)
  const settledMs = Date.parse(`${settledDate}T00:00:00Z`)
  if (!Number.isFinite(cardMs) || !Number.isFinite(settledMs)) return false
  const days = (settledMs - cardMs) / DAY_MS
  return Number.isInteger(days) && days >= 0 && days <= INVESTEC_CARD_SETTLEMENT_WINDOW_DAYS
}

/**
 * Match every non-economic provider fact used by automatic card settlement
 * correlation. This deliberately excludes only the amount equality check so a
 * user-authorized override can reuse the same safety fence without widening the
 * automatic matcher.
 */
export function investecProviderFactPairShapeMatches(
  card: Readonly<Record<string, unknown>>,
  settled: Readonly<Record<string, unknown>>,
): boolean {
  if (hasConflictingLifecycleIds(card) || hasConflictingLifecycleIds(settled)
    || providerLifecycleTransactionId(card) || providerLifecycleTransactionId(settled)
    || normalizedText(card.status) !== 'PENDING' || normalizedText(settled.status) !== 'POSTED'
    || providerTransactionType(settled) !== 'CARDPURCHASES') return false
  const cardFour = terminalFour(card.cardNumber)
  const settledFour = terminalFour(settled.cardNumber)
  const cardReference = providerReference(card)
  const settledReference = providerReference(settled)
  const referencesMatch = cardReference !== undefined && cardReference === settledReference
  // Investec's booked CARDPURCHASES response does not always repeat the card
  // last four. An exact shared provider reference is an equally strong link in
  // that shape; conflicting last-four evidence still fails closed.
  if (cardFour && settledFour && cardFour !== settledFour) return false
  if ((!cardFour || !settledFour) && !referencesMatch) return false
  const cardDate = typeof card.transactionDate === 'string' ? card.transactionDate : undefined
  const settledDate = typeof settled.transactionDate === 'string' ? settled.transactionDate : undefined
  if (!cardDate || !settledDate || !cardSettlementDateMatches(cardDate, settledDate)) return false
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
  return Boolean(originalAmount && originalCurrency && cardCurrency && cardCurrency === originalCurrency
    && settlementAmount && settlementCurrency && accountCurrency && settlementCurrency === accountCurrency
    && originalMagnitude !== undefined && originalMagnitude.units > 0n
    && settlementMagnitude !== undefined && settlementMagnitude.units > 0n)
}

type InvestecMatcherDiagnosticReason =
  | 'NO_CANDIDATE'
  | 'MERCHANT_MISMATCH'
  | 'DIRECTION_MISMATCH'
  | 'CURRENCY_OR_AMOUNT_MISMATCH'
  | 'REVERSE_AMBIGUITY'
  | 'PROVIDER_FACT_MISMATCH'

const INVESTEC_MATCH_DIAGNOSTIC_LIMIT = 100
let investecMatchDiagnosticsEmitted = 0

function resetInvestecMatcherDiagnostics(): void {
  investecMatchDiagnosticsEmitted = 0
}

function emitInvestecMatcherDiagnostic(reason: InvestecMatcherDiagnosticReason, candidateCount: number): void {
  if (investecMatchDiagnosticsEmitted >= INVESTEC_MATCH_DIAGNOSTIC_LIMIT) return
  investecMatchDiagnosticsEmitted += 1
  console.warn('FINANCE_SYNC_INVESTEC_CARD_MATCH_REJECTED', JSON.stringify({
    reason, candidateCount, status: 'unresolved', stage: 'lifecycle_matcher',
  }))
}

function providerFactPairMismatchReason(
  card: Readonly<Record<string, unknown>>,
  settled: Readonly<Record<string, unknown>>,
): InvestecMatcherDiagnosticReason | undefined {
  if (hasConflictingLifecycleIds(card) || hasConflictingLifecycleIds(settled)
    || providerLifecycleTransactionId(card) || providerLifecycleTransactionId(settled)
    || normalizedText(card.status) !== 'PENDING' || normalizedText(settled.status) !== 'POSTED'
    || providerTransactionType(settled) !== 'CARDPURCHASES') return 'PROVIDER_FACT_MISMATCH'
  const cardFour = terminalFour(card.cardNumber)
  const settledFour = terminalFour(settled.cardNumber)
  const cardReference = providerReference(card)
  const settledReference = providerReference(settled)
  if ((cardFour && settledFour && cardFour !== settledFour) || ((!cardFour || !settledFour)
    && (cardReference === undefined || cardReference !== settledReference))) return 'PROVIDER_FACT_MISMATCH'
  const cardDate = typeof card.transactionDate === 'string' ? card.transactionDate : undefined
  const settledDate = typeof settled.transactionDate === 'string' ? settled.transactionDate : undefined
  if (!cardDate || !settledDate || !cardSettlementDateMatches(cardDate, settledDate)) return 'PROVIDER_FACT_MISMATCH'
  const cardDirection = providerDirection(card)
  if (!cardDirection || cardDirection !== providerDirection(settled)) return 'DIRECTION_MISMATCH'
  if (!merchantMatches(card, settled)) return 'MERCHANT_MISMATCH'
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
    || settlementMagnitude === undefined || settlementMagnitude.units <= 0n) return 'CURRENCY_OR_AMOUNT_MISMATCH'
  if (originalCurrency === settlementCurrency && !sameCurrencyAmountWithinTolerance(
    { amount: originalAmount, currency: originalCurrency },
    { amount: settlementAmount, currency: settlementCurrency },
  )) return 'CURRENCY_OR_AMOUNT_MISMATCH'
  return undefined
}

function exactProviderFactPair(card: Readonly<Record<string, unknown>>, settled: Readonly<Record<string, unknown>>): boolean {
  if (!investecProviderFactPairShapeMatches(card, settled)) return false
  const originalAmount = card.originalAmount as string
  const originalCurrency = currencyCode(card.originalCurrency)!
  const settlementAmount = settled.settlementAmount ?? settled.amount
  const settlementCurrency = currencyCode(settled.settlementCurrency ?? settled.currency)!
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
  return { payload, providerVersion: row.provider_version, semanticHash: row.semantic_hash,
    hashSpecificationVersion: row.hash_specification_version, observedAt: row.observed_at, provenance: 'provider-explicit' }
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

interface InvestecLifecycleOverrideRow {
  readonly pending_transaction_id: string
  readonly posted_transaction_id: string
}

async function findManualInvestecLifecycleCounterpart(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: ProjectionConnection,
  payload: Readonly<Record<string, unknown>>,
  isCard: boolean,
  currentTransactionId: string,
): Promise<InvestecCounterpartMatch | undefined> {
  const overrides = await sql<InvestecLifecycleOverrideRow[]>`
    SELECT pending_transaction_id, posted_transaction_id
    FROM investec_card_lifecycle_overrides
    WHERE deployment_id = ${deploymentId}
      AND local_owner_id = ${connection.localOwnerId}
      AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
      AND (pending_transaction_id = ${currentTransactionId} OR posted_transaction_id = ${currentTransactionId})
  `
  if (overrides.length === 0) return undefined
  if (overrides.length !== 1) return { ambiguous: true }
  const override = overrides[0]
  const counterpartTransactionId = isCard ? override.posted_transaction_id : override.pending_transaction_id
  const counterpartProviderVersion = `${isCard ? 'investec:txn:' : 'investec:card:'}${counterpartTransactionId}`
  const rows = await sql<InvestecCounterpartRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (provider_version)
        payload, provider_version, semantic_hash, hash_specification_version, observed_at,
        deployment_id, local_owner_id, source_resource_incarnation_id
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${connection.localOwnerId}
        AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
        AND provider_version = ${counterpartProviderVersion}
      ORDER BY provider_version, recorded_at DESC, observed_at DESC, observation_id DESC
    )
    SELECT payload, provider_version, semantic_hash, hash_specification_version, observed_at
    FROM latest
    WHERE payload->>'transactionId' = ${counterpartTransactionId}
      AND payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = latest.deployment_id AND q.local_owner_id = latest.local_owner_id
          AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
          AND q.provider_version = latest.provider_version AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  const parsed = rows.map(counterpartFromRow).filter((row): row is InvestecCounterpart => row !== undefined)
  if (parsed.length !== 1) return { ambiguous: true }
  const counterpart = parsed[0]
  const card = isCard ? payload : counterpart.payload
  const settled = isCard ? counterpart.payload : payload
  const cardAccountId = typeof card.accountId === 'string' ? card.accountId : undefined
  const settledAccountId = typeof settled.accountId === 'string' ? settled.accountId : undefined
  const validCounterpart = isCard
    ? normalizedText(counterpart.payload.status) === 'POSTED' && providerTransactionType(counterpart.payload) === 'CARDPURCHASES'
    : normalizedText(counterpart.payload.status) === 'PENDING'
  if (!cardAccountId || cardAccountId !== settledAccountId
    || !connection.accountIds.includes(cardAccountId)
    || !validCounterpart
    || !investecProviderFactPairShapeMatches(card, settled)) return { ambiguous: true }
  return { ...counterpart, provenance: 'operator-authorized' }
}

export async function findInvestecCounterpart(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: ProjectionConnection,
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
  // An explicit operator link is durable control-plane evidence. It bypasses
  // only the amount equality fence and is evaluated before provider-fact
  // inference, while conflicting provider lifecycle IDs remain authoritative.
  if (currentTransactionId) {
    const manual = await findManualInvestecLifecycleCounterpart(
      sql, deploymentId, connection, payload, isCard, currentTransactionId,
    )
    if (manual) return manual
  }
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
      SELECT payload, provider_version, semantic_hash, hash_specification_version, observed_at
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
    if (candidates.length > 1 || linkedTransactionId) {
      if (candidates.length > 1) emitInvestecMatcherDiagnostic('REVERSE_AMBIGUITY', candidates.length)
      else emitInvestecMatcherDiagnostic('NO_CANDIDATE', 0)
      return candidates.length > 1 ? { ambiguous: true } : undefined
    }
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
    SELECT payload, provider_version, semantic_hash, hash_specification_version, observed_at
    FROM latest
    WHERE (payload->>'transactionDate')::date BETWEEN
        (${providerDate}::date - INTERVAL '3 days')::date
        AND (${providerDate}::date + INTERVAL '3 days')::date
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
  if (currentCandidates.length !== 1) {
    if (currentCandidates.length > 1) {
      emitInvestecMatcherDiagnostic('REVERSE_AMBIGUITY', currentCandidates.length)
    } else {
      const possible = isCard ? settled : cards
      const reason = possible.length === 0
        ? 'NO_CANDIDATE'
        : (possible.map(row => isCard
          ? providerFactPairMismatchReason(payload, row.payload)
          : providerFactPairMismatchReason(row.payload, payload)).find(value => value !== undefined) ?? 'PROVIDER_FACT_MISMATCH')
      emitInvestecMatcherDiagnostic(reason, possible.length)
    }
    return currentCandidates.length > 1 ? { ambiguous: true } : undefined
  }
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
  if (reverseCount !== 1) {
    emitInvestecMatcherDiagnostic('REVERSE_AMBIGUITY', reverseCount)
    return { ambiguous: true }
  }
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
  provenance: 'provider-explicit' | 'provider-facts-exact' | 'operator-authorized',
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

type InvestecLifecycleDisposition = 'none' | 'delivered' | 'uncertain'

async function investecObservationRevision(
  sql: FinanceSyncSql,
  deploymentId: string,
  localOwnerId: string,
  sourceResourceIncarnationId: string,
  observation: Pick<InvestecCounterpart, 'providerVersion' | 'semanticHash' | 'hashSpecificationVersion'>,
): Promise<number | undefined> {
  const rows = await sql<{ observation_revision: string | number }[]>`
    SELECT observation_revision FROM observation_revision_ledger
    WHERE deployment_id = ${deploymentId}
      AND local_owner_id = ${localOwnerId}
      AND source_resource_incarnation_id = ${sourceResourceIncarnationId}::uuid
      AND provider_version = ${observation.providerVersion}
      AND semantic_hash = ${observation.semanticHash}
      AND hash_specification_version = ${observation.hashSpecificationVersion}
    LIMIT 1
  `
  return rows[0] ? observationRevisionNumber(rows[0].observation_revision) : undefined
}

async function investecLifecycleDisposition(
  sql: FinanceSyncSql,
  connection: ProjectionConnection,
  operation: BridgeDisclosureOperation,
  expectedSuccessor?: string,
): Promise<InvestecLifecycleDisposition> {
  const sourcePrefix = `${operation.deploymentId}:${connection.sourceResourceIncarnationId}:`
  if (!operation.resourceId.startsWith(sourcePrefix)
    || (expectedSuccessor !== undefined && !expectedSuccessor.startsWith(sourcePrefix))) return 'none'
  const rows = await sql<{ state: 'delivered' | 'uncertain' }[]>`
    SELECT state
    FROM projection_outbox
    WHERE deployment_id = ${operation.deploymentId}
      AND local_owner_id = ${operation.subject}
      AND connection_id = ${connection.connectionId}
      AND state IN ('delivered', 'uncertain')
      AND payload->>'kind' = 'ActivityUpsert'
      AND payload->>'resourceId' = ${operation.resourceId}
      AND payload->>'lifecycleSuccessorId' IS NOT NULL
      AND (${expectedSuccessor ?? null}::text IS NULL OR payload->>'lifecycleSuccessorId' = ${expectedSuccessor ?? null})
      AND payload->>'accountId' = ${operation.accountId}
      AND payload->>'subject' = ${operation.subject}
      AND payload->>'resourceId' LIKE ${`${sourcePrefix}%`}
      AND payload->>'lifecycleSuccessorId' LIKE ${`${sourcePrefix}%`}
    ORDER BY state = 'delivered' DESC
    LIMIT 1
  `
  return rows[0]?.state ?? 'none'
}

async function investecProjectionOperations(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: ProjectionConnection,
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
    if (!operation || operation.kind !== 'ActivityUpsert' || operation.activity?.status !== 'PENDING') {
      return operation ? [operation] : []
    }
    const lifecycle = await investecLifecycleDisposition(sql, connection, operation)
    return lifecycle === 'none' ? [operation] : []
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
  const counterpartRevision = await investecObservationRevision(
    sql, deploymentId, connection.localOwnerId, connection.sourceResourceIncarnationId, counterpart,
  )
  // The correlated card payload contains both canonical provider facts. Its
  // revision must dominate either fact, so a late re-observation cannot move
  // metadata backwards when the other leg was revised more recently.
  const postedRevision = Math.max(observation.observationRevision ?? 0, counterpartRevision ?? 0) || undefined
  const lifecycleObservation: ObservationOutput = {
    ...observation,
    semanticHash: lifecycleSemanticHash,
    payload: correlated,
    observationRevision: postedRevision,
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
  const lifecycleDisposition = await investecLifecycleDisposition(sql, connection, lifecycle, lifecycle.lifecycleSuccessorId)
  if (lifecycleDisposition === 'uncertain') return []
  if (lifecycleDisposition === 'delivered') {
    // The settlement is already terminal in Wealthfolio. A provider metadata
    // revision must update that posted resource as an ordinary upsert: replaying
    // the lifecycle would require a predecessor that the receiver has already
    // consumed, and would dead-letter the whole activity batch.
    const revisionKey = createHash('sha256').update([
      'investec-card-posted-revision-v1', lifecycle.idempotencyKey,
    ].join('\\x00'), 'utf8').digest('hex')
    const { lifecycleSuccessorId, ...ordinaryLifecycle } = lifecycle
    if (lifecycleSuccessorId === undefined) return []
    return [{ ...ordinaryLifecycle, resourceId: lifecycleSuccessorId, idempotencyKey: revisionKey }]
  }
  const pending = lifecycleDisposition === 'none'
    ? providerObservationToBridge(connection.provider, pendingObservation, accessGeneration, accountId, connectionId)
    : undefined
  return pending ? [pending, lifecycle] : [lifecycle]
}

interface InvestecPostedCardLifecycleRow {
  readonly payload: unknown
  readonly raw_payload: unknown
  readonly provider_version: string
  readonly semantic_hash: string
  readonly hash_specification_version: number
  readonly observed_at: string
}

/**
 * Re-run every latest posted Investec CARDPURCHASES fact through the lifecycle
 * matcher. Provider cursors and queue delivery are independent, so a posted
 * row can be re-observed after its authorization or after a prior matcher
 * defect. The raw row is immutable and the outbox is idempotent; this sweep is
 * the durable convergence mechanism, not a second source of activity facts.
 */
export async function reconcileInvestecCardLifecycles(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  deps: SyncRuntimeDependencies,
): Promise<number> {
  if (connection.provider !== 'investec') return 0
  // The sweep is the durable observability boundary. Reset the bounded budget
  // so a prior large historical sweep cannot hide later live diagnostics.
  resetInvestecMatcherDiagnostics()
  const rows = await sql<InvestecPostedCardLifecycleRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (payload->>'transactionId')
        payload, raw_payload, provider_version, semantic_hash,
        hash_specification_version, observed_at, deployment_id,
        local_owner_id, source_resource_incarnation_id
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${connection.localOwnerId}
        AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
        AND provider_version LIKE 'investec:txn:%'
      ORDER BY payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, raw_payload, provider_version, semantic_hash,
      hash_specification_version, observed_at
    FROM latest
    WHERE payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND payload->>'status' = 'posted'
      AND (
        payload->>'transactionType' = 'CARDPURCHASES'
        OR payload->'metadata'->'providerOwned'->'investec'->>'providerTransactionType' = 'CARDPURCHASES'
      )
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = latest.deployment_id
          AND q.local_owner_id = latest.local_owner_id
          AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
          AND q.provider_version = latest.provider_version
          AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  const observations: ObservationOutput[] = []
  for (const row of rows) {
    const payload = observationPayload(row.payload)
    if (!payload || typeof payload.transactionId !== 'string' || typeof payload.accountId !== 'string') continue
    observations.push({
      deploymentId,
      localOwnerId: connection.localOwnerId,
      sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
      providerVersion: row.provider_version,
      semanticHash: row.semantic_hash,
      hashSpecificationVersion: row.hash_specification_version,
      domain: 'cash_ledger',
      payload,
      ...(row.raw_payload === null || row.raw_payload === undefined ? {} : { rawPayload: row.raw_payload }),
      observedAt: row.observed_at,
    })
  }
  if (observations.length === 0) return 0
  await deps.persistAndEnqueue(connection, observations, { skipCurrentState: true })
  return observations.length
}

/**
 * Days after which a card authorization with no settlement is retracted.
 * Investec releases abandoned holds well inside this window; the exact
 * settlement window used for matching is deliberately much shorter.
 */
const INVESTEC_CARD_AUTHORIZATION_TTL_DAYS = 10

/**
 * Retire card authorizations the provider never settled.
 *
 * A hold the provider abandons simply stops appearing in its pending feed, so
 * nothing ever supersedes it and the row would otherwise stay PENDING forever
 * (and keep counting as spending). The provider fact is immutable, so the
 * retirement is an append-only revision: the same resource is re-projected with
 * status VOID and a provider-owned reason. A settlement that arrives later
 * still supersedes the retired row through the ordinary lifecycle path.
 */
export async function retireUnmatchedInvestecCardAuthorizations(
  sql: FinanceSyncSql,
  deploymentId: string,
  connection: RuntimeConnection,
  deps: SyncRuntimeDependencies,
  ttlDays = INVESTEC_CARD_AUTHORIZATION_TTL_DAYS,
): Promise<number> {
  if (connection.provider !== 'investec') return 0
  if (!Number.isSafeInteger(ttlDays) || ttlDays < INVESTEC_CARD_SETTLEMENT_WINDOW_DAYS || ttlDays > 90) {
    throw new Error('invalid Investec authorization retirement window')
  }
  const rows = await sql<InvestecPostedCardLifecycleRow[]>`
    WITH latest AS (
      SELECT DISTINCT ON (payload->>'transactionId')
        payload, raw_payload, provider_version, semantic_hash,
        hash_specification_version, observed_at, deployment_id,
        local_owner_id, source_resource_incarnation_id
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${connection.localOwnerId}
        AND source_resource_incarnation_id = ${connection.sourceResourceIncarnationId}::uuid
        AND provider_version LIKE 'investec:card:%'
      ORDER BY payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, raw_payload, provider_version, semantic_hash,
      hash_specification_version, observed_at
    FROM latest
    WHERE payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND upper(coalesce(payload->>'status', '')) = 'PENDING'
      AND coalesce(payload->'metadata'->'providerOwned'->'investec'->>'authorizationRetired', 'false') <> 'true'
      AND coalesce(payload->>'transactionDate', payload->>'date')::date
        <= (now()::date - (${ttlDays} || ' days')::interval)::date
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = latest.deployment_id
          AND q.local_owner_id = latest.local_owner_id
          AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
          AND q.provider_version = latest.provider_version
          AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  const retirements: ObservationOutput[] = []
  const retiredAt = new Date().toISOString()
  for (const row of rows) {
    const payload = observationPayload(row.payload)
    if (!payload || typeof payload.transactionId !== 'string' || typeof payload.accountId !== 'string') continue
    const authorization: ObservationOutput = {
      deploymentId,
      localOwnerId: connection.localOwnerId,
      sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
      providerVersion: row.provider_version,
      semanticHash: row.semantic_hash,
      hashSpecificationVersion: row.hash_specification_version,
      domain: 'cash_ledger',
      payload,
      ...(row.raw_payload === null || row.raw_payload === undefined ? {} : { rawPayload: row.raw_payload }),
      observedAt: row.observed_at,
    }
    // A settlement already in the raw store is paired by the ordinary path.
    const counterpart = await findInvestecCounterpart(sql, deploymentId, connection, authorization)
    if (counterpart !== undefined) continue
    const existingMetadata = recordPayload(payload.metadata)
    const existingProviderOwned = recordPayload(existingMetadata?.providerOwned)
    const existingInvestec = recordPayload(existingProviderOwned?.investec)
    retirements.push({
      ...authorization,
      semanticHash: createHash('sha256').update([
        'investec-card-authorization-retirement-v1', row.provider_version, row.semantic_hash, retiredAt.slice(0, 10),
      ].join('\x00'), 'utf8').digest('hex'),
      payload: Object.freeze({
        ...payload,
        status: 'void',
        metadata: {
          ...(existingMetadata ?? {}),
          providerOwned: {
            ...(existingProviderOwned ?? {}),
            investec: {
              ...(existingInvestec ?? {}),
              authorizationRetired: true,
              authorizationRetiredReason: 'NO_SETTLEMENT_WITHIN_WINDOW',
              authorizationRetiredAt: retiredAt,
              authorizationRetiredWindowDays: ttlDays,
            },
          },
        },
      }),
    })
  }
  if (retirements.length === 0) return 0
  await deps.persistAndEnqueue(connection, retirements, { skipCurrentState: true })
  const projection = await deps.projectPending(connection)
  if (projection.failed > 0) throw new Error('Investec authorization retirement projection is unresolved')
  return retirements.length
}

/**
 * Provider description that names a person sending money by Zelle, e.g.
 * "ZELLE FROM ALEXIS EXAMPLE ON 09/05 REF # REF...". The sending account's own
 * leg carries only the counterparty name, so this side is what proves the rail.
 */
const ZELLE_INBOUND = /ZELLE\s+FROM\s+(.+?)(?:\s+ON\s+|\s+REF\s+|\s*$)/i

function normalizedParty(value: string): string {
  return value.trim().replace(/\s+/g, ' ').toUpperCase()
}

interface PlaidTransferLeg {
  readonly connection: RuntimeConnection
  readonly payload: Readonly<Record<string, unknown>>
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly observedAt: string
  readonly transactionId: string
  readonly description: string
  readonly direction: 'in' | 'out'
  readonly magnitude: string
  readonly date: string | undefined
}

/** Decimal magnitudes only need to compare equal, so normalize their text form. */
function normalizedMagnitude(value: string): string {
  const [whole = '0', fraction = ''] = value.replace(/^[+-]/, '').split('.')
  const normalizedWhole = whole.replace(/^0+(?=\d)/, '')
  const normalizedFraction = fraction.replace(/0+$/, '')
  return normalizedFraction.length > 0 ? `${normalizedWhole}.${normalizedFraction}` : normalizedWhole
}

function plaidTransferLeg(connection: RuntimeConnection, row: {
  payload: unknown; provider_version: string; semantic_hash: string;
  hash_specification_version: number; observed_at: string;
}): PlaidTransferLeg | undefined {
  const payload = recordPayload(row.payload)
  if (!payload) return undefined
  const transactionId = transactionIdOf(payload)
  const description = typeof payload.name === 'string'
    ? payload.name
    : typeof payload.description === 'string' ? payload.description : undefined
  const amount = typeof payload.amount === 'string' ? payload.amount : undefined
  const date = typeof payload.date === 'string' ? payload.date : undefined
  if (!transactionId || !description || !amount) return undefined
  const numeric = Number(amount)
  if (!Number.isFinite(numeric) || numeric === 0) return undefined
  return {
    connection,
    payload,
    providerVersion: row.provider_version,
    semanticHash: row.semantic_hash,
    hashSpecificationVersion: row.hash_specification_version,
    observedAt: row.observed_at,
    transactionId,
    description,
    // Plaid signs depository amounts positively for money out.
    direction: numeric > 0 ? 'out' : 'in',
    magnitude: normalizedMagnitude(amount),
    date,
  }
}

function transactionIdOf(payload: Readonly<Record<string, unknown>>): string | undefined {
  const value = payload.transactionId ?? payload.transaction_id ?? payload.sourceId
  return typeof value === 'string' && value.length > 0 ? value : undefined
}

function dayDifference(left: string | undefined, right: string | undefined): number | undefined {
  if (!left || !right) return undefined
  const leftMs = Date.parse(`${left}T00:00:00Z`)
  const rightMs = Date.parse(`${right}T00:00:00Z`)
  if (!Number.isFinite(leftMs) || !Number.isFinite(rightMs)) return undefined
  return Math.abs(Math.round((rightMs - leftMs) / DAY_MS))
}

/**
 * Link Zelle transfers between the subject's own accounts.
 *
 * A Zelle credit names the sender ("ZELLE FROM ALEXIS EXAMPLE ..."); the sending
 * account's own leg carries only that person's name, so the two legs of one
 * transfer are otherwise unrelated rows that read as income and spending. Pair
 * them only on exact evidence — the recipient's name inside both descriptions,
 * an identical magnitude, opposite directions, a bounded date window, and a
 * one-to-one match in both directions — then re-project both legs as
 * TRANSFER_IN/TRANSFER_OUT sharing one deterministic group id, which is what
 * makes the pair neutral instead of income and spending.
 *
 * Anything ambiguous stays untouched and visible: an unmatched Zelle credit can
 * be genuine income from a person.
 */
export async function linkZelleTransferPairs(
  sql: FinanceSyncSql,
  deploymentId: string,
  connections: readonly RuntimeConnection[],
  deps: SyncRuntimeDependencies,
  windowDays = INVESTEC_CARD_SETTLEMENT_WINDOW_DAYS,
): Promise<number> {
  const plaid = connections.filter(connection => connection.provider === 'plaid')
  if (plaid.length === 0) return 0
  const owner = plaid[0].localOwnerId
  const rows = await sql<Array<{
    payload: unknown; provider_version: string; semantic_hash: string;
    hash_specification_version: number; observed_at: string; source_resource_incarnation_id: string;
  }>>`
    WITH latest AS (
      SELECT DISTINCT ON (source_resource_incarnation_id, payload->>'transactionId')
        payload, provider_version, semantic_hash, hash_specification_version,
        observed_at, source_resource_incarnation_id, deployment_id, local_owner_id,
        recorded_at, observation_id
      FROM raw_observations
      WHERE deployment_id = ${deploymentId}
        AND local_owner_id = ${owner}
        AND provider_version LIKE 'plaid:txn:%'
      ORDER BY source_resource_incarnation_id, payload->>'transactionId', recorded_at DESC, observation_id DESC
    )
    SELECT payload, provider_version, semantic_hash, hash_specification_version,
      observed_at, source_resource_incarnation_id::text
    FROM latest
    WHERE payload->>'isRemoved' IS DISTINCT FROM 'true'
      AND NOT EXISTS (
        SELECT 1 FROM observation_quarantines q
        WHERE q.deployment_id = latest.deployment_id
          AND q.local_owner_id = latest.local_owner_id
          AND q.source_resource_incarnation_id = latest.source_resource_incarnation_id
          AND q.provider_version = latest.provider_version
          AND q.semantic_hash = latest.semantic_hash
          AND q.hash_specification_version = latest.hash_specification_version
      )
  `
  if (rows.length === 0) return 0
  const legs: PlaidTransferLeg[] = []
  for (const row of rows) {
    const connection = plaid.find(candidate => candidate.sourceResourceIncarnationId === row.source_resource_incarnation_id)
    if (!connection) continue
    const leg = plaidTransferLeg(connection, row)
    if (leg) legs.push(leg)
  }
  const inbound = legs.filter(leg => leg.direction === 'in' && ZELLE_INBOUND.test(leg.description))
  if (inbound.length === 0) return 0

  interface MatchedPair { readonly from: PlaidTransferLeg; readonly to: PlaidTransferLeg; readonly party: string }
  const pairs: MatchedPair[] = []
  for (const credit of inbound) {
    const match = credit.description.match(ZELLE_INBOUND)
    const party = match ? normalizedParty(match[1] ?? '') : ''
    if (party.length < 3) continue
    const candidates = legs.filter(leg => leg.direction === 'out'
      && leg.transactionId !== credit.transactionId
      && leg.magnitude === credit.magnitude
      && normalizedParty(leg.description).includes(party)
      && (dayDifference(leg.date, credit.date) ?? Number.POSITIVE_INFINITY) <= windowDays)
    if (candidates.length !== 1) continue
    const debit = candidates[0]!
    // Reverse direction: the debit must name exactly this one credit, or the
    // pair is ambiguous and stays untouched.
    const reverse = inbound.filter(other => other.transactionId !== debit.transactionId
      && other.magnitude === debit.magnitude
      && (dayDifference(other.date, debit.date) ?? Number.POSITIVE_INFINITY) <= windowDays
      && normalizedParty(debit.description).includes(normalizedParty((other.description.match(ZELLE_INBOUND)?.[1]) ?? '')))
    if (reverse.length !== 1 || reverse[0]!.transactionId !== credit.transactionId) continue
    pairs.push({ from: debit, to: credit, party })
  }
  if (pairs.length === 0) return 0

  const batches = new Map<RuntimeConnection, ObservationOutput[]>()
  for (const pair of pairs) {
    const groupId = `zelle-pair:${createHash('sha256').update([
      'plaid-zelle-transfer-pair-v1', pair.from.transactionId, pair.to.transactionId,
    ].join('\x00'), 'utf8').digest('hex').slice(0, 32)}`
    const correlation = {
      kind: 'zelle',
      counterparty: pair.party,
      counterpartTransactionId: pair.to.transactionId,
      pairedWith: pair.from.transactionId,
    }
    for (const [leg, activityType] of [[pair.to, 'TRANSFER_IN'], [pair.from, 'TRANSFER_OUT']] as const) {
      const existingMetadata = recordPayload(leg.payload.metadata)
      const existingProviderOwned = recordPayload(existingMetadata?.providerOwned)
      const observation: ObservationOutput = {
        deploymentId,
        localOwnerId: leg.connection.localOwnerId,
        sourceResourceIncarnationId: leg.connection.sourceResourceIncarnationId,
        providerVersion: leg.providerVersion,
        hashSpecificationVersion: leg.hashSpecificationVersion,
        domain: 'cash_ledger',
        observedAt: leg.observedAt,
        semanticHash: createHash('sha256').update([
          'plaid-zelle-transfer-pair-v1', pair.from.transactionId, pair.to.transactionId,
        ].join('\x00'), 'utf8').digest('hex'),
        payload: Object.freeze({
          ...leg.payload,
          activityType,
          sourceGroupId: groupId,
          metadata: {
            ...(existingMetadata ?? {}),
            providerOwned: {
              ...(existingProviderOwned ?? {}),
              financeSync: { transferPair: correlation },
            },
          },
        }),
      }
      const batch = batches.get(leg.connection) ?? []
      batch.push(observation)
      batches.set(leg.connection, batch)
    }
  }
  let linked = 0
  for (const [connection, observations] of batches) {
    await deps.persistAndEnqueue(connection, observations, { skipCurrentState: true })
    const projection = await deps.projectPending(connection)
    if (projection.failed > 0) throw new Error('Zelle transfer pair projection is unresolved')
    linked += observations.length / 2
  }
  return linked
}

export function assertBridgeConfiguration(env: RuntimeEnv, fixture = false): void {
  const allowedBridgeHosts = bridgeHosts(env)
  const allowedFixtureHosts = fixtureBridgeHosts(env)
  requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', allowedBridgeHosts, fixture, allowedFixtureHosts)
  requiredEndpoint(env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, 'FINANCE_SYNC_BRIDGE_RECEIPT_URL', allowedBridgeHosts, fixture, allowedFixtureHosts)
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
  const allowedFixtureHosts = fixtureBridgeHosts(env)
  if (bridgeInputs.some(Boolean)) {
    const allowedBridgeHosts = bridgeHosts(env)
    bridgeUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_URL, 'FINANCE_SYNC_BRIDGE_URL', allowedBridgeHosts, fixture, allowedFixtureHosts)
    bridgeReceiptUrl = requiredEndpoint(env.FINANCE_SYNC_BRIDGE_RECEIPT_URL, 'FINANCE_SYNC_BRIDGE_RECEIPT_URL', allowedBridgeHosts, fixture, allowedFixtureHosts)
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
      const existingRevision = await tx<{ observation_revision: number }[]>`
        SELECT observation_revision FROM observation_revision_ledger
        WHERE deployment_id = ${enriched.deploymentId}
          AND local_owner_id = ${enriched.localOwnerId}
          AND source_resource_incarnation_id = ${enriched.sourceResourceIncarnationId}::uuid
          AND provider_version = ${enriched.providerVersion}
          AND semantic_hash = ${enriched.semanticHash}
          AND hash_specification_version = ${enriched.hashSpecificationVersion}
        LIMIT 1
      `
      if (existingRevision.length === 0) {
        await tx`
          INSERT INTO observation_revision_ledger (
            deployment_id, local_owner_id, source_resource_incarnation_id,
            provider_version, semantic_hash, hash_specification_version, observation_revision
          ) VALUES (
            ${enriched.deploymentId}, ${enriched.localOwnerId}, ${enriched.sourceResourceIncarnationId}::uuid,
            ${enriched.providerVersion}, ${enriched.semanticHash}, ${enriched.hashSpecificationVersion},
            nextval('finance_sync_observation_revision_seq')
          ) ON CONFLICT DO NOTHING
        `
      }
      insertedCount += inserted.length
      const revisionRows = await tx<{ observation_revision: string | number }[]>`
        SELECT observation_revision FROM observation_revision_ledger
        WHERE deployment_id = ${enriched.deploymentId}
          AND local_owner_id = ${enriched.localOwnerId}
          AND source_resource_incarnation_id = ${enriched.sourceResourceIncarnationId}::uuid
          AND provider_version = ${enriched.providerVersion}
          AND semantic_hash = ${enriched.semanticHash}
          AND hash_specification_version = ${enriched.hashSpecificationVersion}
        LIMIT 1
      `
      const revision = revisionRows[0] ? observationRevisionNumber(revisionRows[0].observation_revision) : undefined
      if (revision !== undefined) {
        enrichedObservations.push({ observation: { ...enriched, observationRevision: revision }, wealthfolioAccountId })
      }
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

  const terminalRevisionResolver = bridgeUrl && bridgeReceiptUrl && bridgeToken ? {
    resolve: async (operation: BridgeDisclosureOperation) => {
      const successor = operation.lifecycleSuccessorId
      if (operation.kind !== 'ActivityUpsert' || !successor) throw new Error('terminal recovery requires a lifecycle activity')
      const retained = await sql<{ lifecycle_operation: BridgeDisclosureOperation; connection_id: string; source_resource_incarnation_id: string; provider_account_id: string }[]>`
        SELECT l1.payload AS lifecycle_operation, selected.connection_id, dpa.source_resource_incarnation_id, dpa.provider_account_id
        FROM projection_outbox selected
        JOIN provider_connections pc ON pc.deployment_id=selected.deployment_id AND pc.local_owner_id=selected.local_owner_id
        JOIN discovered_provider_accounts dpa ON dpa.deployment_id=pc.deployment_id AND dpa.local_owner_id=pc.local_owner_id AND dpa.connection_id=pc.connection_id
          AND selected.connection_id=pc.connection_id::text || ':' || dpa.provider_account_id
        JOIN provider_account_admissions paa ON paa.deployment_id=dpa.deployment_id AND paa.local_owner_id=dpa.local_owner_id
          AND paa.connection_id=dpa.connection_id AND paa.provider_account_id=dpa.provider_account_id
        JOIN projection_outbox l1 ON l1.deployment_id=selected.deployment_id AND l1.local_owner_id=selected.local_owner_id
          AND l1.connection_id=selected.connection_id AND l1.provider_generation=selected.provider_generation
        WHERE selected.deployment_id=${deploymentId} AND selected.local_owner_id=${operation.subject}
          AND selected.payload=${sql.json(operation as never)} AND selected.idempotency_key=${operation.idempotencyKey}
          AND selected.state='dead_letter' AND pc.provider='investec' AND pc.state='active' AND pc.disabled_at IS NULL
          AND pc.provider_generation=selected.provider_generation AND dpa.discovery_generation=pc.provider_generation
          AND paa.provider_generation=pc.provider_generation AND paa.baseline_state='verified' AND paa.revoked_at IS NULL
          AND paa.wealthfolio_account_id=${operation.accountId} AND paa.receiver_access_generation=${operation.accessGeneration}
          AND l1.state='delivered' AND l1.created_at < selected.created_at
          AND l1.payload->>'kind'='ActivityUpsert' AND l1.payload->>'deploymentId'=${deploymentId}
          AND l1.payload->>'subject'=${operation.subject} AND l1.payload->>'accountId'=${operation.accountId}
          AND l1.payload->>'resourceId'=${operation.resourceId} AND l1.payload->>'lifecycleSuccessorId'=${successor}
          AND l1.idempotency_key<>${operation.idempotencyKey} AND l1.payload->>'idempotencyKey'=l1.idempotency_key
        ORDER BY l1.created_at DESC, l1.id DESC LIMIT 1
      `
      const evidence = retained[0]
      if (!evidence) throw new Error('retained delivered lifecycle evidence is missing or outside recovery scope')
      const sourcePrefix = `${deploymentId}:${evidence.source_resource_incarnation_id}:`
      if (operation.deploymentId !== deploymentId || !operation.resourceId.startsWith(sourcePrefix) || !successor.startsWith(sourcePrefix)) throw new Error('recovery incarnation scope mismatch')
      const lifecycleOperation = evidence.lifecycle_operation
      const receiptBase = bridgeReceiptUrl!.toString().replace(/\/+$/, '')
      const response = await fetch(`${receiptBase}/${encodeURIComponent(lifecycleOperation.subject)}/${encodeURIComponent(lifecycleOperation.idempotencyKey)}`, {
        method: 'GET', redirect: 'error', headers: { authorization: `Bearer ${bridgeToken}` },
        signal: AbortSignal.timeout(BRIDGE_REQUEST_TIMEOUT_MS),
      })
      if (response.status === 404) throw new Error('bound lifecycle receipt is missing')
      if (!response.ok) throw new Error('bound lifecycle receipt lookup failed')
      const boundReceipt = await response.json()
      validateBridgeReceipt(lifecycleOperation, boundReceipt)

      // L2 is only the failed-row CAS target. Derive S from the latest retained
      // canonical facts, not L2's obsolete metadata or observation revision.
      const ordinary = await sql.begin('isolation level repeatable read read only', async transaction => {
        const tx = transaction as unknown as FinanceSyncSql
        const [latest] = await tx<InvestecPostedCardLifecycleRow[]>`
          SELECT payload,raw_payload,provider_version,semantic_hash,hash_specification_version,observed_at
          FROM raw_observations WHERE deployment_id=${deploymentId} AND local_owner_id=${operation.subject}
            AND source_resource_incarnation_id=${evidence.source_resource_incarnation_id}::uuid
            AND provider_version LIKE 'investec:txn:%' AND payload->>'transactionId'=${successor.slice(sourcePrefix.length)}
          ORDER BY recorded_at DESC,observation_id DESC LIMIT 1
        `
        const payload = latest && recordPayload(latest.payload)
        if (!latest || !payload || payload.accountId !== evidence.provider_account_id || payload.status !== 'posted' || payload.isRemoved === true) throw new Error('latest canonical settlement is missing or outside recovery scope')
        const observation: ObservationOutput = {
          deploymentId, localOwnerId: operation.subject, sourceResourceIncarnationId: evidence.source_resource_incarnation_id,
          providerVersion: latest.provider_version, semanticHash: latest.semantic_hash, hashSpecificationVersion: latest.hash_specification_version,
          observedAt: latest.observed_at, domain: 'cash_ledger', payload,
        }
        if (await observationIsQuarantined(tx, deploymentId, operation.subject, observation)) throw new Error('latest canonical settlement is quarantined')
        const connection: ProjectionConnection = { provider: 'investec', localOwnerId: operation.subject,
          sourceResourceIncarnationId: evidence.source_resource_incarnation_id, accountIds: [evidence.provider_account_id], connectionId: evidence.connection_id }
        const counterpart = await findInvestecCounterpart(tx, deploymentId, connection, observation)
        if (!counterpart || 'ambiguous' in counterpart || transactionId(counterpart.payload) !== operation.resourceId.slice(sourcePrefix.length)) throw new Error('latest canonical lifecycle pair is outside recovery scope')
        if (await investecObservationRevision(tx, deploymentId, operation.subject, evidence.source_resource_incarnation_id, counterpart) === undefined) throw new Error('latest canonical card revision is missing')
        const observationRevision = await investecObservationRevision(tx, deploymentId, operation.subject, evidence.source_resource_incarnation_id, observation)
        if (observationRevision === undefined) throw new Error('latest canonical revision is missing')
        const operations = await investecProjectionOperations(tx, deploymentId, connection, { ...observation, observationRevision }, operation.accessGeneration, operation.accountId, evidence.connection_id)
        const ordinary = operations[0]
        if (operations.length !== 1 || ordinary.kind !== 'ActivityUpsert' || ordinary.resourceId !== successor || ordinary.lifecycleSuccessorId !== undefined || ordinary.activity?.status !== 'POSTED') throw new Error('latest canonical successor cannot be recovered')
        return ordinary
      })
      return { lifecycleOperation, ordinaryOperation: ordinary, boundReceipt }
    },
    deliver: async (operation: BridgeDisclosureOperation, beforePost?: () => Promise<void>) => {
      const receiptBase = bridgeReceiptUrl!.toString().replace(/\/+$/, '')
      const existing = await fetch(`${receiptBase}/${encodeURIComponent(operation.subject)}/${encodeURIComponent(operation.idempotencyKey)}`, {
        method: 'GET', redirect: 'error', headers: { authorization: `Bearer ${bridgeToken}` },
        signal: AbortSignal.timeout(BRIDGE_REQUEST_TIMEOUT_MS),
      })
      if (existing.ok) return existing.json()
      if (existing.status !== 404) throw new Error('terminal recovery ordinary receipt lookup failed')
      await beforePost?.()
      const response = await fetch(bridgeUrl!, {
        method: 'POST', redirect: 'error', signal: AbortSignal.timeout(BRIDGE_REQUEST_TIMEOUT_MS),
        headers: { 'content-type': 'application/json', authorization: `Bearer ${bridgeToken}`, 'idempotency-key': operation.idempotencyKey },
        body: JSON.stringify(bridgeOperationToWire(operation)),
      })
      if (!response.ok) throw new Error('terminal recovery bridge delivery failed')
      return response.json()
    },
  } : undefined

  return {
    terminalRevisionResolver,
    loadConnections: async () => {
      if (fixtureRuntime) {
        await ensureFixtureAuthority(sql, deploymentId)
        if (!bridgeUrl || !bridgeReceiptUrl || !bridgeToken) throw new Error('bridge configuration is required for fixture connections')
        return admittedFixtureConnections(sql, deploymentId, fixtureRuntime.connections)
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
        const deadLetters = await tx<{ kind: string | null; count: number }[]>`
          SELECT payload->>'kind' AS kind, count(*)::integer AS count
          FROM projection_outbox
          WHERE deployment_id = ${deploymentId}
            AND connection_id = ${connection.connectionId}
            AND provider_generation = ${connection.providerGeneration}
            AND state = 'dead_letter'
          GROUP BY payload->>'kind'
        `
        const deadLetterCount = deadLetters.reduce((total, row) => total + row.count, 0)
        const currentStateDeadLetterCount = deadLetters
          .filter(row => row.kind === 'CurrentStateUpsert')
          .reduce((total, row) => total + row.count, 0)
        // A failed current-state write can make later snapshots unsafe to apply,
        // so retain the original full-stop behavior for that lane. An activity or
        // lifecycle dead letter is independent of a later account snapshot; let
        // the snapshot through while keeping activity delivery and the checkpoint
        // fenced until the dead letter is explicitly resolved. Terminal dead
        // letters are immutable and operator-recoverable through the authenticated
        // recovery control, so they are not in-flight work and must not fail the
        // connection cycle.
        if (currentStateDeadLetterCount > 0) return { delivered: 0, failed: deadLetterCount }

        let delivered = 0
        let deliveryFailures = 0
        const rows = deadLetterCount > 0
          ? await tx<{ id: string; payload: BridgeDisclosureOperation; attempts: number; state: 'pending' | 'uncertain' }[]>`
            SELECT id, payload, attempts, state
            FROM projection_outbox
            WHERE deployment_id = ${deploymentId}
              AND connection_id = ${connection.connectionId}
              AND provider_generation = ${connection.providerGeneration}
              AND state IN ('pending', 'uncertain')
              AND payload->>'kind' = 'CurrentStateUpsert'
            ORDER BY (payload->>'kind' = 'CurrentStateUpsert') DESC,
              CASE WHEN payload->>'kind' = 'CurrentStateUpsert' THEN created_at END DESC,
              created_at, (payload ? 'lifecycleSuccessorId'), id
            LIMIT ${MAX_PROJECTION_BATCH}
            FOR UPDATE SKIP LOCKED
          `
          : await tx<{ id: string; payload: BridgeDisclosureOperation; attempts: number; state: 'pending' | 'uncertain' }[]>`
            SELECT id, payload, attempts, state
            FROM projection_outbox
            WHERE deployment_id = ${deploymentId}
              AND connection_id = ${connection.connectionId}
              AND provider_generation = ${connection.providerGeneration}
              AND state IN ('pending', 'uncertain')
            ORDER BY (payload->>'kind' = 'CurrentStateUpsert') DESC,
              CASE WHEN payload->>'kind' = 'CurrentStateUpsert' THEN created_at END DESC,
              created_at, (payload ? 'lifecycleSuccessorId'), id
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
          const recoverReceipt = async (markRecovered = true): Promise<boolean> => {
            try {
              const lookup = receiptLookup ?? (async (operation: BridgeDisclosureOperation): Promise<unknown | null> => {
                if (!bridgeReceiptUrl || !bridgeToken) return null
                const base = bridgeReceiptUrl.toString().replace(/\/+$/, '')
                const response = await fetch(`${base}/${encodeURIComponent(operation.subject)}/${encodeURIComponent(operation.idempotencyKey)}`, {
                  method: 'GET', redirect: 'error', headers: { authorization: `Bearer ${bridgeToken}` },
                  signal: AbortSignal.timeout(BRIDGE_REQUEST_TIMEOUT_MS),
                })
                if (response.status === 404) return null
                if (!response.ok) throw new Error('bridge receipt lookup failed')
                return response.json()
              })
              const recovered = await lookup(row.payload)
              if (recovered === null) return false
              validateBridgeReceipt(row.payload, recovered)
              if (markRecovered) await markDelivered()
              return true
            } catch {
              return false
            }
          }

          const requireInvestecRecovery = async () => {
            await tx`
              UPDATE projection_outbox
              SET state = 'uncertain', last_error = 'INVESTEC_LIFECYCLE_RECOVERY_REQUIRED'
              WHERE id = ${row.id}::uuid
                AND provider_generation = ${connection.providerGeneration}
                AND state IN ('pending', 'uncertain')
            `
            deliveryFailures++
          }

          if (connection.provider === 'investec'
            && row.payload.kind === 'ActivityUpsert'
            && row.payload.activity?.status === 'PENDING'
            && row.payload.lifecycleSuccessorId === undefined) {
            const lifecycleDisposition = await investecLifecycleDisposition(tx, connection, row.payload)
            if (lifecycleDisposition === 'delivered') {
              if (row.state === 'uncertain') {
                // An uncertain pending POST may already have committed after the
                // terminal lifecycle. Never hide that possibility as suppressed.
                await recoverReceipt(false)
                await requireInvestecRecovery()
              } else {
                await tx`
                  UPDATE projection_outbox
                  SET state = 'suppressed', last_error = 'INVESTEC_LIFECYCLE_ALREADY_TERMINAL'
                  WHERE id = ${row.id}::uuid
                    AND provider_generation = ${connection.providerGeneration}
                    AND state = 'pending'
                `
              }
              continue
            }
            if (lifecycleDisposition === 'uncertain') {
              await requireInvestecRecovery()
              continue
            }
          }

          // Never replay an uncertain commit before attempting receipt recovery.
          // An unresolved lifecycle is a fence: retrying its pending predecessor
          // could resurrect the activity while the terminal receipt is unknown.
          if (connection.provider === 'investec'
            && row.state === 'uncertain' && row.payload.lifecycleSuccessorId !== undefined) {
            // A lifecycle recovery attempt is authoritative for this pass. Do
            // not issue a second lookup or replay the lifecycle after it.
            await recoverReceipt()
            continue
          }
          if (row.state === 'uncertain' && await recoverReceipt()) continue

          let response: Response
          try {
            response = await fetch(bridgeUrl, {
              method: 'POST',
              redirect: 'error',
              signal: AbortSignal.timeout(BRIDGE_REQUEST_TIMEOUT_MS),
              headers: {
                'content-type': 'application/json',
                authorization: `Bearer ${bridgeToken}`,
                'idempotency-key': row.payload.idempotencyKey,
              },
              body: JSON.stringify(bridgeOperationToWire(row.payload)),
            })
          } catch {
            if (await recoverReceipt()) continue
            // The commit may have landed: keep the row uncertain for the bound
            // receipt lookup, but do not spend its terminal budget on an
            // unreachable receiver.
            await tx`
              UPDATE projection_outbox
              SET last_error = 'BRIDGE_RESPONSE_UNCERTAIN', state = 'uncertain'
              WHERE id = ${row.id}::uuid
                AND provider_generation = ${connection.providerGeneration}
            `
            deliveryFailures++
            continue
          }

          if (!response.ok) {
            // 5xx and 429 mean the receiver could not serve the request right
            // now (an unwritable database, a restart, a rate limit). That is
            // transient, so the row stays queued and does not consume the
            // terminal-attempt budget: a receiver outage must never turn into
            // dead letters that an operator has to resolve by hand.
            if (response.status >= 500 || response.status === 429) {
              await tx`
                UPDATE projection_outbox SET last_error = 'BRIDGE_UNAVAILABLE'
                WHERE id = ${row.id}::uuid
                  AND provider_generation = ${connection.providerGeneration}
              `
              deliveryFailures++
              continue
            }
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
            AND state IN ('pending', 'uncertain')
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
