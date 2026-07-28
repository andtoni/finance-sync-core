import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'
import { merchantContext } from '../shared/merchant-context'

// ─── Domain mapping ────────────────────────────────────────────────────────────

/**
 * SnapTrade resource kind → canonical resource domain mapping.
 */
export function snaptradeResourceKind(resource: {
  readonly domain: 'brokerage_account' | 'brokerage_cash' | 'holding' | 'brokerage_activity' | 'security'
}): CanonicalDomain {
  switch (resource.domain) {
    case 'brokerage_account': return 'balance'
    case 'brokerage_cash': return 'cash_ledger'
    case 'holding': return 'holding'
    case 'brokerage_activity': return 'brokerage_activity'
    case 'security': return 'security'
    default: return 'brokerage_activity'
  }
}

// ─── SnapTrade raw API shapes ───────────────────────────────────────────────────

export interface SnapTradeRawSymbol {
  readonly id?: string
  readonly universal_symbol_id?: string
  readonly universal_symbol?: { readonly id?: string }
  readonly symbol?: string | { readonly id?: string; readonly symbol?: string }
  readonly ticker?: string
  readonly name?: string
  readonly currency?: { readonly code?: string }
  readonly type?: { readonly code?: string; readonly description?: string }
}

export interface SnapTradeRawPosition {
  readonly symbol?: SnapTradeRawSymbol
  readonly units?: number
  readonly price?: number
  readonly market_value?: number | string
  readonly cost_basis?: number | string
  readonly average_purchase_price?: number
}

export interface SnapTradeRawTotalValue {
  readonly value?: number | string | null
  readonly currency?: string | null
}

export interface SnapTradeRawAccountDetail {
  readonly id?: string
  readonly name?: string
  readonly number?: string
  readonly institution_name?: string
  readonly raw_type?: string
  readonly meta?: { readonly type?: string }
}

export interface SnapTradeRawAccount {
  readonly account?: SnapTradeRawAccountDetail
  readonly institution_name?: string
  readonly name?: string
  readonly positions?: readonly SnapTradeRawPosition[]
  readonly total_value?: SnapTradeRawTotalValue | null
}

export interface SnapTradeRawActivity {
  readonly id?: string
  readonly account?: { readonly id?: string; readonly number?: string; readonly name?: string }
  readonly symbol?: SnapTradeRawSymbol | null
  readonly type?: string
  readonly units?: number | null
  readonly price?: number | null
  readonly amount?: number | string | null
  readonly currency?: { readonly code?: string } | null
  readonly trade_date?: string | null
  readonly settlement_date?: string | null
  readonly description?: string | null
}

// ─── Transport interface ──────────────────────────────────────────────────────

export interface SnapTradeTransport {
  /** Fetch all brokerage accounts with their positions (holdings). */
  fetchAccounts(): Promise<readonly SnapTradeRawAccount[]>

  /** Fetch the documented bounded activity date range. */
  fetchActivities(startDate: string, endDate: string): Promise<readonly SnapTradeRawActivity[] | { readonly data: readonly SnapTradeRawActivity[] }>
}

// ─── Config and result types ───────────────────────────────────────────────────

export interface SnapTradeSyncConfig {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  /** Optional: only process these account IDs. */
  readonly accountIds?: readonly string[]
  /** ISO date string for the start of the activity window. */
  readonly activitiesStartDate?: string
  readonly activitiesEndDate?: string
  readonly maxActivityRows?: number
  /** Immutable admission mappings. When supplied, unmapped holdings are quarantined and block completion. */
  readonly assetMappings?: Readonly<Record<string, string>>
}

export interface ObservationOutput {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly domain: CanonicalDomain
  readonly payload: Readonly<Record<string, unknown>>
  readonly observedAt: string
}

export interface SnapTradeSyncResult {
  readonly accounts: readonly SnapTradeNormalizedAccount[]
  readonly holdings: readonly SnapTradeNormalizedHolding[]
  readonly activities: readonly SnapTradeNormalizedActivity[]
  readonly observations: readonly ObservationOutput[]
  readonly accountsFetched: number
  readonly holdingsObserved: number
  readonly activitiesObserved: number
  readonly activityPasses: number
  readonly complete: boolean
  readonly quarantinedAssetIds: readonly string[]
}

export interface SnapTradeNormalizedAccount {
  readonly id: string
  readonly name: string
  readonly institutionName: string | null
  readonly accountNumber: string | null
  readonly totalValue: string | null
  readonly currency: string
  readonly brokerTag: string | null
  readonly accountTypeTag: string | null
}

export interface SnapTradeNormalizedHolding {
  readonly accountId: string
  readonly providerAssetId: string | null
  readonly symbol: string
  readonly name: string | null
  readonly units: string
  readonly price: string
  readonly value: string
  readonly currency: string
  readonly costBasis: string | null
  readonly brokerTag: string | null
  readonly accountTypeTag: string | null
  readonly instrumentTypeTag: string | null
}

export interface SnapTradeNormalizedActivity {
  readonly id: string
  readonly accountId: string | null
  /** SnapTrade's stable security id, required to pass an admitted asset mapping. */
  readonly providerAssetId: string | null
  readonly type: string
  readonly symbol: string | null
  readonly units: string | null
  readonly amount: string
  readonly currency: string
  readonly date: string
  readonly description: string | null
}

// ─── Helpers ───────────────────────────────────────────────────────────────────

const HASH_SPEC_VERSION = 1
const DEFAULT_MAX_ACTIVITY_ROWS = 10_000
const ACTIVITY_STABILITY_PASSES = 2

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

function brokerMoney(value: number | string | null | undefined): string | null {
  if (value == null) return null
  return typeof value === 'string' ? value : Number(value).toFixed(2)
}

function decimalString(value: number | null | undefined, fallback = '0.00'): string {
  if (value == null) return fallback
  if (!Number.isFinite(value)) return fallback
  return value.toFixed(4)
}

function priceString(value: number | null | undefined): string {
  if (value == null) return '0.00'
  if (!Number.isFinite(value)) return '0.00'
  return value.toFixed(2)
}

function toIsoDate(raw: string | null | undefined): string {
  if (!raw) return new Date().toISOString()
  try {
    const d = new Date(raw)
    return Number.isFinite(d.getTime()) ? d.toISOString() : new Date().toISOString()
  } catch {
    return new Date().toISOString()
  }
}

function providerAssetId(sym: SnapTradeRawSymbol | undefined): string | null {
  if (!sym) return null
  return sym.id ?? (typeof sym.symbol === 'object' ? sym.symbol?.id : undefined) ?? sym.universal_symbol_id ?? sym.universal_symbol?.id ?? null
}

function extractSymbol(sym: SnapTradeRawSymbol | undefined): string | null {
  if (!sym) return null
  const nested = sym.symbol
  if (typeof nested === 'string') return nested
  if (nested && typeof nested === 'object' && 'symbol' in nested) {
    const inner = (nested as { symbol?: string }).symbol
    if (inner) return inner
  }
  return sym.ticker ?? null
}

function extractCurrency(rawCcy: { readonly code?: string } | string | null | undefined): string {
  if (!rawCcy) return 'USD'
  if (typeof rawCcy === 'string') return rawCcy
  return rawCcy.code ?? 'USD'
}

/** Institution name → broker tag. */
function brokerTag(institution: string | null | undefined): string | null {
  if (!institution) return null
  const s = institution.toLowerCase()
  if (s.includes('schwab')) return 'schwab'
  if (s.includes('fidelity')) return 'fidelity'
  if (s.includes('alpaca')) return 'alpaca'
  if (s.includes('vanguard')) return 'vanguard'
  if (s.includes('robinhood')) return 'robinhood'
  return 'other'
}

/** SnapTrade raw_type + meta → account type tag. */
function accountTypeTag(raw: string | null | undefined, meta: string | null | undefined): string | null {
  const s = `${raw ?? ''} ${meta ?? ''}`.toLowerCase()
  if (!s.trim()) return null
  if (s.includes('roth')) return 'roth_ira'
  if (s.includes('401')) return '401k'
  if (s.includes('hsa')) return 'hsa'
  if (s.includes('traditional') || s.includes('trad') || s.includes('ira')) return 'trad_ira'
  if (s.includes('taxable') || s.includes('individual') || s.includes('margin') || s.includes('brokerage') || s.includes('joint'))
    return 'taxable'
  if (s.includes('529')) return '529'
  if (s.includes('403')) return '403b'
  return null
}

/** Security type code/description → instrument type tag. */
function instrumentTypeTag(
  typeCode: string | null | undefined,
  typeDesc: string | null | undefined,
  hasSymbol: boolean,
): string | null {
  const s = `${typeCode ?? ''} ${typeDesc ?? ''}`.toLowerCase()
  if (s.includes('cash') || !hasSymbol) return 'cash'
  if (s.includes('etf') || s.includes('exchange traded')) return 'etf'
  if (s.includes('mutual') || s.includes('open end') || s.includes('closed end')) return 'mutual_fund'
  if (s.includes('crypto') || s.includes('digital')) return 'crypto'
  if (s.includes('option')) return 'option'
  if (s.includes('stock') || s.includes('equity') || s.includes('adr')) return 'equity'
  return typeCode || typeDesc ? 'equity' : null
}

/** Extract a stable account id from a SnapTrade account object. */
function accountIdOf(acct: SnapTradeRawAccount): string {
  return acct.account?.id ?? acct.account?.number ?? acct.account?.name ?? acct.name ?? 'unknown'
}

/** Map SnapTrade activity type to canonical kind. */
function activityKindFrom(type: string | null | undefined): string | null {
  const s = (type ?? '').trim().toUpperCase()
  if (!s) return null
  if (s === 'BUY') return 'buy'
  if (s === 'SELL') return 'sell'
  if (s === 'DIVIDEND' || s === 'DISTRIBUTION') return 'dividend'
  if (s === 'CONTRIBUTION' || s === 'DEPOSIT') return 'contribution'
  if (s === 'WITHDRAWAL') return 'withdrawal'
  if (s === 'TRANSFER' || s === 'EXTERNAL_TRANSFER' || s === 'INTERNAL_TRANSFER') return 'transfer'
  if (s === 'FEE') return 'fee'
  if (s === 'INTEREST') return 'interest'
  return null
}

// ─── Normalization functions ───────────────────────────────────────────────────

/**
 * Normalize a SnapTrade account, extracting metadata and total value.
 */
export function normalizeAccount(acct: SnapTradeRawAccount): SnapTradeNormalizedAccount {
  const detail = acct.account
  const id = accountIdOf(acct)
  const name = detail?.name ?? acct.name ?? id
  const institution = detail?.institution_name ?? acct.institution_name ?? null
  const totalValue = brokerMoney(acct.total_value?.value)
  const currencyVal = extractCurrency(acct.total_value?.currency)
  return {
    id,
    name,
    institutionName: institution,
    accountNumber: detail?.number ?? null,
    totalValue,
    currency: currencyVal,
    brokerTag: brokerTag(institution),
    accountTypeTag: accountTypeTag(detail?.raw_type, detail?.meta?.type),
  }
}

/**
 * Normalize positions/holdings from an account's positions array.
 */
export function normalizeHoldings(
  account: SnapTradeRawAccount,
): SnapTradeNormalizedHolding[] {
  const accountId = accountIdOf(account)
  const institution = account.account?.institution_name ?? account.institution_name ?? null
  const accTypeRaw = account.account?.raw_type
  const accTypeMeta = account.account?.meta?.type
  const broker = brokerTag(institution)
  const accType = accountTypeTag(accTypeRaw, accTypeMeta)

  const positions = account.positions ?? []
  return positions.map((pos) => {
    const sym = extractSymbol(pos.symbol)
    const symbol = sym ?? '?'
    const name = pos.symbol?.name ?? sym ?? symbol
    const units = decimalString(pos.units)
    const price = priceString(pos.price)
    const value = brokerMoney(pos.market_value) ?? '0.00'
    const currencyVal = extractCurrency(pos.symbol?.currency)
    const costBasis = brokerMoney(pos.cost_basis) ??
      (pos.average_purchase_price != null ? (Number(pos.units ?? 0) * Number(pos.average_purchase_price)).toFixed(2) : null)
    const instrType = instrumentTypeTag(
      pos.symbol?.type?.code,
      pos.symbol?.type?.description,
      sym != null,
    )

    return {
      accountId,
      providerAssetId: pos.symbol?.id ?? (typeof pos.symbol?.symbol === 'object' ? pos.symbol.symbol.id : undefined)
        ?? pos.symbol?.universal_symbol_id ?? pos.symbol?.universal_symbol?.id ?? null,
      symbol,
      name: name ?? symbol,
      units,
      price,
      value,
      currency: currencyVal,
      costBasis,
      brokerTag: broker,
      accountTypeTag: accType,
      instrumentTypeTag: instrType,
    }
  })
}

/**
 * Normalize a SnapTrade activities payload into canonical activity records.
 * Accepts both bare array and `{ data: [...] }` paginated wrapper.
 * Unmappable rows are silently skipped.
 */
export function normalizeActivities(
  payload: unknown,
): SnapTradeNormalizedActivity[] {
  const list: readonly SnapTradeRawActivity[] = Array.isArray(payload)
    ? (payload as SnapTradeRawActivity[])
    : Array.isArray((payload as { readonly data?: unknown[] } | null)?.data)
      ? (payload as { readonly data: SnapTradeRawActivity[] }).data
      : []

  const out: SnapTradeNormalizedActivity[] = []
  for (const a of list) {
    if (!a || typeof a !== 'object' || !a.id) continue
    const kind = activityKindFrom(a.type)
    if (!kind) continue
    const dateRaw = a.trade_date ?? a.settlement_date
    if (!dateRaw) continue
    const date = String(dateRaw).slice(0, 10)
    const symbol = extractSymbol(a.symbol ?? undefined)
    const unitsNum = a.units != null ? Number(a.units) : null
    const units = unitsNum != null && unitsNum !== 0 && Number.isFinite(unitsNum) ? unitsNum.toFixed(4) : null
    const amount = brokerMoney(a.amount) ?? (units != null && a.price != null ? (Number(units) * Number(a.price)).toFixed(2) : null)
    if (amount == null) continue
    out.push({
      id: 'snaptrade:' + a.id,
      accountId: a.account?.id ?? a.account?.number ?? null,
      providerAssetId: providerAssetId(a.symbol ?? undefined),
      type: kind,
      symbol,
      units,
      amount,
      currency: extractCurrency(a.currency),
      date,
      description: a.description ?? null,
    })
  }
  return out
}

// ─── Adapter sync function ─────────────────────────────────────────────────────

/**
 * Run one SnapTrade sync session:
 * 1. Fetch all accounts with positions (holdings)
 * 2. Extract account-level balance observations
 * 3. Extract holding observations per account (cash positions → cash_ledger)
 * 4. Fetch activities for the configured date window
 * 5. Produce canonical ObservationOutput records with semantic hashes
 *
 * Partial failures are tolerated — observations accumulated before an error
 * are returned.
 */
export async function runSnapTradeSync(
  config: SnapTradeSyncConfig,
  transport: SnapTradeTransport,
): Promise<SnapTradeSyncResult> {
  const observations: ObservationOutput[] = []
  const accounts: SnapTradeNormalizedAccount[] = []
  const holdings: SnapTradeNormalizedHolding[] = []
  const activities: SnapTradeNormalizedActivity[] = []
  let accountsFetched = 0
  let holdingsObserved = 0
  let activitiesObserved = 0
  let complete = true
  const quarantinedAssetIds = new Set<string>()

  // 1. Fetch accounts with positions
  let rawAccounts: readonly SnapTradeRawAccount[]
  try {
    rawAccounts = await transport.fetchAccounts()
    accountsFetched = rawAccounts.length
  } catch {
    return { accounts: [], holdings: [], activities: [], observations, accountsFetched: 0, holdingsObserved: 0, activitiesObserved: 0, activityPasses: 0, complete: false, quarantinedAssetIds: [] }
  }

  // Filter to configured account IDs if provided
  if (config.accountIds && config.accountIds.length > 0) {
    const idSet = new Set(config.accountIds)
    rawAccounts = rawAccounts.filter(a => idSet.has(accountIdOf(a)))
  }

  // Short-circuit: no accounts, nothing to sync
  if (rawAccounts.length === 0) {
    return { accounts: [], holdings: [], activities: [], observations, accountsFetched, holdingsObserved: 0, activitiesObserved: 0, activityPasses: 0, complete: true, quarantinedAssetIds: [] }
  }

  // 2. Process each account
  for (const rawAcct of rawAccounts) {
    const normAccount = normalizeAccount(rawAcct)
    accounts.push(normAccount)

    // Emit account balance observation (domain: balance)
    const balancePayload = {
      accountId: normAccount.id,
      accountName: normAccount.name,
      institutionName: normAccount.institutionName,
      accountNumber: normAccount.accountNumber,
      totalValue: normAccount.totalValue,
      currency: normAccount.currency,
      brokerTag: normAccount.brokerTag,
      accountTypeTag: normAccount.accountTypeTag,
    }
    const balanceSemantic = [
      config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
      'balance', normAccount.id, normAccount.totalValue ?? '0.00', normAccount.currency,
    ].join('\x00')
    const balanceHash = sha256(balanceSemantic)

    observations.push({
      deploymentId: config.deploymentId,
      localOwnerId: config.localOwnerId,
      sourceResourceIncarnationId: config.sourceResourceIncarnationId,
      providerVersion: 'snaptrade:account:' + normAccount.id,
      semanticHash: balanceHash,
      hashSpecificationVersion: HASH_SPEC_VERSION,
      domain: 'balance',
      payload: Object.freeze(balancePayload) as Readonly<Record<string, unknown>>,
      observedAt: new Date().toISOString(),
    })

    // Emit holding observations (holdings + cash positions)
    const normHoldings = normalizeHoldings(rawAcct)
    for (const h of normHoldings) {
      const mappedAssetId = h.providerAssetId && config.assetMappings?.[h.providerAssetId]
      if (config.assetMappings && !mappedAssetId) {
        // Unknown securities never become a guessed Wealthfolio asset or a checkpoint-eligible snapshot.
        quarantinedAssetIds.add(h.providerAssetId ?? `unidentified:${h.accountId}:${h.symbol}`)
        complete = false
        continue
      }
      holdings.push(h)
      holdingsObserved++

      const domain: CanonicalDomain = h.instrumentTypeTag === 'cash' ? 'cash_ledger' : 'holding'
      const payload = {
        accountId: h.accountId,
        providerAssetId: h.providerAssetId,
        ...(mappedAssetId ? { wealthfolioAssetId: mappedAssetId } : {}),
        symbol: h.symbol,
        name: h.name,
        units: h.units,
        price: h.price,
        value: h.value,
        currency: h.currency,
        costBasis: h.costBasis,
        brokerTag: h.brokerTag,
        instrumentTypeTag: h.instrumentTypeTag,
      }
      const semanticPayload = [
        config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
        domain, h.accountId, h.symbol, h.units, h.value, h.currency,
      ].join('\x00')
      const semanticHash = sha256(semanticPayload)

      observations.push({
        deploymentId: config.deploymentId,
        localOwnerId: config.localOwnerId,
        sourceResourceIncarnationId: config.sourceResourceIncarnationId,
        providerVersion: 'snaptrade:holding:' + h.accountId + ':' + h.symbol,
        semanticHash,
        hashSpecificationVersion: HASH_SPEC_VERSION,
        domain,
        payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
        observedAt: new Date().toISOString(),
      })
    }
  }

  // 3. Fetch activities
  const startDate = config.activitiesStartDate ?? new Date(Date.now() - 90 * 86_400_000).toISOString().slice(0, 10)
  const endDate = config.activitiesEndDate ?? new Date().toISOString().slice(0, 10)

  let activityPasses = 0
  try {
    // The documented user-level endpoint has a bounded date window but no
    // cursor parameter. Require two identical bounded snapshots instead of
    // guessing undocumented pagination fields or accepting a moving partial run.
    const maxRows = config.maxActivityRows ?? DEFAULT_MAX_ACTIVITY_ROWS
    if (!Number.isSafeInteger(maxRows) || maxRows < 1) throw new Error('invalid activity bound')
    let stableActivities: SnapTradeNormalizedActivity[] | null = null
    let stableSignature: string | null = null
    for (let pass = 0; pass < ACTIVITY_STABILITY_PASSES; pass++) {
      const actPayload = await transport.fetchActivities(startDate, endDate)
      activityPasses++
      const normalized = normalizeActivities(actPayload)
      if (normalized.length > maxRows) throw new Error('activity bound exceeded')
      const signature = sha256(JSON.stringify([...normalized].sort((a, b) => a.id.localeCompare(b.id))))
      if (stableSignature !== null && signature !== stableSignature) throw new Error('activity snapshot unstable')
      stableActivities = normalized
      stableSignature = signature
    }

    for (const a of stableActivities ?? []) {
      const requiresAssetMapping = a.symbol !== null
      const mappedAssetId = a.providerAssetId && config.assetMappings?.[a.providerAssetId]
      if (config.assetMappings && requiresAssetMapping && !mappedAssetId) {
        quarantinedAssetIds.add(a.providerAssetId ?? `unidentified:activity:${a.accountId ?? 'unknown'}:${a.id}`)
        complete = false
        continue
      }
      activities.push(a)
      activitiesObserved++

      const payload = {
        sourceId: a.id,
        accountId: a.accountId,
        providerAssetId: a.providerAssetId,
        ...(mappedAssetId ? { wealthfolioAssetId: mappedAssetId } : {}),
        type: a.type,
        symbol: a.symbol,
        units: a.units,
        amount: a.amount,
        currency: a.currency,
        date: a.date,
        description: a.description,
        merchantContext: merchantContext({
          provider: 'snaptrade', rawDescription: a.description, merchantDisplay: a.description,
          activityType: a.type,
          // Explicit brokerage provenance keeps these semantics out of household spending categorisation.
          providerHint: { lane: 'brokerage', symbol: a.symbol },
        }),
      }
      const semanticPayload = [
        config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
        'brokerage_activity', a.id, a.providerAssetId ?? '', a.type, a.amount, a.currency, a.date,
      ].join('\x00')
      const semanticHash = sha256(semanticPayload)

      observations.push({
        deploymentId: config.deploymentId,
        localOwnerId: config.localOwnerId,
        sourceResourceIncarnationId: config.sourceResourceIncarnationId,
        providerVersion: 'snaptrade:activity:' + a.id,
        semanticHash,
        hashSpecificationVersion: HASH_SPEC_VERSION,
        domain: 'brokerage_activity',
        payload: Object.freeze(payload) as Readonly<Record<string, unknown>>,
        observedAt: toIsoDate(a.date),
      })
    }
  } catch {
    // Never promote the restart cursor for a partial provider snapshot.
    complete = false
  }

  return {
    accounts: Object.freeze(accounts),
    holdings: Object.freeze(holdings),
    activities: Object.freeze(activities),
    observations: Object.freeze(observations),
    accountsFetched,
    holdingsObserved,
    activitiesObserved,
    activityPasses,
    complete,
    quarantinedAssetIds: Object.freeze([...quarantinedAssetIds].sort()),
  }
}
