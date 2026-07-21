import { createHash } from 'node:crypto'
import type { ProviderName } from '../probes/provider-registry'
import type { ObservationOutput } from '../probes/plaid/plaid-adapter'

/**
 * Current-state wire arithmetic is:
 *   account value = cashBalance + sum(positions[].marketValue) - liabilityBalance.
 * liabilityBalance is always a positive debt magnitude; the receiver subtracts it exactly once.
 */
export interface CurrentStatePosition {
  readonly symbol: string
  readonly quantity: string
  readonly price: string
  readonly marketValue: string
  readonly currency: string
  readonly costBasis?: string
}

export interface CurrentStatePayload {
  /** Receiver (Wealthfolio) account id, not the provider account id. */
  readonly accountId: string
  readonly providerConnectionId: string
  /** Stable immutable resource incarnation, never a poll-time identifier. */
  readonly providerIncarnationId: string
  readonly providerGeneration: number
  readonly observedAt: string
  /** Monotonic poll ordering derived from observedAt epoch milliseconds. */
  readonly revision: number
  readonly asOfDate: string
  readonly currency: string
  readonly cashBalance: string
  /** Positive debt magnitude. Zero for asset accounts. */
  readonly liabilityBalance: string
  /** Complete replacement set. An empty array explicitly means no positions. */
  readonly positions: readonly CurrentStatePosition[]
}

export interface AdmittedCurrentStateAccount {
  readonly providerAccountId: string
  readonly accountId: string
  /** Stable provider asset id to admitted Wealthfolio asset id. */
  readonly assetMappings?: Readonly<Record<string, string>>
}

export interface CurrentStateAssemblyContext {
  readonly providerConnectionId: string
  readonly providerIncarnationId: string
  readonly providerGeneration: number
}

export interface AssembledCurrentState {
  readonly providerAccountId: string
  readonly payload: CurrentStatePayload
  /** SHA-256 of the complete, canonical payload. */
  readonly semanticHash: string
}

function record(value: unknown): Readonly<Record<string, unknown>> | null {
  return value !== null && typeof value === 'object' && !Array.isArray(value)
    ? value as Readonly<Record<string, unknown>>
    : null
}

function decimal(value: unknown, field = 'decimal'): string {
  if ((typeof value !== 'string' && typeof value !== 'number') || String(value).trim() === '') throw new Error(`incomplete current-state snapshot: ${field}`)
  const input = String(value).trim()
  if (!/^[+-]?\d+(?:\.\d+)?$/.test(input)) throw new Error(`incomplete current-state snapshot: ${field}`)
  const negative = input.startsWith('-')
  const unsigned = input.replace(/^[+-]/, '')
  const [integer, fraction = ''] = unsigned.split('.')
  const canonicalInteger = integer.replace(/^0+(?=\d)/, '')
  const canonicalFraction = fraction.replace(/0+$/, '')
  const magnitude = canonicalFraction ? `${canonicalInteger}.${canonicalFraction}` : canonicalInteger
  return negative && magnitude !== '0' ? `-${magnitude}` : magnitude
}

function decimalParts(value: string): { coefficient: bigint; scale: number } {
  const canonical = decimal(value)
  const negative = canonical.startsWith('-')
  const unsigned = negative ? canonical.slice(1) : canonical
  const [integer, fraction = ''] = unsigned.split('.')
  return {
    coefficient: BigInt(`${integer}${fraction}`) * (negative ? -1n : 1n),
    scale: fraction.length,
  }
}

function subtractDecimal(left: string, right: string): string {
  const a = decimalParts(left); const b = decimalParts(right)
  const scale = Math.max(a.scale, b.scale)
  const coefficient = a.coefficient * (10n ** BigInt(scale - a.scale)) - b.coefficient * (10n ** BigInt(scale - b.scale))
  const negative = coefficient < 0n
  const digits = (negative ? -coefficient : coefficient).toString().padStart(scale + 1, '0')
  const magnitude = scale === 0 ? digits : `${digits.slice(0, -scale)}.${digits.slice(-scale)}`
  return decimal(`${negative ? '-' : ''}${magnitude}`)
}

function absoluteDecimal(value: string): string { return value.startsWith('-') ? value.slice(1) : value }

function currency(value: unknown): string {
  if (typeof value !== 'string' || !/^[A-Za-z]{3}$/.test(value.trim())) throw new Error('incomplete current-state snapshot: currency')
  return value.trim().toUpperCase()
}

function canonicalJson(value: unknown): string {
  if (value === null) return 'null'
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') {
    const input = value as Readonly<Record<string, unknown>>
    return `{${Object.keys(input).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(input[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}

export function currentStateSemanticHash(payload: CurrentStatePayload): string {
  return createHash('sha256').update(canonicalJson(payload), 'utf8').digest('hex')
}

function isBalanceObservation(provider: ProviderName, observation: ObservationOutput): boolean {
  const version = observation.providerVersion
  switch (provider) {
    case 'investec': return version.startsWith('investec:balance:')
    case 'mercury': return version.startsWith('mercury:balance:')
    case 'plaid': return version.startsWith('plaid:balance:')
    case 'snaptrade': return version.startsWith('snaptrade:account:')
  }
}

function asOfDate(observations: readonly ObservationOutput[]): string {
  const dates = observations.map(item => {
    const explicit = item.payload.asOfDate
    const candidate = typeof explicit === 'string' ? explicit : item.observedAt.slice(0, 10)
    if (!/^\d{4}-\d{2}-\d{2}$/.test(candidate)) throw new Error('incomplete current-state snapshot: asOfDate')
    return candidate
  }).sort()
  if (dates.length === 0) throw new Error('incomplete current-state snapshot: asOfDate')
  return dates[dates.length - 1]
}

function holdingPosition(observation: ObservationOutput, accountCurrency: string): CurrentStatePosition | null {
  if (observation.domain !== 'holding') return null
  const payload = observation.payload
  if (payload.instrumentTypeTag === 'cash') return null
  if (typeof payload.symbol !== 'string' || payload.symbol.length === 0) throw new Error('incomplete current-state snapshot: holding symbol')
  const positionCurrency = currency(payload.currency)
  if (positionCurrency !== accountCurrency) throw new Error('incomplete current-state snapshot: mixed position currency')
  const position: CurrentStatePosition = {
    symbol: payload.symbol,
    quantity: decimal(payload.units, 'holding quantity'),
    price: decimal(payload.price, 'holding price'),
    marketValue: decimal(payload.value, 'holding market value'),
    currency: positionCurrency,
    ...(payload.costBasis === null || payload.costBasis === undefined
      ? {}
      : { costBasis: decimal(payload.costBasis, 'holding cost basis') }),
  }
  return Object.freeze(position)
}

/**
 * Assemble one atomic replacement payload per admitted provider account.
 * This accepts observations from one completed provider run only. It refuses the
 * entire run if any admitted account lacks its current balance fact; callers
 * therefore cannot accidentally emit a partial snapshot or reuse history rows.
 */
export function assembleCurrentState(
  provider: ProviderName,
  observations: readonly ObservationOutput[],
  admittedAccounts: readonly AdmittedCurrentStateAccount[],
  context?: CurrentStateAssemblyContext,
): readonly AssembledCurrentState[] {
  const first = observations[0]
  const binding = context ?? {
    providerConnectionId: first?.sourceResourceIncarnationId ?? '',
    providerIncarnationId: first?.sourceResourceIncarnationId ?? '',
    providerGeneration: 0,
  }
  if (!binding.providerConnectionId || !binding.providerIncarnationId
    || !Number.isSafeInteger(binding.providerGeneration) || binding.providerGeneration < 0) {
    throw new Error('incomplete current-state snapshot: provider binding')
  }
  const admittedIds = new Set(admittedAccounts.map(item => item.providerAccountId))
  if (admittedIds.size !== admittedAccounts.length) throw new Error('duplicate admitted current-state account')
  const byAccount = new Map<string, ObservationOutput[]>()
  for (const observation of observations) {
    const providerAccountId = observation.payload.accountId
    if (typeof providerAccountId !== 'string' || !admittedIds.has(providerAccountId)) continue
    const bucket = byAccount.get(providerAccountId) ?? []
    bucket.push(observation)
    byAccount.set(providerAccountId, bucket)
  }

  const assembled = admittedAccounts.map(admitted => {
    const accountObservations = byAccount.get(admitted.providerAccountId) ?? []
    const balances = accountObservations.filter(observation => isBalanceObservation(provider, observation))
    if (balances.length !== 1) throw new Error(`incomplete current-state snapshot: balance for ${admitted.providerAccountId}`)
    const balance = balances[0]
    const accountCurrency = currency(balance.payload.currency)
    let cash = '0'
    let liability = '0'
    let positions: CurrentStatePosition[] = []

    if (provider === 'snaptrade') {
      const total = decimal(balance.payload.totalValue, 'SnapTrade total value')
      const holdingObservations = accountObservations.filter(item => item.domain === 'holding' && item.payload.instrumentTypeTag !== 'cash')
      const mappings = admitted.assetMappings ?? {}
      if (holdingObservations.some(item => typeof item.payload.providerAssetId !== 'string'
        || !mappings[item.payload.providerAssetId])) {
        throw new Error(`incomplete current-state snapshot: unmapped position for ${admitted.providerAccountId}`)
      }
      positions = holdingObservations.map(item => holdingPosition(item, accountCurrency)).filter((item): item is CurrentStatePosition => item !== null)
      const invested = positions.reduce((sum, item) => subtractDecimal(sum, `-${item.marketValue}`), '0')
      // SnapTrade total_value is the account total. The residual after the
      // complete non-cash position set is the cash/current balance.
      cash = subtractDecimal(total, invested)
    } else if (balance.domain === 'liability') {
      const current = decimal(balance.payload.currentBalance, 'liability current balance')
      liability = absoluteDecimal(current)
      if (provider === 'plaid') {
        const liabilityFacts = accountObservations.filter(item => item.providerVersion.startsWith('plaid:liability:'))
        if (liabilityFacts.length !== 1) throw new Error(`incomplete current-state snapshot: Plaid liability for ${admitted.providerAccountId}`)
      }
    } else {
      cash = decimal(balance.payload.currentBalance, 'current balance')
    }

    positions.sort((a, b) => a.symbol.localeCompare(b.symbol))
    const observedAt = accountObservations.map(item => item.observedAt).sort().at(-1)
    if (!observedAt || !Number.isFinite(Date.parse(observedAt))) throw new Error('incomplete current-state snapshot: observedAt')
    const revision = Date.parse(observedAt)
    const payload: CurrentStatePayload = Object.freeze({
      accountId: admitted.accountId,
      providerConnectionId: binding.providerConnectionId,
      providerIncarnationId: binding.providerIncarnationId,
      providerGeneration: binding.providerGeneration,
      observedAt,
      revision,
      asOfDate: asOfDate([balance, ...accountObservations.filter(item => item.domain === 'holding')]),
      currency: accountCurrency,
      cashBalance: cash,
      liabilityBalance: liability,
      positions: Object.freeze(positions),
    })
    return Object.freeze({
      providerAccountId: admitted.providerAccountId,
      payload,
      semanticHash: currentStateSemanticHash(payload),
    })
  })

  return Object.freeze(assembled)
}
