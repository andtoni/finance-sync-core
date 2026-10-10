
import { createHash } from 'node:crypto'
import type { CanonicalDomain } from '../../domain'
import { merchantContext } from '../shared/merchant-context'
import { recoverInvestecCardExecution, sanitizeInvestecCardEnvelope } from '../../investec-card-code-v1'
import { investecCardObservation } from '../../investec-card-observation'

export function investecResourceKind(): CanonicalDomain {
  return 'cash_ledger'
}

export interface ObservationOutput {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  readonly providerVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
  readonly domain: CanonicalDomain
  /** Normalized canonical fact used for identity and projection. */
  readonly payload: Readonly<Record<string, unknown>>
  /** Exact provider wire object, retained separately from normalized payload. */
  readonly rawPayload?: unknown
  readonly observedAt: string
}

interface InvestecTransaction {
  readonly uuid?: string
  readonly status?: string
  readonly postedOrder?: number
  readonly type?: string
  readonly transactionType?: string
  readonly amount?: string | number
  readonly postingDate?: string
  readonly actionDate?: string
  readonly transactionDate?: string
  readonly valueDate?: string
  readonly settlementDate?: string
  readonly settlementAmount?: string | number
  readonly settlementCurrency?: string
  readonly description?: string
  readonly reference?: string
  readonly merchantId?: string
  readonly merchantCode?: string
  readonly merchantReference?: string
  readonly merchant?: unknown
  readonly cardNumber?: string
  readonly runningBalance?: string | number
  readonly pendingTransactionId?: string
  readonly postedTransactionId?: string
  readonly settlementTransactionId?: string
  readonly successorTransactionId?: string
  readonly cardTransactionId?: string
  readonly linkedTransactionId?: string
  readonly providerLifecycleTransactionId?: string
}

export interface InvestecCardExecution {
  readonly executionId?: string
  readonly sandbox?: boolean
  readonly type?: string
  readonly authorizationApproved?: boolean | null
  readonly logs?: readonly { createdAt?: string; content?: string }[]
  readonly completedAt?: string
  readonly error?: unknown
  readonly Error?: unknown
  readonly [key: string]: unknown
}

export interface InvestecTransport {
  listAccounts(): Promise<readonly { accountId: string; productName?: string; accountName?: string; accountNumber?: string; referenceName?: string }[]>
  /** Official Card API audit/backstop; callers must ignore sandbox or incomplete executions. */
  listCards?(): Promise<readonly { cardKey?: string; accountId?: string; accountNumber?: string }[]>
  listCardExecutions?(cardKey: string): Promise<readonly InvestecCardExecution[]>
  getBalance(accountId: string): Promise<{ currency?: string; currentBalance?: string | number; availableBalance?: string | number }>
  getTransactionPage(accountId: string, window: { fromDate: string; toDate: string }, cursor?: string): Promise<{
    transactions: readonly InvestecTransaction[]
    nextCursor?: string
  }>
}

export interface InvestecAdapterConfig {
  readonly deploymentId: string
  readonly localOwnerId: string
  readonly sourceResourceIncarnationId: string
  /** Stable provider-connection scope shared by all admitted account legs. */
  readonly providerConnectionId?: string
  readonly fromDate: string
  readonly toDate: string
  readonly eligibleProductNames: readonly string[]
  readonly accountIds?: readonly string[]
  readonly maxPages?: number
}

export interface InvestecDiscoveredAccount {
  readonly providerAccountId: string
  readonly name: string
  readonly type: string
  readonly currency: string
  readonly mask?: string
}

/** Strictly normalize the documented account-list and balance responses for onboarding. */
export async function discoverInvestecAccounts(
  transport: InvestecTransport,
  eligibleProductNames: readonly string[],
): Promise<readonly InvestecDiscoveredAccount[]> {
  const eligible = new Set(eligibleProductNames)
  const discovered: InvestecDiscoveredAccount[] = []
  for (const account of await transport.listAccounts()) {
    if (typeof account.accountId !== 'string' || !account.accountId
      || typeof account.productName !== 'string' || !eligible.has(account.productName)) continue
    const balance = await transport.getBalance(account.accountId)
    const currency = balance.currency?.trim().toUpperCase()
    const name = account.accountName ?? account.referenceName ?? account.productName
    if (!name || !currency || !/^[A-Z]{3}$/.test(currency)) throw new Error('invalid Investec account discovery response')
    const mask = account.accountNumber?.slice(-4)
    discovered.push({
      providerAccountId: account.accountId,
      name,
      type: account.productName,
      currency,
      ...(mask ? { mask } : {}),
    })
  }
  return Object.freeze(discovered)
}

export interface InvestecAdapterResult {
  readonly accounts: readonly { accountRef: string; eligible: boolean; currency: string; displayName: string }[]
  readonly transferAccounts: Readonly<Record<string, string>>
  readonly observations: readonly ObservationOutput[]
  readonly quarantine: readonly { reason: string; accountRef: string; transactionRef?: string }[]
  readonly pagesPolled: number
  readonly paginationComplete: boolean
  readonly nextCursor: string | null
}

const BALANCE_HASH_SPEC_VERSION = 4
const AUDIT_HASH_SPEC_VERSION = 4
const TRANSACTION_HASH_SPEC_VERSION = 5
const PRIME_SAVER_IDENTITY_VERSION = 2

function sha256(input: string): string {
  return createHash('sha256').update(input, 'utf8').digest('hex')
}

function redact(value: string): string {
  if (value.length < 4) return value
  return '\u2026' + value.slice(-4)
}

function investecDecimal(value: unknown): string | null {
  if (typeof value !== 'string' && (typeof value !== 'number' || !Number.isFinite(value))) return null
  const trimmed = String(value).trim()
  return /^[+-]?\d+(?:\.\d+)?$/.test(trimmed) ? trimmed : null
}

/**
 * PrimeSaver returns booked rows with postedOrder=0 and an empty uuid. Those
 * rows are valid, but unlike Private Bank transactions they have no upstream
 * identity. Keep the identity tied to the provider's stable transaction
 * reference (usually the MBP/UBP reference embedded in the narrative), or to
 * the immutable-looking economic fields when no reference is available.
 *
 * Deliberately excluded: postingDate, actionDate, valueDate and runningBalance.
 * Investec revises those fields while retaining the same booked row; including
 * them creates a new resource ID and re-projects a duplicate activity.
 */
function identityText(value: unknown): string {
  return typeof value === 'string' ? value.trim().replace(/\s+/g, ' ').toUpperCase() : ''
}

function normalizedDirection(type: unknown, transactionType: unknown): string {
  const direct = identityText(type)
  if (direct === 'DEBIT' || direct === 'CREDIT') return direct
  const legacy = identityText(transactionType)
  return legacy === 'DEBIT' || legacy === 'CREDIT' ? legacy : ''
}

function normalizedProviderTransactionType(type: unknown, transactionType: unknown): string {
  const providerType = identityText(transactionType)
  if (providerType && providerType !== 'DEBIT' && providerType !== 'CREDIT') return providerType
  const legacyType = identityText(type)
  return legacyType !== 'DEBIT' && legacyType !== 'CREDIT' ? legacyType : ''
}

function stableProviderReference(reference: unknown, description: unknown): string | null {
  for (const candidate of [reference, description]) {
    const text = identityText(candidate)
    if (!text) continue
    const embedded = text.match(/\b(?:MBP|UBP)\d{6,}\b/)
    if (embedded) return embedded[0]
    if (candidate === reference) return text
  }
  return null
}

interface PrimeSaverIdentity {
  readonly transactionId: string
  readonly identityKey: string
  readonly strongReference: boolean
}

function primeSaverIdentity(
  accountId: string,
  amount: unknown,
  type: unknown,
  transactionType: unknown,
  description: unknown,
  reference: unknown,
  cardNumber: unknown,
  date: unknown,
): PrimeSaverIdentity | null {
  const normalizedAmount = investecDecimal(amount)
  if (!normalizedAmount) return null
  const providerReference = stableProviderReference(reference, description)
  const strongReference = providerReference !== null
  // Without the provider's stable reference, a missing transactionDate cannot
  // distinguish repeated same-value PrimeSaver rows. Fail closed instead of
  // assigning multiple economic facts to one synthetic resource.
  if (!strongReference && (typeof date !== 'string' || !date.trim())) return null
  const legacyType = identityText(type)
  const legacyTransactionType = identityText(transactionType)
  // v2 stored direction in transactionType. The current normalized payload
  // stores it in type and keeps the provider subtype separately; collapse that
  // split for identity so unchanged rows retain their deployed v2 resource ID.
  const comparableType = legacyType === 'DEBIT' || legacyType === 'CREDIT' ? '' : legacyType
  const comparableTransactionType = legacyType === 'DEBIT' || legacyType === 'CREDIT'
    ? legacyType
    : legacyTransactionType
  const fingerprint = strongReference
    ? [
      `prime-saver-identity-v${PRIME_SAVER_IDENTITY_VERSION}`,
      'reference', accountId, providerReference,
    ]
    : [
      `prime-saver-identity-v${PRIME_SAVER_IDENTITY_VERSION}`,
      'fallback', accountId, normalizedAmount,
      comparableType, comparableTransactionType, identityText(description),
      identityText(cardNumber), typeof date === 'string' ? date.trim() : '',
    ]
  const identityKey = `v${PRIME_SAVER_IDENTITY_VERSION}:${sha256(JSON.stringify(fingerprint))}`
  return {
    transactionId: `synthetic-${identityKey}`,
    identityKey,
    strongReference,
  }
}

/**
 * Compute the exact normalized key used only to discover legacy resource IDs.
 * Unlike the resource identity, this key uses fields retained in normalized
 * observations and excludes the provider date and running balance. It is not
 * a fuzzy match: account, amount, narrative, type, and card identity must all
 * agree, while an embedded MBP/UBP reference remains the preferred key.
 */
export function investecPrimeSaverSupersessionKey(payload: Readonly<Record<string, unknown>>): string | null {
  const accountId = typeof payload.accountId === 'string' ? payload.accountId : ''
  const normalizedAmount = investecDecimal(payload.amount)
  if (!accountId || !normalizedAmount) return null
  const providerReference = stableProviderReference(payload.reference, payload.description)
  if (providerReference !== null) return `reference-v1:${accountId}:${providerReference}`
  const description = identityText(payload.description)
  if (!description) return null
  const fingerprint = [
    'prime-saver-supersession-v1',
    accountId,
    normalizedAmount,
    // Keep the fallback supersession key compatible with legacy v2 payloads,
    // which stored direction in transactionType and did not preserve the new
    // normalized provider subtype separately.
    normalizedDirection(payload.type, payload.transactionType),
    description,
    identityText(payload.cardNumber),
  ]
  return `fallback-v1:${sha256(JSON.stringify(fingerprint))}`
}

/** Compute the same PrimeSaver identity from a normalized observation payload. */
export function investecPrimeSaverIdentityKey(payload: Readonly<Record<string, unknown>>): string | null {
  const key = primeSaverIdentity(
    String(payload.accountId ?? ''),
    payload.amount,
    payload.type,
    payload.transactionType,
    payload.description,
    payload.reference,
    payload.cardNumber,
    payload.transactionDate,
  )
  return key?.identityKey ?? null
}

function transactionIdentity(
  accountId: string,
  productName: string | undefined,
  transaction: InvestecTransaction,
): { transactionId: string; primeSaverIdentityKey?: string } | null {
  const uuid = transaction.uuid?.trim()
  if (uuid) return { transactionId: uuid }
  const normalizedProduct = productName?.replace(/\s+/g, '').toLowerCase()
  if (normalizedProduct !== 'primesaver' || transaction.postedOrder !== 0) return null
  // Only the provider transaction date is an economic identity field. Value
  // and posting dates are mutable booking facts and are deliberately excluded.
  const date = transaction.transactionDate
  const identity = primeSaverIdentity(
    accountId,
    transaction.amount,
    transaction.type,
    transaction.transactionType,
    transaction.description,
    transaction.reference,
    transaction.cardNumber,
    date,
  )
  if (!identity) return null
  return { transactionId: identity.transactionId, primeSaverIdentityKey: identity.identityKey }
}

function providerField(transaction: InvestecTransaction, names: readonly string[]): string | undefined {
  const record = transaction as unknown as Record<string, unknown>
  for (const name of names) {
    const value = record[name]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}

function providerMerchantIdentity(transaction: InvestecTransaction): string | undefined {
  const direct = providerField(transaction, ['merchantId', 'merchantCode', 'merchantReference'])
  if (direct) return direct
  if (!transaction.merchant || typeof transaction.merchant !== 'object' || Array.isArray(transaction.merchant)) return undefined
  const merchant = transaction.merchant as Record<string, unknown>
  for (const name of ['merchantId', 'merchantCode', 'merchantReference', 'id']) {
    const value = merchant[name]
    if (typeof value === 'string' && value.trim()) return value.trim()
  }
  return undefined
}

function providerDecimalField(transaction: InvestecTransaction, names: readonly string[]): string | undefined {
  const record = transaction as unknown as Record<string, unknown>
  for (const name of names) {
    const value = investecDecimal(record[name])
    if (value !== null) return value
  }
  return undefined
}

function providerLifecycleTransactionIds(transaction: InvestecTransaction): readonly string[] {
  const ids = new Set<string>()
  for (const value of [
    transaction.providerLifecycleTransactionId, transaction.pendingTransactionId, transaction.postedTransactionId,
    transaction.settlementTransactionId, transaction.successorTransactionId,
    transaction.cardTransactionId, transaction.linkedTransactionId,
  ]) {
    if (typeof value === 'string' && value.trim()) ids.add(value.trim())
  }
  return [...ids]
}

function providerLifecycleTransactionId(transaction: InvestecTransaction): string | undefined {
  const ids = providerLifecycleTransactionIds(transaction)
  return ids.length === 1 ? ids[0] : undefined
}

function hasConflictingLifecycleIds(transaction: InvestecTransaction): boolean {
  return providerLifecycleTransactionIds(transaction).length > 1
}

function providerCurrencyField(transaction: InvestecTransaction, names: readonly string[]): string | undefined {
  const value = providerField(transaction, names)?.toUpperCase()
  return value && /^[A-Z]{3}$/.test(value) ? value : undefined
}

function pocketSourceGroupId(
  providerScope: string,
  transaction: InvestecTransaction,
  providerTransactionType: string,
): string | undefined {
  if (providerTransactionType !== 'MCPBAPOCKETTRANSFERS') return undefined
  const groupReference = providerField(transaction, [
    'pocketTransferId', 'pocketGroupId', 'transactionGroupId', 'groupId',
    'groupReference', 'transactionGroupReference',
  ])
  return groupReference ? `investec:pocket:${providerScope}:${groupReference}` : undefined
}

function normalizedAccountNumber(value: unknown): string | undefined {
  if (typeof value !== 'string') return undefined
  const digits = value.replace(/\D/g, '')
  return digits.length >= 4 ? digits : undefined
}

function canonicalJson(value: unknown): string {
  if (value === null || value === undefined) return 'null'
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') return `{${Object.keys(value as object).sort().map(key => `${JSON.stringify(key)}:${canonicalJson((value as Record<string, unknown>)[key])}`).join(',')}}`
  return JSON.stringify(value)
}

function sanitizeAuditEvidence(value: unknown, key = '', parentKey = ''): unknown {
  if (typeof value === 'string' && key === 'content') {
    try { return JSON.stringify(sanitizeAuditEvidence(JSON.parse(value))) } catch { return value }
  }
  const panField = /(?:^pan$|cardnumber|card_number|display)/i.test(key)
    || (parentKey.toLowerCase() === 'card' && key.toLowerCase() === 'number')
  if (panField && (typeof value === 'string' || typeof value === 'number')) {
    const digits = String(value).replace(/\D/g, '')
    return digits.length >= 4 ? `••••${digits.slice(-4)}` : '[REDACTED]'
  }
  if (Array.isArray(value)) return value.map(item => sanitizeAuditEvidence(item, key, parentKey))
  if (value && typeof value === 'object') return Object.fromEntries(
    Object.entries(value as Record<string, unknown>)
      .map(([name, item]) => [name, sanitizeAuditEvidence(item, name, key)]),
  )
  return value
}

type CardRecoveryDiagnostic = {
  readonly stage: 'card_list' | 'account_mapping' | 'execution_list'
  readonly status: 'failed' | 'invalid'
  readonly errorClass: string
  readonly mappingStage?: 'account_id' | 'account_number' | 'card_identity'
  readonly httpStatus?: number
}

function safeHttpStatus(error: unknown): { httpStatus?: number } {
  const status = (error as { status?: unknown } | null)?.status
  return typeof status === 'number' && Number.isInteger(status) && status >= 100 && status <= 599 ? { httpStatus: status } : {}
}

function safeErrorClass(error: unknown): string {
  const standardErrorClasses = new Set([
    'Error', 'AggregateError', 'EvalError', 'RangeError', 'ReferenceError',
    'SyntaxError', 'TypeError', 'URIError',
  ])
  if (error instanceof Error && standardErrorClasses.has(error.name)) return error.name
  if (error === null) return 'NULL_ERROR'
  if (typeof error === 'object') return 'OBJECT_ERROR'
  return `${typeof error}_error`.toUpperCase()
}

function logCardRecoveryDiagnostic(diagnostic: CardRecoveryDiagnostic): void {
  // Deliberately log only fixed stage/status/error-class fields. Provider
  // messages, response bodies, identifiers, and card/financial values are not
  // part of recovery diagnostics.
  console.error('FINANCE_SYNC_CARD_EXECUTION_RECOVERY_FAILED', JSON.stringify(diagnostic))
}

function cardAuditObservation(config: InvestecAdapterConfig, execution: InvestecCardExecution, accountId: string | null, cardKey: string | null, outcome: string, reason: string): ObservationOutput | null {
  const executionId = typeof execution.executionId === 'string' ? execution.executionId.trim() : ''
  if (!executionId) return null
  const payload = Object.freeze({ auditType: 'INVESTEC_CARD_EXECUTION', executionId, cardKey, accountId, outcome, reason })
  const rawPayload = sanitizeAuditEvidence(execution)
  return Object.freeze({ deploymentId: config.deploymentId, localOwnerId: config.localOwnerId, sourceResourceIncarnationId: config.sourceResourceIncarnationId, providerVersion: `investec:card-execution:${executionId}`, semanticHash: sha256([config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'investec_card_audit', executionId, canonicalJson(rawPayload), outcome, reason].join('\x00')), hashSpecificationVersion: AUDIT_HASH_SPEC_VERSION, domain: 'provider_audit', payload, rawPayload, observedAt: typeof execution.completedAt === 'string' && Number.isFinite(Date.parse(execution.completedAt)) ? new Date(execution.completedAt).toISOString() : new Date().toISOString() })
}

async function collectInvestecCardExecutions(
  config: InvestecAdapterConfig,
  transport: InvestecTransport,
  accounts: readonly { accountId: string; accountNumber?: string }[],
  accountCurrencies: ReadonlyMap<string, string>,
  observations: ObservationOutput[],
): Promise<void> {
  if (!transport.listCards || !transport.listCardExecutions) return
  let cards: readonly { cardKey?: string; accountId?: string; accountNumber?: string }[]
  try {
    cards = await transport.listCards()
  } catch (error) {
    // Card execution recovery is additive evidence. A provider-side outage or
    // unsupported Card API must not block the ordinary PB ledger cycle.
    logCardRecoveryDiagnostic({ stage: 'card_list', status: 'failed', errorClass: safeErrorClass(error), ...safeHttpStatus(error) })
    return
  }
  const uniqueAccounts = (rows: readonly { accountId: string; accountNumber?: string }[]) =>
    [...new Map(rows.map(account => [account.accountId, account])).values()]

  for (const card of cards) {
    const cardKey = typeof card.cardKey === 'string' && card.cardKey.trim() ? card.cardKey.trim() : undefined
    if (!cardKey) {
      logCardRecoveryDiagnostic({ stage: 'account_mapping', status: 'invalid', errorClass: 'MISSING_CARD_KEY', mappingStage: 'card_identity' })
      continue
    }
    const explicitAccountId = typeof card.accountId === 'string' && card.accountId.trim()
      ? card.accountId.trim()
      : undefined
    const explicitAccountNumber = normalizedAccountNumber(card.accountNumber)
    const byId = explicitAccountId
      ? uniqueAccounts(accounts.filter(account => account.accountId === explicitAccountId))
      : []
    const byNumber = explicitAccountNumber
      ? uniqueAccounts(accounts.filter(account => normalizedAccountNumber(account.accountNumber) === explicitAccountNumber))
      : []

    let mapped: { accountId: string; accountNumber?: string } | undefined
    let mappingReason = 'AMBIGUOUS_CARD_ACCOUNT'
    if (explicitAccountId && explicitAccountNumber) {
      if (byId.length === 1 && byNumber.length === 1 && byId[0].accountId === byNumber[0].accountId) {
        mapped = byId[0]
      } else {
        mappingReason = 'CONFLICTING_CARD_ACCOUNT_IDENTIFIERS'
      }
    } else {
      const candidates = explicitAccountId ? byId : byNumber
      if (candidates.length === 1) mapped = candidates[0]
    }

    if (!mapped) {
      logCardRecoveryDiagnostic({
        stage: 'account_mapping', status: 'invalid', errorClass: mappingReason,
        mappingStage: explicitAccountId ? 'account_id' : 'account_number',
      })
    }

    let executions: readonly InvestecCardExecution[]
    try {
      executions = await transport.listCardExecutions(cardKey)
    } catch (error) {
      logCardRecoveryDiagnostic({ stage: 'execution_list', status: 'failed', errorClass: safeErrorClass(error), ...safeHttpStatus(error) })
      continue
    }
    for (const execution of executions) {
      const executionId = typeof execution.executionId === 'string' ? execution.executionId.trim() : ''
      if (!executionId) continue
      const recovered = recoverInvestecCardExecution(execution)
      let authorization: ObservationOutput | null = null
      if (recovered && mapped) {
        authorization = investecCardObservation({
          deploymentId: config.deploymentId,
          localOwnerId: config.localOwnerId,
          sourceResourceIncarnationId: config.sourceResourceIncarnationId,
          providerAccountId: mapped.accountId,
          accountCurrency: accountCurrencies.get(mapped.accountId) ?? '',
          payload: recovered.transaction as never,
          receivedAt: recovered.providerEventTime,
        })
      }

      const outcome = authorization ? 'AUTHORIZATION_RECOVERED' : 'AUDIT_ONLY'
      const reason = authorization
        ? 'COMPLETE_AFTER_TRANSACTION'
        : !mapped
          ? mappingReason
          : recovered
            ? 'AUTHORIZATION_NORMALIZATION_FAILED'
            : 'INCOMPLETE_OR_NON_TRANSACTION'
      const audit = cardAuditObservation(
        config,
        execution,
        mapped?.accountId ?? null,
        cardKey,
        outcome,
        reason,
      )
      if (audit) observations.push(audit)
      if (!authorization || !recovered) continue
      observations.push(Object.freeze({
        ...authorization,
        rawPayload: sanitizeInvestecCardEnvelope({
          schemaVersion: 1,
          eventType: recovered.eventType,
          providerEventTime: recovered.providerEventTime,
          transaction: recovered.transaction,
        }),
      }))
    }
  }
}

export async function runInvestecAdapter(config: InvestecAdapterConfig, transport: InvestecTransport): Promise<InvestecAdapterResult> {
  const eligibleSet = new Set(config.eligibleProductNames)
  const admittedAccounts = config.accountIds ? new Set(config.accountIds) : undefined
  const allAccounts = await transport.listAccounts()
  const accounts: { accountRef: string; eligible: boolean; currency: string; displayName: string }[] = []
  const observations: ObservationOutput[] = []
  const quarantine: { reason: string; accountRef: string; transactionRef?: string }[] = []
  const accountCurrencies = new Map<string, string>()
  const transferAccounts: Record<string, string> = {}
  let pagesPolled = 0
  let paginationComplete = true
  let nextCursor: string | null = null
  const seenAccountIds = new Set<string>()

  for (const account of allAccounts) {
    if (seenAccountIds.has(account.accountId)) continue
    seenAccountIds.add(account.accountId)
    if (admittedAccounts) {
      if (!admittedAccounts.has(account.accountId)) continue
    } else if (!account.productName || !eligibleSet.has(account.productName)) {
      continue
    }
    const accountRef = redact(account.accountId)
    const balance = await transport.getBalance(account.accountId)
    // Capture the successful fetch instant. The configured transaction window
    // is not evidence of when this independently fetched current balance was observed.
    const balanceObservedAt = new Date().toISOString()
    const rawCurrency = balance.currency?.trim().toUpperCase()
    if (!rawCurrency || !/^[A-Z]{3}$/.test(rawCurrency)) {
      quarantine.push({ reason: 'MISSING_ACCOUNT_CURRENCY', accountRef })
      continue
    }
    const displayName = account.accountName ?? account.productName ?? accountRef
    accounts.push({ accountRef, eligible: true, currency: rawCurrency, displayName })
    const fullAccountNumber = normalizedAccountNumber(account.accountNumber)
    if (fullAccountNumber) transferAccounts[account.accountId] = fullAccountNumber
    accountCurrencies.set(account.accountId, rawCurrency)
    const currentBalance = investecDecimal(balance.currentBalance)
    if (currentBalance === null) {
      quarantine.push({ reason: 'MISSING_CURRENT_BALANCE', accountRef })
      paginationComplete = false
      continue
    }
    const balancePayload = Object.freeze({
      accountId: account.accountId,
      currentBalance,
      currency: rawCurrency,
      asOfDate: config.toDate,
    })
    observations.push({
      deploymentId: config.deploymentId,
      localOwnerId: config.localOwnerId,
      sourceResourceIncarnationId: config.sourceResourceIncarnationId,
      providerVersion: `investec:balance:${account.accountId}`,
      semanticHash: sha256([config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId, 'balance', JSON.stringify(balancePayload)].join('\x00')),
      hashSpecificationVersion: BALANCE_HASH_SPEC_VERSION,
      domain: 'balance',
      payload: balancePayload,
      rawPayload: balance,
      observedAt: balanceObservedAt,
    })

    let cursor: string | undefined
    let accountComplete = false
    for (let page = 0; page < (config.maxPages ?? 50); page++) {
      try {
        const pageResult = await transport.getTransactionPage(account.accountId, { fromDate: config.fromDate, toDate: config.toDate }, cursor)
        pagesPolled++
        for (const txn of pageResult.transactions) {
          const status = (txn.status ?? '').toUpperCase()
          if (status !== 'POSTED') {
            quarantine.push({
              reason: status === 'PENDING' ? 'PENDING_NOT_PROJECTED' : 'UNKNOWN_OR_UNBOOKED_STATUS',
              accountRef,
              transactionRef: txn.uuid ? redact(txn.uuid) : undefined,
            })
            continue
          }
          // A UUID-bearing postedOrder=0 row is still the provisional shape used
          // by the card/statement feed. PrimeSaver's booked rows have no UUID and
          // also use postedOrder=0, so transactionIdentity handles that valid case.
          if (status === 'POSTED' && txn.postedOrder === 0 && txn.uuid?.trim()) {
            quarantine.push({ reason: 'POSTED_ORDER_ZERO_PROVISIONAL', accountRef, transactionRef: redact(txn.uuid) })
            continue
          }
          // Investec's transactionDate is the date the transaction occurred; postingDate is
          // when it was entered into the account ledger and can be days later (PrimeSaver
          // exposes this as the poll/current posting date).
          const date = txn.transactionDate ?? txn.valueDate ?? txn.postingDate
          if (!date) { quarantine.push({ reason: 'MISSING_DATE', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : undefined }); continue }
          const identity = transactionIdentity(account.accountId, account.productName, txn)
          if (!identity) { quarantine.push({ reason: 'MISSING_IDENTITY', accountRef }); continue }
          const transactionId = identity.transactionId

          const rawAmount = investecDecimal(txn.amount)
          if (rawAmount === null) {
            quarantine.push({ reason: 'INVALID_AMOUNT', accountRef, transactionRef: txn.uuid ? redact(txn.uuid) : transactionId })
            continue
          }
          const magnitude = rawAmount.replace(/^[+-]/, '')
          const isZero = /^0+(?:\.0+)?$/.test(magnitude)
          const providerDirection = normalizedDirection(txn.type, txn.transactionType) || null
          const signed = providerDirection === 'DEBIT' && !isZero ? `-${magnitude}` : magnitude

          const providerTransactionType = normalizedProviderTransactionType(txn.type, txn.transactionType) || null
          const providerScope = config.providerConnectionId === undefined
            ? config.sourceResourceIncarnationId
            : config.providerConnectionId.split(':', 1)[0]
          const sourceGroupId = providerTransactionType === 'MCPBAPOCKETTRANSFERS'
            ? pocketSourceGroupId(providerScope, txn, providerTransactionType)
            : undefined
          const postingDate = txn.postingDate ?? null
          const actionDate = txn.actionDate ?? null
          const transactionDate = txn.transactionDate ?? null
          const valueDate = txn.valueDate ?? null
          const fee = providerDecimalField(txn, ['fee', 'feeAmount', 'transactionFee'])
          const tax = providerDecimalField(txn, ['tax', 'taxAmount', 'transactionTax'])
          const fxRate = providerDecimalField(txn, ['fxRate', 'exchangeRate', 'conversionRate'])
          const originalAmount = providerDecimalField(txn, ['originalAmount', 'cardAmount', 'foreignAmount'])
          const originalCurrency = providerCurrencyField(txn, ['originalCurrency', 'cardCurrency', 'foreignCurrency'])
          const settlementAmount = providerDecimalField(txn, ['settlementAmount'])
          const settlementCurrency = providerCurrencyField(txn, ['settlementCurrency'])
          const settlementDate = providerField(txn, ['settlementDate'])
          const lifecycleIdConflict = hasConflictingLifecycleIds(txn)
          const providerLifecycleId = providerLifecycleTransactionId(txn)
          const description = txn.description ?? null
          const merchantIdentity = providerMerchantIdentity(txn)
          const namedUsdPocket = providerTransactionType === 'MCPBAPOCKETTRANSFERS'
            && typeof description === 'string'
            && /USD\s+POCKET/i.test(description)
          const completeOriginalEvidence = originalAmount !== undefined && originalCurrency !== undefined
          const currencyEvidenceReview = namedUsdPocket && !completeOriginalEvidence
          const currencyEvidence = providerTransactionType === 'MCPBAPOCKETTRANSFERS'
            ? {
                accountCurrency: rawCurrency,
                originalAmount: completeOriginalEvidence ? originalAmount : null,
                originalCurrency: completeOriginalEvidence ? originalCurrency : null,
                fxRate: fxRate ?? null,
                status: completeOriginalEvidence ? 'provider_explicit' : 'account_only',
              }
            : undefined
          const payload = {
            transactionId,
            accountId: account.accountId,
            accountRef: redact(account.accountId),
            amount: signed,
            currency: rawCurrency,
            ...(settlementAmount === undefined ? {} : { settlementAmount }),
            ...(settlementCurrency === undefined ? {} : { settlementCurrency }),
            ...(fee === undefined ? {} : { fee }),
            ...(tax === undefined ? {} : { tax }),
            ...(fxRate === undefined ? {} : { fxRate }),
            ...(completeOriginalEvidence ? { originalAmount, originalCurrency } : {}),
            date,
            transactionDate,
            postingDate,
            actionDate,
            valueDate,
            ...(settlementDate === undefined ? {} : { settlementDate }),
            description,
            type: providerDirection,
            direction: providerDirection,
            sourceType: providerTransactionType,
            subtype: providerTransactionType === 'MCPBAPOCKETTRANSFERS' ? 'POCKET_TRANSFER' : null,
            transactionType: providerTransactionType,
            cardNumber: txn.cardNumber ? redact(txn.cardNumber) : null,
            reference: txn.reference ?? null,
            ...(providerLifecycleId === undefined ? {} : { providerLifecycleTransactionId: providerLifecycleId }),
            ...(lifecycleIdConflict ? { providerLifecycleIdConflict: true } : {}),
            status: status.toLowerCase(),
            ...(currencyEvidenceReview || lifecycleIdConflict ? { needsReview: true } : {}),
            ...(sourceGroupId === undefined ? {} : { sourceGroupId }),
            ...(identity.primeSaverIdentityKey === undefined ? {} : { primeSaverIdentityKey: identity.primeSaverIdentityKey }),
            metadata: {
              providerOwned: {
                investec: {
                  providerTransactionType,
                  providerDirection,
                  uuid: txn.uuid ?? null,
                  postedOrder: txn.postedOrder ?? null,
                  transactionDate,
                  postingDate,
                  actionDate,
                  valueDate,
                  ...(lifecycleIdConflict ? { providerLifecycleIdConflict: true } : {}),
                  ...(sourceGroupId === undefined ? {} : { pocketSourceGroupId: sourceGroupId }),
                  ...(currencyEvidence === undefined ? {} : { currencyEvidence }),
                  ...(merchantIdentity === undefined ? {} : { merchantIdentity }),
                },
              },
            },
            merchantContext: merchantContext({
              provider: 'investec', rawDescription: txn.description ?? null,
              merchantDisplay: txn.description ?? txn.reference ?? null, amount: signed.startsWith('-') ? -1 : 1,
              providerHint: {
                ...(txn.reference ? { reference: txn.reference } : {}),
                ...(providerTransactionType ? { transactionType: providerTransactionType } : {}),
              },
            }),
          }
          const semanticStr = [
            config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
            'cash_ledger', JSON.stringify(payload),
          ].join('\x00')
          observations.push({
            deploymentId: config.deploymentId,
            localOwnerId: config.localOwnerId,
            sourceResourceIncarnationId: config.sourceResourceIncarnationId,
            providerVersion: 'investec:txn:' + transactionId,
            semanticHash: sha256(semanticStr),
            hashSpecificationVersion: TRANSACTION_HASH_SPEC_VERSION,
            domain: 'cash_ledger',
            payload: payload as Readonly<Record<string, unknown>>,
            rawPayload: txn,
            // Account transaction dates are date-only facts. Keep the real poll
            // observation time here rather than fabricating a midnight timestamp.
            observedAt: new Date().toISOString(),
          })
        }
        cursor = pageResult.nextCursor
        if (!cursor) { accountComplete = true; break }
        nextCursor = cursor
      } catch {
        paginationComplete = false
        break
      }
    }
    if (!accountComplete) paginationComplete = false
  }
  if (paginationComplete) {
    const admittedCardAccounts = allAccounts.filter(account => !admittedAccounts || admittedAccounts.has(account.accountId))
    await collectInvestecCardExecutions(
      config,
      transport,
      admittedCardAccounts,
      accountCurrencies,
      observations,
    )
  }
  if (paginationComplete) correlateExactOwnAccountTransfers(observations, allAccounts, config)
  const finalizedObservations = observations.map(observation => {
    const payload = Object.freeze({ ...observation.payload })
    const semanticHash = observation.domain === 'cash_ledger'
      && (payload.activityType !== undefined || payload.sourceGroupId !== undefined)
      ? sha256([
        config.deploymentId, config.localOwnerId, config.sourceResourceIncarnationId,
        'cash_ledger', JSON.stringify(payload), 'investec-transfer-pair-v1',
      ].join('\x00'))
      : observation.semanticHash
    return Object.freeze({ ...observation, payload, semanticHash })
  })
  return Object.freeze({ accounts, transferAccounts: Object.freeze(transferAccounts), observations: finalizedObservations, quarantine, pagesPolled, paginationComplete, nextCursor })
}

interface TransferCandidate {
  readonly observation: ObservationOutput
  readonly payload: Record<string, unknown>
  readonly accountNumber: string
  readonly direction: 'CREDIT' | 'DEBIT'
  readonly amount: string
  readonly currency: string
  readonly date: string
}

function normalizedDecimal(value: unknown): string | undefined {
  if (typeof value !== 'string' && typeof value !== 'number') return undefined
  const text = String(value).trim().replace(/^\+/, '')
  if (!/^-?\d+(?:\.\d+)?$/.test(text)) return undefined
  const negative = text.startsWith('-')
  const unsigned = text.replace(/^-/, '')
  const [whole, fraction = ''] = unsigned.split('.')
  const normalizedWhole = whole.replace(/^0+(?=\d)/, '') || '0'
  const normalizedFraction = fraction.replace(/0+$/, '')
  const magnitude = normalizedFraction ? `${normalizedWhole}.${normalizedFraction}` : normalizedWhole
  return negative && magnitude !== '0' ? `-${magnitude}` : magnitude
}

function transferNarrative(payload: Readonly<Record<string, unknown>>): string {
  return [payload.description, payload.reference].filter(value => typeof value === 'string').join(' ')
}

const NARRATIVE_MONTHS = ['JAN', 'FEB', 'MAR', 'APR', 'MAY', 'JUN', 'JUL', 'AUG', 'SEP', 'OCT', 'NOV', 'DEC']

/**
 * The day a PrimeSaver transfer took effect, as the provider writes it into
 * the narrative ("Value Date 09Oct26"). A transfer made late in the day is
 * dated the next day in transactionDate, while its counterpart in the other
 * account and its own provisional row both carry the value date.
 */
export function investecNarrativeValueDate(description: unknown): string | undefined {
  if (typeof description !== 'string') return undefined
  const match = description.match(/\bValue Date (\d{2})([A-Za-z]{3})(\d{2})\b/)
  if (!match) return undefined
  const month = NARRATIVE_MONTHS.indexOf(match[2].toUpperCase())
  if (month < 0) return undefined
  return `20${match[3]}-${String(month + 1).padStart(2, '0')}-${match[1]}`
}

function narrativeContainsAccountNumber(payload: Readonly<Record<string, unknown>>, accountNumber: string): boolean {
  if (!accountNumber) return false
  for (const value of [payload.description, payload.reference]) {
    if (typeof value !== 'string') continue
    // Permit spaces/dashes within the account number, but do not allow digits
    // from separate narrative fields or unrelated digit runs to concatenate.
    const pattern = accountNumber.split('').join('[\\s-]*')
    if (new RegExp(`(?:^|\\D)${pattern}(?:$|\\D)`).test(value)) return true
  }
  return false
}

export function correlateInvestecTransferObservations(
  observations: readonly ObservationOutput[],
  accountNumbers: Readonly<Record<string, string>>,
  providerConnectionId: string,
): ObservationOutput[] {
  const cloned = observations.map(observation => ({ ...observation, payload: { ...observation.payload } }))
  correlateExactOwnAccountTransfers(
    cloned,
    Object.entries(accountNumbers).map(([accountId, accountNumber]) => ({ accountId, accountNumber })),
    { providerConnectionId, sourceResourceIncarnationId: providerConnectionId, deploymentId: observations[0]?.deploymentId ?? '', localOwnerId: observations[0]?.localOwnerId ?? '', fromDate: '', toDate: '', eligibleProductNames: [] },
  )
  return cloned
}

export function finalizeInvestecObservations(observations: readonly ObservationOutput[]): readonly ObservationOutput[] {
  return observations.map(observation => {
    const payload = Object.freeze({ ...observation.payload })
    const semanticHash = observation.domain === 'cash_ledger'
      && (payload.activityType !== undefined || payload.sourceGroupId !== undefined)
      ? sha256([observation.deploymentId, observation.localOwnerId, observation.sourceResourceIncarnationId, 'cash_ledger', JSON.stringify(payload), 'investec-transfer-pair-v1'].join('\x00'))
      : observation.semanticHash
    return Object.freeze({ ...observation, payload, semanticHash })
  })
}

function correlateExactOwnAccountTransfers(
  observations: ObservationOutput[],
  accounts: readonly { accountId: string; accountNumber?: string }[],
  config: InvestecAdapterConfig,
): void {
  const accountNumbers = new Map<string, string>()
  const invalidAccounts = new Set<string>()
  const usedNumbers = new Map<string, string>()
  for (const account of accounts) {
    const accountId = typeof account.accountId === 'string' ? account.accountId : ''
    const number = normalizedAccountNumber(account.accountNumber)
    if (!accountId || !number || invalidAccounts.has(accountId)) continue
    const existing = accountNumbers.get(accountId)
    const owner = usedNumbers.get(number)
    if ((existing && existing !== number) || (owner && owner !== accountId)) {
      invalidAccounts.add(accountId)
      if (existing) accountNumbers.delete(accountId)
      if (owner && owner !== accountId) {
        invalidAccounts.add(owner)
        accountNumbers.delete(owner)
      }
      continue
    }
    accountNumbers.set(accountId, number)
    usedNumbers.set(number, accountId)
  }
  const candidates: TransferCandidate[] = []
  for (const observation of observations) {
    if (observation.domain !== 'cash_ledger') continue
    const payload = observation.payload as Record<string, unknown>
    const sourceType = identityText(payload.sourceType)
    if (sourceType === 'MCPBAPOCKETTRANSFERS' || sourceType === 'CARD' || payload.providerLifecycleIdConflict === true) continue
    const accountId = typeof payload.accountId === 'string' ? payload.accountId : ''
    const accountNumber = accountNumbers.get(accountId)
    const direction = identityText(payload.direction ?? payload.type)
    const amount = normalizedDecimal(payload.amount)
    const currency = identityText(payload.currency)
    const date = investecNarrativeValueDate(payload.description)
      ?? (typeof payload.transactionDate === 'string' ? payload.transactionDate
        : typeof payload.date === 'string' ? payload.date : '')
    if (!accountNumber || invalidAccounts.has(accountId) || !['CREDIT', 'DEBIT'].includes(direction) || !amount || !currency || !date) continue
    candidates.push({ observation, payload, accountNumber, direction: direction as 'CREDIT' | 'DEBIT', amount: amount.replace(/^-/, ''), currency, date })
  }
  const edges = new Map<TransferCandidate, TransferCandidate[]>()
  for (const candidate of candidates) {
    const counterparties = candidates.filter(other => other !== candidate
      && other.payload.accountId !== candidate.payload.accountId
      && other.direction !== candidate.direction
      && other.amount === candidate.amount
      && other.currency === candidate.currency
      && other.date === candidate.date
      && narrativeContainsAccountNumber(candidate.payload, accountNumbers.get(String(other.payload.accountId)) ?? '')
      && narrativeContainsAccountNumber(other.payload, candidate.accountNumber))
    edges.set(candidate, counterparties)
  }
  for (const [candidate, counterparties] of edges) {
    if (counterparties.length !== 1) continue
    const counterpart = counterparties[0]
    if (edges.get(counterpart)?.length !== 1 || edges.get(counterpart)?.[0] !== candidate) continue
    const ids = [String(candidate.payload.transactionId), String(counterpart.payload.transactionId)].sort()
    const scope = config.providerConnectionId ?? config.sourceResourceIncarnationId
    const framed = ['investec-transfer-group-v1', scope, ...ids].map(value => `${value.length}:${value}`).join('\x00')
    const groupId = `investec:transfer:v1:${sha256(framed)}`
    for (const leg of [candidate, counterpart]) {
      leg.payload.activityType = leg.direction === 'DEBIT' ? 'TRANSFER_OUT' : 'TRANSFER_IN'
      leg.payload.sourceGroupId = groupId
    }
  }
}
