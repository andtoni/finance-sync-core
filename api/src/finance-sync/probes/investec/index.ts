export interface InvestecProbeAccount {
  readonly accountId: string
  readonly productName?: string
}

export interface InvestecProbeTransaction {
  readonly uuid?: string
  readonly status?: string
  readonly postedOrder?: number
  readonly type?: string
  readonly amount?: number
  readonly postingDate?: string
  readonly transactionDate?: string
  readonly valueDate?: string
}

export interface InvestecProbeTransport {
  listAccounts(): Promise<readonly InvestecProbeAccount[]>
  getBalance(accountId: string): Promise<{ readonly currency?: string }>
  getTransactionPage(
    accountId: string,
    window: { readonly fromDate: string; readonly toDate: string },
    cursor?: string,
  ): Promise<{ readonly transactions: readonly InvestecProbeTransaction[]; readonly nextCursor?: string }>
}

export interface InvestecProbeOptions {
  readonly fromDate: string
  readonly toDate: string
  readonly eligibleProductNames: readonly string[]
  readonly maxPages: number
}

type QuarantineReason =
  | 'POSTED_ORDER_ZERO_PROVISIONAL'
  | 'UNKNOWN_OR_UNBOOKED_STATUS'
  | 'MISSING_ACCOUNT_CURRENCY'
  | 'MISSING_DATE'
  | 'MISSING_IDENTITY'

interface ProbeRow {
  readonly accountRef: string
  readonly amount: string
  readonly currency: string
  readonly date: string
  readonly identityRef: string
}

export interface InvestecProbeResult {
  readonly capability: 'supported' | 'supported_with_quarantine' | 'unsupported'
  readonly projectionEnabled: false
  readonly accounts: readonly { accountRef: string; eligible: true; currency: string }[]
  readonly booked: readonly ProbeRow[]
  readonly quarantine: readonly { reason: QuarantineReason; accountRef: string }[]
  readonly evidence: {
    readonly window: { fromDate: string; toDate: string }
    readonly pages: number
    readonly paginationComplete: boolean
    readonly bookedOnly: true
    readonly accountCurrencyProven: boolean
  }
  readonly uncertainty: readonly string[]
}

const redact = (value: string): string => `…${value.slice(-4)}`

export async function runInvestecG0Probe(
  transport: InvestecProbeTransport,
  options: InvestecProbeOptions,
): Promise<InvestecProbeResult> {
  const window = { fromDate: options.fromDate, toDate: options.toDate }
  const eligible = new Set(options.eligibleProductNames)
  const accounts: { accountRef: string; eligible: true; currency: string }[] = []
  const booked: ProbeRow[] = []
  const quarantine: { reason: QuarantineReason; accountRef: string }[] = []
  const uncertainty = new Set(['SIGN_SEMANTICS', 'DATE_SEMANTICS', 'IDENTITY_STABILITY'])
  let pages = 0
  let paginationComplete = true
  let accountCurrencyProven = true

  for (const account of await transport.listAccounts()) {
    if (!account.productName || !eligible.has(account.productName)) continue
    const accountRef = redact(account.accountId)
    const balance = await transport.getBalance(account.accountId)
    const currency = balance.currency?.trim().toUpperCase()
    if (!currency) {
      accountCurrencyProven = false
      quarantine.push({ reason: 'MISSING_ACCOUNT_CURRENCY', accountRef })
      continue
    }
    accounts.push({ accountRef, eligible: true, currency })

    let cursor: string | undefined
    let complete = false
    for (let pageNumber = 0; pageNumber < Math.max(0, options.maxPages); pageNumber++) {
      const page = await transport.getTransactionPage(account.accountId, window, cursor)
      pages++
      for (const transaction of page.transactions) {
        if (transaction.postedOrder === 0) {
          quarantine.push({ reason: 'POSTED_ORDER_ZERO_PROVISIONAL', accountRef })
          continue
        }
        if (transaction.status?.toUpperCase() !== 'POSTED') {
          if (transaction.status?.toUpperCase() !== 'PENDING') quarantine.push({ reason: 'UNKNOWN_OR_UNBOOKED_STATUS', accountRef })
          continue
        }
        const date = transaction.transactionDate ?? transaction.valueDate ?? transaction.postingDate
        if (!date) { quarantine.push({ reason: 'MISSING_DATE', accountRef }); continue }
        if (!transaction.uuid) { quarantine.push({ reason: 'MISSING_IDENTITY', accountRef }); continue }
        const amount = Math.abs(Number(transaction.amount ?? 0))
        const signed = transaction.type?.toUpperCase() === 'DEBIT' && amount > 0 ? -amount : amount
        booked.push({ accountRef, amount: signed.toFixed(2), currency, date, identityRef: redact(transaction.uuid) })
      }
      cursor = page.nextCursor
      if (!cursor) { complete = true; break }
    }
    if (!complete) paginationComplete = false
  }

  if (!paginationComplete) uncertainty.add('PAGINATION_COMPLETENESS')
  const unsupported = !accountCurrencyProven || accounts.length === 0 || !paginationComplete
  return {
    capability: unsupported ? 'unsupported' : quarantine.length ? 'supported_with_quarantine' : 'supported',
    projectionEnabled: false,
    accounts, booked: unsupported ? [] : booked, quarantine,
    evidence: { window, pages, paginationComplete, bookedOnly: true, accountCurrencyProven },
    uncertainty: [...uncertainty],
  }
}
