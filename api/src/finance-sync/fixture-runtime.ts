/**
 * Disposable local-only provider runtime. It is enabled only with
 * FINANCE_SYNC_FIXTURE_MODE=true and emits synthetic observations through the
 * same adapters, storage, outbox, and Wealthfolio bridge as a real run.
 *
 * No provider credential or live endpoint is used by this path.
 */
import { FinanceSyncConnectControl } from './connect-control'
import type { RuntimeConnection } from './runtime'
import type { FinanceSyncSql } from './storage/pg'
import { PostgresOnboardingRepository } from './onboarding-repository'
import type {
  PlaidAccount,
  PlaidSyncPage,
  PlaidTransport,
} from './probes/plaid/plaid-adapter'
import type {
  SnapTradeRawAccount,
  SnapTradeRawActivity,
  SnapTradeTransport,
} from './probes/snaptrade/snaptrade-adapter'
import type { InvestecTransport } from './probes/investec/investec-adapter'
import type { ProviderTransportFactories } from './provider-runtime'

export const FIXTURE_OWNER_SUBJECT = 'urn:wealthfolio:password#local'
const OWNER_SUBJECT = FIXTURE_OWNER_SUBJECT
const PLAID_SOURCE = '00000000-0000-0000-0000-000000000101'
const SAVINGS_SOURCE = '00000000-0000-0000-0000-000000000102'
const CARD_SOURCE = '00000000-0000-0000-0000-000000000103'
const SNAPTRADE_SOURCE = '00000000-0000-0000-0000-000000000104'
const INVESTEC_SOURCE = '00000000-0000-4000-8000-000000000105'
const INVESTEC_CONNECTION_ID = '00000000-0000-0000-0000-000000000305'
const INVESTEC_ACCOUNT_ID = 'fixture-investec-card'
const INVESTEC_ACCOUNT_NUMBER = '1000000000014'

interface FixturePlaidAccount {
  readonly id: string
  readonly name: string
  readonly type: string
  readonly subtype: string
  readonly current: number
  readonly wealthfolioAccountId: string
  readonly sourceResourceIncarnationId: string
}

const PLAID_ACCOUNTS: readonly FixturePlaidAccount[] = [
  {
    id: 'fixture-checking', name: 'Fixture Checking', type: 'depository', subtype: 'checking', current: 1_250,
    wealthfolioAccountId: 'fixture-checking', sourceResourceIncarnationId: PLAID_SOURCE,
  },
  {
    id: 'fixture-savings', name: 'Fixture Savings', type: 'depository', subtype: 'savings', current: 51_500,
    wealthfolioAccountId: 'fixture-savings', sourceResourceIncarnationId: SAVINGS_SOURCE,
  },
  {
    id: 'fixture-card', name: 'Fixture Credit Card', type: 'credit', subtype: 'credit card', current: -100,
    wealthfolioAccountId: 'fixture-card', sourceResourceIncarnationId: CARD_SOURCE,
  },
]

const PLAID_TRANSACTIONS = [
  { transaction_id: 'fixture-paycheck', account_id: 'fixture-checking', amount: -2_500, date: '2026-08-01', name: 'Fixture payroll deposit' },
  { transaction_id: 'fixture-groceries', account_id: 'fixture-checking', amount: 84.5, date: '2026-08-01', name: 'Fixture Market', merchant_name: 'Fixture Market' },
  { transaction_id: 'fixture-reimbursement', account_id: 'fixture-checking', amount: -75, date: '2026-08-02', name: 'Fixture reimbursement' },
  { transaction_id: 'fixture-prime-saver-out', account_id: 'fixture-checking', amount: 51_300, date: '2026-08-02', name: 'Prime Saver transfer' },
  { transaction_id: 'fixture-card-purchase', account_id: 'fixture-card', amount: 120, date: '2026-08-02', name: 'Fixture card purchase', merchant_name: 'Fixture Store' },
  { transaction_id: 'fixture-card-refund', account_id: 'fixture-card', amount: -20, date: '2026-08-03', name: 'Fixture card refund' },
  { transaction_id: 'fixture-prime-saver-in', account_id: 'fixture-savings', amount: -51_300, date: '2026-08-02', name: 'Prime Saver transfer' },
] as const

const INVESTEC_TRANSACTIONS = [
  {
    uuid: 'fixture-posted-order-equal', status: 'POSTED', postedOrder: 1,
    type: 'DEBIT', transactionType: 'CARDPURCHASES', amount: '123.45',
    transactionDate: '2026-08-20', postingDate: '2026-08-21',
    description: 'SAMPLEMART ONLINE EQUAL', reference: 'posted-equal', cardNumber: INVESTEC_ACCOUNT_NUMBER,
  },
  {
    uuid: 'fixture-posted-order-1', status: 'POSTED', postedOrder: 1,
    type: 'DEBIT', transactionType: 'CARDPURCHASES', amount: '1345.13',
    transactionDate: '2026-08-18', postingDate: '2026-08-19',
    description: 'SAMPLEMART ONLINE STORE', reference: 'posted-order-1', cardNumber: INVESTEC_ACCOUNT_NUMBER,
  },
  {
    uuid: 'fixture-posted-order-2', status: 'POSTED', postedOrder: 1,
    type: 'DEBIT', transactionType: 'CARDPURCHASES', amount: '1430.40',
    transactionDate: '2026-08-27', postingDate: '2026-08-28',
    description: 'SAMPLEMART ONLINE MARKET', reference: 'posted-order-2', cardNumber: INVESTEC_ACCOUNT_NUMBER,
  },
  {
    uuid: 'fixture-posted-wrong-merchant', status: 'POSTED', postedOrder: 1,
    type: 'DEBIT', transactionType: 'CARDPURCHASES', amount: '50.00',
    transactionDate: '2026-08-18', postingDate: '2026-08-19',
    description: 'FIXTURE OTHER MERCHANT', reference: 'posted-other', cardNumber: INVESTEC_ACCOUNT_NUMBER,
  },
] as const

const INVESTEC_CARD_EXECUTIONS = [
  {
    executionId: 'fixture-auth-equal', sandbox: false, type: 'after_transaction', authorizationApproved: null,
    completedAt: '2026-08-20T12:00:01.000Z', logs: [{ content: JSON.stringify({
      schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-08-20T12:00:00.000Z',
      transaction: {
        accountNumber: INVESTEC_ACCOUNT_NUMBER, centsAmount: 12345, currencyCode: 'ZAR',
        dateTime: '2026-08-20T12:00:00.000Z', reference: 'card-equal',
        merchant: { name: 'SAMPLEMART ONLINE EQUAL' }, card: { display: '••••0014' }, type: 'card',
      },
    }) }],
  },
  {
    executionId: 'fixture-auth-order-1', sandbox: false, type: 'after_transaction', authorizationApproved: null,
    completedAt: '2026-08-17T12:00:01.000Z', logs: [{ content: JSON.stringify({
      schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-08-17T12:00:00.000Z',
      transaction: {
        accountNumber: INVESTEC_ACCOUNT_NUMBER, centsAmount: 135738, currencyCode: 'ZAR',
        dateTime: '2026-08-17T12:00:00.000Z', reference: 'card-order-1',
        merchant: { name: 'SAMPLEMART ONLINE' }, card: { display: '••••0014' }, type: 'card',
      },
    }) }],
  },
  {
    executionId: 'fixture-auth-order-2', sandbox: false, type: 'after_transaction', authorizationApproved: null,
    completedAt: '2026-08-26T12:00:01.000Z', logs: [{ content: JSON.stringify({
      schemaVersion: 1, eventType: 'after_transaction', providerEventTime: '2026-08-26T12:00:00.000Z',
      transaction: {
        accountNumber: INVESTEC_ACCOUNT_NUMBER, centsAmount: 143386, currencyCode: 'ZAR',
        dateTime: '2026-08-26T12:00:00.000Z', reference: 'card-order-2',
        merchant: { name: 'SAMPLEMART ONLINE' }, card: { display: '••••0014' }, type: 'card',
      },
    }) }],
  },
] as const

function fixtureInvestecTransport(): InvestecTransport {
  return {
    async listAccounts() {
      return [{ accountId: INVESTEC_ACCOUNT_ID, productName: 'Private Bank Account', accountName: 'Fixture Investec Card', accountNumber: INVESTEC_ACCOUNT_NUMBER }]
    },
    async getBalance() {
      return { currency: 'ZAR', currentBalance: '10000.00', availableBalance: '10000.00' }
    },
    async getTransactionPage() {
      return { transactions: INVESTEC_TRANSACTIONS }
    },
    async listCards() {
      return [{ cardKey: 'fixture-card-0014', accountId: INVESTEC_ACCOUNT_ID, accountNumber: INVESTEC_ACCOUNT_NUMBER }]
    },
    async listCardExecutions() {
      return INVESTEC_CARD_EXECUTIONS
    },
  }
}

const SNAPTRADE_ACTIVITIES: readonly SnapTradeRawActivity[] = [
  {
    id: 'fixture-alpaca-contribution', account: { id: 'fixture-alpaca' }, type: 'CONTRIBUTION',
    amount: '500', currency: { code: 'USD' }, trade_date: '2026-08-03', description: 'Fixture brokerage contribution',
  },
]

const SNAPTRADE_ACCOUNTS: readonly SnapTradeRawAccount[] = [
  {
    account: { id: 'fixture-alpaca', name: 'Fixture Alpaca Taxable', institution_name: 'Fixture Alpaca', raw_type: 'Individual Brokerage' },
    positions: [], total_value: { value: '500', currency: 'USD' },
  },
]

function fixtureAccountMap(env: NodeJS.ProcessEnv): Readonly<Record<string, string>> {
  const raw = env.FINANCE_SYNC_FIXTURE_ACCOUNT_MAP_JSON
  if (!raw) return Object.freeze(Object.fromEntries(PLAID_ACCOUNTS.map(account => [account.id, account.id]).concat([
    ['fixture-alpaca', 'fixture-alpaca'], [INVESTEC_ACCOUNT_ID, INVESTEC_ACCOUNT_ID],
  ])))
  try {
    const parsed = JSON.parse(raw) as unknown
    if (!parsed || typeof parsed !== 'object' || Array.isArray(parsed)) throw new Error('fixture account map must be an object')
    const map = parsed as Record<string, unknown>
    const result: Record<string, string> = {}
    for (const providerId of [...PLAID_ACCOUNTS.map(account => account.id), 'fixture-alpaca', INVESTEC_ACCOUNT_ID]) {
      if (typeof map[providerId] !== 'string' || map[providerId].length === 0) throw new Error(`missing fixture account mapping for ${providerId}`)
      result[providerId] = map[providerId]
    }
    return Object.freeze(result)
  } catch (error) {
    throw new Error(`invalid FINANCE_SYNC_FIXTURE_ACCOUNT_MAP_JSON: ${String(error)}`)
  }
}

function plaidAccount(account: FixturePlaidAccount): PlaidAccount {
  return {
    account_id: account.id,
    name: account.name,
    type: account.type,
    subtype: account.subtype,
    balances: { current: account.current, iso_currency_code: 'USD' },
  }
}

function fixturePlaidTransport(account: FixturePlaidAccount): PlaidTransport {
  const accountData = plaidAccount(account)
  return {
    async getAccounts() { return [accountData] },
    async getLiabilities() {
      return account.type === 'credit' ? { credit: [{ account_id: account.id, last_payment_amount: 20 }] } : {}
    },
    async syncTransactions(_accessToken, _cursor): Promise<PlaidSyncPage> {
      return {
        added: PLAID_TRANSACTIONS.filter(transaction => transaction.account_id === account.id),
        modified: [],
        removed: [],
        next_cursor: 'fixture-v1',
        has_more: false,
      }
    },
  }
}

function fixtureSnapTradeTransport(): SnapTradeTransport {
  return {
    async fetchAccounts() { return SNAPTRADE_ACCOUNTS },
    async fetchActivities(_startDate, _endDate, accountIds) {
      if (accountIds.length === 0) throw new Error('fixture SnapTrade activities require admitted account IDs')
      const admitted = new Set(accountIds)
      return SNAPTRADE_ACTIVITIES.filter(activity => {
        const accountId = activity.account && typeof activity.account === 'object' && !Array.isArray(activity.account)
          && typeof activity.account.id === 'string' ? activity.account.id : undefined
        return accountId !== undefined && admitted.has(accountId)
      })
    },
  }
}

function commonConnection(
  connectionId: string,
  sourceResourceIncarnationId: string,
  accountId: string,
  wealthfolioAccountId: string,
  deploymentId: string,
  accessGeneration: number,
) {
  return {
    connectionId,
    deploymentId,
    localOwnerId: OWNER_SUBJECT,
    sourceResourceIncarnationId,
    accountIds: [accountId],
    wealthfolioAccountIds: { [accountId]: wealthfolioAccountId },
    providerGeneration: 1,
    receiverAccessGeneration: accessGeneration,
    secretReference: 'FIXTURE_ONLY',
  }
}

function positiveFixtureGeneration(env: NodeJS.ProcessEnv): number {
  const value = Number(env.FINANCE_SYNC_FIXTURE_ACCESS_GENERATION ?? '1')
  return Number.isSafeInteger(value) && value > 0 ? value : 1
}

export interface FixtureRuntime {
  readonly connections: readonly RuntimeConnection[]
  readonly factories: ProviderTransportFactories
  readonly fixtureAccountIds: readonly string[]
}

export function fixtureModeEnabled(env: NodeJS.ProcessEnv): boolean {
  return env.FINANCE_SYNC_FIXTURE_MODE === 'true'
}

export function createFixtureRuntime(deploymentId: string, env: NodeJS.ProcessEnv = process.env): FixtureRuntime {
  const accessGeneration = positiveFixtureGeneration(env)
  const wealthfolioAccountIds = fixtureAccountMap(env)
  const plaidConnections: RuntimeConnection[] = PLAID_ACCOUNTS.map(account => ({
    ...commonConnection(
      `fixture-plaid:${account.id}`,
      account.sourceResourceIncarnationId,
      account.id,
      wealthfolioAccountIds[account.id],
      deploymentId,
      accessGeneration,
    ),
    provider: 'plaid',
    clientId: 'fixture-only',
    clientSecret: 'fixture-only',
    accessToken: 'fixture-only',
  }))
  const snaptradeConnection: RuntimeConnection = {
    ...commonConnection('fixture-snaptrade:fixture-alpaca', SNAPTRADE_SOURCE, 'fixture-alpaca', wealthfolioAccountIds['fixture-alpaca'], deploymentId, accessGeneration),
    provider: 'snaptrade',
    clientId: 'fixture-only',
    consumerKey: 'fixture-only',
    authMode: 'commercial',
    userId: 'fixture-only',
    userSecret: 'fixture-only',
    assetMappings: {},
    activitiesStartDate: '2026-08-01',
    activitiesEndDate: '2026-08-04',
  }
  const investecConnection: RuntimeConnection = {
    ...commonConnection(`${INVESTEC_CONNECTION_ID}:${INVESTEC_ACCOUNT_ID}`, INVESTEC_SOURCE, INVESTEC_ACCOUNT_ID, wealthfolioAccountIds[INVESTEC_ACCOUNT_ID], deploymentId, accessGeneration),
    provider: 'investec',
    clientId: 'fixture-only',
    clientSecret: 'fixture-only',
    apiKey: 'fixture-only',
    eligibleProductNames: ['Private Bank Account'],
    fromDate: '2026-08-01',
    toDate: '2026-08-31',
  }

  const factories: ProviderTransportFactories = {
    plaid: connection => {
      const account = PLAID_ACCOUNTS.find(item => item.id === connection.accountIds[0])
      if (!account) throw new Error('fixture Plaid account is not configured')
      return fixturePlaidTransport(account)
    },
    snaptrade: () => fixtureSnapTradeTransport(),
    investec: () => fixtureInvestecTransport(),
    mercury: () => { throw new Error('Mercury is unavailable in fixture mode') },
  }

  const connections = env.FINANCE_SYNC_FIXTURE_INVESTEC_ONLY === 'true'
    ? [investecConnection]
    : [...plaidConnections, snaptradeConnection, investecConnection]
  return {
    connections,
    factories,
    fixtureAccountIds: connections.flatMap(connection => connection.accountIds.map(accountId => connection.wealthfolioAccountIds[accountId])),
  }
}

export function createFixtureConnectControl(
  sql: FinanceSyncSql,
  deploymentId: string,
  connections: () => Promise<readonly RuntimeConnection[]>,
): FinanceSyncConnectControl {
  const onboarding = new PostgresOnboardingRepository(sql, deploymentId)
  return new FinanceSyncConnectControl({
    deploymentId,
    connections,
    secretStore: {
      available: false,
      allowedSlots: [],
      storePlaidAccessToken: async () => { throw new Error('fixture Plaid credentials are unavailable') },
    },
    sessionStore: {
      create: async () => undefined,
      consume: async () => undefined,
      release: async () => undefined,
    },
    onboarding,
  })
}

export const fixtureAuthorityRows = Object.freeze([
  { connectionId: 'fixture-plaid:fixture-checking', provider: 'plaid', providerConnectionId: 'fixture-plaid-checking', providerPrincipalId: '00000000-0000-0000-0000-000000000201', connectionIncarnationId: '00000000-0000-0000-0000-000000000301', accountId: 'fixture-checking', accountIncarnationId: '00000000-0000-0000-0000-000000000401', sourceResourceIncarnationId: PLAID_SOURCE },
  { connectionId: 'fixture-plaid:fixture-savings', provider: 'plaid', providerConnectionId: 'fixture-plaid-savings', providerPrincipalId: '00000000-0000-0000-0000-000000000202', connectionIncarnationId: '00000000-0000-0000-0000-000000000302', accountId: 'fixture-savings', accountIncarnationId: '00000000-0000-0000-0000-000000000402', sourceResourceIncarnationId: SAVINGS_SOURCE },
  { connectionId: 'fixture-plaid:fixture-card', provider: 'plaid', providerConnectionId: 'fixture-plaid-card', providerPrincipalId: '00000000-0000-0000-0000-000000000203', connectionIncarnationId: '00000000-0000-0000-0000-000000000303', accountId: 'fixture-card', accountIncarnationId: '00000000-0000-0000-0000-000000000403', sourceResourceIncarnationId: CARD_SOURCE },
  { connectionId: 'fixture-snaptrade:fixture-alpaca', provider: 'snaptrade', providerConnectionId: 'fixture-snaptrade', providerPrincipalId: '00000000-0000-0000-0000-000000000204', connectionIncarnationId: '00000000-0000-0000-0000-000000000304', accountId: 'fixture-alpaca', accountIncarnationId: '00000000-0000-0000-0000-000000000404', sourceResourceIncarnationId: SNAPTRADE_SOURCE },
  { connectionId: INVESTEC_CONNECTION_ID, provider: 'investec', providerConnectionId: 'fixture-investec', providerPrincipalId: '00000000-0000-0000-0000-000000000205', connectionIncarnationId: INVESTEC_CONNECTION_ID, accountId: INVESTEC_ACCOUNT_ID, accountIncarnationId: '00000000-0000-0000-0000-000000000405', sourceResourceIncarnationId: INVESTEC_SOURCE },
])
