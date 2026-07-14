import { describe, expect, it } from 'vitest'
import {
  runPlaidCapabilityProbe,
  type PlaidProbeTransport,
  type PlaidSyncResponse,
} from './plaid-probe'

function fixtureTransport(responses: Array<PlaidSyncResponse | Error>) {
  const requests: Parameters<PlaidProbeTransport>[0][] = []
  const transport: PlaidProbeTransport = async request => {
    requests.push(request)
    const response = responses.shift()
    if (response instanceof Error) throw response
    if (!response) throw new Error('fixture exhausted')
    return response
  }
  return { transport, requests }
}

const page = (overrides: Partial<PlaidSyncResponse> = {}): PlaidSyncResponse => ({
  added: [], modified: [], removed: [], accounts: [{ account_id: 'account-secret' }],
  next_cursor: 'cursor-final-secret', has_more: false, ...overrides,
})

const base = {
  mode: 'sandbox' as const,
  itemKey: 'probe-item',
  accessToken: 'fixture-token',
  products: ['transactions'] as const,
  accountIds: ['account-secret'] as const,
  historyDays: 14,
  initialCursor: 'cursor-original-secret',
}

describe('disposable Plaid G0 capability probe', () => {
  it('records typed sandbox evidence without projection or scheduling', async () => {
    const { transport, requests } = fixtureTransport([page({
      added: [{ transaction_id: 'transaction-secret', account_id: 'account-secret' }],
    })])

    const result = await runPlaidCapabilityProbe(base, transport)

    expect(result).toMatchObject({
      provider: 'plaid', mode: 'sandbox', outcome: 'proven', disposable: true,
      projection: 'disabled', scheduling: 'disabled', wealthfolioWrites: 0,
      products: ['transactions'], accountCount: 1, historyDays: 14,
      cursor: { scope: 'item', pagesStaged: 1, mutationRestarts: 0, promoted: true },
    })
    expect(requests).toEqual([expect.objectContaining({
      endpoint: '/transactions/sync', accessToken: 'fixture-token', cursor: 'cursor-original-secret',
      options: { account_ids: ['account-secret'], days_requested: 14 },
    })])
    expect(JSON.stringify(result)).not.toMatch(/secret|fixture-token/)
  })

  it('restarts a mutated multi-page batch from the original Item cursor and promotes only the complete retry', async () => {
    const mutation = Object.assign(new Error('mutation'), { code: 'TRANSACTIONS_SYNC_MUTATION_DURING_PAGINATION' })
    const { transport, requests } = fixtureTransport([
      page({ next_cursor: 'staged-one', has_more: true }), mutation,
      page({ next_cursor: 'retry-one', has_more: true }), page({ next_cursor: 'complete', has_more: false }),
    ])

    const result = await runPlaidCapabilityProbe(base, transport)

    expect(requests.map(request => request.cursor)).toEqual([
      'cursor-original-secret', 'staged-one', 'cursor-original-secret', 'retry-one',
    ])
    expect(result.cursor).toMatchObject({ pagesStaged: 2, mutationRestarts: 1, promoted: true })
  })

  it.each(['synthetic', 'sandbox', 'constrained_live'] as const)(
    'distinguishes %s evidence in the typed result', async mode => {
      const { transport } = fixtureTransport([page()])
      expect((await runPlaidCapabilityProbe({ ...base, mode }, transport)).mode).toBe(mode)
    },
  )

  it('fails closed before transport when bounds or products are unsafe', async () => {
    const { transport, requests } = fixtureTransport([page()])
    const result = await runPlaidCapabilityProbe({
      ...base, accountIds: ['a', 'b', 'c', 'd', 'e'], products: ['transactions', 'balance'] as never,
      historyDays: 31,
    }, transport)

    expect(result).toMatchObject({ outcome: 'contradicted', failureCode: 'UNSAFE_PROBE_CONFIGURATION' })
    expect(result.cursor.promoted).toBe(false)
    expect(requests).toHaveLength(0)
  })

  it('probes an explicitly requested liabilities product with the same account bound', async () => {
    const { transport, requests } = fixtureTransport([page(), page()])
    const result = await runPlaidCapabilityProbe({ ...base, products: ['transactions', 'liabilities'] }, transport)

    expect(requests.map(request => request.endpoint)).toEqual(['/transactions/sync', '/liabilities/get'])
    expect(requests[1]?.options.account_ids).toEqual(['account-secret'])
    expect(result).toMatchObject({ outcome: 'proven', products: ['transactions', 'liabilities'] })
  })

  it('fails closed on transport errors and never exposes payload evidence', async () => {
    const { transport } = fixtureTransport([new Error('token fixture-token account-secret')])
    const result = await runPlaidCapabilityProbe(base, transport)

    expect(result).toMatchObject({ outcome: 'inconclusive', failureCode: 'TRANSPORT_FAILURE' })
    expect(result.evidence).toEqual(expect.arrayContaining([{ kind: 'failure', value: 'redacted' }]))
    expect(JSON.stringify(result)).not.toMatch(/fixture-token|account-secret/)
  })
})
