import { describe, expect, it } from 'vitest'
import { investecCardObservation, reprojectRetainedInvestecCards } from './investec-card-drainer'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const input = {
  deploymentId: 'wealthfolio-production',
  localOwnerId: 'urn:wealthfolio:password#local',
  sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
  providerAccountId: '1000000000000000000000001',
  accountCurrency: 'ZAR',
}

describe('Investec card observations', () => {
  it('preserves a card authorization as an explicitly pending native cash fact', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '10000000001', centsAmount: 576, originalCentsAmount: 576, currencyCode: 'usd', type: 'card',
        dateTime: '2026-08-05T16:00:00.000Z', reference: '500000001',
        merchant: { name: 'SAASCO' }, card: { display: '400000XXXXXX0011' },
      },
    })

    expect(observation).toMatchObject({
      providerVersion: 'investec:card:investec-card:1000000000000000000000001:reference-500000001',
      observedAt: '2026-08-05T16:00:00.000Z',
      payload: {
        transactionId: 'investec-card:1000000000000000000000001:reference-500000001',
        amount: '-5.76', currency: 'ZAR', originalCurrency: 'USD',
        settlementCurrency: 'ZAR', date: '2026-08-05', transactionDate: '2026-08-05',
        description: 'SAASCO', status: 'pending', reference: '500000001',
      },
    })
    expect(observation?.payload.merchantContext).toMatchObject({
      merchantDisplay: 'SAASCO', direction: 'outflow',
      providerHint: { reference: '500000001' },
    })
  })

  it('does not invent an original currency or amount when the webhook omits card currency', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '10000000001', centsAmount: 100000, type: 'card',
        dateTime: '2026-08-06T02:30:00.000Z', reference: '500000002',
        merchant: { name: 'SYNTHETIC SERVICE FEE' },
      },
    })

    expect(observation?.payload).toMatchObject({ amount: '-1000.00', currency: 'ZAR', settlementCurrency: 'ZAR' })
    expect(observation?.payload.originalAmount).toBeUndefined()
    expect(observation?.payload.originalCurrency).toBeUndefined()
    expect(observation?.payload.metadata).toMatchObject({
      providerOwned: { investec: { currencyEvidence: {
        accountCurrency: 'ZAR', originalAmount: null, originalCurrency: null, status: 'account_only',
      } } },
    })
  })

  it('reprojects retained webhook payloads through the corrected normalizer', async () => {
    const connection: RuntimeConnection = {
      deploymentId: 'wealthfolio-production', provider: 'investec', connectionId: 'connection:provider-account',
      clientId: 'client', clientSecret: 'secret', apiKey: 'key', localOwnerId: 'urn:wealthfolio:password#local',
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['provider-account'],
      wealthfolioAccountIds: { 'provider-account': 'wealthfolio-account' }, providerGeneration: 1,
      receiverAccessGeneration: 1, secretReference: 'INVESTEC', eligibleProductNames: ['Private Bank Account'],
      fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    const rawPayload = {
      accountNumber: '10000000001', centsAmount: 100000, type: 'card',
      dateTime: '2026-08-06T02:30:00.000Z', reference: '500000002',
      merchant: { name: 'SYNTHETIC SERVICE FEE' },
    }
    const sql = (async (strings: TemplateStringsArray) => {
      const text = strings.join('?')
      if (/FROM raw_observations/i.test(text)) return [{
        payload: { transactionId: 'investec-card:provider-account:reference-500000002', accountId: 'provider-account' },
        raw_payload: rawPayload,
        semantic_hash: 'old-hash', observed_at: '2026-08-06T02:31:00.000Z',
        source_resource_incarnation_id: connection.sourceResourceIncarnationId,
      }]
      if (/FROM discovered_provider_accounts/i.test(text)) return [{
        local_owner_id: connection.localOwnerId, connection_id: 'connection', provider_account_id: 'provider-account',
        wealthfolio_account_id: 'wealthfolio-account', source_resource_incarnation_id: connection.sourceResourceIncarnationId,
        currency: 'ZAR', mask: '3333',
      }]
      return []
    }) as unknown as FinanceSyncSql
    const persisted: ReadonlyArray<Readonly<Record<string, unknown>>>[] = []
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async (_runtimeConnection: RuntimeConnection, observations: ReadonlyArray<{ payload: Readonly<Record<string, unknown>> }>) => {
        persisted.push(observations.map(observation => observation.payload))
        return observations.length
      },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
    } as unknown as SyncRuntimeDependencies

    await expect(reprojectRetainedInvestecCards(sql, 'wealthfolio-production', deps)).resolves.toBe(1)
    expect(persisted).toHaveLength(1)
    expect(persisted[0][0]).toMatchObject({ amount: '-1000.00', currency: 'ZAR' })
    expect(persisted[0][0].originalAmount).toBeUndefined()
    expect(persisted[0][0].originalCurrency).toBeUndefined()
  })

  it('is deterministic for webhook redelivery and ignores the merchant currency leg', () => {
    const payload = {
      accountNumber: '10000000001', centsAmount: 576, currencyCode: 'usd', type: 'card',
      dateTime: '2026-08-05T16:00:00.000Z', reference: '500000001', merchant: { name: 'SAASCO' },
    }
    const first = investecCardObservation({ ...input, payload })
    const second = investecCardObservation({ ...input, payload })
    expect(first?.semanticHash).toBe(second?.semanticHash)
    expect(first?.payload.currency).toBe('ZAR')
    expect(first?.payload.originalAmount).toBeUndefined()
    expect(first?.payload.originalCurrency).toBeUndefined()
    expect(first?.payload.settlementCurrency).toBe('ZAR')
    expect(first?.hashSpecificationVersion).toBe(3)
  })

  it('hashes a newly supplied provider authorization time even with a stable reference', () => {
    const base = {
      ...input,
      payload: {
        accountNumber: '10000000001', centsAmount: 576, currencyCode: 'usd', type: 'card',
        reference: '500000001', merchant: { name: 'SAASCO' },
      },
      receivedAt: '2026-08-05T16:00:00.000Z',
    }
    const withoutProviderTime = investecCardObservation(base)
    const withProviderTime = investecCardObservation({
      ...base,
      payload: { ...base.payload, dateTime: '2026-08-05T16:07:01.000Z' },
    })
    expect(withProviderTime?.semanticHash).not.toBe(withoutProviderTime?.semanticHash)
  })

  it('keeps card credits positive even when the webhook uses centsAmount', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        centsAmount: 1250, currencyCode: 'usd', type: 'CREDIT',
        dateTime: '2026-08-05T16:08:00.000Z', reference: 'refund-1',
        merchant: { name: 'SAASCO REFUND' },
      },
    })
    expect(observation?.payload.amount).toBe('12.50')
    expect(observation?.payload.currency).toBe('ZAR')
    expect(observation?.payload.type).toBe('CREDIT')
  })

  it('does not create a zero-value card activity', () => {
    expect(investecCardObservation({
      ...input,
      payload: { accountNumber: '10000000001', centsAmount: 0, dateTime: '2026-08-05T13:40:00.000Z', merchant: { name: 'Visa Provisioning Service' } },
    })).toBeNull()
  })
})
