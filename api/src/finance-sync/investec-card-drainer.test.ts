import { describe, expect, it } from 'vitest'
import { investecCardObservation, reprojectRetainedInvestecCards } from './investec-card-drainer'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const input = {
  deploymentId: 'wealthfolio-production',
  localOwnerId: 'urn:wealthfolio:password#local',
  sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
  providerAccountId: 'provider-account',
  accountCurrency: 'ZAR',
}

describe('Investec card observations', () => {
  it('preserves a card authorization as an explicitly pending native cash fact', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '90000000000', centsAmount: 1234, originalCentsAmount: 1234, currencyCode: 'usd', type: 'card',
        dateTime: '2026-01-05T16:00:00.000Z', reference: 'TEST-REF-001',
        merchant: { name: 'SYNTHETIC MERCHANT' }, card: { display: 'TEST-CARD-0011' },
      },
    })

    expect(observation).toMatchObject({
      providerVersion: 'investec:card:investec-card:provider-account:reference-TEST-REF-001',
      observedAt: '2026-01-05T16:00:00.000Z',
      payload: {
        transactionId: 'investec-card:provider-account:reference-TEST-REF-001',
        amount: '-12.34', currency: 'ZAR', originalCurrency: 'USD',
        settlementCurrency: 'ZAR', date: '2026-01-05', transactionDate: '2026-01-05',
        description: 'SYNTHETIC MERCHANT', status: 'pending', reference: 'TEST-REF-001',
      },
    })
    expect(observation?.payload.merchantContext).toMatchObject({
      merchantDisplay: 'SYNTHETIC MERCHANT', direction: 'outflow',
      providerHint: { reference: 'TEST-REF-001' },
    })
  })

  it('does not invent an original currency or amount when the webhook omits card currency', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '90000000000', centsAmount: 12345, type: 'card',
        dateTime: '2026-01-06T02:30:00.000Z', reference: 'TEST-REF-002',
        merchant: { name: 'SYNTHETIC CARD MERCHANT' },
      },
    })

    expect(observation?.payload).toMatchObject({ amount: '-123.45', currency: 'ZAR', settlementCurrency: 'ZAR' })
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
      accountNumber: '90000000000', centsAmount: 12345, type: 'card',
      dateTime: '2026-01-06T02:30:00.000Z', reference: 'TEST-REF-002',
      merchant: { name: 'SYNTHETIC CARD MERCHANT' },
    }
    const sql = (async (strings: TemplateStringsArray) => {
      const text = strings.join('?')
      if (/FROM raw_observations/i.test(text)) return [{
        payload: { transactionId: 'investec-card:provider-account:reference-TEST-REF-002', accountId: 'provider-account' },
        raw_payload: rawPayload,
        semantic_hash: 'old-hash', observed_at: '2026-01-06T02:31:00.000Z',
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
    expect(persisted[0][0]).toMatchObject({ amount: '-123.45', currency: 'ZAR' })
    expect(persisted[0][0].originalAmount).toBeUndefined()
    expect(persisted[0][0].originalCurrency).toBeUndefined()
  })

  it('is deterministic for webhook redelivery and ignores the merchant currency leg', () => {
    const payload = {
      accountNumber: '90000000000', centsAmount: 1234, currencyCode: 'usd', type: 'card',
      dateTime: '2026-01-05T16:00:00.000Z', reference: 'TEST-REF-001', merchant: { name: 'SYNTHETIC MERCHANT' },
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
        accountNumber: '90000000000', centsAmount: 1234, currencyCode: 'usd', type: 'card',
        reference: 'TEST-REF-001', merchant: { name: 'SYNTHETIC MERCHANT' },
      },
      receivedAt: '2026-01-05T16:00:00.000Z',
    }
    const withoutProviderTime = investecCardObservation(base)
    const withProviderTime = investecCardObservation({
      ...base,
      payload: { ...base.payload, dateTime: '2026-01-05T16:07:01.000Z' },
    })
    expect(withProviderTime?.semanticHash).not.toBe(withoutProviderTime?.semanticHash)
  })

  it('keeps card credits positive even when the webhook uses centsAmount', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        centsAmount: 2345, currencyCode: 'usd', type: 'CREDIT',
        dateTime: '2026-01-05T16:08:00.000Z', reference: 'refund-1',
        merchant: { name: 'SYNTHETIC MERCHANT REFUND' },
      },
    })
    expect(observation?.payload.amount).toBe('23.45')
    expect(observation?.payload.currency).toBe('ZAR')
    expect(observation?.payload.type).toBe('CREDIT')
  })

  it('does not create a zero-value card activity', () => {
    expect(investecCardObservation({
      ...input,
      payload: { accountNumber: '90000000000', centsAmount: 0, dateTime: '2026-01-05T13:40:00.000Z', merchant: { name: 'SYNTHETIC PROVISIONING' } },
    })).toBeNull()
  })
})
