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
  it('masks full PANs in normalized card evidence', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '90000000000', centsAmount: 1234, currencyCode: 'usd', type: 'card',
        dateTime: '2026-01-05T16:00:00.000Z', reference: 'PAN-TEST',
        merchant: { name: 'SYNTHETIC MERCHANT' }, card: { display: '4111 1111 1111 1111' },
      },
    })
    expect(observation?.payload.cardNumber).toBe('••••1111')
    expect(JSON.stringify(observation?.payload)).not.toContain('4111111111111111')
  })
  it('preserves a card authorization as an explicitly pending native cash fact', () => {
    const observation = investecCardObservation({
      ...input,
      payload: {
        accountNumber: '90000000000', centsAmount: 1234, originalCentsAmount: 9999, currencyCode: 'usd', type: 'card',
        dateTime: '2026-01-05T16:00:00.000Z', reference: 'TEST-REF-001',
        merchant: { name: 'SYNTHETIC MERCHANT' }, card: { display: 'TEST-CARD-0011' },
      },
    })

    expect(observation).toMatchObject({
      providerVersion: 'investec:card:investec-card:provider-account:reference-TEST-REF-001',
      observedAt: '2026-01-05T16:00:00.000Z',
      payload: {
        transactionId: 'investec-card:provider-account:reference-TEST-REF-001',
        amount: '-12.34', currency: 'USD', originalAmount: '12.34', originalCurrency: 'USD',
        date: '2026-01-05', transactionDate: '2026-01-05',
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

    expect(observation).toBeNull()
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
      accountNumber: '90000000000', centsAmount: 12345, currencyCode: 'usd', type: 'card',
      dateTime: '2026-01-06T02:30:00.000Z', reference: 'TEST-REF-002',
      merchant: { name: 'SYNTHETIC CARD MERCHANT' },
    }
    const sql = (async (strings: TemplateStringsArray) => {
      const text = strings.join('?')
      if (/FROM raw_observations/i.test(text)) return [{
        payload: { transactionId: 'investec-card:provider-account:reference-TEST-REF-002', accountId: 'provider-account' },
        raw_payload: {
          schemaVersion: 1,
          eventType: 'after_transaction',
          providerEventTime: rawPayload.dateTime,
          transaction: rawPayload,
        },
        semantic_hash: 'old-hash', hash_specification_version: 3, observed_at: '2026-01-06T02:31:00.000Z',
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
    const persistedRawPayloads: unknown[] = []
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async (_runtimeConnection: RuntimeConnection, observations: ReadonlyArray<{
        payload: Readonly<Record<string, unknown>>
        rawPayload?: unknown
      }>) => {
        persisted.push(observations.map(observation => observation.payload))
        persistedRawPayloads.push(...observations.map(observation => observation.rawPayload))
        return observations.length
      },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
    } as unknown as SyncRuntimeDependencies

    await expect(reprojectRetainedInvestecCards(sql, 'wealthfolio-production', deps)).resolves.toBe(1)
    expect(persisted).toHaveLength(1)
    expect(persisted[0][0]).toMatchObject({ amount: '-123.45', currency: 'USD' })
    expect(persisted[0][0].originalAmount).toBe('123.45')
    expect(persisted[0][0].originalCurrency).toBe('USD')
    expect(persistedRawPayloads).toEqual([rawPayload])
  })

  it('keeps retained replay pending when any retained card cannot be mapped exactly', async () => {
    const connection = {
      deploymentId: 'wealthfolio-production', provider: 'investec', connectionId: 'connection:provider-account',
      localOwnerId: 'urn:wealthfolio:password#local', sourceResourceIncarnationId: input.sourceResourceIncarnationId,
    } as RuntimeConnection
    const sql = (async (strings: TemplateStringsArray) => {
      if (/FROM raw_observations/i.test(strings.join('?'))) return [{
        payload: { transactionId: 'retained-unmapped', accountId: 'provider-account' },
        raw_payload: {
          accountNumber: '90000000000', centsAmount: 12345, currencyCode: 'usd', type: 'card',
          dateTime: '2026-01-06T02:30:00.000Z', reference: 'RETAINED-UNMAPPED',
          merchant: { name: 'SYNTHETIC CARD MERCHANT' },
        },
        semantic_hash: 'old-hash', hash_specification_version: 3,
        observed_at: '2026-01-06T02:31:00.000Z', source_resource_incarnation_id: input.sourceResourceIncarnationId,
      }]
      return []
    }) as unknown as FinanceSyncSql
    let persisted = false
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async () => { persisted = true; return 0 },
      projectPending: async () => ({ delivered: 0, failed: 0 }),
    } as unknown as SyncRuntimeDependencies

    await expect(reprojectRetainedInvestecCards(sql, connection.deploymentId, deps))
      .rejects.toThrow('Investec retained card projection is unresolved')
    expect(persisted).toBe(false)
  })

  it('reprojects when the semantic hash is unchanged across hash specification versions', async () => {
    const connection: RuntimeConnection = {
      deploymentId: 'wealthfolio-production', provider: 'investec', connectionId: 'connection:provider-account',
      clientId: 'client', clientSecret: 'secret', apiKey: 'key', localOwnerId: 'urn:wealthfolio:password#local',
      sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001', accountIds: ['provider-account'],
      wealthfolioAccountIds: { 'provider-account': 'wealthfolio-account' }, providerGeneration: 1,
      receiverAccessGeneration: 1, secretReference: 'INVESTEC', eligibleProductNames: ['Private Bank Account'],
      fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    const rawPayload = {
      accountNumber: '90000000000', centsAmount: 12345, originalCentsAmount: 12345, currencyCode: 'zar', type: 'card',
      dateTime: '2026-01-06T02:30:00.000Z', reference: 'TEST-REF-SAME-HASH',
      merchant: { name: 'SYNTHETIC CARD MERCHANT' },
    }
    const normalized = investecCardObservation({
      deploymentId: connection.deploymentId, localOwnerId: connection.localOwnerId,
      sourceResourceIncarnationId: connection.sourceResourceIncarnationId,
      providerAccountId: 'provider-account', accountCurrency: 'ZAR', payload: rawPayload,
    })
    expect(normalized).not.toBeNull()
    const sql = (async (strings: TemplateStringsArray) => {
      const text = strings.join('?')
      if (/FROM raw_observations/i.test(text)) return [{
        payload: { transactionId: normalized?.payload.transactionId, accountId: 'provider-account' },
        raw_payload: rawPayload, semantic_hash: normalized?.semanticHash, hash_specification_version: 3,
        observed_at: '2026-01-06T02:31:00.000Z', source_resource_incarnation_id: connection.sourceResourceIncarnationId,
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
      persistAndEnqueue: async (_runtimeConnection: RuntimeConnection, observations: ReadonlyArray<{
        payload: Readonly<Record<string, unknown>>
        hashSpecificationVersion: number
      }>) => {
        persisted.push(observations.map(observation => observation.payload))
        expect(observations[0]?.hashSpecificationVersion).toBe(4)
        return observations.length
      },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
    } as unknown as SyncRuntimeDependencies

    await expect(reprojectRetainedInvestecCards(sql, connection.deploymentId, deps)).resolves.toBe(1)
    expect(persisted).toHaveLength(1)
    expect(persisted[0][0]).toMatchObject({ currency: 'ZAR', originalAmount: '123.45', originalCurrency: 'ZAR' })
  })

  it('is deterministic for webhook redelivery and uses the provider currency leg', () => {
    const payload = {
      accountNumber: '90000000000', centsAmount: 1234, currencyCode: 'usd', type: 'card',
      dateTime: '2026-01-05T16:00:00.000Z', reference: 'TEST-REF-001', merchant: { name: 'SYNTHETIC MERCHANT' },
    }
    const first = investecCardObservation({ ...input, payload })
    const second = investecCardObservation({ ...input, payload })
    expect(first?.semanticHash).toBe(second?.semanticHash)
    expect(first?.payload.currency).toBe('USD')
    expect(first?.payload.originalAmount).toBe('12.34')
    expect(first?.payload.originalCurrency).toBe('USD')
    expect(first?.payload.settlementCurrency).toBeUndefined()
    expect(first?.hashSpecificationVersion).toBe(4)
  })

  it('rejects an invalid provider authorization time instead of using receipt time', () => {
    const observation = investecCardObservation({
      ...input,
      receivedAt: '2026-01-05T16:00:00.000Z',
      payload: {
        centsAmount: 1234, currencyCode: 'usd', type: 'card', dateTime: 'not-a-date',
        reference: 'invalid-date-1', merchant: { name: 'SYNTHETIC MERCHANT' },
      },
    })
    expect(observation).toBeNull()
    for (const dateTime of ['2026-01-05', '2026-01-05T16:00:00', '2026-02-30T16:00:00Z']) {
      expect(investecCardObservation({
        ...input,
        payload: {
          centsAmount: 1234, currencyCode: 'usd', type: 'card', dateTime,
          reference: 'invalid-date-2', merchant: { name: 'SYNTHETIC MERCHANT' },
        },
      })).toBeNull()
    }
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
    expect(observation?.payload.currency).toBe('USD')
    expect(observation?.payload.type).toBe('CREDIT')
  })

  it('rejects conflicting lifecycle identifiers', () => {
    expect(investecCardObservation({
      ...input,
      payload: {
        centsAmount: 1234, currencyCode: 'usd', type: 'card', dateTime: '2026-01-05T13:40:00.000Z',
        providerLifecycleTransactionId: 'settled-1', pendingTransactionId: 'settled-2',
        merchant: { name: 'SYNTHETIC PROVISIONING' },
      },
    })).toBeNull()
  })

  it('does not create a zero-value card activity', () => {
    expect(investecCardObservation({
      ...input,
      payload: { accountNumber: '90000000000', centsAmount: 0, dateTime: '2026-01-05T13:40:00.000Z', merchant: { name: 'SYNTHETIC PROVISIONING' } },
    })).toBeNull()
  })
})
