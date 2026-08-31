import { describe, expect, it } from 'vitest'
import { createInvestecCardLifecycleLinkControl } from './investec-card-lifecycle-link'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const subject = 'urn:wealthfolio:test#owner'
const sourceResourceIncarnationId = '00000000-0000-4000-8000-000000000001'
const providerAccountId = 'provider-account'
const pendingTransactionId = `investec-card:${providerAccountId}:reference-order-1`
const postedTransactionId = 'posted-order-1'

const cardPayload = {
  transactionId: pendingTransactionId,
  accountId: providerAccountId,
  amount: '-1357.38',
  currency: 'ZAR',
  originalAmount: '1357.38',
  originalCurrency: 'ZAR',
  date: '2026-08-17',
  transactionDate: '2026-08-17',
  description: 'SAMPLEMART ONLINE',
  type: 'DEBIT',
  direction: 'DEBIT',
  transactionType: 'CARD',
  cardNumber: '••••0014',
  reference: 'order-1',
  status: 'pending',
  metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
}

const postedPayload = {
  transactionId: postedTransactionId,
  accountId: providerAccountId,
  amount: '-1345.13',
  currency: 'ZAR',
  date: '2026-08-18',
  transactionDate: '2026-08-18',
  description: 'SAMPLEMART ONLINE STORE',
  type: 'DEBIT',
  direction: 'DEBIT',
  transactionType: 'CARDPURCHASES',
  cardNumber: '0014',
  reference: 'posted-1',
  status: 'posted',
}

const connection = {
  deploymentId: 'dep-1', provider: 'investec', connectionId: 'connection:provider-account',
  clientId: 'client', clientSecret: 'secret', apiKey: 'key', localOwnerId: subject,
  sourceResourceIncarnationId, accountIds: [providerAccountId],
  wealthfolioAccountIds: { [providerAccountId]: 'wealthfolio-account' },
  providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'INVESTEC',
  eligibleProductNames: ['Private Bank Account'], fromDate: '2026-08-01', toDate: '2026-08-31',
} as RuntimeConnection

function row(payload: Record<string, unknown>, providerVersion: string, semanticHash: string) {
  return {
    payload,
    raw_payload: { fixture: true },
    provider_version: providerVersion,
    semantic_hash: semanticHash,
    hash_specification_version: 4,
    observed_at: '2026-08-18T08:00:00.000Z',
  }
}

function controlSql(options: { prior?: readonly Record<string, unknown>[] } = {}) {
  const prior = options.prior ?? []
  let call = 0
  return (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join('?')
    call += 1
    if (/FROM raw_observations/i.test(text)) {
      return [
        row(cardPayload, `investec:card:${pendingTransactionId}`, 'a'.repeat(64)),
        row(postedPayload, `investec:txn:${postedTransactionId}`, 'b'.repeat(64)),
      ]
    }
    if (/FROM investec_card_lifecycle_overrides/i.test(text)) return prior
    if (/INSERT INTO investec_card_lifecycle_overrides/i.test(text)) return [{ override_id: 'override-1' }]
    return []
  }) as unknown as FinanceSyncSql
}

describe('owner-authorized Investec card lifecycle links', () => {
  it('records the exact pair and reprojects it through the normal lifecycle path', async () => {
    const persisted: unknown[][] = []
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async (_connection: RuntimeConnection, observations: readonly unknown[]) => {
        persisted.push([...observations])
        return observations.length
      },
      projectPending: async () => ({ delivered: 2, failed: 0 }),
    } as unknown as SyncRuntimeDependencies
    const control = createInvestecCardLifecycleLinkControl(controlSql(), 'dep-1', deps)

    const result = await control.create(subject, {
      sourceResourceIncarnationId,
      pendingTransactionId,
      postedTransactionId,
      reason: 'same synthetic online order with adjusted final amount',
    })

    expect(result).toMatchObject({
      overrideId: 'override-1', inserted: true, delivered: 2,
      pendingResourceId: `dep-1:${sourceResourceIncarnationId}:${pendingTransactionId}`,
      postedResourceId: `dep-1:${sourceResourceIncarnationId}:${postedTransactionId}`,
    })
    expect(persisted).toHaveLength(1)
    expect(persisted[0]).toHaveLength(2)
  })

  it('is idempotent for the same pair and rejects reuse of either fact', async () => {
    const prior = [{
      override_id: 'override-existing',
      pending_transaction_id: pendingTransactionId,
      posted_transaction_id: postedTransactionId,
    }]
    let persisted = 0
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async () => { persisted += 1; return 1 },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
    } as unknown as SyncRuntimeDependencies
    const control = createInvestecCardLifecycleLinkControl(controlSql({ prior }), 'dep-1', deps)
    const request = { sourceResourceIncarnationId, pendingTransactionId, postedTransactionId, reason: 'retry' }
    await expect(control.create(subject, request)).resolves.toMatchObject({ overrideId: 'override-existing', inserted: false })
    expect(persisted).toBe(1)

    const pendingConflict = createInvestecCardLifecycleLinkControl(
      controlSql({ prior: [{ ...prior[0], posted_transaction_id: 'other-posted' }] }),
      'dep-1',
      deps,
    )
    await expect(pendingConflict.create(subject, request)).rejects.toMatchObject({ status: 409 })

    const crossRoleConflict = createInvestecCardLifecycleLinkControl(
      controlSql({ prior: [{
        override_id: 'override-cross-role',
        pending_transaction_id: 'other-pending',
        posted_transaction_id: pendingTransactionId,
      }] }),
      'dep-1',
      deps,
    )
    await expect(crossRoleConflict.create(subject, request)).rejects.toMatchObject({ status: 409 })
  })

  it('rejects control characters in the operator reason', async () => {
    const deps = { loadConnections: async () => [connection] } as unknown as SyncRuntimeDependencies
    const control = createInvestecCardLifecycleLinkControl(controlSql(), 'dep-1', deps)
    await expect(control.create(subject, {
      sourceResourceIncarnationId,
      pendingTransactionId,
      postedTransactionId,
      reason: 'same order\nforged audit line',
    })).rejects.toMatchObject({ status: 400 })
  })

  it('fails closed when the selected facts do not share the provider lifecycle shape', async () => {
    const sql = (async (strings: TemplateStringsArray) => {
      const text = strings.join('?')
      if (/FROM raw_observations/i.test(text)) {
        return [
          row(cardPayload, `investec:card:${pendingTransactionId}`, 'a'.repeat(64)),
          row({ ...postedPayload, description: 'DIFFERENT MERCHANT' }, `investec:txn:${postedTransactionId}`, 'b'.repeat(64)),
        ]
      }
      return []
    }) as unknown as FinanceSyncSql
    const deps = { loadConnections: async () => [connection] } as unknown as SyncRuntimeDependencies
    const control = createInvestecCardLifecycleLinkControl(sql, 'dep-1', deps)
    await expect(control.create(subject, {
      sourceResourceIncarnationId, pendingTransactionId, postedTransactionId, reason: 'not the same merchant',
    })).rejects.toMatchObject({ status: 409 })
  })
})
