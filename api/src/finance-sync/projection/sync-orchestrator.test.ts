
import { describe, expect, it } from 'vitest'
import { runPlaidSyncSession, type PlaidSyncSession } from './sync-orchestrator'

describe('Sync orchestrator', () => {
  it('runs a Plaid sync session end-to-end', async () => {
    const session: PlaidSyncSession = {
      provider: 'plaid',
      accessToken: 'access-sandbox-test',
      deploymentId: 'g8-orch-test',
      localOwnerId: 'urn:wealthfolio:test#alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
      wealthfolioAccountIds: { 'acc-1': 'wf-acc-uuid-1' },
      accessGeneration: 1,
      transport: {
        syncTransactions: async (_token, cursor) => {
          if (!cursor) {
            return {
              added: [{ transaction_id: 'txn-1', account_id: 'acc-1', amount: 42.50, date: '2026-07-11', name: 'GITHUB' }],
              modified: [], removed: [],
              accounts: [{ account_id: 'acc-1', name: 'Checking', type: 'depository', subtype: 'checking', balances: { current: 5000, iso_currency_code: 'USD' } }],
              next_cursor: 'done', has_more: false,
            }
          }
          return { added: [], modified: [], removed: [], accounts: [], next_cursor: cursor, has_more: false }
        },
      },
      bridgeTransport: {
        commit: async (op) => {
          const operation = op.kind === 'ActivityUpsert' ? 'activity_upsert' : op.kind === 'ActivityDelete' ? 'activity_delete' : 'projection_fence'
          const successor = op.lifecycleSuccessorId ?? null
          const fields = [op.deploymentId, op.subject, op.idempotencyKey, String(op.accessGeneration), operation, op.resourceId, op.accountId, successor === null ? '0' : '1', successor ?? '', 'accepted', '0', '']
          const encoder = new TextEncoder()
          return {
            deploymentId: op.deploymentId,
            accountId: op.accountId,
            subject: op.subject,
            idempotencyKey: op.idempotencyKey,
            accessGeneration: op.accessGeneration,
            operation,
            resourceId: op.resourceId,
            lifecycleSuccessorId: successor,
            outcome: 'Accepted',
            bindingVersion: 3,
            semanticHash: null,
            receiptBinding: `v3;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`,
          }
        },
      },
    }

    const result = await runPlaidSyncSession(session)
    expect(result.provider).toBe('plaid')
    expect(result.observationsCount).toBe(2) // transaction plus complete balance fact
    expect(result.committedCount).toBe(1)
    expect(result.errors.length).toBe(0)
    expect(result.newCursor).toBe('done')
  })

  it('handles unmapped accounts gracefully', async () => {
    const session: PlaidSyncSession = {
      provider: 'plaid',
      accessToken: 'access-sandbox-test',
      deploymentId: 'g8-orch-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
      wealthfolioAccountIds: {},
      accessGeneration: 1,
      transport: {
        syncTransactions: async () => ({
          added: [{ transaction_id: 'txn-1', account_id: 'acc-1', amount: 10, date: '2026-07-11', name: 'TEST' }],
          modified: [], removed: [],
          accounts: [{ account_id: 'acc-1', name: 'X', type: 'depository', subtype: 'checking', balances: { current: 100, iso_currency_code: 'USD' } }],
          next_cursor: null, has_more: false,
        }),
      },
      bridgeTransport: { commit: async () => { throw new Error('should not be called') } },
    }
    const result = await runPlaidSyncSession(session)
    expect(result.observationsCount).toBe(2) // transaction plus complete balance fact
    expect(result.committedCount).toBe(0)
    expect(result.errors.length).toBe(1)
    expect(result.errors[0]).toContain('No Wealthfolio mapping')
  })

  it('recovers from transport failures', async () => {
    const session: PlaidSyncSession = {
      provider: 'plaid',
      accessToken: 'access-sandbox-test',
      deploymentId: 'g8-orch-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      accountIds: ['acc-1'],
      wealthfolioAccountIds: {},
      accessGeneration: 1,
      transport: {
        syncTransactions: async () => { throw new Error('NETWORK_DOWN') },
      },
      bridgeTransport: { commit: async () => { throw new Error('n/a') } },
    }
    const result = await runPlaidSyncSession(session)
    expect(result.errors.length).toBeGreaterThanOrEqual(1)
    expect(result.observationsCount).toBe(0)
  })
})
