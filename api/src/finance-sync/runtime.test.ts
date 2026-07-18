import { describe, expect, it, vi } from 'vitest'
import { runConfiguredSyncCycle, type SyncRuntimeDependencies } from './runtime'

describe('configured sync runtime', () => {
  it('persists observations, enqueues bridge work, projects it, and advances its cursor only after success', async () => {
    const calls: string[] = []
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [{ provider: 'plaid', connectionId: 'item-1', deploymentId: 'd', accessToken: 'token', localOwnerId: 'owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001', accountIds: ['a'], wealthfolioAccountIds: { a: 'wf-a' }, accessGeneration: 1 }],
      loadCheckpoint: async () => null,
      runProvider: async (_connection, cursor) => ({ nextCursor: cursor === null ? 'cursor-1' : cursor, observations: [{ deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001', providerVersion: 'v1', semanticHash: 'a'.repeat(64), hashSpecificationVersion: 1, domain: 'cash_ledger', observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: 'a', amount: '12.50', currency: 'USD', date: '2026-07-11' } }] }),
      persistAndEnqueue: async (_connection, observations) => { calls.push(`persist+enqueue:${observations.length}`); return observations.length },
      projectPending: async () => { calls.push('project'); return { delivered: 1, failed: 0 } },
      saveCheckpoint: async (_connection, cursor) => { calls.push(`checkpoint:${cursor}`) },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result).toEqual({ connections: 1, observations: 1, delivered: 1, failures: 0 })
    expect(calls).toEqual(['persist+enqueue:1', 'project', 'checkpoint:cursor-1'])
  })

  it('redacts provider failures from operational logs', async () => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [{ provider: 'plaid', connectionId: 'sensitive-connection', deploymentId: 'd', accessToken: 'sensitive-token', localOwnerId: 'owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001', accountIds: ['sensitive-account'], wealthfolioAccountIds: { 'sensitive-account': 'wf-a' }, accessGeneration: 1 }],
      loadCheckpoint: async () => null,
      runProvider: async () => { throw new Error('provider payload sensitive-token sensitive-account') },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    const result = await runConfiguredSyncCycle('d', deps)

    expect(result.failures).toBe(1)
    expect(log).toHaveBeenCalledWith('Finance Sync connection cycle failed')
    expect(JSON.stringify(log.mock.calls)).not.toMatch(/sensitive-token|sensitive-account|sensitive-connection|provider payload/)
    log.mockRestore()
  })

  it('does not advance a cursor when projection fails', async () => {
    let checkpointed = false
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [{ provider: 'plaid', connectionId: 'item-1', deploymentId: 'd', accessToken: 'token', localOwnerId: 'owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001', accountIds: ['a'], wealthfolioAccountIds: { a: 'wf-a' }, accessGeneration: 1 }],
      loadCheckpoint: async () => null,
      runProvider: async () => ({ nextCursor: 'cursor-1', observations: [] }),
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 1 }),
      saveCheckpoint: async () => { checkpointed = true },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result.failures).toBe(1)
    expect(checkpointed).toBe(false)
  })
})
