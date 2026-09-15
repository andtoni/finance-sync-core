import { describe, expect, it, vi } from 'vitest'
import { runConfiguredSyncCycle, type SyncRuntimeDependencies } from './runtime'
import type { ObservationOutput } from './probes/investec/investec-adapter'

const plaidConnection = (connectionId: string, accessToken: string, accountId = 'a') => ({
  provider: 'plaid' as const, connectionId, providerConnectionId: connectionId, deploymentId: 'd', clientId: 'client', clientSecret: 'secret', accessToken,
  localOwnerId: 'owner', sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  accountIds: [accountId], wealthfolioAccountIds: { [accountId]: 'wf-a' }, providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT',
})

const investecConnection = (connectionId: string, accountId: string, incarnation: string, localOwnerId = 'owner') => ({
  provider: 'investec' as const, connectionId, deploymentId: 'd', clientId: 'client', clientSecret: 'secret', apiKey: 'key',
  localOwnerId, sourceResourceIncarnationId: incarnation, accountIds: [accountId], wealthfolioAccountIds: { [accountId]: `wf-${accountId}` }, providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT', eligibleProductNames: [], fromDate: '2026-01-01', toDate: '2026-07-11',
})

describe('configured sync runtime', () => {
  it('pairs exact transfers across singleton Investec runtime connections before persistence', async () => {
    const first = investecConnection('provider:a', 'a', '00000000-0000-0000-0000-000000000010')
    const second = investecConnection('provider:b', 'b', '00000000-0000-0000-0000-000000000011')
    const makeObservation = (accountId: string, amount: string, direction: string, id: string, incarnation: string) => ({ deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: incarnation, providerVersion: `investec:txn:${id}`, semanticHash: 'a'.repeat(64), hashSpecificationVersion: 4, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId, transactionId: id, amount, currency: 'USD', date: '2026-07-11', transactionDate: '2026-07-11', direction, type: direction, sourceType: 'TRANSFER', description: `Transfer to ${direction === 'DEBIT' ? '22223333' : '11112222'}`, reference: null } })
    const persisted: ObservationOutput[][] = []
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [first, second], loadCheckpoint: async () => null,
      runProvider: async connection => connection.connectionId === 'provider:a'
        ? { nextCursor: null, transferAccounts: { a: '11112222' } as Record<string, string>, observations: [makeObservation('a', '-10.00', 'DEBIT', 'a-tx', first.sourceResourceIncarnationId)] }
        : { nextCursor: null, transferAccounts: { b: '22223333' } as Record<string, string>, observations: [makeObservation('b', '10.00', 'CREDIT', 'b-tx', second.sourceResourceIncarnationId)] },
      persistAndEnqueue: async (_connection, rows) => { persisted.push([...rows]); return rows.length },
      persistAndEnqueueBatch: async batches => {
        for (const batch of batches) persisted.push([...batch.observations])
        return batches.reduce((sum, batch) => sum + batch.observations.length, 0)
      },
      projectPending: async () => ({ delivered: 0, failed: 0 }), saveCheckpoint: async () => {},
    }
    await runConfiguredSyncCycle('d', deps)
    expect(persisted).toHaveLength(2)
    const firstPayload = persisted[0][0].payload as Record<string, unknown>
    const secondPayload = persisted[1][0].payload as Record<string, unknown>
    expect(firstPayload.sourceGroupId).toMatch(/^investec:transfer:v1:/)
    expect(firstPayload.activityType).toBe('TRANSFER_OUT')
    expect(secondPayload.activityType).toBe('TRANSFER_IN')
    expect(firstPayload.sourceGroupId).toBe(secondPayload.sourceGroupId)
  })
  it('does not mix separate Investec provider principals or owners', async () => {
    const first = investecConnection('provider:a', 'a', '00000000-0000-0000-0000-000000000020', 'owner-a')
    const second = investecConnection('provider:b', 'b', '00000000-0000-0000-0000-000000000021', 'owner-b')
    const persisted: ObservationOutput[][] = []
    const makeObservation = (accountId: string, direction: string, id: string, incarnation: string) => ({ deploymentId: 'd', localOwnerId: direction === 'DEBIT' ? 'owner-a' : 'owner-b', sourceResourceIncarnationId: incarnation, providerVersion: `investec:txn:${id}`, semanticHash: 'a'.repeat(64), hashSpecificationVersion: 4, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId, transactionId: id, amount: direction === 'DEBIT' ? '-10.00' : '10.00', currency: 'USD', date: '2026-07-11', transactionDate: '2026-07-11', direction, type: direction, sourceType: 'TRANSFER', description: `Transfer to ${direction === 'DEBIT' ? '22223333' : '11112222'}`, reference: null } })
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [first, second], loadCheckpoint: async () => null,
      runProvider: async connection => connection.connectionId === 'provider:a' ? { nextCursor: null, transferAccounts: { a: '11112222' } as Record<string, string>, observations: [makeObservation('a', 'DEBIT', 'a-tx', first.sourceResourceIncarnationId)] } : { nextCursor: null, transferAccounts: { b: '22223333' } as Record<string, string>, observations: [makeObservation('b', 'CREDIT', 'b-tx', second.sourceResourceIncarnationId)] },
      persistAndEnqueue: async (_connection, rows) => { persisted.push([...rows]); return rows.length }, projectPending: async () => ({ delivered: 0, failed: 0 }), saveCheckpoint: async () => {},
    }
    await runConfiguredSyncCycle('d', deps)
    expect((persisted[0][0].payload as Record<string, unknown>).sourceGroupId).toBeUndefined()
    expect((persisted[1][0].payload as Record<string, unknown>).sourceGroupId).toBeUndefined()
  })

  it('does not correlate when an expected Investec sibling fails', async () => {
    const first = investecConnection('provider:a', 'a', '00000000-0000-0000-0000-000000000030')
    const second = investecConnection('provider:b', 'b', '00000000-0000-0000-0000-000000000031')
    const persisted: ObservationOutput[][] = []
    const observation = (connection: typeof first) => ({ deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: connection.sourceResourceIncarnationId, providerVersion: 'investec:txn:tx', semanticHash: 'a'.repeat(64), hashSpecificationVersion: 4, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: connection.accountIds[0], transactionId: 'tx', amount: '-10.00', currency: 'USD', date: '2026-07-11', transactionDate: '2026-07-11', direction: 'DEBIT', type: 'DEBIT', sourceType: 'TRANSFER', description: 'Transfer to 22223333', reference: null } })
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [first, second], loadCheckpoint: async () => null,
      runProvider: async connection => { if (connection.connectionId === second.connectionId) throw new Error('failed sibling'); return { nextCursor: null, transferAccounts: { a: '11112222' }, observations: [observation(first)] } },
      persistAndEnqueue: async (_connection, rows) => { persisted.push([...rows]); return rows.length }, projectPending: async () => ({ delivered: 0, failed: 0 }), saveCheckpoint: async () => {},
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result.failures).toBe(1)
    expect(persisted).toEqual([])
  })

  it('keeps an exact transfer group unprojected when sibling persistence fails and continues unrelated connections', async () => {
    const first = investecConnection('provider:a', 'a', '00000000-0000-0000-0000-000000000040')
    const second = investecConnection('provider:b', 'b', '00000000-0000-0000-0000-000000000041')
    const unrelated = plaidConnection('plaid:c', 'token-c')
    const persisted: string[] = []
    const projected: string[] = []
    const checkpointed: string[] = []
    const observation = (connection: typeof first, direction: string, id: string) => ({ deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: connection.sourceResourceIncarnationId, providerVersion: `investec:txn:${id}`, semanticHash: 'a'.repeat(64), hashSpecificationVersion: 4, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: connection.accountIds[0], transactionId: id, amount: direction === 'DEBIT' ? '-10.00' : '10.00', currency: 'USD', date: '2026-07-11', transactionDate: '2026-07-11', direction, type: direction, sourceType: 'TRANSFER', description: `Transfer to ${direction === 'DEBIT' ? '22223333' : '11112222'}`, reference: null } })
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [first, second, unrelated], loadCheckpoint: async () => null,
      runProvider: async connection => connection.provider === 'investec'
        ? connection.connectionId === first.connectionId
          ? { nextCursor: null, transferAccounts: { a: '11112222' } as Record<string, string>, observations: [observation(first, 'DEBIT', 'a-tx')] }
          : { nextCursor: null, transferAccounts: { b: '22223333' } as Record<string, string>, observations: [observation(second, 'CREDIT', 'b-tx')] }
        : { nextCursor: null, observations: [{ deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: unrelated.sourceResourceIncarnationId, providerVersion: 'plaid:tx:c', semanticHash: 'b'.repeat(64), hashSpecificationVersion: 1, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: 'a', amount: '1', currency: 'USD', date: '2026-07-11' } }] },
      persistAndEnqueue: async (connection, rows) => { persisted.push(connection.connectionId); return rows.length },
      persistAndEnqueueBatch: async () => { throw new Error('atomic persist failed') },
      projectPending: async connection => { projected.push(connection.connectionId); return { delivered: 1, failed: 0 } },
      saveCheckpoint: async connection => { checkpointed.push(connection.connectionId) },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result.failures).toBe(1)
    expect(projected).toEqual(['plaid:c'])
    expect(checkpointed).toEqual(['plaid:c'])
    expect(persisted).toEqual(['plaid:c'])
  })

  it('isolates projection exceptions to one uncorrelated connection', async () => {
    const first = plaidConnection('plaid:a', 'token-a')
    const second = plaidConnection('plaid:b', 'token-b')
    const projected: string[] = []
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [first, second], loadCheckpoint: async () => null,
      runProvider: async () => ({ nextCursor: null, observations: [] }), persistAndEnqueue: async () => 0,
      projectPending: async connection => { projected.push(connection.connectionId); if (connection === first) throw new Error('projection failed'); return { delivered: 0, failed: 0 } }, saveCheckpoint: async () => {},
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result.failures).toBe(1)
    expect(projected).toEqual(['plaid:a', 'plaid:b'])
  })

  it('persists observations, enqueues bridge work, projects it, and advances its cursor only after success', async () => {
    const calls: string[] = []
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-1', 'token')],
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

  it('does not load connections, call providers, or project when providers are disabled', async () => {
    const loadConnections = vi.fn()
    const runProvider = vi.fn()
    const projectPending = vi.fn()
    const deps: SyncRuntimeDependencies = {
      loadConnections,
      loadCheckpoint: vi.fn(),
      runProvider,
      persistAndEnqueue: vi.fn(),
      projectPending,
      saveCheckpoint: vi.fn(),
    }

    await expect(runConfiguredSyncCycle('d', deps, { providersEnabled: false, projectionEnabled: false }))
      .resolves.toEqual({ connections: 0, observations: 0, delivered: 0, failures: 0 })
    expect(loadConnections).not.toHaveBeenCalled()
    expect(runProvider).not.toHaveBeenCalled()
    expect(projectPending).not.toHaveBeenCalled()
  })

  it('persists provider observations without bridge projection when projection is disabled', async () => {
    const calls: string[] = []
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-1', 'token')],
      loadCheckpoint: async () => null,
      runProvider: async () => ({ nextCursor: 'cursor-1', observations: [] }),
      persistAndEnqueue: async () => { calls.push('persist+enqueue'); return 0 },
      projectPending: async () => { calls.push('project'); return { delivered: 0, failed: 0 } },
      saveCheckpoint: async () => { calls.push('checkpoint') },
    }

    await expect(runConfiguredSyncCycle('d', deps, { providersEnabled: true, projectionEnabled: false }))
      .resolves.toEqual({ connections: 1, observations: 0, delivered: 0, failures: 0 })
    expect(calls).toEqual(['persist+enqueue', 'checkpoint'])
  })

  it('redacts provider failures from operational logs', async () => {
    const log = vi.spyOn(console, 'error').mockImplementation(() => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('sensitive-connection', 'sensitive-token', 'sensitive-account')],
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

  it('emits bounded Plaid ingestion counters for rows it could not admit', async () => {
    const log = vi.spyOn(console, 'log').mockImplementation(() => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-1', 'token')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint: async () => {},
      runProvider: async () => ({
        nextCursor: 'cursor-1',
        observations: [],
        plaidSync: {
          added: 2, modified: 1, removed: 0,
          skippedUnadmittedAccount: 3, skippedMissingAccountMetadata: 0, skippedMissingCurrency: 0,
        },
      }),
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      await runConfiguredSyncCycle('d', deps, { providersEnabled: true, projectionEnabled: true })
      const observed = log.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_OBSERVED')
      expect(observed).toBeDefined()
      expect(JSON.parse(String(observed![1]))).toEqual({
        added: 2, modified: 1, removed: 0,
        skippedUnadmittedAccount: 3, skippedMissingAccountMetadata: 0, skippedMissingCurrency: 0,
      })
    } finally {
      log.mockRestore()
    }
  })

  it('classifies a Plaid item failure with an allowlisted code and no provider text', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-1', 'token')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint: async () => {},
      runProvider: async () => {
        throw Object.assign(new Error('plaid request failed: HTTP 400 item-1 secret-account'), { code: 'ITEM_LOGIN_REQUIRED' })
      },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result.failures).toBe(1)
      const diagnostic = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_FAILED')
      expect(diagnostic).toBeDefined()
      expect(JSON.parse(String(diagnostic![1]))).toEqual({ stage: 'item_sync', errorClass: 'Error', code: 'ITEM_LOGIN_REQUIRED' })
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/secret-account|item-1|plaid request failed/)
    } finally {
      error.mockRestore()
    }
  })

  it('does not advance a cursor when projection fails', async () => {
    let checkpointed = false
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-1', 'token')],
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

  it('Plaid item cursor: one provider call per item, shared cursor, observations for both accounts, cursor promoted only after both succeed', async () => {
    const itemConnectionId = 'plaid-item-1'
    const accountA = 'acc-checking'
    const accountB = 'acc-credit'
    const wfA = 'wf-acc-checking'
    const wfB = 'wf-acc-credit'
    const sourceA = 'plaid:item-1:acc-checking'
    const sourceB = 'plaid:item-1:acc-credit'
    const connA = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountA}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: sourceA,
      sourceResourceIncarnationIds: { [accountA]: sourceA, [accountB]: sourceB },
      accountIds: [accountA],
      wealthfolioAccountIds: { [accountA]: wfA },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    const connB = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountB}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: sourceB,
      sourceResourceIncarnationIds: { [accountA]: sourceA, [accountB]: sourceB },
      accountIds: [accountB],
      wealthfolioAccountIds: { [accountB]: wfB },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    const observationsA: ObservationOutput[] = []
    const observationsB: ObservationOutput[] = []
    let providerCalls = 0
    let savedCursor: string | null = null
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [connA, connB],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      runProvider: async (connection) => {
        providerCalls++
        // Item-level connection has combined accountIds
        const allIds = connection.accountIds
        // Build observations for both accounts with correct sourceResourceIncarnationId per account
        const obs = [...allIds].map((acctId, idx) => ({
          deploymentId: 'd',
          localOwnerId: 'owner',
          sourceResourceIncarnationId: acctId === accountA ? sourceA : sourceB,
          providerVersion: `plaid:txn:txn-${idx}`,
          semanticHash: 'a'.repeat(64),
          hashSpecificationVersion: 1,
          domain: 'cash_ledger' as const,
          observedAt: '2026-07-11T00:00:00.000Z',
          payload: { accountId: acctId, transactionId: `txn-${idx}`, amount: '1', currency: 'USD', date: '2026-07-11' },
        }))
        return { nextCursor: 'item-cursor-1', observations: obs }
      },
      persistAndEnqueue: async (connection, obs) => {
        if (connection.connectionId.includes(accountA)) observationsA.push(...obs)
        else if (connection.connectionId.includes(accountB)) observationsB.push(...obs)
        return obs.length
      },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
      saveCheckpoint: async () => {},
      savePlaidItemCheckpoint: async (_connection, cursor) => { savedCursor = cursor },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(providerCalls).toBe(1)
    expect(observationsA.length).toBe(1)
    expect(observationsB.length).toBe(1)
    expect(observationsA[0].sourceResourceIncarnationId).toBe(sourceA)
    expect(observationsB[0].sourceResourceIncarnationId).toBe(sourceB)
    expect(savedCursor).toBe('item-cursor-1')
    expect(result.observations).toBe(2)
    expect(result.failures).toBe(0)
  })

  it('holds the Plaid item cursor when current-state delivery succeeds but activity work remains unresolved', async () => {
    const connection = plaidConnection('plaid-held-cursor', 'token')
    let checkpointed = false
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [connection],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      runProvider: async () => ({ nextCursor: 'item-cursor-after-current-state', observations: [] }),
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 1, failed: 1 }),
      saveCheckpoint: async () => {},
      savePlaidItemCheckpoint: async () => { checkpointed = true },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(result.delivered).toBe(1)
    expect(result.failures).toBeGreaterThan(0)
    expect(checkpointed).toBe(false)
  })

  it('Plaid item fails closed on credential mismatch', async () => {
    const itemConnectionId = 'plaid-item-mismatch'
    const accountA = 'acc-a'
    const accountB = 'acc-b'
    const connA = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountA}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: 'src-a',
      sourceResourceIncarnationIds: { [accountA]: 'src-a', [accountB]: 'src-b' },
      accountIds: [accountA],
      wealthfolioAccountIds: { [accountA]: 'wf-a' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c1',
      clientSecret: 's1',
      accessToken: 'tok',
    }
    const connB = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountB}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: 'src-b',
      sourceResourceIncarnationIds: { [accountA]: 'src-a', [accountB]: 'src-b' },
      accountIds: [accountB],
      wealthfolioAccountIds: { [accountB]: 'wf-b' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c2',
      clientSecret: 's1',
      accessToken: 'tok',
    }
    let providerCalls = 0
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [connA, connB],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      runProvider: async () => { providerCalls++; return { nextCursor: 'c', observations: [] } },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
      savePlaidItemCheckpoint: async () => {},
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(providerCalls).toBe(0)
    expect(result.failures).toBeGreaterThan(0)
  })

  it('Plaid item fails closed on overlapping account IDs', async () => {
    const itemConnectionId = 'plaid-item-overlap'
    const accountA = 'acc-same'
    const connA = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountA}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: 'src-a',
      sourceResourceIncarnationIds: { [accountA]: 'src-a' },
      accountIds: [accountA],
      wealthfolioAccountIds: { [accountA]: 'wf-a' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    const connB = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountA}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: 'src-b',
      sourceResourceIncarnationIds: { [accountA]: 'src-b' },
      accountIds: [accountA],
      wealthfolioAccountIds: { [accountA]: 'wf-a' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    let providerCalls = 0
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [connA, connB],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      runProvider: async () => { providerCalls++; return { nextCursor: 'c', observations: [] } },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
      savePlaidItemCheckpoint: async () => {},
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(providerCalls).toBe(0)
    expect(result.failures).toBeGreaterThan(0)
  })

  it('Plaid item fails closed when second account projection fails', async () => {
    const itemConnectionId = 'plaid-item-failure'
    const accountA = 'acc-a'
    const accountB = 'acc-b'
    const sourceA = 'src-a'
    const sourceB = 'src-b'
    const connA = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountA}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: sourceA,
      sourceResourceIncarnationIds: { [accountA]: sourceA, [accountB]: sourceB },
      accountIds: [accountA],
      wealthfolioAccountIds: { [accountA]: 'wf-a' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    const connB = {
      provider: 'plaid' as const,
      connectionId: `${itemConnectionId}:${accountB}`,
      providerConnectionId: itemConnectionId,
      deploymentId: 'd',
      localOwnerId: 'owner',
      sourceResourceIncarnationId: sourceB,
      sourceResourceIncarnationIds: { [accountA]: sourceA, [accountB]: sourceB },
      accountIds: [accountB],
      wealthfolioAccountIds: { [accountB]: 'wf-b' },
      providerGeneration: 1,
      receiverAccessGeneration: 1,
      secretReference: 'TEST_SLOT',
      clientId: 'c',
      clientSecret: 's',
      accessToken: 'tok',
    }
    let saved = false
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [connA, connB],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      runProvider: async () => ({ nextCursor: 'item-cursor-2', observations: [
        { deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: sourceA, providerVersion: 'plaid:txn:1', semanticHash: 'a'.repeat(64), hashSpecificationVersion: 1, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: accountA, transactionId: '1', amount: '1', currency: 'USD', date: '2026-07-11' } },
        { deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: sourceB, providerVersion: 'plaid:txn:2', semanticHash: 'b'.repeat(64), hashSpecificationVersion: 1, domain: 'cash_ledger' as const, observedAt: '2026-07-11T00:00:00.000Z', payload: { accountId: accountB, transactionId: '2', amount: '1', currency: 'USD', date: '2026-07-11' } },
      ] }),
      persistAndEnqueue: async () => 1,
      projectPending: async (connection) => {
        if (connection.connectionId.includes(accountA)) return { delivered: 1, failed: 0 }
        return { delivered: 0, failed: 1 }
      },
      saveCheckpoint: async () => {},
      savePlaidItemCheckpoint: async () => { saved = true },
    }
    const result = await runConfiguredSyncCycle('d', deps)
    expect(saved).toBe(false)
    expect(result.failures).toBeGreaterThan(0)
  })
})
