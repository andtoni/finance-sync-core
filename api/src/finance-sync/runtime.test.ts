import { describe, expect, it, vi } from 'vitest'
import { runConfiguredSyncCycle, type SyncRuntimeDependencies } from './runtime'
import { financeSyncHealthResponse, syncCycleOutcome } from './health'
import { PLAID_ITEM_ERROR_CODES } from './provider-runtime'
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
    expect(result).toEqual({ connections: 1, observations: 1, delivered: 1, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
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
      .resolves.toEqual({ connections: 0, observations: 0, delivered: 0, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
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
      .resolves.toEqual({ connections: 1, observations: 0, delivered: 0, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
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
    expect(log).toHaveBeenCalledWith('Finance Sync connection cycle failed', expect.stringContaining('"stage":"provider_run"'))
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
          skippedUnadmittedAccount: 3, skippedMissingAccountMetadata: 0, skippedMissingCurrency: 0, skippedMissingBalance: 0, skippedAccountNotReturned: 0,
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
        skippedUnadmittedAccount: 3, skippedMissingAccountMetadata: 0, skippedMissingCurrency: 0, skippedMissingBalance: 0, skippedAccountNotReturned: 0,
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
        throw Object.assign(new Error('plaid request failed: HTTP 400 item-1 secret-account'), { code: 'INSTITUTION_DOWN', errorType: 'INSTITUTION_ERROR' })
      },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result.failures).toBe(1)
      expect(result.plaidItemConditions).toEqual([])
      const diagnostic = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_FAILED')
      expect(diagnostic).toBeDefined()
      expect(JSON.parse(String(diagnostic![1]))).toEqual({ stage: 'item_sync', errorClass: 'Error', code: 'INSTITUTION_DOWN', errorType: 'INSTITUTION_ERROR' })
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/secret-account|item-1|plaid request failed/)
    } finally {
      error.mockRestore()
    }
  })

  it('persists a balance-less Plaid account without a snapshot while its siblings keep theirs', async () => {
    const log = vi.spyOn(console, 'log').mockImplementation(() => {})
    const item = 'plaid-item-1'
    const sourceIds = { 'acc-checking': 'source-checking', 'acc-credit': 'source-credit' }
    const account = (accountId: keyof typeof sourceIds) => ({
      ...plaidConnection(`${item}:${accountId}`, 'token', accountId), providerConnectionId: item,
      sourceResourceIncarnationId: sourceIds[accountId], sourceResourceIncarnationIds: sourceIds,
      wealthfolioAccountIds: { [accountId]: `wf-${accountId}` },
    })
    const transaction = (accountId: keyof typeof sourceIds): ObservationOutput => ({
      deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: sourceIds[accountId], providerVersion: `plaid:txn:${accountId}`,
      semanticHash: 'a'.repeat(64), hashSpecificationVersion: 2, domain: 'cash_ledger', observedAt: '2026-07-10T00:00:00.000Z',
      payload: { accountId, transactionId: `txn-${accountId}`, amount: '1', currency: 'USD', date: '2026-07-10' },
    })
    const persistAndEnqueue = vi.fn(async (_connection: unknown, rows: readonly ObservationOutput[]) => rows.length)
    const savePlaidItemCheckpoint = vi.fn(async () => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [account('acc-checking'), account('acc-credit')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint,
      runProvider: async () => ({
        nextCursor: 'cursor-1',
        observations: [transaction('acc-checking'), transaction('acc-credit')],
        currentStateUnavailableAccountIds: ['acc-checking'],
        plaidSync: {
          added: 2, modified: 0, removed: 0,
          skippedUnadmittedAccount: 0, skippedMissingAccountMetadata: 0, skippedMissingCurrency: 0, skippedMissingBalance: 1, skippedAccountNotReturned: 0,
        },
      }),
      persistAndEnqueue,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result).toEqual({ connections: 2, observations: 2, delivered: 0, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
      expect(persistAndEnqueue.mock.calls).toEqual([
        [expect.objectContaining({ accountIds: ['acc-checking'] }), [transaction('acc-checking')], { skipCurrentState: true }],
        [expect.objectContaining({ accountIds: ['acc-credit'] }), [transaction('acc-credit')]],
      ])
      expect(savePlaidItemCheckpoint).toHaveBeenCalledWith(expect.objectContaining({ providerConnectionId: item }), 'cursor-1')
      const observed = log.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_OBSERVED')
      expect(JSON.parse(String(observed![1]))).toMatchObject({ added: 2, skippedMissingBalance: 1 })
      expect(JSON.stringify(log.mock.calls)).not.toMatch(/acc-checking|acc-credit|plaid-item-1/)
    } finally {
      log.mockRestore()
    }
  })

  it('reports an Item-level Plaid error as an Item condition without failing the cycle or health', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const calls: string[] = []
    let loginRequired = true
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-login', 'token-login'), plaidConnection('item-healthy', 'token-healthy', 'b')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint: async connection => { calls.push(`checkpoint:${connection.providerConnectionId}`) },
      runProvider: async connection => {
        if (connection.providerConnectionId === 'item-login' && loginRequired) {
          throw Object.assign(new Error('plaid request failed: HTTP 400 item-login secret-account'), { code: 'ITEM_LOGIN_REQUIRED', errorType: 'ITEM_ERROR' })
        }
        return { nextCursor: 'cursor-1', observations: [] }
      },
      persistAndEnqueue: async connection => { calls.push(`persist:${connection.providerConnectionId}`); return 0 },
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result).toEqual({
        connections: 2, observations: 0, delivered: 0, failures: 0,
        plaidItemConditions: [{ providerConnectionId: 'item-login', reason: 'ITEM_LOGIN_REQUIRED' }],
        plaidReissuedAccounts: [],
      })
      // Nothing is ingested or checkpointed for the Item awaiting its owner; the healthy Item is unaffected.
      expect(calls).toEqual(['persist:item-healthy', 'checkpoint:item-healthy'])
      // A cycle whose only problems are Item conditions is a healthy cycle.
      const outcome = syncCycleOutcome(result)
      expect(outcome).toEqual({ cycleReady: true, cycleFailed: false, event: 'FINANCE_SYNC_CYCLE_OK' })
      expect(financeSyncHealthResponse(true, outcome.cycleReady, result.connections, outcome.cycleFailed, 'disabled'))
        .toMatchObject({ status: 200, body: { ok: true, providerReady: true } })
      const diagnostic = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_FAILED')
      expect(JSON.parse(String(diagnostic![1]))).toEqual({ stage: 'item_sync', errorClass: 'Error', code: 'ITEM_LOGIN_REQUIRED', errorType: 'ITEM_ERROR' })
      const attention = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_NEEDS_ATTENTION')
      expect(JSON.parse(String(attention![1]))).toEqual({ reason: 'ITEM_LOGIN_REQUIRED' })
      expect(error.mock.calls.some(call => call[0] === 'Finance Sync connection cycle failed')).toBe(false)
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/secret-account|item-login|plaid request failed/)

      // A later successful sync clears the condition.
      loginRequired = false
      calls.length = 0
      const recovered = await runConfiguredSyncCycle('d', deps)
      expect(recovered).toEqual({ connections: 2, observations: 0, delivered: 0, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
      expect(calls).toContain('checkpoint:item-login')
    } finally {
      error.mockRestore()
    }
  })

  it('treats a provider outage as the provider\'s condition, not a failed cycle', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const outage = (fields: Record<string, unknown>, name = 'Error') => Object.assign(new Error('investec request failed: HTTP 504 secret-account'), fields, { name })
    const cycle = (failure: unknown) => runConfiguredSyncCycle('d', {
      loadConnections: async () => [investecConnection('provider:a', 'a', '00000000-0000-0000-0000-000000000010')],
      loadCheckpoint: async () => null,
      runProvider: async () => { throw failure },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => { throw new Error('checkpoint must not move') },
    })
    try {
      for (const failure of [
        outage({ status: 504 }), outage({ status: 500 }), outage({ status: 429 }), outage({}, 'TimeoutError'),
        Object.assign(new TypeError('fetch failed'), { cause: { code: 'ECONNREFUSED' } }),
      ]) {
        const result = await cycle(failure)
        expect(result.failures).toBe(0)
        expect(syncCycleOutcome(result)).toMatchObject({ cycleFailed: false, event: 'FINANCE_SYNC_CYCLE_OK' })
      }
      const logged = error.mock.calls.filter(call => call[0] === 'FINANCE_SYNC_PROVIDER_UNAVAILABLE')
      expect(logged).toHaveLength(5)
      expect(JSON.parse(String(logged[0][1]))).toMatchObject({ provider: 'investec', status: 504 })
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/secret-account|request failed/)

      // A rejected request or a fault in this service is still a failed cycle.
      for (const failure of [outage({ status: 401 }), outage({ status: 400 }), new Error('invalid Investec accounts response')]) {
        const result = await cycle(failure)
        expect(result.failures).toBe(1)
        expect(syncCycleOutcome(result)).toMatchObject({ cycleFailed: true })
      }
    } finally {
      error.mockRestore()
    }
  })

  describe('a Plaid account re-issued under a new account id', () => {
    const identity = { mask: '0042', name: 'Everyday Checking', currency: 'USD' }
    const dead = {
      ...plaidConnection('item-dead', 'token-dead', 'acc-old'), sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000002',
      accountIdentities: { 'acc-old': identity },
    }
    const live = plaidConnection('item-live', 'token-live', 'acc-live')
    const observation = (transactionId: string, date: string, amount: string, name: string) => ({
      deploymentId: 'd', localOwnerId: dead.localOwnerId, sourceResourceIncarnationId: dead.sourceResourceIncarnationId,
      providerVersion: `plaid:txn:${transactionId}`, semanticHash: transactionId, hashSpecificationVersion: 1, domain: 'cash_ledger' as const,
      payload: { transactionId, accountId: 'acc-old', date, amount, name }, observedAt: `${date}T00:00:00.000Z`,
    })
    const cycle = (itemsReturningIt: readonly string[], history?: SyncRuntimeDependencies['loadPlaidReissuedHistory']) => {
      const persisted: Array<{ connection: string; transactions: unknown[] }> = []
      const checkpoints: string[] = []
      const candidates: unknown[] = []
      const deps: SyncRuntimeDependencies = {
        loadConnections: async () => [dead, live, plaidConnection('item-other', 'token-other', 'acc-other')],
        loadCheckpoint: async () => null,
        loadPlaidItemCheckpoint: async () => null,
        savePlaidItemCheckpoint: async connection => { checkpoints.push(String(connection.providerConnectionId)) },
        runProvider: async connection => {
          if (connection.providerConnectionId === 'item-dead') {
            throw Object.assign(new Error('plaid request failed'), { code: 'NO_ACCOUNTS', errorType: 'ITEM_ERROR' })
          }
          candidates.push((connection as { reissueCandidates?: unknown }).reissueCandidates)
          if (!itemsReturningIt.includes(String(connection.providerConnectionId))) return { nextCursor: 'cursor-1', observations: [] }
          return {
            nextCursor: 'cursor-1', accountsReissued: { 'acc-old': 'acc-new' },
            observations: [
              observation('older', '2026-08-01', '5', 'OLD'), observation('held', '2026-09-05', '12.5', 'COFFEE'),
              observation('held-twice', '2026-09-05', '12.5', 'COFFEE'), observation('new', '2026-09-20', '7', 'LUNCH'),
            ],
          }
        },
        loadPlaidReissuedHistory: history,
        persistAndEnqueue: async (connection, observations) => {
          persisted.push({ connection: connection.connectionId, transactions: observations.map(item => item.payload.transactionId) })
          return observations.length
        },
        projectPending: async () => ({ delivered: 0, failed: 0 }),
        saveCheckpoint: async () => {},
      }
      return runConfiguredSyncCycle('d', deps).then(result => ({ result, persisted, checkpoints, candidates }))
    }

    it('is ingested from the one Item that returns it, without the history already held', async () => {
      const error = vi.spyOn(console, 'error').mockImplementation(() => {})
      try {
        const { result, persisted, checkpoints, candidates } = await cycle(['item-live'], async (connection, returnedAccountId) => {
          expect([connection.connectionId, returnedAccountId]).toEqual([dead.connectionId, 'acc-new'])
          return [{ date: '2026-09-05', amount: '12.5', name: 'COFFEE' }, { date: '2026-09-01', amount: '3', name: 'TEA' }]
        })
        // Every Item is offered the stored identity of the owner's admitted accounts.
        expect(candidates[0]).toEqual({ 'acc-old': { ...identity, sourceResourceIncarnationId: dead.sourceResourceIncarnationId } })
        // Before the overlap window: dropped. One held twin is dropped once; the second identical row is new.
        expect(persisted.find(row => row.connection === dead.connectionId)?.transactions).toEqual(['held-twice', 'new'])
        // The dead Item needs nothing from its owner once its account is ingested elsewhere.
        expect(result.plaidItemConditions).toEqual([])
        // The owner can move the admission to where it is returned now.
        expect(result.plaidReissuedAccounts).toEqual([{
          wealthfolioAccountId: 'wf-a',
          from: { connectionId: 'item-dead', providerAccountId: 'acc-old' },
          to: { connectionId: 'item-live', providerAccountId: 'acc-new' },
        }])
        expect(result.failures).toBe(0)
        expect(checkpoints).toContain('item-live')
      } finally {
        error.mockRestore()
      }
    })

    it('is left alone when two Items return it, and the dead Item keeps its condition', async () => {
      const error = vi.spyOn(console, 'error').mockImplementation(() => {})
      try {
        const { result, persisted } = await cycle(['item-live', 'item-other'], async () => [])
        expect(persisted.some(row => row.connection === dead.connectionId)).toBe(false)
        expect(result.plaidItemConditions).toEqual([{ providerConnectionId: 'item-dead', reason: 'NO_ACCOUNTS' }])
        expect(result.plaidReissuedAccounts).toEqual([])
        expect(result.failures).toBe(0)
      } finally {
        error.mockRestore()
      }
    })

    it('holds the Item cursor when the held history cannot be read', async () => {
      const error = vi.spyOn(console, 'error').mockImplementation(() => {})
      try {
        const { result, persisted, checkpoints } = await cycle(['item-live'])
        expect(persisted.some(row => row.connection === dead.connectionId)).toBe(false)
        expect(checkpoints).not.toContain('item-live')
        expect(result.failures).toBeGreaterThan(0)
        expect(result.plaidItemConditions).toEqual([{ providerConnectionId: 'item-dead', reason: 'NO_ACCOUNTS' }])
      } finally {
        error.mockRestore()
      }
    })

    it('filters a replayed stream of a moved admission by the history it holds under its earlier account id', async () => {
      const moved = { ...plaidConnection('item-live', 'token-live', 'acc-new'), sourceResourceIncarnationId: dead.sourceResourceIncarnationId }
      const persisted: unknown[][] = []
      const asked: unknown[] = []
      const deps: SyncRuntimeDependencies = {
        loadConnections: async () => [moved],
        loadCheckpoint: async () => null,
        loadPlaidItemCheckpoint: async () => null,
        savePlaidItemCheckpoint: async () => {},
        // The Item is replayed from its origin: the whole history arrives again under the account's own id.
        runProvider: async () => ({
          nextCursor: 'cursor-1',
          observations: [
            observation('older', '2026-08-01', '5', 'OLD'), observation('held', '2026-09-05', '12.5', 'COFFEE'),
            observation('new', '2026-09-20', '7', 'LUNCH'),
          ],
        }),
        loadPlaidReissuedHistory: async (connection, returnedAccountId) => {
          asked.push([connection.connectionId, returnedAccountId])
          return [{ date: '2026-09-05', amount: '12.5', name: 'COFFEE' }]
        },
        persistAndEnqueue: async (_connection, observations) => {
          persisted.push(observations.map(item => item.payload.transactionId))
          return observations.length
        },
        projectPending: async () => ({ delivered: 0, failed: 0 }),
        saveCheckpoint: async () => {},
      }
      const result = await runConfiguredSyncCycle('d', deps)
      expect(asked).toEqual([['item-live', 'acc-new']])
      expect(persisted).toEqual([['new']])
      expect(result).toMatchObject({ failures: 0, plaidReissuedAccounts: [] })
    })

    it('does not read held history for a cycle that brought no transactions', async () => {
      const history = vi.fn(async () => [])
      const deps: SyncRuntimeDependencies = {
        loadConnections: async () => [live],
        loadCheckpoint: async () => null,
        loadPlaidItemCheckpoint: async () => null,
        savePlaidItemCheckpoint: async () => {},
        runProvider: async () => ({ nextCursor: 'cursor-1', observations: [] }),
        loadPlaidReissuedHistory: history,
        persistAndEnqueue: async () => 0,
        projectPending: async () => ({ delivered: 0, failed: 0 }),
        saveCheckpoint: async () => {},
      }
      await expect(runConfiguredSyncCycle('d', deps)).resolves.toMatchObject({ failures: 0 })
      expect(history).not.toHaveBeenCalled()
    })
  })

  const plaidItemCycle = (failure: unknown, persistFailure?: unknown) => runConfiguredSyncCycle('d', {
    loadConnections: async () => [plaidConnection('item-1', 'token')],
    loadCheckpoint: async () => null,
    loadPlaidItemCheckpoint: async () => null,
    savePlaidItemCheckpoint: async () => {},
    runProvider: async () => {
      if (failure !== undefined) throw failure
      return { nextCursor: 'cursor-1', observations: [] }
    },
    persistAndEnqueue: async () => {
      if (persistFailure !== undefined) throw persistFailure
      return 0
    },
    projectPending: async () => ({ delivered: 0, failed: 0 }),
    saveCheckpoint: async () => {},
  })
  const plaidFailure = (fields: Record<string, string>) => Object.assign(new Error('plaid request failed: HTTP 400 secret-account'), fields)

  it('treats every documented ITEM_ERROR answer to the Item request as an Item condition with its fixed reason', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      const reasons = [...PLAID_ITEM_ERROR_CODES].filter(code => code !== 'PRODUCT_NOT_READY')
      expect(reasons).toHaveLength(26)
      for (const code of reasons) {
        const result = await plaidItemCycle(plaidFailure({ code, errorType: 'ITEM_ERROR' }))
        expect(result.failures).toBe(0)
        expect(result.plaidItemConditions).toEqual([{ providerConnectionId: 'item-1', reason: code }])
        expect(syncCycleOutcome(result).event).toBe('FINANCE_SYNC_CYCLE_OK')
      }
      // Plaid sent no code, or one outside the fixed list: still this Item's condition, with no reason to show.
      for (const fields of [{ errorType: 'ITEM_ERROR' }, { code: 'INSTITUTION_DOWN', errorType: 'ITEM_ERROR' }] as Record<string, string>[]) {
        const result = await plaidItemCycle(plaidFailure(fields))
        expect(result.failures).toBe(0)
        expect(result.plaidItemConditions).toEqual([{ providerConnectionId: 'item-1' }])
      }
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/secret-account|item-1|plaid request failed/)
    } finally {
      error.mockRestore()
    }
  })

  it('still fails the cycle and health for every failure that is not an Item condition', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    try {
      for (const fields of [
        { code: 'INTERNAL_SERVER_ERROR', errorType: 'API_ERROR' },
        { code: 'RATE_LIMIT_EXCEEDED', errorType: 'RATE_LIMIT_EXCEEDED' },
        { code: 'INSTITUTION_DOWN', errorType: 'INSTITUTION_ERROR' },
        { code: 'INVALID_ACCESS_TOKEN', errorType: 'INVALID_INPUT' },
        { code: 'INVALID_REQUEST', errorType: 'INVALID_REQUEST' },
        // A documented Item code without Plaid's ITEM_ERROR type is unclassified.
        { code: 'ITEM_LOGIN_REQUIRED' },
        { code: 'ITEM_LOGIN_REQUIRED', errorType: 'API_ERROR' },
        // Plaid is still preparing data: transient, nothing for the owner to do.
        { code: 'PRODUCT_NOT_READY', errorType: 'ITEM_ERROR' },
        {},
      ] as Record<string, string>[]) {
        const result = await plaidItemCycle(plaidFailure(fields))
        expect(result.failures).toBe(1)
        expect(result.plaidItemConditions).toEqual([])
        const outcome = syncCycleOutcome(result)
        expect(outcome.event).toBe('FINANCE_SYNC_CYCLE_FAILED')
        expect(financeSyncHealthResponse(true, outcome.cycleReady, result.connections, outcome.cycleFailed, 'disabled').status).toBe(503)
      }
      for (const failure of [new Error('INCOMPLETE_ACCOUNT_BALANCE_CURRENCY'), new TypeError('fetch failed'), 'not an error']) {
        const result = await plaidItemCycle(failure)
        expect(result.failures).toBe(1)
        expect(result.plaidItemConditions).toEqual([])
      }

      // The same error outside the Item's provider request is not an Item condition.
      const persisted = await plaidItemCycle(undefined, plaidFailure({ code: 'ITEM_LOGIN_REQUIRED', errorType: 'ITEM_ERROR' }))
      expect(persisted.failures).toBeGreaterThan(0)
      expect(persisted.plaidItemConditions).toEqual([])
      expect(error.mock.calls.some(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_NEEDS_ATTENTION')).toBe(false)
    } finally {
      error.mockRestore()
    }
  })

  it('fails the cycle when one Item has an Item condition and another has a service failure', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [plaidConnection('item-owner', 'token-owner'), plaidConnection('item-outage', 'token-outage', 'b')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint: async () => {},
      runProvider: async connection => {
        throw connection.providerConnectionId === 'item-owner'
          ? plaidFailure({ code: 'NO_ACCOUNTS', errorType: 'ITEM_ERROR' })
          : plaidFailure({ code: 'INTERNAL_SERVER_ERROR', errorType: 'API_ERROR' })
      },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result).toEqual({
        connections: 2, observations: 0, delivered: 0, failures: 1,
        plaidItemConditions: [{ providerConnectionId: 'item-owner', reason: 'NO_ACCOUNTS' }],
        plaidReissuedAccounts: [],
      })
      const outcome = syncCycleOutcome(result)
      expect(outcome).toEqual({ cycleReady: false, cycleFailed: true, event: 'FINANCE_SYNC_CYCLE_FAILED' })
      expect(financeSyncHealthResponse(true, outcome.cycleReady, result.connections, outcome.cycleFailed, 'disabled'))
        .toMatchObject({ status: 503, body: { ok: false, providerReady: false } })
      expect(error.mock.calls.filter(call => call[0] === 'Finance Sync connection cycle failed')).toHaveLength(1)
    } finally {
      error.mockRestore()
    }
  })

  it('writes nothing for an account Plaid did not return, keeps its siblings syncing, and holds the Item cursor', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const log = vi.spyOn(console, 'log').mockImplementation(() => {})
    const item = 'plaid-item-1'
    const sourceIds = { 'acc-checking': 'source-checking', 'acc-credit': 'source-credit' }
    const account = (accountId: keyof typeof sourceIds) => ({
      ...plaidConnection(`${item}:${accountId}`, 'token', accountId), providerConnectionId: item,
      sourceResourceIncarnationId: sourceIds[accountId], sourceResourceIncarnationIds: sourceIds,
      wealthfolioAccountIds: { [accountId]: `wf-${accountId}` },
    })
    const observation = (accountId: keyof typeof sourceIds, providerVersion: string): ObservationOutput => ({
      deploymentId: 'd', localOwnerId: 'owner', sourceResourceIncarnationId: sourceIds[accountId], providerVersion,
      semanticHash: 'a'.repeat(64), hashSpecificationVersion: 2, domain: 'cash_ledger', observedAt: '2026-07-10T00:00:00.000Z',
      payload: providerVersion.startsWith('plaid:removed:') ? { transactionId: 'removed-1', isRemoved: true } : { accountId, transactionId: `txn-${accountId}`, amount: '1', currency: 'USD', date: '2026-07-10' },
    })
    let missing = true
    const persistAndEnqueue = vi.fn(async (_connection: unknown, rows: readonly ObservationOutput[]) => rows.length)
    const projectPending = vi.fn(async () => ({ delivered: 0, failed: 0 }))
    const savePlaidItemCheckpoint = vi.fn(async () => {})
    const deps: SyncRuntimeDependencies = {
      loadConnections: async () => [account('acc-checking'), account('acc-credit')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => 'cursor-0',
      savePlaidItemCheckpoint,
      runProvider: async () => ({
        nextCursor: missing ? 'cursor-0' : 'cursor-1',
        // A removal tombstone is staged for every admitted resource, including the missing account's.
        observations: [observation('acc-checking', 'plaid:removed:removed-1'), observation('acc-credit', 'plaid:txn:acc-credit'),
          ...(missing ? [] : [observation('acc-checking', 'plaid:txn:acc-checking')])],
        accountsNotReturned: missing ? ['acc-checking'] : [],
      }),
      persistAndEnqueue,
      projectPending,
      saveCheckpoint: async () => {},
    }

    try {
      const result = await runConfiguredSyncCycle('d', deps)
      expect(result).toEqual({
        connections: 2, observations: 1, delivered: 0, failures: 0,
        plaidItemConditions: [{ providerConnectionId: item, reason: 'ACCOUNT_NOT_RETURNED' }],
        plaidReissuedAccounts: [],
      })
      expect(persistAndEnqueue.mock.calls).toEqual([
        [expect.objectContaining({ accountIds: ['acc-credit'] }), [observation('acc-credit', 'plaid:txn:acc-credit')]],
      ])
      expect(projectPending.mock.calls).toEqual([[expect.objectContaining({ accountIds: ['acc-credit'] })]])
      expect(savePlaidItemCheckpoint).not.toHaveBeenCalled()
      expect(syncCycleOutcome(result).event).toBe('FINANCE_SYNC_CYCLE_OK')
      const attention = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_NEEDS_ATTENTION')
      expect(JSON.parse(String(attention![1]))).toEqual({ reason: 'ACCOUNT_NOT_RETURNED' })
      expect(error.mock.calls.some(call => call[0] === 'Finance Sync connection cycle failed' || call[0] === 'Finance Sync Plaid item cursor held')).toBe(false)
      expect(JSON.stringify([...error.mock.calls, ...log.mock.calls])).not.toMatch(/acc-checking|acc-credit|plaid-item-1/)

      // Once Plaid returns the account, the held cursor's stream is ingested for it and the cursor moves.
      missing = false
      persistAndEnqueue.mockClear()
      const recovered = await runConfiguredSyncCycle('d', deps)
      expect(recovered).toEqual({ connections: 2, observations: 3, delivered: 0, failures: 0, plaidItemConditions: [], plaidReissuedAccounts: [] })
      expect(persistAndEnqueue.mock.calls.map(call => (call[0] as { accountIds: string[] }).accountIds[0])).toEqual(['acc-checking', 'acc-credit'])
      expect(savePlaidItemCheckpoint).toHaveBeenCalledWith(expect.objectContaining({ providerConnectionId: item }), 'cursor-1')
    } finally {
      error.mockRestore()
      log.mockRestore()
    }
  })

  it('still counts a failure when a sibling of a missing account cannot be persisted', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const item = 'plaid-item-1'
    const account = (accountId: string) => ({ ...plaidConnection(`${item}:${accountId}`, 'token', accountId), providerConnectionId: item })
    try {
      const result = await runConfiguredSyncCycle('d', {
        loadConnections: async () => [account('acc-checking'), account('acc-credit')],
        loadCheckpoint: async () => null,
        loadPlaidItemCheckpoint: async () => null,
        savePlaidItemCheckpoint: async () => { throw new Error('the cursor must stay held') },
        runProvider: async () => ({ nextCursor: null, observations: [], accountsNotReturned: ['acc-checking'] }),
        persistAndEnqueue: async () => { throw new Error('incomplete current-state snapshot: balance for acc-credit') },
        projectPending: async () => ({ delivered: 0, failed: 0 }),
        saveCheckpoint: async () => {},
      })
      expect(result.failures).toBe(2)
      expect(result.plaidItemConditions).toEqual([{ providerConnectionId: item, reason: 'ACCOUNT_NOT_RETURNED' }])
      expect(syncCycleOutcome(result).event).toBe('FINANCE_SYNC_CYCLE_FAILED')
    } finally {
      error.mockRestore()
    }
  })

  it('emits an internal failure detail only for Finance Sync fixed codes', async () => {
    const error = vi.spyOn(console, 'error').mockImplementation(() => {})
    const failing = (message: string) => ({
      loadConnections: async () => [plaidConnection('item-1', 'token')],
      loadCheckpoint: async () => null,
      loadPlaidItemCheckpoint: async () => null,
      savePlaidItemCheckpoint: async () => {},
      runProvider: async () => { throw new Error(message) },
      persistAndEnqueue: async () => 0,
      projectPending: async () => ({ delivered: 0, failed: 0 }),
      saveCheckpoint: async () => {},
    }) as SyncRuntimeDependencies

    try {
      await runConfiguredSyncCycle('d', failing('INCOMPLETE_ACCOUNT_BALANCE_CURRENCY'))
      const internal = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_FAILED')
      expect(JSON.parse(String(internal![1]))).toEqual({ stage: 'item_sync', errorClass: 'Error', detail: 'INCOMPLETE_ACCOUNT_BALANCE_CURRENCY' })

      error.mockClear()
      await runConfiguredSyncCycle('d', failing('provider payload sensitive-token'))
      const provider = error.mock.calls.find(call => call[0] === 'FINANCE_SYNC_PLAID_ITEM_FAILED')
      expect(JSON.parse(String(provider![1]))).toEqual({ stage: 'item_sync', errorClass: 'Error' })
      expect(JSON.stringify(error.mock.calls)).not.toMatch(/provider payload|sensitive-token/)
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
