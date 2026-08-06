import { describe, expect, it } from 'vitest'
import { assertBridgeConfiguration, createRuntimeDependencies } from './runtime-deps'
import { investecPrimeSaverIdentityKey } from './probes/investec/investec-adapter'
import type { BridgeDisclosureOperation } from './projection/bridge-client'
import type { RuntimeConnection } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const CONNECTION: RuntimeConnection = {
  provider: 'plaid',
  connectionId: 'item-1',
  deploymentId: 'dep-1',
  clientId: 'client-test',
  clientSecret: 'secret-test',
  accessToken: 'access-test',
  localOwnerId: 'urn:wealthfolio:test#owner',
  sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  accountIds: ['acc-1'],
  wealthfolioAccountIds: { 'acc-1': 'wf-acc-1' },
  providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'TEST_SLOT',
}
const { accessToken: CONNECTION_ACCESS_TOKEN, clientId: CONNECTION_CLIENT_ID, clientSecret: CONNECTION_CLIENT_SECRET, ...CONNECTION_METADATA } = CONNECTION

function env(overrides: Record<string, string | undefined> = {}) {
  return {
    FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([CONNECTION_METADATA]),
    FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ 'item-1': {
      accessToken: CONNECTION_ACCESS_TOKEN, clientId: CONNECTION_CLIENT_ID, clientSecret: CONNECTION_CLIENT_SECRET,
    } }),
    FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com',
    FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge',
    FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
    FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test',
    FINANCE_SYNC_BRIDGE_TOKEN: 'token-test',
    ...overrides,
  }
}

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v3;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}

function receipt(op: BridgeDisclosureOperation) {
  const operation = op.kind === 'ActivityUpsert' ? 'activity_upsert' : op.kind === 'ActivityDelete' ? 'activity_delete' : op.kind === 'CurrentStateUpsert' ? 'current_state_upsert' : 'projection_fence'
  const successor = op.lifecycleSuccessorId ?? null
  const semanticHash = op.kind === 'CurrentStateUpsert' ? op.semanticHash ?? null : null
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
    semanticHash,
    receiptBinding: binding([
      op.deploymentId, op.subject, op.idempotencyKey, String(op.accessGeneration), operation,
      op.resourceId, op.accountId, successor === null ? '0' : '1', successor ?? '', 'accepted',
      semanticHash === null ? '0' : '1', semanticHash ?? '',
    ]),
  }
}

function outboxSql(row: { id: string; payload: BridgeDisclosureOperation; attempts: number }, priorPayloads: unknown[] = []) {
  let state: 'pending' | 'uncertain' | 'delivered' | 'dead_letter' = 'pending'
  let attempts = row.attempts
  const queries: string[] = []
  const enqueued: BridgeDisclosureOperation[] = []
  const rawPayloads: unknown[] = []
  const sql = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join('?')
    if (/INSERT\s+INTO\s+raw_observations/i.test(text)) {
      const objects = values.filter(value => value !== null && typeof value === 'object')
      if (objects.length > 1) rawPayloads.push(objects.at(-1))
    }
    if (/INSERT\s+INTO\s+projection_outbox/i.test(text)) {
      const payload = values.find(value => value !== null && typeof value === 'object' && 'kind' in value) as BridgeDisclosureOperation | undefined
      if (payload) enqueued.push(payload)
    }
    queries.push(text)
    if (/FROM\s+raw_observations/i.test(text) && /provider_version/i.test(text) && /semantic_hash/i.test(text)) {
      const lifecyclePayloads = /providerLifecycleTransactionId/.test(text)
        ? priorPayloads.filter(payload => {
          const value = payload && typeof payload === 'object' ? payload as Record<string, unknown> : {}
          const currentId = values.at(-1)
          const linkedId = values.at(-2)
          return value.transactionId === linkedId || value.providerLifecycleTransactionId === currentId
        })
        : priorPayloads
      return lifecyclePayloads.map((payload, index) => {
        const value = payload && typeof payload === 'object' ? payload as Record<string, unknown> : {}
        return {
          payload,
          provider_version: typeof value.providerVersion === 'string' ? value.providerVersion : `investec:txn:prior-${index}`,
          semantic_hash: typeof value.semanticHash === 'string' ? value.semanticHash : `prior-hash-${index}`,
          observed_at: typeof value.observedAt === 'string' ? value.observedAt : '2026-08-06T00:00:00.000Z',
        }
      })
    }
    if (/SELECT\s+payload\s+FROM\s+raw_observations/i.test(text)) {
      return priorPayloads.map(payload => ({ payload }))
    }
    if (/count\(\*\)/i.test(text)) {
      const count = /state\s*=\s*'dead_letter'/i.test(text)
        ? Number(state === 'dead_letter')
        : Number(state === 'pending' || state === 'dead_letter')
      return [{ count }]
    }
    if (/SELECT\s+id,\s*payload,\s*attempts/i.test(text)) {
      return state === 'pending' || state === 'uncertain' ? [{ ...row, attempts, state }] : []
    }
    if (/UPDATE\s+projection_outbox/i.test(text)) {
      attempts += 1
      if (/state = 'delivered'/i.test(text)) state = 'delivered'
      if (/state = 'uncertain'/i.test(text)) state = 'uncertain'
      if (/state = 'dead_letter'/i.test(text)) state = 'dead_letter'
    }
    return []
  }) as unknown as FinanceSyncSql
  ;(sql as unknown as { begin: (fn: (tx: FinanceSyncSql) => Promise<unknown>) => Promise<unknown> }).begin = fn => fn(sql)
  ;(sql as unknown as { json: (value: unknown) => unknown }).json = value => value
  return { sql, state: () => state, attempts: () => attempts, queries, enqueued, rawPayloads }
}

describe('createRuntimeDependencies startup validation', () => {
  it('allows an explicitly empty connection set', () => {
    expect(createRuntimeDependencies({} as FinanceSyncSql, 'dep-1', { FINANCE_SYNC_CONNECTIONS_JSON: '[]' })).toBeDefined()
  })

  it('requires complete bridge configuration before provider onboarding', () => {
    expect(() => assertBridgeConfiguration({})).toThrow('BRIDGE_ALLOWED_HOSTS')
    expect(() => assertBridgeConfiguration({
      FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge',
      FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test',
    })).toThrow('BRIDGE_TOKEN')
  })

  it('keeps fixture bridge delivery on loopback', () => {
    const fixtureEnv = {
      FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge',
      FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'https://bridge.test/receipts',
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test',
      FINANCE_SYNC_BRIDGE_TOKEN: 'fixture-token',
    }
    expect(() => assertBridgeConfiguration(fixtureEnv, true)).toThrow('loopback')
    expect(() => assertBridgeConfiguration({
      ...fixtureEnv,
      FINANCE_SYNC_BRIDGE_URL: 'http://127.0.0.1:8088/bridge',
      FINANCE_SYNC_BRIDGE_RECEIPT_URL: 'http://127.0.0.1:8088/receipts',
      FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: '127.0.0.1',
    }, true)).not.toThrow()
  })

  it.each([
    ['non-HTTPS Plaid URL', { FINANCE_SYNC_PLAID_URL: 'http://sandbox.plaid.com' }, /HTTPS/],
    ['unapproved Plaid host', { FINANCE_SYNC_PLAID_URL: 'https://plaid.example.test' }, /not allowed/],
    ['missing bridge allowlist', { FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: undefined }, /ALLOWED_HOSTS/],
    ['missing bridge token', { FINANCE_SYNC_BRIDGE_TOKEN: undefined }, /BRIDGE_TOKEN/],
  ] as const)('fails closed for %s', (_name, patch, message) => {
    expect(() => createRuntimeDependencies({} as FinanceSyncSql, 'dep-1', env(patch))).toThrow(message)
  })

  it('rejects non-qualified subjects and incomplete account mappings', () => {
    const badSubject = { ...CONNECTION_METADATA, localOwnerId: 'owner' }
    expect(() => createRuntimeDependencies({} as FinanceSyncSql, 'dep-1', env({
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([badSubject]),
    }))).toThrow('issuer-qualified')
    const badMapping = { ...CONNECTION_METADATA, wealthfolioAccountIds: {} }
    expect(() => createRuntimeDependencies({} as FinanceSyncSql, 'dep-1', env({
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([badMapping]),
    }))).toThrow('every provider account')
  })
})

describe('checkpoint generation identity', () => {
  it('uses provider generation in checkpoint reads, writes, and conflict identity', async () => {
    const queries: string[] = []
    const sql = (async (strings: TemplateStringsArray) => {
      queries.push(strings.join('?'))
      return []
    }) as unknown as FinanceSyncSql
    ;(sql as unknown as { begin: (fn: (tx: FinanceSyncSql) => Promise<unknown>) => Promise<unknown> }).begin = fn => fn(sql)
    const deps = createRuntimeDependencies(sql, 'dep-1', env())
    await expect(deps.loadCheckpoint(CONNECTION)).resolves.toBeNull()
    await deps.saveCheckpoint(CONNECTION, 'cursor-1')
    expect(queries.find(query => /SELECT cursor FROM sync_checkpoints/i.test(query))).toMatch(/provider_generation/i)
    expect(queries.find(query => /INSERT INTO sync_checkpoints/i.test(query)))
      .toMatch(/ON CONFLICT \(deployment_id, connection_id, provider_generation\)/i)
  })
})

describe('persistAndEnqueue current-state contract', () => {
  it('enqueues transaction activity unchanged plus one atomic complete current-state payload', async () => {
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000099', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 })
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env())
    const common = {
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      hashSpecificationVersion: 1, observedAt: '2026-07-18T00:00:00.000Z',
    } as const
    await deps.persistAndEnqueue(CONNECTION, [
      { ...common, providerVersion: 'plaid:txn:t-1', semanticHash: 'a'.repeat(64), domain: 'cash_ledger', payload: { transactionId: 't-1', accountId: 'acc-1', amount: '5', currency: 'USD', date: '2026-07-18' } },
      { ...common, providerVersion: 'plaid:balance:acc-1', semanticHash: 'b'.repeat(64), domain: 'cash_ledger', payload: { accountId: 'acc-1', currentBalance: '95', currency: 'USD', asOfDate: '2026-07-18' } },
    ])
    expect(fixture.enqueued.map(operation => operation.kind)).toEqual(['ActivityUpsert', 'CurrentStateUpsert'])
    expect(fixture.enqueued[1].currentState).toEqual({
      accountId: 'wf-acc-1', providerConnectionId: 'item-1', providerIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerGeneration: 1, observedAt: '2026-07-18T00:00:00.000Z', revision: 1784332800000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '95', liabilityBalance: '0', positions: [],
    })
    expect(fixture.enqueued[1].semanticHash).toMatch(/^[a-f0-9]{64}$/)
    expect(fixture.queries.filter(query => /INSERT\s+INTO\s+projection_outbox/i.test(query))
      .every(query => /provider_generation/i.test(query))).toBe(true)
  })
})

describe('PrimeSaver identity supersession projection', () => {
  it('attaches fallback legacy IDs when the provider date changes', async () => {
    const priorPayload = {
      transactionId: 'synthetic-legacy-interest', accountId: 'acc-1', amount: '107.49', currency: 'ZAR', date: '2026-06-30', transactionDate: '2026-06-30',
      transactionType: 'CREDIT', description: 'Interest Value Date 01Jul26', reference: null, cardNumber: null,
    }
    const stableKey = investecPrimeSaverIdentityKey({ ...priorPayload, date: '2026-08-04' })
    expect(stableKey).toBeTruthy()
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000097', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [priorPayload])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-07-01', toDate: '2026-07-31',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-interest-current', semanticHash: 'd'.repeat(64),
      hashSpecificationVersion: 2, observedAt: '2026-07-11T00:00:00.000Z', domain: 'cash_ledger',
      payload: {
        ...priorPayload, date: '2026-08-04', transactionId: 'synthetic-v2-interest-current', primeSaverIdentityKey: stableKey,
      },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toEqual([
      'dep-1:00000000-0000-0000-0000-000000000001:synthetic-legacy-interest',
    ])
  })

  it('attaches legacy resource IDs when a stable identity replaces old provider IDs', async () => {
    const priorPayload = {
      transactionId: 'synthetic-legacy', accountId: 'acc-1', amount: '5', currency: 'ZAR', date: '2026-07-11', transactionDate: '2026-07-11',
      transactionType: 'CREDIT', description: 'Internal Transaction MBP000123', reference: null, cardNumber: null,
    }
    const stableKey = investecPrimeSaverIdentityKey(priorPayload)
    expect(stableKey).toBeTruthy()
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000098', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [priorPayload])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-07-01', toDate: '2026-07-31',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-current', semanticHash: 'c'.repeat(64),
      hashSpecificationVersion: 2, observedAt: '2026-07-11T00:00:00.000Z', domain: 'cash_ledger',
      payload: {
        ...priorPayload, transactionId: 'synthetic-v2-current', primeSaverIdentityKey: stableKey,
      },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toEqual([
      'dep-1:00000000-0000-0000-0000-000000000001:synthetic-legacy',
    ])
  })
})

describe('Investec card and poll lifecycle correlation', () => {
  const INVESTEC_CONNECTION: RuntimeConnection = {
    ...CONNECTION,
    provider: 'investec',
    connectionId: 'investec-connection',
    accountIds: ['acc-1'],
    wealthfolioAccountIds: { 'acc-1': 'wf-acc-1' },
    apiKey: 'api-key',
    eligibleProductNames: ['Private Bank Account'],
    fromDate: '2026-08-01',
    toDate: '2026-08-06',
  }

  const settledPayload = {
    transactionId: 'settled-1', providerVersion: 'investec:txn:settled-1', semanticHash: 'settled-hash',
    observedAt: '2026-08-06T10:00:00.000Z', accountId: 'acc-1', amount: '-193.91', currency: 'ZAR',
    date: '2026-08-06', postingDate: '2026-08-06', settlementDate: '2026-08-06',
    description: 'SYNTHETIC SERVICE FEE', reference: 'ref-1', type: 'DEBIT',
    status: 'posted', providerLifecycleTransactionId: 'card-1',
  }
  const cardPayload = {
    transactionId: 'card-1', providerVersion: 'investec:card:card-1', semanticHash: 'card-hash',
    observedAt: '2026-08-05T10:00:00.000Z', accountId: 'acc-1', amount: '-11.50', currency: 'USD',
    date: '2026-08-05', description: 'SYNTHETIC SERVICE FEE', reference: 'ref-1', type: 'DEBIT',
    transactionType: 'CARD', cardNumber: '400000XXXXXX0011', status: 'pending', providerLifecycleTransactionId: 'settled-1',
  }

  it.each([
    ['card-first', cardPayload, [settledPayload]],
    ['poll-first', settledPayload, [cardPayload]],
  ])('creates distinct pending and lifecycle operations for %s arrival', async (_label, currentPayload, priorPayloads) => {
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000099', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, priorPayloads)
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: String(currentPayload.providerVersion), semanticHash: String(currentPayload.semanticHash),
      hashSpecificationVersion: 2, observedAt: String(currentPayload.observedAt), domain: 'cash_ledger',
      payload: currentPayload,
      rawPayload: { wireTransactionId: currentPayload.transactionId },
    }], { skipCurrentState: true })
    const activities = fixture.enqueued.filter(operation => operation.kind === 'ActivityUpsert')
    expect(activities).toHaveLength(2)
    expect(new Set(activities.map(operation => operation.idempotencyKey)).size).toBe(2)
    expect(activities.some(operation => operation.lifecycleSuccessorId?.endsWith(':settled-1'))).toBe(true)
    expect(activities.some(operation => operation.activity?.originalCurrency === 'USD')).toBe(true)
    expect(activities.some(operation => operation.lifecycleSuccessorId
      && operation.activity?.currency === 'ZAR'
      && operation.activity?.amount === '193.91'
      && operation.activity?.originalCurrency === 'USD')).toBe(true)
    expect(activities.some(operation => operation.activity?.settlementCurrency === 'ZAR')).toBe(true)
    expect(activities.every(operation => operation.activity?.sourceGroupId === undefined)).toBe(true)
    expect(fixture.rawPayloads).toContainEqual({ wireTransactionId: currentPayload.transactionId })
  })

  it('does not correlate a card and booked row without explicit provider linkage', async () => {
    const { providerLifecycleTransactionId: _ignoredCardLink, ...unlinkedCardPayload } = cardPayload
    const { providerLifecycleTransactionId: _ignoredSettledLink, ...unlinkedSettledPayload } = settledPayload
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000100', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [unlinkedSettledPayload])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: String(unlinkedCardPayload.providerVersion), semanticHash: String(unlinkedCardPayload.semanticHash),
      hashSpecificationVersion: 2, observedAt: String(unlinkedCardPayload.observedAt), domain: 'cash_ledger',
      payload: unlinkedCardPayload,
    }], { skipCurrentState: true })
    const activityOperations = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    const [operation] = activityOperations
    expect(activityOperations).toHaveLength(1)
    expect(operation.activity?.needsReview).toBeUndefined()
    expect(operation.lifecycleSuccessorId).toBeUndefined()
  })
})

describe('projectPending transaction and receipt contract', () => {
  const operation: BridgeDisclosureOperation = {
    deploymentId: 'dep-1',
    accountId: 'wf-acc-1',
    subject: CONNECTION.localOwnerId,
    idempotencyKey: 'key-1',
    accessGeneration: 1,
    kind: 'ActivityDelete',
    resourceId: 'resource-1',
  }

  it('claims inside a transaction and marks only a completely bound receipt delivered', async () => {
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000010', payload: operation, attempts: 0 })
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => new Response(JSON.stringify(receipt(operation)), { status: 200 })) as typeof fetch
    try {
      const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env())
      await expect(deps.projectPending(CONNECTION)).resolves.toEqual({ delivered: 1, failed: 0 })
      expect(fixture.state()).toBe('delivered')
      expect(fixture.queries.some(query => /FOR UPDATE SKIP LOCKED/i.test(query))).toBe(true)
      expect(fixture.queries.filter(query => /projection_outbox/i.test(query))
        .every(query => /provider_generation/i.test(query))).toBe(true)
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('fences a successful commit with an invalid receipt as uncertain', async () => {
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000011', payload: operation, attempts: 2 })
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => new Response(JSON.stringify({ outcome: 'Accepted' }), { status: 200 })) as typeof fetch
    try {
      const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env())
      await expect(deps.projectPending(CONNECTION)).resolves.toEqual({ delivered: 0, failed: 1 })
      expect(fixture.state()).toBe('uncertain')
      expect(fixture.attempts()).toBe(3)
      await expect(deps.projectPending(CONNECTION)).resolves.toEqual({ delivered: 0, failed: 1 })
      expect(fixture.state()).toBe('uncertain')
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})
