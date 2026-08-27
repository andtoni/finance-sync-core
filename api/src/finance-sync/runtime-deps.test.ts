import { describe, expect, it } from 'vitest'
import { assertBridgeConfiguration, createRuntimeDependencies, reconcileInvestecCardLifecycles } from './runtime-deps'
import { investecPrimeSaverIdentityKey } from './probes/investec/investec-adapter'
import { bridgeOperationToWire, currentStateToBridge, validateBridgeReceipt } from './projection/bridge-client'
import { currentStateSemanticHash } from './projection/current-state'
import type { BridgeDisclosureOperation } from './projection/bridge-client'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
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
  const storedRows: unknown[] = []
  const allRows = () => [...priorPayloads, ...storedRows]
  const unwrap = (payload: unknown): { payload: unknown; rawPayload: unknown } => {
    if (payload && typeof payload === 'object' && 'payload' in payload
      && ('rawPayload' in payload || 'raw_payload' in payload)) {
      const value = payload as Record<string, unknown>
      return { payload: value.payload, rawPayload: value.rawPayload ?? value.raw_payload }
    }
    return { payload, rawPayload: null }
  }
  const sql = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join('?')
    if (/INSERT\s+INTO\s+raw_observations/i.test(text)) {
      const payloadIndex = values.findIndex(value => value !== null && typeof value === 'object'
        && ('transactionId' in value || 'accountId' in value))
      if (payloadIndex >= 0) {
        const payload = values[payloadIndex]
        const rawPayload = values[payloadIndex + 1]
        if (rawPayload !== null && typeof rawPayload === 'object') rawPayloads.push(rawPayload)
        storedRows.push({
          payload,
          rawPayload,
          providerVersion: values[3],
          semanticHash: values[4],
          observedAt: values[8],
        })
      }
    }
    if (/INSERT\s+INTO\s+projection_outbox/i.test(text)) {
      const payload = values.find(value => value !== null && typeof value === 'object' && 'kind' in value) as BridgeDisclosureOperation | undefined
      if (payload) enqueued.push(payload)
    }
    queries.push(text)
    if (/FROM\s+raw_observations/i.test(text) && /provider_version/i.test(text) && /semantic_hash/i.test(text)) {
      const lifecyclePayloads = /providerLifecycleTransactionId/.test(text)
        ? allRows().filter(payload => {
          const value = unwrap(payload).payload && typeof unwrap(payload).payload === 'object'
            ? unwrap(payload).payload as Record<string, unknown> : {}
          const currentId = values.at(-1)
          const linkedId = values.at(-2)
          return value.transactionId === linkedId || value.providerLifecycleTransactionId === currentId
        })
        : allRows()
      return lifecyclePayloads.map((payload, index) => {
        const unwrapped = unwrap(payload)
        const value = unwrapped.payload && typeof unwrapped.payload === 'object'
          ? unwrapped.payload as Record<string, unknown> : {}
        return {
          payload: unwrapped.payload,
          raw_payload: unwrapped.rawPayload,
          provider_version: typeof value.providerVersion === 'string' ? value.providerVersion : `investec:txn:prior-${index}`,
          semantic_hash: typeof value.semanticHash === 'string' ? value.semanticHash : `prior-hash-${index}`,
          observed_at: typeof value.observedAt === 'string' ? value.observedAt : '2026-08-06T00:00:00.000Z',
        }
      })
    }
    if (/WITH\s+latest/i.test(text) && /FROM\s+raw_observations/i.test(text)) {
      const rows = allRows().filter(payload => {
        const value = unwrap(payload).payload
        return !(value && typeof value === 'object'
          && (value as Record<string, unknown>).transactionId === values.at(-1))
      })
      return rows.map(payload => {
        const value = unwrap(payload)
        return {
          payload: value.payload,
          raw_payload: value.rawPayload,
          provider_version: 'investec:txn:prior',
        }
      })
    }
    if (/SELECT\s+payload(?:,\s*raw_payload,\s*provider_version)?\s+FROM\s+raw_observations/i.test(text)) {
      const rows = allRows().filter(payload => {
        if (!/payload->>'transactionId'\s*<>/i.test(text)) return true
        const value = unwrap(payload).payload
        return !(value && typeof value === 'object'
          && (value as Record<string, unknown>).transactionId === values.at(-1))
      })
      return rows.map(payload => {
        if (payload && typeof payload === 'object' && 'payload' in payload
          && ('rawPayload' in payload || 'raw_payload' in payload)) {
          const value = payload as Record<string, unknown>
          return { payload: value.payload, raw_payload: value.rawPayload ?? value.raw_payload, provider_version: 'investec:txn:prior' }
        }
        return { payload, raw_payload: null, provider_version: 'investec:txn:prior' }
      })
    }
    if (/GROUP BY payload->>'kind'/i.test(text)) {
      return state === 'dead_letter' ? [{ kind: row.payload.kind, count: 1 }] : []
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

function projectOutboxSql(input: readonly { id: string; payload: BridgeDisclosureOperation; attempts: number; state: 'pending' | 'uncertain' | 'delivered' | 'dead_letter' }[]) {
  const rows = input.map(row => ({ ...row }))
  const queries: string[] = []
  const sql = (async (strings: TemplateStringsArray, ...values: unknown[]) => {
    const text = strings.join('?')
    queries.push(text)
    if (/GROUP BY payload->>'kind'/i.test(text)) {
      const counts = new Map<string | null, number>()
      for (const row of rows.filter(row => row.state === 'dead_letter')) {
        const kind = row.payload.kind ?? null
        counts.set(kind, (counts.get(kind) ?? 0) + 1)
      }
      return [...counts.entries()].map(([kind, count]) => ({ kind, count }))
    }
    if (/SELECT\s+id,\s*payload,\s*attempts/i.test(text)) {
      const currentOnly = /payload->>'kind'\s*=\s*'CurrentStateUpsert'/i.test(text)
      return rows.filter(row => (row.state === 'pending' || row.state === 'uncertain')
        && (!currentOnly || row.payload.kind === 'CurrentStateUpsert'))
    }
    if (/UPDATE\s+projection_outbox/i.test(text)) {
      const id = values.find(value => typeof value === 'string' && rows.some(row => row.id === value))
      const row = rows.find(candidate => candidate.id === id)
      if (row) {
        row.attempts += 1
        if (/state = 'delivered'/i.test(text)) row.state = 'delivered'
        if (/state = 'uncertain'/i.test(text)) row.state = 'uncertain'
        if (/state = 'dead_letter'/i.test(text)) row.state = 'dead_letter'
      }
      return []
    }
    if (/count\(\*\)/i.test(text)) {
      return [{ count: rows.filter(row => row.state === 'pending' || row.state === 'uncertain' || row.state === 'dead_letter').length }]
    }
    return []
  }) as unknown as FinanceSyncSql
  ;(sql as unknown as { begin: (fn: (tx: FinanceSyncSql) => Promise<unknown>) => Promise<unknown> }).begin = fn => fn(sql)
  ;(sql as unknown as { json: (value: unknown) => unknown }).json = value => value
  return { sql, rows, queries }
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

  it('does not suppress a distinct high-precision PrimeSaver amount', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-strong', accountId: 'acc-1', amount: '9007199254740993', currency: 'ZAR',
      date: '2026-08-07', transactionDate: '2026-08-07', type: 'CREDIT',
      description: 'Synthetic inbound Value Date 07Aug26 MBP9000000001 Synthetic Holder 9001',
      reference: null, cardNumber: null,
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'synthetic-v2-weak', amount: '9007199254740992',
      description: 'Synthetic Holder 9001',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000096', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload), providerVersion: 'investec:txn:synthetic-v2-strong', semanticHash: 'strong-hash' },
      rawPayload: { runningBalance: '98765.43' },
    }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-weak', semanticHash: 'weak-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: { ...weakPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(weakPayload) },
      rawPayload: { runningBalance: 0 },
    }], { skipCurrentState: true })
    expect(fixture.enqueued).toHaveLength(1)
    expect(fixture.enqueued[0].resourceId).toContain('synthetic-v2-weak')
  })

  it('does not suppress a UUID-backed activity that resembles a weak PrimeSaver row', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-strong', accountId: 'acc-1', amount: '3150', currency: 'ZAR',
      date: '2026-08-07', transactionDate: '2026-08-07', type: 'CREDIT',
      description: 'Synthetic inbound Value Date 07Aug26 MBP9000000001 Synthetic Holder 9001',
      reference: null, cardNumber: null,
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'provider-uuid-weak',
      description: 'Synthetic Holder 9001',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000094', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload) },
      rawPayload: { runningBalance: '98765.43' },
    }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:provider-uuid-weak', semanticHash: 'uuid-weak-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: weakPayload,
      rawPayload: { runningBalance: 0 },
    }], { skipCurrentState: true })
    expect(fixture.enqueued).toHaveLength(1)
    expect(fixture.enqueued[0].resourceId).toContain('provider-uuid-weak')
  })

  it('suppresses a weak PrimeSaver upsert before enqueue when both rows arrive together', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-strong', accountId: 'acc-1', amount: '3150', currency: 'ZAR',
      date: '2026-08-07', transactionDate: '2026-08-07', type: 'CREDIT',
      description: 'Synthetic inbound Value Date 07Aug26 MBP9000000001 Synthetic Holder 9001',
      reference: 'provider-ref-42', cardNumber: null,
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'synthetic-v2-weak', reference: null,
      description: 'Synthetic Holder 9001',
    }
    const strongObservation = {
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-strong', semanticHash: 'strong-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger' as const,
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload) },
      rawPayload: { runningBalance: '98765.43' },
    }
    const weakObservation = {
      ...strongObservation,
      providerVersion: 'investec:txn:synthetic-v2-weak', semanticHash: 'weak-hash',
      payload: { ...weakPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(weakPayload) },
      rawPayload: { runningBalance: 0 },
    }
    const priorStrong = {
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload) },
      rawPayload: { runningBalance: '98765.43' },
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000093', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [priorStrong, priorStrong])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [weakObservation, strongObservation], { skipCurrentState: true })
    expect(fixture.enqueued).toHaveLength(1)
    expect(fixture.enqueued[0].resourceId).toContain('synthetic-v2-strong')
    expect(fixture.enqueued[0].supersededResourceIds).toEqual([
      'dep-1:00000000-0000-0000-0000-000000000001:synthetic-v2-weak',
    ])
  })

  it('leaves a weak PrimeSaver row visible when strong identities are ambiguous', async () => {
    const weakPayload = {
      transactionId: 'synthetic-v2-weak', accountId: 'acc-1', amount: '3150', currency: 'ZAR',
      date: '2026-08-07', transactionDate: '2026-08-07', type: 'CREDIT',
      description: 'Synthetic Holder 9001', reference: null, cardNumber: null,
    }
    const strongPayloadA = {
      ...weakPayload, transactionId: 'synthetic-v2-strong-a', reference: 'provider-ref-a',
      description: 'Synthetic inbound A Synthetic Holder 9001',
    }
    const strongPayloadB = {
      ...weakPayload, transactionId: 'synthetic-v2-strong-b', reference: 'provider-ref-b',
      description: 'Synthetic inbound B Synthetic Holder 9001',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000092', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [
      {
        payload: { ...weakPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(weakPayload) },
        rawPayload: { runningBalance: 0 },
      },
      {
        payload: { ...strongPayloadB, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayloadB) },
        rawPayload: { runningBalance: '98765.43' },
      },
    ])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-strong-a', semanticHash: 'strong-a-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: { ...strongPayloadA, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayloadA) },
      rawPayload: { runningBalance: '98765.43' },
    }], { skipCurrentState: true })
    expect(fixture.enqueued).toHaveLength(1)
    expect(fixture.enqueued[0].supersededResourceIds).toBeUndefined()
  })

  it('does not reconcile PrimeSaver rows when direction evidence is missing', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-unknown-direction-strong', accountId: 'acc-1', amount: '4000', currency: 'ZAR',
      date: '2026-08-06', transactionDate: '2026-08-06',
      description: 'Internal Transaction MBP9000000003 Synthetic Holder 9003', reference: null, cardNumber: null,
      primeSaverIdentityKey: 'v2:unknown-direction-strong',
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'synthetic-v2-unknown-direction-weak',
      description: 'Synthetic Holder 9003', primeSaverIdentityKey: 'v2:unknown-direction-weak',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000097', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{ payload: weakPayload, rawPayload: null }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-unknown-direction-strong', semanticHash: 'unknown-direction-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: strongPayload, rawPayload: { runningBalance: '98765.43' },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toBeUndefined()
  })

  it('does not supersede a strong row when multiple weak identities match', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-multiple-strong', accountId: 'acc-1', amount: '4000', currency: 'ZAR',
      date: '2026-08-06', transactionDate: '2026-08-06', type: 'CREDIT',
      description: 'Internal Transaction MBP9000000004 Synthetic Holder 9004', reference: null, cardNumber: null,
    }
    const weakPayload = (transactionId: string, identityKey: string) => ({
      ...strongPayload, transactionId, description: 'Synthetic Holder 9004', primeSaverIdentityKey: identityKey,
    })
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000098', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [
      { payload: weakPayload('synthetic-v2-multiple-weak-a', 'v2:multiple-weak-a'), rawPayload: null },
      { payload: weakPayload('synthetic-v2-multiple-weak-b', 'v2:multiple-weak-b'), rawPayload: { runningBalance: 0 } },
    ])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-multiple-strong', semanticHash: 'multiple-weak-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: { ...strongPayload, primeSaverIdentityKey: 'v2:multiple-strong' },
      rawPayload: { runningBalance: '98765.43' },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toBeUndefined()
  })

  it('supersedes a previously projected weak PrimeSaver row when the strong row arrives', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-strong', accountId: 'acc-1', amount: '3150', currency: 'ZAR',
      date: '2026-08-07', transactionDate: '2026-08-07', type: 'CREDIT',
      description: 'Synthetic inbound Value Date 07Aug26 MBP9000000001 Synthetic Holder 9001',
      reference: null, cardNumber: null,
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'synthetic-v2-weak',
      description: 'Synthetic Holder 9001',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000095', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{
      payload: { ...weakPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(weakPayload), providerVersion: 'investec:txn:synthetic-v2-weak', semanticHash: 'weak-hash' },
      rawPayload: { runningBalance: 0 },
    }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-strong', semanticHash: 'strong-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload) },
      rawPayload: { runningBalance: '98765.43' },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toEqual([
      'dep-1:00000000-0000-0000-0000-000000000001:synthetic-v2-weak',
    ])
  })

  it('reconciles a pre-raw-payload weak PrimeSaver row', async () => {
    const strongPayload = {
      transactionId: 'synthetic-v2-legacy-strong', accountId: 'acc-1', amount: '4000', currency: 'ZAR',
      date: '2026-08-06', transactionDate: '2026-08-06', type: 'CREDIT',
      description: 'Internal Transaction PMC Inbounds Value Date 06Aug26 MBP9000000002 Synthetic Holder 9002',
      reference: null, cardNumber: null,
    }
    const weakPayload = {
      ...strongPayload, transactionId: 'synthetic-v2-legacy-weak', description: 'Synthetic Holder 9002',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000096', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{
      payload: { ...weakPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(weakPayload), providerVersion: 'investec:txn:synthetic-v2-legacy-weak', semanticHash: 'legacy-weak-hash' },
      rawPayload: null,
    }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    const investecConnection: RuntimeConnection = {
      ...CONNECTION, provider: 'investec', connectionId: 'investec-connection',
      apiKey: 'api-key', eligibleProductNames: ['PrimeSaver'], fromDate: '2026-08-01', toDate: '2026-08-07',
    }
    await deps.persistAndEnqueue(investecConnection, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: 'investec:txn:synthetic-v2-legacy-strong', semanticHash: 'legacy-strong-hash',
      hashSpecificationVersion: 3, observedAt: '2026-08-07T08:00:00.000Z', domain: 'cash_ledger',
      payload: { ...strongPayload, primeSaverIdentityKey: investecPrimeSaverIdentityKey(strongPayload) },
      rawPayload: { runningBalance: '98765.43' },
    }], { skipCurrentState: true })
    expect(fixture.enqueued[0].supersededResourceIds).toEqual([
      'dep-1:00000000-0000-0000-0000-000000000001:synthetic-v2-legacy-weak',
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
    observedAt: '2026-08-06T10:00:00.000Z', accountId: 'acc-1', amount: '-123.45', currency: 'ZAR',
    date: '2026-08-06', postingDate: '2026-08-06', settlementDate: '2026-08-06',
    description: 'SYNTHETIC CARD MERCHANT', reference: 'ref-1', type: 'DEBIT', transactionType: 'CARDPURCHASES',
    status: 'posted', providerLifecycleTransactionId: 'card-1',
  }
  const cardPayload = {
    transactionId: 'card-1', providerVersion: 'investec:card:card-1', semanticHash: 'card-hash',
    observedAt: '2026-08-05T10:00:00.000Z', accountId: 'acc-1', amount: '-12.34', currency: 'ZAR',
    originalAmount: '12.34', originalCurrency: 'USD', settlementCurrency: 'ZAR',
    date: '2026-08-05', description: 'SYNTHETIC CARD MERCHANT', reference: 'ref-1', type: 'DEBIT',
    transactionType: 'CARD', cardNumber: 'TEST-CARD-0011', status: 'pending', providerLifecycleTransactionId: 'settled-1',
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
      && operation.activity?.amount === '123.45'
      && operation.activity?.originalCurrency === 'USD')).toBe(true)
    expect(activities.some(operation => operation.activity?.settlementCurrency === 'ZAR')).toBe(true)
    expect(activities.every(operation => operation.activity?.sourceGroupId === undefined)).toBe(true)
    expect(fixture.rawPayloads).toContainEqual({ wireTransactionId: currentPayload.transactionId })
  })

  const inferredSettledPayload = {
    transactionId: 'facts-settled-1', providerVersion: 'investec:txn:facts-settled-1', semanticHash: 'facts-settled-hash',
    observedAt: '2026-08-14T22:00:00.000Z', accountId: 'acc-1', amount: '-191.13', currency: 'ZAR',
    settlementAmount: '-191.13', settlementCurrency: 'ZAR', date: '2026-08-14', transactionDate: '2026-08-14',
    postingDate: '2026-08-14', settlementDate: '2026-08-14', description: 'DMI* ACME SALES & SERV ACME.COM US',
    type: 'DEBIT', direction: 'DEBIT', transactionType: 'CARDPURCHASES', cardNumber: '400000xxxxxx0011', status: 'posted',
  }
  const inferredCardPayload = {
    transactionId: 'facts-card-1', providerVersion: 'investec:card:facts-card-1', semanticHash: 'facts-card-hash',
    observedAt: '2026-08-14T17:00:00.000Z', accountId: 'acc-1', amount: '-1349.99', currency: 'USD',
    originalAmount: '1349.99', originalCurrency: 'USD', date: '2026-08-14', transactionDate: '2026-08-14',
    authorizationDateTime: '2026-08-14T17:00:00.000Z', description: 'DMI* ACME SALES & SERV',
    type: 'DEBIT', direction: 'DEBIT', transactionType: 'CARD', cardNumber: '400000XXXXXX0011', status: 'pending',
    metadata: { providerOwned: { investec: { accountCurrency: 'ZAR' } } },
  }

  it.each([
    ['card-first', inferredCardPayload, [inferredSettledPayload]],
    ['poll-first', inferredSettledPayload, [inferredCardPayload]],
  ])('correlates exact provider facts without a lifecycle ID (%s)', async (_label, currentPayload, priorPayloads) => {
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000102', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, priorPayloads)
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: String(currentPayload.providerVersion), semanticHash: String(currentPayload.semanticHash),
      hashSpecificationVersion: 4, observedAt: String(currentPayload.observedAt), domain: 'cash_ledger',
      payload: currentPayload,
    }], { skipCurrentState: true })
    const activities = fixture.enqueued.filter(operation => operation.kind === 'ActivityUpsert')
    expect(activities).toHaveLength(2)
    expect(activities[0].lifecycleSuccessorId).toBeUndefined()
    expect(activities[1].lifecycleSuccessorId).toContain(':facts-settled-1')
    expect(activities[1].activity).toMatchObject({
      status: 'POSTED', activityType: 'WITHDRAWAL', amount: '191.13', currency: 'ZAR',
      originalAmount: '1349.99', originalCurrency: 'USD', settlementCurrency: 'ZAR',
    })
    expect(activities[1].activity?.metadata).toMatchObject({
      providerOwned: { investecCorrelation: { confidence: 'provider-facts-exact' } },
    })
  })

  it('correlates a same-currency settlement posted within the provider window', async () => {
    const settled = {
      ...inferredSettledPayload,
      transactionId: 'date-drift-settled',
      providerVersion: 'investec:txn:date-drift-settled',
      semanticHash: 'date-drift-settled-hash',
      date: '2026-08-15',
      transactionDate: '2026-08-15',
      postingDate: '2026-08-15',
      settlementDate: '2026-08-15',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000110', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [settled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: inferredCardPayload.providerVersion, semanticHash: 'date-drift-card-hash',
      hashSpecificationVersion: 4, observedAt: inferredCardPayload.observedAt, domain: 'cash_ledger',
      payload: inferredCardPayload,
    }], { skipCurrentState: true })
    const operations = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operations).toHaveLength(2)
    expect(operations.some(operation => operation.lifecycleSuccessorId?.endsWith(':date-drift-settled'))).toBe(true)
  })

  it('sweeps retained posted card facts through lifecycle projection even without a new provider row', async () => {
    let persisted: readonly unknown[] = []
    const sql = (async () => [{
      payload: inferredSettledPayload, raw_payload: null,
      provider_version: inferredSettledPayload.providerVersion, semantic_hash: 'sweep-settled-hash',
      hash_specification_version: 4, observed_at: inferredSettledPayload.observedAt,
    }]) as unknown as FinanceSyncSql
    const deps = {
      persistAndEnqueue: async (_connection: RuntimeConnection, observations: readonly unknown[]) => { persisted = observations; return 1 },
    } as unknown as SyncRuntimeDependencies
    await expect(reconcileInvestecCardLifecycles(sql, 'dep-1', INVESTEC_CONNECTION, deps)).resolves.toBe(1)
    expect(persisted).toHaveLength(1)
    expect(persisted[0]).toMatchObject({ providerVersion: inferredSettledPayload.providerVersion, semanticHash: 'sweep-settled-hash' })
  })

  it('correlates a posted-first one-day date-drift settlement on a later pass', async () => {
    const card = { ...inferredCardPayload, transactionId: 'posted-first-date-drift-card', providerVersion: 'investec:card:posted-first-date-drift-card', semanticHash: 'posted-first-date-drift-card-hash' }
    const settled = {
      ...inferredSettledPayload,
      transactionId: 'posted-first-date-drift-settled',
      providerVersion: 'investec:txn:posted-first-date-drift-settled',
      semanticHash: 'posted-first-date-drift-settled-hash',
      date: '2026-08-15', transactionDate: '2026-08-15', postingDate: '2026-08-15', settlementDate: '2026-08-15',
    }
    const fixture = outboxSql({ id: '00000000-0000-4000-8000-000000000111', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [card])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: settled.providerVersion, semanticHash: settled.semanticHash,
      hashSpecificationVersion: 4, observedAt: settled.observedAt, domain: 'cash_ledger', payload: settled,
    }], { skipCurrentState: true })
    const operations = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operations.some(operation => operation.lifecycleSuccessorId?.endsWith(':posted-first-date-drift-settled'))).toBe(true)
  })

  it('strips provisional spending metadata from the settled successor', async () => {
    const cardWithProvisional = {
      ...inferredCardPayload,
      transactionId: 'provisional-card',
      providerVersion: 'investec:card:provisional-card',
      semanticHash: 'provisional-card-hash',
      metadata: { providerOwned: { investec: { accountCurrency: 'ZAR', provisionalSpending: true, provisionalSpendingReason: 'AUTHORIZED_NO_SETTLEMENT' } } },
    }
    const settled = {
      ...inferredSettledPayload,
      transactionId: 'provisional-settled',
      providerVersion: 'investec:txn:provisional-settled',
      semanticHash: 'provisional-settled-hash',
      description: 'DMI* ACME SALES & SERV',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000105', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [settled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: cardWithProvisional.providerVersion, semanticHash: cardWithProvisional.semanticHash,
      hashSpecificationVersion: 4, observedAt: cardWithProvisional.observedAt, domain: 'cash_ledger',
      payload: cardWithProvisional,
    }], { skipCurrentState: true })
    const activities = fixture.enqueued.filter(operation => operation.kind === 'ActivityUpsert')
    expect(activities).toHaveLength(2)
    const settledActivity = activities.find(op => op.lifecycleSuccessorId?.endsWith(':provisional-settled'))?.activity
    expect(settledActivity).toBeDefined()
    expect(settledActivity?.metadata).not.toMatchObject({
      providerOwned: { investec: { provisionalSpending: true, provisionalSpendingReason: 'AUTHORIZED_NO_SETTLEMENT' } },
    })
    expect(settledActivity?.metadata).toMatchObject({
      providerOwned: { investecCorrelation: expect.anything() },
    })
  })

  it('correlates an exact settlement when the booked row omits card last four', async () => {
    const card = {
      ...inferredCardPayload,
      transactionId: 'reference-card-1',
      providerVersion: 'investec:card:reference-card-1',
      semanticHash: 'reference-card-hash',
      cardNumber: undefined,
      reference: 'AMAZON-0001',
    }
    const settled = {
      ...inferredSettledPayload,
      transactionId: 'reference-settled-1',
      providerVersion: 'investec:txn:reference-settled-1',
      semanticHash: 'reference-settled-hash',
      cardNumber: undefined,
      reference: 'AMAZON-0001',
    }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000109', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [settled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: card.providerVersion, semanticHash: card.semanticHash,
      hashSpecificationVersion: 4, observedAt: card.observedAt, domain: 'cash_ledger', payload: card,
    }], { skipCurrentState: true })
    const operations = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operations).toHaveLength(2)
    expect(operations.some(operation => operation.lifecycleSuccessorId?.endsWith(':reference-settled-1'))).toBe(true)
  })
  it('leaves ambiguous exact provider facts visible without a lifecycle successor', async () => {
    const secondSettled = { ...inferredSettledPayload, transactionId: 'facts-settled-2', providerVersion: 'investec:txn:facts-settled-2', semanticHash: 'facts-settled-hash-2', amount: '-192.00', settlementAmount: '-192.00' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000103', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [inferredSettledPayload, secondSettled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: inferredCardPayload.providerVersion, semanticHash: 'facts-card-ambiguous-hash',
      hashSpecificationVersion: 4, observedAt: inferredCardPayload.observedAt, domain: 'cash_ledger',
      payload: inferredCardPayload,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
    expect(operation.activity?.needsReview).toBe(true)
  })

  it('does not infer a lifecycle when equal-currency amounts differ', async () => {
    const card = { ...inferredCardPayload, transactionId: 'same-currency-card', providerVersion: 'investec:card:same-currency-card', semanticHash: 'same-currency-card-hash', currency: 'ZAR', originalCurrency: 'ZAR', originalAmount: '10.00', amount: '-10.00' }
    const settled = { ...inferredSettledPayload, transactionId: 'same-currency-settled', providerVersion: 'investec:txn:same-currency-settled', semanticHash: 'same-currency-settled-hash', description: 'DMI* ACME SALES & SERV ACME.COM ZA', amount: '-12.00', settlementAmount: '-12.00' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000104', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [settled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: card.providerVersion, semanticHash: card.semanticHash,
      hashSpecificationVersion: 4, observedAt: card.observedAt, domain: 'cash_ledger', payload: card,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
    expect(operation.activity?.needsReview).toBeUndefined()
  })

  it('does not infer a lifecycle from malformed currency evidence', async () => {
    const card = { ...inferredCardPayload, transactionId: 'malformed-currency-card', providerVersion: 'investec:card:malformed-currency-card', semanticHash: 'malformed-currency-card-hash', currency: 'USDX', originalCurrency: 'USDX' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000108', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [inferredSettledPayload])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: card.providerVersion, semanticHash: card.semanticHash,
      hashSpecificationVersion: 4, observedAt: card.observedAt, domain: 'cash_ledger', payload: card,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
  })

  it('does not infer a lifecycle when an explicit link is unresolved', async () => {
    const linkedCard = { ...inferredCardPayload, providerLifecycleTransactionId: 'missing-settlement' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000105', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [inferredSettledPayload])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: linkedCard.providerVersion, semanticHash: 'linked-card-hash',
      hashSpecificationVersion: 4, observedAt: linkedCard.observedAt, domain: 'cash_ledger', payload: linkedCard,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
  })

  it('marks a conflicting explicit backlink ambiguous', async () => {
    const conflictingCard = { ...inferredCardPayload, providerLifecycleTransactionId: 'facts-settled-1' }
    const conflictingSettled = { ...inferredSettledPayload, providerLifecycleTransactionId: 'different-card' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000107', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [conflictingSettled])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: conflictingCard.providerVersion, semanticHash: 'conflicting-card-hash',
      hashSpecificationVersion: 4, observedAt: conflictingCard.observedAt, domain: 'cash_ledger', payload: conflictingCard,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
    expect(operation.activity?.needsReview).toBe(true)
  })

  it('does not infer a lifecycle for a pocket transfer', async () => {
    const pocket = { ...inferredSettledPayload, transactionId: 'pocket-settled', providerVersion: 'investec:txn:pocket-settled', semanticHash: 'pocket-hash', transactionType: 'MCPBAPOCKETTRANSFERS', sourceType: 'MCPBAPOCKETTRANSFERS', providerLifecycleTransactionId: 'pocket-card' }
    const pocketCard = { ...inferredCardPayload, providerLifecycleTransactionId: 'pocket-settled' }
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000106', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [pocket])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: pocketCard.providerVersion, semanticHash: 'pocket-card-hash',
      hashSpecificationVersion: 4, observedAt: pocketCard.observedAt, domain: 'cash_ledger', payload: pocketCard,
    }], { skipCurrentState: true })
    const [operation] = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert')
    expect(operation.lifecycleSuccessorId).toBeUndefined()
  })

  it('does not invent original evidence while correlating a card without provider currency', async () => {
    const { originalAmount: _originalAmount, originalCurrency: _originalCurrency, ...cardWithoutEvidence } = cardPayload
    const fixture = outboxSql({ id: '00000000-0000-0000-0000-000000000101', payload: {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId,
      idempotencyKey: 'unused', accessGeneration: 1, kind: 'ActivityDelete', resourceId: 'unused',
    }, attempts: 0 }, [{ ...settledPayload, originalAmount: '99.99' }])
    const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env({ FINANCE_SYNC_CONNECTIONS_JSON: '[]' }))
    await deps.persistAndEnqueue(INVESTEC_CONNECTION, [{
      deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId,
      sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId,
      providerVersion: String(cardWithoutEvidence.providerVersion), semanticHash: 'card-without-evidence-hash',
      hashSpecificationVersion: 3, observedAt: String(cardWithoutEvidence.observedAt), domain: 'cash_ledger',
      payload: cardWithoutEvidence,
    }], { skipCurrentState: true })
    const activities = fixture.enqueued.filter(item => item.kind === 'ActivityUpsert').map(item => item.activity)
    expect(activities).toHaveLength(2)
    expect(activities.every(activity => activity?.originalAmount === undefined && activity?.originalCurrency === undefined)).toBe(true)
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
      expect(fixture.queries.some(query => /ORDER BY \(payload->>'kind' = 'CurrentStateUpsert'\) DESC,[\s\S]*CASE WHEN payload->>'kind' = 'CurrentStateUpsert' THEN created_at END DESC/i.test(query))).toBe(true)
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

  it('delivers current state past an activity dead letter but keeps later activity fenced', async () => {
    const state = {
      accountId: 'wf-acc-1', providerConnectionId: 'item-1',
      providerIncarnationId: CONNECTION.sourceResourceIncarnationId, providerGeneration: 1,
      observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '100', liabilityBalance: '0', positions: [],
    } as const
    const currentState = currentStateToBridge(
      { deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId, sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId },
      state,
      currentStateSemanticHash(state),
      1,
      CONNECTION.connectionId,
    )
    const activity = {
      deploymentId: 'dep-1', accountId: 'wf-acc-1', subject: CONNECTION.localOwnerId, accessGeneration: 1,
      kind: 'ActivityUpsert' as const, resourceId: 'later-activity', idempotencyKey: 'later-activity-key',
      activity: {
        accountId: 'wf-acc-1', activityType: 'WITHDRAWAL', status: 'POSTED',
        activityDate: '2026-07-18', activityDatePrecision: 'DATE' as const,
        amount: '5', currency: 'USD', sourceGroupId: 'provider-group',
      },
    }
    const deadLetter = {
      ...activity, resourceId: 'prime-saver-dead-letter', idempotencyKey: 'prime-saver-dead-letter-key',
      activity: { ...activity.activity, sourceGroupId: 'conflicting-provider-group' },
    }
    expect(() => bridgeOperationToWire(currentState)).not.toThrow()
    expect(() => validateBridgeReceipt(currentState, receipt(currentState))).not.toThrow()
    const fixture = projectOutboxSql([
      { id: 'dead-letter', payload: deadLetter, attempts: 3, state: 'dead_letter' },
      { id: 'current-state', payload: currentState, attempts: 0, state: 'pending' },
      { id: 'later-activity', payload: activity, attempts: 0, state: 'pending' },
    ])
    const originalFetch = globalThis.fetch
    const calls: string[] = []
    globalThis.fetch = (async (_input, init) => {
      const wire = JSON.parse(String(init?.body)) as Record<string, unknown>
      calls.push(String(wire.kind))
      expect(wire.kind).toBe('CurrentStateUpsert')
      return new Response(JSON.stringify(receipt(currentState)), { status: 200 })
    }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env())
      await expect(deps.projectPending(CONNECTION)).resolves.toEqual({ delivered: 1, failed: 2 })
      expect(calls).toEqual(['CurrentStateUpsert'])
      expect(fixture.rows.map(row => [row.id, row.state])).toEqual([
        ['dead-letter', 'dead_letter'], ['current-state', 'delivered'], ['later-activity', 'pending'],
      ])
      expect(fixture.queries.some(query => /payload->>'kind'\s*=\s*'CurrentStateUpsert'/i.test(query))).toBe(true)
    } finally {
      globalThis.fetch = originalFetch
    }
  })

  it('keeps a dead-lettered current-state lane fully stopped', async () => {
    const state = {
      accountId: 'wf-acc-1', providerConnectionId: 'item-1',
      providerIncarnationId: CONNECTION.sourceResourceIncarnationId, providerGeneration: 1,
      observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '100', liabilityBalance: '0', positions: [],
    } as const
    const currentState = currentStateToBridge(
      { deploymentId: 'dep-1', localOwnerId: CONNECTION.localOwnerId, sourceResourceIncarnationId: CONNECTION.sourceResourceIncarnationId },
      state,
      currentStateSemanticHash(state),
      1,
      CONNECTION.connectionId,
    )
    const fixture = projectOutboxSql([{ id: 'current-dead-letter', payload: currentState, attempts: 3, state: 'dead_letter' }])
    const originalFetch = globalThis.fetch
    globalThis.fetch = (async () => { throw new Error('must not dispatch') }) as typeof fetch
    try {
      const deps = createRuntimeDependencies(fixture.sql, 'dep-1', env())
      await expect(deps.projectPending(CONNECTION)).resolves.toEqual({ delivered: 0, failed: 1 })
      expect(fixture.rows[0]?.state).toBe('dead_letter')
    } finally {
      globalThis.fetch = originalFetch
    }
  })
})
