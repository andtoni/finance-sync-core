import { describe, expect, it } from 'vitest'
import { createRuntimeDependencies } from './runtime-deps'
import type { BridgeDisclosureOperation } from './projection/bridge-client'
import type { RuntimeConnection } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const CONNECTION: RuntimeConnection = {
  provider: 'plaid',
  connectionId: 'item-1',
  deploymentId: 'dep-1',
  accessToken: 'access-test',
  localOwnerId: 'urn:wealthfolio:test#owner',
  sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
  accountIds: ['acc-1'],
  wealthfolioAccountIds: { 'acc-1': 'wf-acc-1' },
  accessGeneration: 1,
}
const { accessToken: CONNECTION_ACCESS_TOKEN, ...CONNECTION_METADATA } = CONNECTION

function env(overrides: Record<string, string | undefined> = {}) {
  return {
    FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([CONNECTION_METADATA]),
    FINANCE_SYNC_PROVIDER_SECRETS_JSON: JSON.stringify({ 'item-1': { accessToken: CONNECTION_ACCESS_TOKEN } }),
    FINANCE_SYNC_PLAID_URL: 'https://sandbox.plaid.com',
    FINANCE_SYNC_PLAID_CLIENT_ID: 'client-test',
    FINANCE_SYNC_PLAID_SECRET: 'secret-test',
    FINANCE_SYNC_BRIDGE_URL: 'https://bridge.test/bridge',
    FINANCE_SYNC_BRIDGE_ALLOWED_HOSTS: 'bridge.test',
    FINANCE_SYNC_BRIDGE_TOKEN: 'token-test',
    ...overrides,
  }
}

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v1;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}

function receipt(op: BridgeDisclosureOperation) {
  const operation = op.kind === 'ActivityUpsert' ? 'activity_upsert' : op.kind === 'ActivityDelete' ? 'activity_delete' : 'projection_fence'
  const successor = op.lifecycleSuccessorId ?? null
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
    bindingVersion: 1,
    receiptBinding: binding([
      op.deploymentId, op.subject, op.idempotencyKey, String(op.accessGeneration), operation,
      op.resourceId, op.accountId, successor ?? '<null>', 'accepted',
    ]),
  }
}

function outboxSql(row: { id: string; payload: BridgeDisclosureOperation; attempts: number }) {
  let state: 'pending' | 'uncertain' | 'delivered' | 'dead_letter' = 'pending'
  let attempts = row.attempts
  const queries: string[] = []
  const sql = (async (strings: TemplateStringsArray, ..._values: unknown[]) => {
    const text = strings.join('?')
    queries.push(text)
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
  return { sql, state: () => state, attempts: () => attempts, queries }
}

describe('createRuntimeDependencies startup validation', () => {
  it('allows an explicitly empty connection set', () => {
    expect(createRuntimeDependencies({} as FinanceSyncSql, 'dep-1', { FINANCE_SYNC_CONNECTIONS_JSON: '[]' })).toBeDefined()
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
