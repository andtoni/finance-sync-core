import { describe, expect, it } from 'vitest'
import {
  idempotencyKey,
  cashObservationToBridge,
  commitObservation,
  commitObservationBatch,
  bridgeOperationToWire,
  type BridgeTransport,
  type BridgeDisclosureOperation,
  type BridgeReceipt,
} from './bridge-client'

const wireOperation = { ActivityUpsert: 'activity_upsert', ActivityDelete: 'activity_delete', ProjectionFence: 'projection_fence' } as const
const baseOperation = (overrides: Partial<BridgeDisclosureOperation> = {}): BridgeDisclosureOperation => ({
  deploymentId: 'dep-1',
  accountId: 'account-1',
  idempotencyKey: 'key-1',
  subject: 'urn:wealthfolio:test#alexis',
  accessGeneration: 1,
  kind: 'ActivityUpsert',
  resourceId: 'resource-1',
  ...overrides,
})

function binding(fields: readonly string[]): string {
  const encoder = new TextEncoder()
  return `v1;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}

function boundReceipt(op: BridgeDisclosureOperation, overrides: Partial<BridgeReceipt> = {}): BridgeReceipt {
  const successor = op.lifecycleSuccessorId ?? null
  return {
    deploymentId: op.deploymentId,
    accountId: op.accountId,
    subject: op.subject,
    idempotencyKey: op.idempotencyKey,
    accessGeneration: op.accessGeneration,
    operation: wireOperation[op.kind],
    resourceId: op.resourceId,
    lifecycleSuccessorId: successor,
    outcome: 'Accepted',
    bindingVersion: 1,
    receiptBinding: binding([
      op.deploymentId, op.subject, op.idempotencyKey, String(op.accessGeneration), wireOperation[op.kind],
      op.resourceId, op.accountId, successor ?? '<null>', 'accepted',
    ]),
    ...overrides,
  }
}

const mockTransport = (): BridgeTransport => ({ commit: async op => boundReceipt(op) })

describe('Bridge client', () => {
  it('requires an issuer-qualified subject', () => {
    expect(() => cashObservationToBridge({
      deploymentId: 'g8-test', localOwnerId: 'alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'abc123', payload: { transactionId: 'txn-1', amount: '1.00' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'wf-acc-uuid-1')).toThrow('issuer-qualified')
  })

  it('generates deterministic deployment-scoped idempotency keys', () => {
    expect(idempotencyKey('dep-1', 'hash-a')).toBe(idempotencyKey('dep-1', 'hash-a'))
    expect(idempotencyKey('dep-1', 'hash-a')).not.toBe(idempotencyKey('dep-2', 'hash-a'))
    expect(idempotencyKey('dep-1', 'hash-a')).toHaveLength(40)
  })

  it('normalizes Plaid positive spending to withdrawal', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8-test', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'plaid:item-1',
      semanticHash: 'abc123', payload: { transactionId: 'txn-1', amount: '42.50', currency: 'USD', date: '2026-07-11', name: 'AMAZON' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'wf-acc-uuid-1')
    expect(op.activity).toMatchObject({ accountId: 'wf-acc-uuid-1', amount: '42.5', activityType: 'WITHDRAWAL', status: 'POSTED' })
  })

  it('normalizes Plaid negative income to deposit', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'aaa', payload: { transactionId: 't1', amount: '-100.00', currency: 'USD', date: '2026-07-11', name: 'SALARY' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'acc-1')
    expect(op.activity).toMatchObject({ activityType: 'DEPOSIT', amount: '100' })
  })

  it.each(['0', '-0'])('fails closed for a zero Plaid amount (%s)', amount => {
    expect(() => cashObservationToBridge({
      deploymentId: 'g8', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'zero', payload: { transactionId: 't0', amount, currency: 'USD', date: '2026-07-11' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'acc-1')).toThrow('invalid provider amount')
  })

  it('projects removals as deletes and posted successors as lifecycle migrations', () => {
    const base = {
      deploymentId: 'g8', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'aaa', observedAt: '2026-07-11T00:00:00.000Z',
    }
    const removed = cashObservationToBridge({ ...base, payload: { transactionId: 'old', accountId: 'p1', isRemoved: true } }, 1, 'wf-1')
    expect(removed).toMatchObject({ kind: 'ActivityDelete', resourceId: 'g8:inc-1:old', accountId: 'wf-1' })
    const posted = cashObservationToBridge({ ...base, payload: { transactionId: 'posted', pendingTransactionId: 'pending', amount: '1', currency: 'USD', date: '2026-07-11' } }, 1, 'wf-1')
    expect(posted).toMatchObject({ resourceId: 'g8:inc-1:pending', lifecycleSuccessorId: 'g8:inc-1:posted' })
  })

  it('validates the complete canonical receipt binding', async () => {
    const op = baseOperation()
    await expect(commitObservation(mockTransport(), op)).resolves.toEqual(boundReceipt(op))
    for (const receipt of [
      boundReceipt(op, { accountId: 'wrong' }),
      boundReceipt(op, { lifecycleSuccessorId: 'wrong' }),
      boundReceipt(op, { receiptBinding: 'v1;invalid' }),
    ]) {
      await expect(commitObservation({ commit: async () => receipt }, op, 1)).rejects.toThrow(/binding/i)
    }
  })

  it.each([
    ['ActivityUpsert', 'ActivityUpsert'],
    ['ActivityDelete', 'ActivityDelete'],
    ['ProjectionFence', 'ProjectionFence'],
  ] as const)('serializes %s using the receiver enum value', (kind, expected) => {
    expect(bridgeOperationToWire(baseOperation({ kind })).kind).toBe(expected)
  })

  it('rejects mismatched operation and subject', async () => {
    const op = baseOperation({ kind: 'ActivityDelete' })
    await expect(commitObservation({ commit: async () => boundReceipt(op, { operation: 'activity_upsert' }) }, op, 1)).rejects.toThrow('operation mismatch')
    await expect(commitObservation({ commit: async () => boundReceipt(op, { subject: 'other' }) }, op, 1)).rejects.toThrow('subject mismatch')
  })

  it('retries transport failures', async () => {
    let calls = 0
    const op = baseOperation()
    const transport: BridgeTransport = { commit: async () => { if (++calls < 2) throw new Error('temporary'); return boundReceipt(op) } }
    await expect(commitObservation(transport, op)).resolves.toEqual(boundReceipt(op))
    expect(calls).toBe(2)
  })

  it('exhausts retries and batches sequentially', async () => {
    const failing: BridgeTransport = { commit: async () => { throw new Error('always fails') } }
    await expect(commitObservation(failing, baseOperation(), 2)).rejects.toThrow('always fails')
    const receipts = await commitObservationBatch(mockTransport(), [baseOperation(), baseOperation({ idempotencyKey: 'key-2' })])
    expect(receipts).toHaveLength(2)
  })
})
