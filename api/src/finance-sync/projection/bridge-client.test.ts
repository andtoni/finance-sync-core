
import { describe, expect, it, vi } from 'vitest'
import {
  idempotencyKey,
  cashObservationToBridge,
  commitObservation,
  commitObservationBatch,
  type BridgeTransport,
  type BridgeDisclosureOperation,
  type BridgeReceipt,
} from './bridge-client'

const mockTransport = (overrides: Partial<BridgeTransport> = {}): BridgeTransport => ({
  commit: async (op) => ({
    subject: op.subject,
    idempotencyKey: op.idempotencyKey,
    accessGeneration: op.accessGeneration,
    operation: op.kind,
    resourceId: op.resourceId,
    outcome: 'Accepted',
  }),
  ...overrides,
})

describe('Bridge client', () => {
  it('generates deterministic idempotency keys', () => {
    const k1 = idempotencyKey('dep-1', 'hash-a')
    const k2 = idempotencyKey('dep-1', 'hash-a')
    const k3 = idempotencyKey('dep-2', 'hash-a')
    expect(k1).toBe(k2)
    expect(k1).not.toBe(k3)
    expect(k1.length).toBe(40)
  })

  it('converts cash observation to bridge operation', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8-test',
      localOwnerId: 'alexis',
      sourceResourceIncarnationId: 'plaid:item-1',
      semanticHash: 'abc123',
      payload: { transactionId: 'txn-1', amount: '-42.50', currency: 'USD', date: '2026-07-11', name: 'AMAZON' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'wf-acc-uuid-1')
    expect(op.kind).toBe('ActivityUpsert')
    expect(op.subject).toBe('alexis')
    expect(op.activity?.accountId).toBe('wf-acc-uuid-1')
    expect(op.activity?.amount).toBe('42.50')
    expect(op.activity?.activityType).toBe('WITHDRAWAL')
    expect(op.idempotencyKey.length).toBe(40)
  })

  it('handles credit as DEPOSIT', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8', localOwnerId: 'alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'aaa',
      payload: { transactionId: 't1', amount: '100.00', currency: 'USD', date: '2026-07-11', name: 'SALARY', transactionType: 'CREDIT' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'acc-1')
    expect(op.activity?.activityTypeOverride).toBe('DEPOSIT')
  })

  it('commits single observation via transport', async () => {
    const transport = mockTransport()
    const op: BridgeDisclosureOperation = {
      idempotencyKey: 'key-1', subject: 'alexis', accessGeneration: 1,
      kind: 'ActivityUpsert', resourceId: 'r-1',
    }
    const receipt = await commitObservation(transport, op)
    expect(receipt.outcome).toBe('Accepted')
  })

  it('retries on transport failure', async () => {
    let calls = 0
    const transport: BridgeTransport = {
      commit: async () => {
        calls++
        if (calls < 2) throw new Error('temporary')
        return { subject: 'a', idempotencyKey: 'k', accessGeneration: 1, operation: 'Upsert', resourceId: 'r', outcome: 'Accepted' }
      },
    }
    const receipt = await commitObservation(transport, { idempotencyKey: 'k', subject: 'a', accessGeneration: 1, kind: 'ActivityUpsert', resourceId: 'r' })
    expect(calls).toBe(2)
    expect(receipt.outcome).toBe('Accepted')
  })

  it('exhausts retries and throws', async () => {
    const transport: BridgeTransport = {
      commit: async () => { throw new Error('always fails') },
    }
    await expect(commitObservation(transport, { idempotencyKey: 'k', subject: 'a', accessGeneration: 1, kind: 'ActivityUpsert', resourceId: 'r' }, 2))
      .rejects.toThrow('always fails')
  })

  it('batches multiple observations', async () => {
    const transport = mockTransport()
    const ops = [
      { idempotencyKey: 'k1', subject: 'alexis', accessGeneration: 1, kind: 'ActivityUpsert' as const, resourceId: 'r1' },
      { idempotencyKey: 'k2', subject: 'alexis', accessGeneration: 1, kind: 'ActivityUpsert' as const, resourceId: 'r2' },
    ]
    const receipts = await commitObservationBatch(transport, ops)
    expect(receipts.length).toBe(2)
    expect(receipts[0].outcome).toBe('Accepted')
    expect(receipts[1].outcome).toBe('Accepted')
  })
})
