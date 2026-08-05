import { describe, expect, it } from 'vitest'
import {
  idempotencyKey,
  cashObservationToBridge,
  commitObservation,
  commitObservationBatch,
  bridgeOperationToWire,
  currentStateToBridge,
  type BridgeTransport,
  type BridgeDisclosureOperation,
  type BridgeReceipt,
} from './bridge-client'

const wireOperation = {
  ActivityUpsert: 'activity_upsert', ActivityDelete: 'activity_delete',
  ProjectionFence: 'projection_fence', CurrentStateUpsert: 'current_state_upsert',
} as const
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
  return `v3;${fields.map(value => `${encoder.encode(value).byteLength}:${value}`).join('')}`
}

function boundReceipt(op: BridgeDisclosureOperation, overrides: Partial<BridgeReceipt> = {}): BridgeReceipt {
  const successor = op.lifecycleSuccessorId ?? null
  const semanticHash = op.kind === 'CurrentStateUpsert' ? op.semanticHash ?? null : null
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
    bindingVersion: 3,
    semanticHash,
    receiptBinding: binding([
      op.deploymentId, op.subject, op.idempotencyKey, String(op.accessGeneration), wireOperation[op.kind],
      op.resourceId, op.accountId, successor === null ? '0' : '1', successor ?? '',
      'accepted', semanticHash === null ? '0' : '1', semanticHash ?? '',
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
    expect(op.activity).toMatchObject({ accountId: 'wf-acc-uuid-1', amount: '42.50', activityType: 'WITHDRAWAL', status: 'POSTED', notes: 'AMAZON' })
  })

  it('projects the provider merchant display as notes without projecting category hints', () => {
    const merchantContext = { merchantKeyVersion: 1, normalizationVersion: 1, rawDescription: 'RAW', merchantDisplay: 'Merchant', merchantKey: 'v1:key', direction: 'outflow', activityType: null, providerHint: { personalFinanceCategory: { primary: 'FOOD' } }, provenance: { source: 'provider_observation', provider: 'plaid' } }
    const op = cashObservationToBridge({
      deploymentId: 'g8', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'merchant', payload: { transactionId: 't1', amount: '12', currency: 'USD', date: '2026-07-11', merchantContext },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'acc-1')
    expect(op.activity?.merchantContext).toEqual(merchantContext)
    expect(op.activity?.notes).toBe('Merchant')
    expect(bridgeOperationToWire(op)).toMatchObject({ activity: { notes: 'Merchant', merchant_context: { merchant_key: 'v1:key', merchant_key_version: 1, normalization_version: 1 } } })
    expect(Object.keys((bridgeOperationToWire(op).activity as Record<string, unknown>).merchant_context as object)).toEqual(['merchant_key', 'merchant_key_version', 'normalization_version'])
  })

  it('preserves provider decimal text while normalizing sign and direction', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8-test', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'precise', payload: { transactionId: 'txn-precise', amount: '-123456789.123456789123456789', currency: 'ZAR', date: '2026-07-11' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'wf-acc-uuid-1')
    expect(op.activity).toMatchObject({ activityType: 'DEPOSIT', amount: '123456789.123456789123456789' })
  })

  it('normalizes Plaid negative income to deposit', () => {
    const op = cashObservationToBridge({
      deploymentId: 'g8', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1',
      semanticHash: 'aaa', payload: { transactionId: 't1', amount: '-100.00', currency: 'USD', date: '2026-07-11', name: 'SALARY' },
      observedAt: '2026-07-11T00:00:00.000Z',
    }, 1, 'acc-1')
    expect(op.activity).toMatchObject({ activityType: 'DEPOSIT', amount: '100.00', notes: 'SALARY' })
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

  it('serializes and receipt-binds the complete current-state payload hash', async () => {
    const currentState = {
      accountId: 'wf-1', providerConnectionId: 'connection-1', providerIncarnationId: 'inc-1', providerGeneration: 2,
      observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '30', liabilityBalance: '20',
      positions: [{ symbol: 'VTI', quantity: '2', price: '40', marketValue: '80', currency: 'USD' }],
    }
    const { currentStateSemanticHash } = await import('./current-state')
    expect(currentStateSemanticHash(currentState)).toBe('5ef55832c52dae3d2b629d685ea7e78472813c74430c44f4b7f791383e2ce96a')
    const op = currentStateToBridge({ deploymentId: 'dep-1', localOwnerId: 'urn:wealthfolio:test#alexis', sourceResourceIncarnationId: 'inc-1' }, currentState, currentStateSemanticHash(currentState), 2, 'connection-1')
    expect(bridgeOperationToWire(op)).toEqual({
      idempotency_key: op.idempotencyKey,
      subject: 'urn:wealthfolio:test#alexis', access_generation: 2, kind: 'CurrentStateUpsert', resource_id: 'dep-1:inc-1:current-state:wf-1',
      current_state: { account_id: 'wf-1', provider_connection_id: 'connection-1', provider_incarnation_id: 'inc-1', provider_generation: 2, observed_at: '2026-07-18T12:00:00.000Z', revision: 1784376000000, as_of_date: '2026-07-18', currency: 'USD', cash_balance: '30', liability_balance: '20', positions: [{ symbol: 'VTI', quantity: '2', price: '40', market_value: '80', currency: 'USD' }] },
      semantic_hash: op.semanticHash,
    })
    await expect(commitObservation(mockTransport(), op, 1)).resolves.toEqual(boundReceipt(op))
    const mutated = { ...op, currentState: { ...currentState, cashBalance: '31' } }
    expect(() => bridgeOperationToWire(mutated)).toThrow('payload binding mismatch')
    await expect(commitObservation(mockTransport(), mutated, 1)).rejects.toThrow('payload binding mismatch')
  })

  it('validates receiver-canonical v3 12-field receipt vectors for every operation kind', async () => {
    const op = baseOperation()
    expect(boundReceipt(op).receiptBinding).toBe('v3;5:dep-127:urn:wealthfolio:test#alexis5:key-11:115:activity_upsert10:resource-19:account-11:00:8:accepted1:00:')
    await expect(commitObservation(mockTransport(), op)).resolves.toEqual(boundReceipt(op))
    for (const kind of ['ActivityUpsert', 'ActivityDelete', 'ProjectionFence'] as const) {
      expect(boundReceipt(baseOperation({ kind })).receiptBinding).toContain('8:accepted1:00:')
    }
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
