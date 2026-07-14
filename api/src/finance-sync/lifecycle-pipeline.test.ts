import { describe, expect, it } from 'vitest'
import { qualifyCashObservation } from './canonical/quarantine'
import { assertCurrency, canonicalObservationIdentity } from './domain'
import {
  g1FreshPipeline,
  g1IngestObservation,
  g1ProduceRevision,
  g1EnqueueOutbox,
  g1RecordReceipt,
  g1ApplyEdit,
  g1CorrectProvider,
  g1Remove,
} from './lifecycle-pipeline'

const H1 = 'a'.repeat(64)
const H2 = 'b'.repeat(64)
const H3 = 'c'.repeat(64)
const H4 = 'd'.repeat(64)
const H5 = 'e'.repeat(64)

describe('G1 synthetic cash lifecycle (pure pipeline)', () => {

  it('observation → canonical revision → outbox → receipt', () => {
    let state = g1FreshPipeline('deploy-g1', 'owner-a', 'activity-1', 'src-res-1')

    state = g1IngestObservation(
      state,
      { amount: '100.00', currency: 'USD', description: 'Test transaction' },
      H1, 'v1', '2026-07-11T12:00:00.000Z',
    )
    expect(state.observations).toHaveLength(1)

    state = g1ProduceRevision(state, H1, 'mvp-1')
    expect(state.canonical).not.toBeNull()
    expect(state.canonical!.revision).toBe(1)

    state = g1EnqueueOutbox(state, 'upsert_activity', { amount: '100.00', currency: 'USD' })
    expect(state.outbox).toHaveLength(1)
    expect(state.outbox[0].canonicalRevision).toBe(1)

    state = g1RecordReceipt(state, 'wf-activity-1', 'created')
    expect(state.receipts).toHaveLength(1)
    expect(state.receipts[0].outcome).toBe('created')
  })

  it('user edit after receipt preserves overlay', () => {
    let state = g1FreshPipeline('deploy-g1', 'owner-a', 'activity-1', 'src-res-1')
    state = g1IngestObservation(state, { amount: '50.00' }, H2, 'v1', '2026-07-11T13:00:00.000Z')
    state = g1ProduceRevision(state, H2, 'mvp-1')
    state = g1EnqueueOutbox(state, 'upsert_activity', { amount: '50.00' })
    state = g1RecordReceipt(state, 'wf-activity-1', 'created')

    state = g1ApplyEdit(state, 'Groceries', 'Weekly shop')
    expect(state.userOverlay).not.toBeNull()
    expect(state.userOverlay!.category).toBe('Groceries')
    expect(state.userOverlay!.revision).toBe(1)

    // Idempotent edit doesn't bump revision
    const unchanged = g1ApplyEdit(state, 'Groceries', 'Weekly shop')
    expect(unchanged.userOverlay!.revision).toBe(1)
  })

  it('provider correction creates new revision, user overlay preserved', () => {
    let state = g1FreshPipeline('deploy-g1', 'owner-a', 'activity-1', 'src-res-1')
    state = g1IngestObservation(state, { amount: '50.00' }, H2, 'v1', '2026-07-11T14:00:00.000Z')
    state = g1ProduceRevision(state, H2, 'mvp-1')
    state = g1EnqueueOutbox(state, 'upsert_activity', { amount: '50.00' })
    state = g1RecordReceipt(state, 'wf-activity-1', 'created')
    state = g1ApplyEdit(state, 'Dining', 'Lunch')

    state = g1CorrectProvider(state, { amount: '55.00', correction: 'tip added' }, H3, 'mvp-1')
    expect(state.canonical!.revision).toBe(2)
    expect(state.outbox).toHaveLength(2)
    expect(state.userOverlay).not.toBeNull()
    expect(state.userOverlay!.category).toBe('Dining')
  })

  it('removal creates final tombstone revision', () => {
    let state = g1FreshPipeline('deploy-g1', 'owner-a', 'activity-1', 'src-res-1')
    state = g1IngestObservation(state, { amount: '25.00' }, H4, 'v1', '2026-07-11T15:00:00.000Z')
    state = g1ProduceRevision(state, H4, 'mvp-1')
    state = g1EnqueueOutbox(state, 'upsert_activity', { amount: '25.00' })
    state = g1RecordReceipt(state, 'wf-activity-1', 'created')

    state = g1Remove(state, 'mvp-1')
    expect(state.canonical!.revision).toBe(2)
    expect(state.outbox).toHaveLength(2)
    expect(state.outbox[1].payload).toMatchObject({ operation: 'remove' })
  })

  it('replay: same observation does not bump revision', () => {
    let state = g1FreshPipeline('deploy-g1', 'owner-a', 'activity-1', 'src-res-1')
    state = g1IngestObservation(state, { amount: '100.00' }, H1, 'v1', '2026-07-11T16:00:00.000Z')
    state = g1ProduceRevision(state, H1, 'mvp-1')
    expect(state.canonical!.revision).toBe(1)

    // Same observation replayed — no new revision (same content)
    state = g1IngestObservation(state, { amount: '100.00' }, H1, 'v1', '2026-07-11T16:00:00.000Z')
    state = g1ProduceRevision(state, H1, 'mvp-1')
    expect(state.canonical!.revision).toBe(1) // No bump
  })

  it('full lifecycle: observe → canon → outbox → receipt → edit → correct → remove', () => {
    let state = g1FreshPipeline('deploy-full', 'owner-a', 'activity-full', 'src-res-full')

    // 1. Initial ingestion
    state = g1IngestObservation(
      state, { amount: '200.00', currency: 'USD', description: 'Full test' },
      H4, 'v1', '2026-07-11T17:00:00.000Z',
    )
    expect(state.observations).toHaveLength(1)

    // 2. Canonical revision
    state = g1ProduceRevision(state, H4, 'mvp-1')
    expect(state.canonical!.revision).toBe(1)

    // 3. Outbox
    state = g1EnqueueOutbox(state, 'upsert_activity', { amount: '200.00', currency: 'USD' })
    expect(state.outbox).toHaveLength(1)

    // 4. Receipt
    state = g1RecordReceipt(state, 'wf-activity-full', 'created')
    expect(state.receipts).toHaveLength(1)

    // 5. User edit
    state = g1ApplyEdit(state, 'Income', 'Freelance payment')
    expect(state.userOverlay!.category).toBe('Income')

    // 6. Provider correction
    state = g1CorrectProvider(state,
      { amount: '210.00', currency: 'USD', description: 'Corrected with fee' },
      H5, 'mvp-1',
    )
    expect(state.canonical!.revision).toBe(2)

    // 7. Second user edit after correction
    state = g1ApplyEdit(state, 'Income', 'Freelance + fee adjustment')
    expect(state.userOverlay!.revision).toBe(2)

    // 8. Removal
    state = g1Remove(state, 'mvp-1')
    expect(state.canonical!.revision).toBe(3)

    // Verify complete history
    expect(state.observations).toHaveLength(1)
    expect(state.outbox).toHaveLength(3)
    expect(state.receipts).toHaveLength(1)
    expect(state.userOverlay!.revision).toBe(2)
  })
})

describe('G1 domain field ownership', () => {
  it('cash ledger observation requires proven currency or quarantines', () => {
    expect(qualifyCashObservation({ amount: '100.00', accountCurrency: null }).publishable).toBe(false)
    expect(qualifyCashObservation({ amount: '100.00', accountCurrency: 'USD' }).publishable).toBe(true)
    expect(() => qualifyCashObservation({ amount: '-0', accountCurrency: 'USD' })).toThrow('INVALID amount')
  })

  it('assertCurrency rejects empty/null and trims', () => {
    expect(() => assertCurrency(null)).toThrow()
    expect(() => assertCurrency('')).toThrow()
    expect(() => assertCurrency('  ')).toThrow()
    expect(assertCurrency(' usd ')).toBe('USD')
  })

  it('canonical identity is deterministic from observation envelope', () => {
    const id1 = canonicalObservationIdentity({
      deploymentId: 'd1', localOwnerId: 'o1', sourceResourceIncarnationId: 's1',
      providerVersion: 'v1', semanticHash: H1, hashSpecificationVersion: 1,
    })
    const id2 = canonicalObservationIdentity({
      deploymentId: 'd1', localOwnerId: 'o1', sourceResourceIncarnationId: 's1',
      providerVersion: 'v1', semanticHash: H1, hashSpecificationVersion: 1,
    })
    expect(id1).toBe(id2)

    const id3 = canonicalObservationIdentity({
      deploymentId: 'd1', localOwnerId: 'o1', sourceResourceIncarnationId: 's1',
      providerVersion: 'v2', semanticHash: H1, hashSpecificationVersion: 1,
    })
    expect(id1).not.toBe(id3)
  })
})
