import { describe, expect, it } from 'vitest'
import {
  g1FreshPipeline,
  g1IngestObservation,
  g1ProduceRevision,
  g1EnqueueOutbox,
  g1RecordReceipt,
  g1ApplyEdit,
  g1CorrectProvider,
  g1Remove,
  type PipelineState,
} from './lifecycle-pipeline'

// Deterministic hex hashes for reproducible testing
function hash(c: string): string { return c.repeat(64) }
const H_A = hash('a')
const H_B = hash('b')
const H_C = hash('c')
const H_D = hash('d')
const H_E = hash('e')
const H_F = hash('f')

/**
 * G3 property tests: canonical fixture invariants and replay determinism.
 *
 * These tests are state-machine/property style — they verify that
 * reordering, replaying, and repeating operations always produce the
 * same canonical state, and that user overlays survive provider actions.
 */

// ─── Property: canonical revisions are monotonic and deterministic ────────────

describe('G3 canonical revision determinism', () => {
  it('same sequence of observations produces same revision chain', () => {
    const build = (): PipelineState => {
      let s = g1FreshPipeline('d', 'o', 'e', 's')
      s = g1IngestObservation(s, { amount: '100' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
      s = g1ProduceRevision(s, H_A, 'mvp-1')
      s = g1IngestObservation(s, { amount: '50' }, H_B, 'v1', '2026-07-11T11:00:00.000Z')
      s = g1ProduceRevision(s, H_B, 'mvp-1')
      return s
    }
    const a = build()
    const b = build()
    expect(a.canonical!.revision).toBe(b.canonical!.revision)
    expect(a.canonical!.semanticHash).toBe(b.canonical!.semanticHash)
    expect(a.observations).toHaveLength(b.observations.length)
  })

  it('repeated same observation never bumps revision', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    for (let i = 0; i < 5; i++) {
      s = g1IngestObservation(s, { amount: '100' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
      s = g1ProduceRevision(s, H_A, 'mvp-1')
    }
    expect(s.canonical!.revision).toBe(1)
  })

  it('different observations always bump revision', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    s = g1IngestObservation(s, { amount: '100' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    s = g1ProduceRevision(s, H_A, 'mvp-1')
    expect(s.canonical!.revision).toBe(1)

    s = g1IngestObservation(s, { amount: '200' }, H_B, 'v1', '2026-07-11T11:00:00.000Z')
    s = g1ProduceRevision(s, H_B, 'mvp-1')
    expect(s.canonical!.revision).toBe(2)
  })

  it('revision increments are strictly monotonic', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    const hashes = [H_A, H_B, H_C, H_D, H_E, H_F]
    for (let i = 0; i < hashes.length; i++) {
      s = g1IngestObservation(s, { idx: i }, hashes[i], 'v1', `2026-07-11T${10 + i}:00:00.000Z`)
      s = g1ProduceRevision(s, hashes[i], 'mvp-1')
      expect(s.canonical!.revision).toBe(i + 1)
    }
  })
})

// ─── Property: outbox matches canonical chain ─────────────────────────────────

describe('G3 outbox integrity', () => {
  it('every outbox entry has matching canonical revision', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    const ops = [
      () => { s = g1IngestObservation(s, { a: 1 }, H_A, 'v1', '2026-07-11T10:00:00.000Z'); s = g1ProduceRevision(s, H_A, 'mvp-1'); s = g1EnqueueOutbox(s, 'upsert_activity', { a: 1 }); },
      () => { s = g1IngestObservation(s, { a: 2 }, H_B, 'v1', '2026-07-11T11:00:00.000Z'); s = g1ProduceRevision(s, H_B, 'mvp-1'); s = g1EnqueueOutbox(s, 'upsert_activity', { a: 2 }); },
      () => { s = g1Remove(s, 'mvp-1'); },
    ]
    ops.forEach(op => op())

    let lastRev = 0
    for (const entry of s.outbox) {
      expect(entry.canonicalRevision).toBeGreaterThan(lastRev)
      lastRev = entry.canonicalRevision
    }
    expect(lastRev).toBe(s.canonical!.revision)
  })
})

// ─── Property: user overlay survives provider actions ─────────────────────────

describe('G3 overlay preservation', () => {
  it('user overlay survives correction', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    s = g1IngestObservation(s, { amt: '50' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    s = g1ProduceRevision(s, H_A, 'mvp-1')
    s = g1EnqueueOutbox(s, 'upsert_activity', { amt: '50' })
    s = g1RecordReceipt(s, 'wf-1', 'created')
    s = g1ApplyEdit(s, 'Groceries', 'note')

    // Correction
    s = g1CorrectProvider(s, { amt: '55' }, H_B, 'mvp-1')
    expect(s.userOverlay).not.toBeNull()
    expect(s.userOverlay!.category).toBe('Groceries')
    expect(s.userOverlay!.revision).toBe(1) // same revision since values unchanged
  })

  it('user edit revision increments on change, not on identity', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    s = g1IngestObservation(s, { amt: '50' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    s = g1ProduceRevision(s, H_A, 'mvp-1')
    s = g1EnqueueOutbox(s, 'upsert_activity', { amt: '50' })
    s = g1RecordReceipt(s, 'wf-1', 'created')

    s = g1ApplyEdit(s, 'A', null)
    expect(s.userOverlay!.revision).toBe(1)
    s = g1ApplyEdit(s, 'B', null)
    expect(s.userOverlay!.revision).toBe(2)
    s = g1ApplyEdit(s, 'B', null) // same — no bump
    expect(s.userOverlay!.revision).toBe(2)
  })

  it('user overlay survives removal', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    s = g1IngestObservation(s, { amt: '50' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    s = g1ProduceRevision(s, H_A, 'mvp-1')
    s = g1EnqueueOutbox(s, 'upsert_activity', { amt: '50' })
    s = g1RecordReceipt(s, 'wf-1', 'created')
    s = g1ApplyEdit(s, 'Groceries', 'note')

    s = g1Remove(s, 'mvp-1')
    expect(s.userOverlay).not.toBeNull()
    expect(s.userOverlay!.category).toBe('Groceries')
  })
})

// ─── Property: observations are append-only ──────────────────────────────────

describe('G3 append-only observation invariant', () => {
  it('observations array is never mutated in place', () => {
    let s = g1FreshPipeline('d', 'o', 'e', 's')
    const obs1 = s.observations
    s = g1IngestObservation(s, { amt: '100' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    const obs2 = s.observations
    s = g1IngestObservation(s, { amt: '200' }, H_B, 'v1', '2026-07-11T11:00:00.000Z')
    const obs3 = s.observations
    s = g1IngestObservation(s, { amt: '100' }, H_A, 'v1', '2026-07-11T10:00:00.000Z')
    const obs4 = s.observations

    expect(obs1).not.toBe(obs2) // new object
    expect(obs2).not.toBe(obs3) // new object
    expect(obs2).toHaveLength(1)
    expect(obs3).toHaveLength(2)
    // Duplicate observation is not appended
    expect(obs4).toBe(obs3) // same frozen object — no-op
  })
})
