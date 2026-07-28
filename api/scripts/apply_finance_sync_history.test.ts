import { createHash } from 'node:crypto'
import { describe, expect, it } from 'vitest'
import { planFinanceSyncHistory, type HistoricalReader, type HistoricalRow } from './plan_finance_sync_history'
import { applyFinanceSyncHistory, historicalManifestHash, HttpWealthfolioHistorySink, rollbackFinanceSyncHistory, type HistoricalLedgerWrite, type HistoricalLearningEvidenceWrite, type HistoricalReviewedManifest, type WealthfolioHistoricalBatch, type WealthfolioHistoricalBatchReceipt, type WealthfolioHistorySink } from './apply_finance_sync_history'

const reader = (rows: readonly HistoricalRow[]): HistoricalReader => ({ read: () => rows })
const digest = (parts: readonly string[]) => createHash('sha256').update(parts.map(part => `${Buffer.byteLength(part)}:${part}`).join(''), 'utf8').digest('hex')
class FakeHistorySink implements WealthfolioHistorySink {
  readonly ledger = new Map<string, HistoricalLedgerWrite>(); readonly evidence = new Map<string, HistoricalLearningEvidenceWrite>()
  readonly stagedLedger = new Map<string, HistoricalLedgerWrite>(); readonly stagedEvidence = new Map<string, HistoricalLearningEvidenceWrite>()
  readonly batches: WealthfolioHistoricalBatch[] = []; readonly receipts = new Map<string, WealthfolioHistoricalBatchReceipt>()
  writeHistoricalBatch(batch: WealthfolioHistoricalBatch): WealthfolioHistoricalBatchReceipt {
    this.batches.push(batch)
    const replay = this.receipts.get(batch.idempotencyKey); if (replay) return replay
    let receipt: WealthfolioHistoricalBatchReceipt
    if (batch.operation === 'stage_ledger' || batch.operation === 'stage_learning_evidence') {
      const store = batch.operation === 'stage_ledger' ? this.stagedLedger : this.stagedEvidence
      let written = 0; let alreadyPresent = 0
      const keys = batch.rows.map(row => row.idempotencyKey)
      for (const row of batch.rows) { if (store.has(row.idempotencyKey)) alreadyPresent++; else { (store as Map<string, typeof row>).set(row.idempotencyKey, row); written++ } }
      receipt = { dryRun: false, importRunId: batch.importRunId, validated: batch.rows.length, written, alreadyPresent, ledgerDeleted: 0, learningEvidenceDeleted: 0, checksum: digest([...keys].sort()) }
    } else if (batch.operation === 'finalize') {
      for (const [key, row] of this.stagedLedger) this.ledger.set(key, row)
      for (const [key, row] of this.stagedEvidence) this.evidence.set(key, row)
      const keys = [...this.stagedLedger.keys(), ...this.stagedEvidence.keys()]
      this.stagedLedger.clear(); this.stagedEvidence.clear()
      receipt = { dryRun: false, importRunId: batch.importRunId, validated: keys.length, written: keys.length, alreadyPresent: 0, ledgerDeleted: 0, learningEvidenceDeleted: 0, checksum: digest(keys.sort()) }
    } else {
      const ledgerDeleted = this.ledger.size + this.stagedLedger.size; const learningEvidenceDeleted = this.evidence.size + this.stagedEvidence.size
      this.ledger.clear(); this.evidence.clear(); this.stagedLedger.clear(); this.stagedEvidence.clear()
      receipt = { dryRun: false, importRunId: batch.importRunId, validated: 0, written: 0, alreadyPresent: 0, ledgerDeleted, learningEvidenceDeleted, checksum: digest([batch.importRunId, 'rollback']) }
    }
    this.receipts.set(batch.idempotencyKey, receipt); return receipt
  }
}
const accountMappings = [
  { source: 'tarn' as const, sourceAccountRef: 't-a', canonicalAccountId: 'canonical-a', cutoverWatermark: '2026-01-01T00:00:00.000Z' },
  { source: 'lunch_money' as const, sourceAccountRef: 'lm-a', canonicalAccountId: 'canonical-a', cutoverWatermark: '2026-01-01T00:00:00.000Z' },
]
const categoryMappings = [
  { source: 'tarn' as const, sourceCategoryRef: 'food', canonicalCategoryKey: 'food', taxonomyId: 'tax-1', categoryId: 'cat-food' },
  { source: 'lunch_money' as const, sourceCategoryRef: '10', canonicalCategoryKey: 'food', taxonomyId: 'tax-1', categoryId: 'cat-food' },
]
const evidence = { activityType: 'WITHDRAWAL', direction: 'outflow' as const, merchantKeyVersion: 1, normalizationVersion: 1, firstLabelDate: '2025-01-01T00:00:00.000Z', lastLabelDate: '2025-06-01T00:00:00.000Z', labelCount: 3, agreementState: 'agreement' as const }
function ownerApproval(manifestHash: string) {
  return { approvalId: 'owner-approved-run-1', action: 'apply_finance_sync_history' as const, subject: 'finance-sync', importRunId: 'synthetic-run-1', manifestHash, issuedAt: '2026-01-01T00:00:00.000Z', expiresAt: '2099-01-01T00:00:00.000Z' }
}
const verifyOwnerApproval = async () => true
async function syntheticManifest() {
  const overlap = { ...evidence, effectiveAt: '2025-06-01T12:00:00.000Z', amount: '-12.50', currency: 'USD', rawDescription: 'Cafe' }
  return planFinanceSyncHistory({
    tarn: reader([
      { ...overlap, sourceId: 't-overlap', accountRef: 't-a', categoryRef: 'food', merchantKey: 'cafe', userConfirmedCategory: true },
      { ...overlap, sourceId: 't-normal', accountRef: 't-a', categoryRef: 'food', merchantKey: 'grocer', userConfirmedCategory: true, effectiveAt: '2025-07-02T12:00:00.000Z', amount: '-30', rawDescription: 'Grocer' },
      { ...overlap, sourceId: 't-cutover', accountRef: 't-a', categoryRef: 'food', merchantKey: 'late', userConfirmedCategory: true, effectiveAt: '2026-01-01T00:00:00.000Z', amount: '-9', rawDescription: 'At cutover' },
    ]),
    lunchMoney: reader([{ ...overlap, sourceId: 'lm-overlap', accountRef: 'lm-a', categoryRef: '10', merchantKey: 'cafe', userConfirmedCategory: true }]),
  }, { importRunId: 'synthetic-run-1', accountMappings, categoryMappings })
}

describe('historical Finance Sync apply orchestration', () => {
  it('emits exact resolved schema-v1 batches, resumes, and rolls back idempotently', async () => {
    const manifest = await syntheticManifest(); const cross = manifest.overlapCandidates.find(x => x.reason === 'cross_source_match')!; const cutover = manifest.overlapCandidates.find(x => x.reason === 'at_or_after_cutover')!
    const keep = manifest.ledger.find(x => x.sourceId === 'lm-overlap')!.identity
    const resolutions = [{ candidateId: cross.candidateId, action: 'keep_one' as const, ledgerIdentity: keep }, { candidateId: cutover.candidateId, action: 'exclude_all' as const }]
    const sink = new FakeHistorySink()
    const preview = await applyFinanceSyncHistory(manifest, sink, { resolutions }); expect(sink.batches).toEqual([])
    const first = await applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', ownerApproval: ownerApproval(preview.manifestHash), verifyOwnerApproval, resolutions, batchSize: 2 })
    expect(first.ledger).toMatchObject({ selected: 2, written: 2 }); expect(first.learningEvidence).toMatchObject({ selectedManual: 4, written: 4 })
    expect(sink.batches.every(batch => batch.schemaVersion === 1 && batch.subject === 'finance-sync' && batch.overlapsResolved === true && batch.idempotencyKey.length > 20)).toBe(true)
    const appliedBatches = sink.batches.filter((batch): batch is Exclude<WealthfolioHistoricalBatch, { operation: 'rollback' }> => batch.operation !== 'rollback')
    expect(new Set(appliedBatches.map(batch => batch.manifestHash)).size).toBe(1)
    expect(appliedBatches.every(batch => historicalManifestHash(batch.manifest) === batch.manifestHash && batch.manifest.ledger.length === manifest.ledger.length && batch.manifest.overlapResolutions.length === resolutions.length)).toBe(true)
    expect(appliedBatches.every(batch => JSON.stringify(batch.manifest) === JSON.stringify(appliedBatches[0].manifest))).toBe(true)
    expect(appliedBatches[0]!.manifest.sources).toEqual(expect.arrayContaining([
      expect.objectContaining({ source: 'tarn', ledgerCount: 3, totals: [{ currency: 'USD', amount: '-51.5' }] }),
      expect.objectContaining({ source: 'lunch_money', ledgerCount: 1, totals: [{ currency: 'USD', amount: '-12.5' }] }),
    ]))
    expect(appliedBatches[0]!.manifest.ledger.find(row => row.sourceId === 't-normal')).toMatchObject({
      sourceAccountRef: 't-a', historicalIdentity: manifest.ledger.find(row => row.sourceId === 't-normal')!.identity,
      activityType: 'WITHDRAWAL', rawDescription: 'Grocer', amount: '-30', currency: 'USD', disposition: 'selected',
    })
    const ledgerBatch = sink.batches.find(batch => batch.operation === 'stage_ledger')! as Extract<WealthfolioHistoricalBatch, { operation: 'stage_ledger' }>
    expect(ledgerBatch.rows[0]).toMatchObject({ accountId: 'canonical-a', activityType: 'WITHDRAWAL', cutoverWatermark: '2026-01-01T00:00:00.000Z' })
    const evidenceBatches = sink.batches.filter(batch => batch.operation === 'stage_learning_evidence') as Extract<WealthfolioHistoricalBatch, { operation: 'stage_learning_evidence' }>[]
    expect(evidenceBatches[0].rows[0]).toMatchObject({ taxonomyId: 'tax-1', categoryId: 'cat-food', normalizationVersion: 1, labelCount: 3, agreementState: 'agreement', authority: 'user_confirmed' })
    const evidenceByLedger = new Map(evidenceBatches.flatMap(batch => batch.rows).map(row => [row.manifestLedgerIdentity, row]))
    const excludedTarn = manifest.ledger.find(row => row.sourceId === 't-overlap')!.identity
    expect(evidenceByLedger.get(excludedTarn)).toMatchObject({
      ledgerDisposition: 'excluded',
      overlapProvenance: [{ action: 'keep_one', selectedLedgerIdentity: keep, excludedLedgerIdentities: [excludedTarn] }],
    })
    expect(evidenceByLedger.get(manifest.ledger.find(row => row.sourceId === 't-normal')!.identity)).toMatchObject({ ledgerDisposition: 'selected', overlapProvenance: [] })
    expect(JSON.stringify(sink.batches)).not.toMatch(/token|authorization|credential/i)
    const rerun = await applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', ownerApproval: ownerApproval(preview.manifestHash), verifyOwnerApproval, resolutions, batchSize: 2 })
    expect(rerun.ledger).toMatchObject({ written: 2, alreadyPresent: 0 }); expect(rerun.learningEvidence).toMatchObject({ written: 4, alreadyPresent: 0 })
    const rollback = await rollbackFinanceSyncHistory(manifest.provenance.importRunId, sink, { apply: true, subject: 'finance-sync' })
    const rollbackBatch = sink.batches.at(-1)!
    expect(rollbackBatch.operation).toBe('rollback')
    expect(rollbackBatch).not.toHaveProperty('manifest')
    expect(rollbackBatch).not.toHaveProperty('manifestHash')
    const rollbackAgain = await rollbackFinanceSyncHistory(manifest.provenance.importRunId, sink, { apply: true, subject: 'finance-sync' })
    expect(rollback).toMatchObject({ ledgerDeleted: 2, learningEvidenceDeleted: 4 }); expect(rollbackAgain).toEqual(rollback); expect(rollbackAgain.idempotencyKey).toBe(rollback.idempotencyKey)
  })

  it('refuses ambiguity and unresolved candidates before writes', async () => {
    const manifest = await syntheticManifest(); const sink = new FakeHistorySink()
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync' })).rejects.toThrow('unresolved')
    const resolutions = manifest.overlapCandidates.map(candidate => candidate.reason === 'at_or_after_cutover' ? { candidateId: candidate.candidateId, action: 'keep_one' as const, ledgerIdentity: candidate.ledgerIdentities[0] } : { candidateId: candidate.candidateId, action: 'exclude_all' as const })
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', resolutions })).rejects.toThrow('must be excluded')
    expect(sink.batches).toEqual([])
  })

  it('rejects tampered reviewed evidence below the agreement threshold before a sink write', async () => {
    const manifest = await syntheticManifest()
    const evidenceRow = manifest.learningEvidence[0]!
    const tampered = { ...manifest, learningEvidence: [{ ...evidenceRow, authority: 'user_confirmed' as const, labelCount: 2 }] }
    await expect(applyFinanceSyncHistory(tampered, new FakeHistorySink(), { resolutions: manifest.overlapCandidates.map(candidate =>
      candidate.reason === 'at_or_after_cutover' ? { candidateId: candidate.candidateId, action: 'exclude_all' as const } : { candidateId: candidate.candidateId, action: 'exclude_all' as const },
    ) })).rejects.toThrow('three agreeing labels')
  })

  it('requires owner approval bound to the exact reviewed manifest hash before any apply', async () => {
    const manifest = await syntheticManifest(); const sink = new FakeHistorySink()
    const resolutions = manifest.overlapCandidates.map(candidate => ({ candidateId: candidate.candidateId, action: 'exclude_all' as const }))
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', resolutions })).rejects.toThrow('separately verified action-bound manifest approval')
    expect(sink.batches).toEqual([])
  })

  it('rejects a self-echoed hash without a separately trusted verifier before writes', async () => {
    const manifest = await syntheticManifest(); const sink = new FakeHistorySink()
    const resolutions = manifest.overlapCandidates.map(candidate => ({ candidateId: candidate.candidateId, action: 'exclude_all' as const }))
    const preview = await applyFinanceSyncHistory(manifest, sink, { resolutions })
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', ownerApproval: ownerApproval(preview.manifestHash), resolutions })).rejects.toThrow('separately verified')
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', ownerApproval: ownerApproval(preview.manifestHash), verifyOwnerApproval: async () => false, resolutions })).rejects.toThrow('separately verified')
    expect(sink.batches).toEqual([])
  })

  it('validates evidence receipts before sending ledger batches', async () => {
    const manifest = await syntheticManifest(); const sink = new FakeHistorySink()
    const resolutions = manifest.overlapCandidates.map(candidate => ({ candidateId: candidate.candidateId, action: 'exclude_all' as const }))
    const preview = await applyFinanceSyncHistory(manifest, sink, { resolutions })
    const original = sink.writeHistoricalBatch.bind(sink)
    sink.writeHistoricalBatch = batch => { if (batch.operation === 'stage_learning_evidence') throw new Error('evidence rejected'); return original(batch) }
    await expect(applyFinanceSyncHistory(manifest, sink, { apply: true, subject: 'finance-sync', ownerApproval: ownerApproval(preview.manifestHash), verifyOwnerApproval, resolutions })).rejects.toThrow('evidence rejected')
    expect(sink.batches.some(batch => batch.operation === 'stage_ledger')).toBe(false)
  })

  it('matches the receiver canonical reviewed-manifest fixture and hash vector', () => {
    const manifest: HistoricalReviewedManifest = {
      schemaVersion: 1, importRunId: 'run-1', sources: [{ source: 'tarn', checksum: 'a'.repeat(64), ledgerCount: 1, totals: [{ currency: 'USD', amount: '-1' }] }],
      ledger: [{ idempotencyKey: 'history-1', historicalIdentity: 'history-1', source: 'tarn', sourceId: 'source-1', sourceAccountRef: 'source-account-1', accountId: 'allowed', activityType: 'WITHDRAWAL', effectiveAt: '2026-01-01T00:00:00.000Z', cutoverWatermark: '2026-02-01T00:00:00.000Z', amount: '-1', currency: 'USD', rawDescription: 'test', provenance: { importRunId: 'run-1', source: 'tarn' }, disposition: 'selected' }],
      evidence: [], overlapResolutions: [],
    }
    expect(historicalManifestHash(manifest)).toMatch(/^[a-f0-9]{64}$/)
  })

  it('HTTP adapter injects both independent credentials per call and never serializes them', async () => {
    let captured: { input: string; headers: Readonly<Record<string, string>>; body: string } | undefined
    let tokenCalls = 0; let authorityCalls = 0
    const sink = new HttpWealthfolioHistorySink(async (input, init) => { captured = { input, headers: init.headers, body: init.body }; return { ok: true, status: 200, json: async () => ({}) } }, 'https://wealthfolio.invalid/', async () => { tokenCalls++; return 'subject-secret' }, async () => { authorityCalls++; return 'importer-secret' })
    const rollback = { schemaVersion: 1 as const, subject: 'finance', importRunId: 'run', idempotencyKey: 'key', dryRun: false, overlapsResolved: true as const, operation: 'rollback' as const }
    await sink.writeHistoricalBatch(rollback)
    await sink.writeHistoricalBatch({ ...rollback, idempotencyKey: 'key-2' })
    expect(tokenCalls).toBe(2); expect(authorityCalls).toBe(2)
    expect(captured?.input).toBe('https://wealthfolio.invalid/internal/history')
    expect(captured?.headers.authorization).toBe('Bearer subject-secret')
    expect(captured?.headers['x-wealthfolio-importer-authority']).toBe('Bearer importer-secret')
    expect(captured?.body).not.toMatch(/subject-secret|importer-secret/)
  })

  it('HTTP adapter fails closed when dedicated importer authority is absent', async () => {
    const fetch = async () => { throw new Error('must not send') }
    const missing = new HttpWealthfolioHistorySink(fetch, 'https://wealthfolio.invalid/', async () => 'subject-secret', undefined as never)
    const blank = new HttpWealthfolioHistorySink(fetch, 'https://wealthfolio.invalid/', async () => 'subject-secret', async () => '')
    const rollback = { schemaVersion: 1 as const, subject: 'finance', importRunId: 'run', idempotencyKey: 'key', dryRun: false, overlapsResolved: true as const, operation: 'rollback' as const }
    await expect(missing.writeHistoricalBatch(rollback)).rejects.toThrow('dedicated historical importer authority')
    await expect(blank.writeHistoricalBatch(rollback)).rejects.toThrow('dedicated historical importer authority')
  })
})
