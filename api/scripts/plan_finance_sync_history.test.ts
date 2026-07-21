import { describe, expect, it } from 'vitest'
import { planFinanceSyncHistory, type HistoricalReader, type HistoricalRow } from './plan_finance_sync_history'

const reader = (rows: readonly HistoricalRow[]): HistoricalReader => ({ read: () => rows })
const mappings = {
  accountMappings: [
    { source: 'tarn' as const, sourceAccountRef: 'a', canonicalAccountId: 'canonical-a', cutoverWatermark: '2026-01-01T00:00:00.000Z' },
    { source: 'lunch_money' as const, sourceAccountRef: 'lm-a', canonicalAccountId: 'canonical-a', cutoverWatermark: '2026-01-01T00:00:00.000Z' },
  ],
  categoryMappings: [
    { source: 'tarn' as const, sourceCategoryRef: 'food', canonicalCategoryKey: 'food', taxonomyId: 'tax-1', categoryId: 'cat-food' },
    { source: 'lunch_money' as const, sourceCategoryRef: '10', canonicalCategoryKey: 'food', taxonomyId: 'tax-1', categoryId: 'cat-food' },
  ],
}
const base = {
  effectiveAt: '2025-01-02T00:00:00.000Z', amount: '-12.50', currency: 'USD', rawDescription: 'Original text',
  activityType: 'WITHDRAWAL', direction: 'outflow' as const, merchantKey: 'merchant-1', merchantKeyVersion: 1,
  normalizationVersion: 1, firstLabelDate: '2025-01-02T00:00:00.000Z', lastLabelDate: '2025-01-03T00:00:00.000Z',
  labelCount: 2, agreementState: 'agreement' as const,
}

describe('historical Finance Sync migration planner', () => {
  it('emits strict canonical ledger/evidence lanes, overlap and rollback manifests', async () => {
    const manifest = await planFinanceSyncHistory({
      tarn: reader([{ ...base, sourceId: 't-1', accountRef: 'a', categoryRef: 'food', userConfirmedCategory: true }]),
      lunchMoney: reader([{ ...base, sourceId: 'lm-1', accountRef: 'lm-a', categoryRef: '10', userConfirmedCategory: true }]),
    }, { importRunId: 'run-1', ...mappings })
    expect(manifest.mode).toBe('dry-run')
    expect(manifest.ledger).toHaveLength(2)
    expect(manifest.ledger.every(row => row.activityType === 'WITHDRAWAL' && row.cutoverWatermark === mappings.accountMappings[0].cutoverWatermark)).toBe(true)
    expect(manifest.learningEvidence.map(x => x.authority).sort()).toEqual(['historical_hint', 'user_confirmed'])
    expect(manifest.learningEvidence[0]).toMatchObject({ accountId: 'canonical-a', taxonomyId: 'tax-1', categoryId: 'cat-food', merchantKeyVersion: 1, normalizationVersion: 1, labelCount: 2, agreementState: 'agreement' })
    expect(manifest.overlapCandidates).toHaveLength(1)
    expect(manifest.rollback.ledgerIdentities).toEqual(manifest.ledger.map(x => x.identity))
  })

  it('fails closed on missing explicit semantics and ambiguous mappings', async () => {
    const invalid = (overrides: Partial<HistoricalRow>) => reader([{ ...base, sourceId: 'x', accountRef: 'a', ...overrides } as HistoricalRow])
    await expect(planFinanceSyncHistory({ tarn: invalid({ activityType: '' }), lunchMoney: reader([]) }, { importRunId: 'run', ...mappings })).rejects.toThrow('activityType')
    await expect(planFinanceSyncHistory({ tarn: invalid({ categoryRef: 'food', merchantKey: undefined }), lunchMoney: reader([]) }, { importRunId: 'run', ...mappings })).rejects.toThrow('merchantKey')
    await expect(planFinanceSyncHistory({ tarn: reader([]), lunchMoney: reader([]) }, { importRunId: 'run', accountMappings: mappings.accountMappings, categoryMappings: [...mappings.categoryMappings, { ...mappings.categoryMappings[0], taxonomyId: 'other' }] })).rejects.toThrow('category mapping collision')
    await expect(planFinanceSyncHistory({ tarn: reader([]), lunchMoney: reader([]) }, { importRunId: 'run', ...mappings, apply: true })).rejects.toThrow('dry-run only')
  })
})
