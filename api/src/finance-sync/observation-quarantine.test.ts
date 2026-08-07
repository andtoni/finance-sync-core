import { describe, expect, it } from 'vitest'
import { createObservationQuarantineControl } from './observation-quarantine'
import type { FinanceSyncSql } from './storage/pg'

const request = {
  sourceResourceIncarnationId: '00000000-0000-4000-8000-000000000001',
  providerVersion: 'investec:txn:synthetic-1',
  semanticHash: 'a'.repeat(64),
  hashSpecificationVersion: 3,
  reason: 'exact synthetic observation correction',
}

function fakeSql() {
  const queries: string[] = []
  let inserted = true
  const sql = (async (strings: TemplateStringsArray) => {
    const text = strings.join('?')
    queries.push(text)
    if (/FROM raw_observations/i.test(text)) return [{ observation_id: '00000000-0000-4000-8000-000000000002', transaction_id: 'synthetic-1' }]
    if (/INSERT INTO observation_quarantines/i.test(text)) {
      if (!inserted) return []
      inserted = false
      return [{ quarantine_id: '00000000-0000-4000-8000-000000000003', created_at: '2026-01-01T00:00:00.000Z' }]
    }
    if (/FROM observation_quarantines/i.test(text)) return [{ quarantine_id: '00000000-0000-4000-8000-000000000003', created_at: '2026-01-01T00:00:00.000Z' }]
    if (/SELECT state, count/i.test(text)) return [
      { state: 'pending', count: 2 }, { state: 'uncertain', count: 1 }, { state: 'delivered', count: 1 },
    ]
    if (/WITH changed AS/i.test(text)) return [{ count: 3 }]
    return []
  }) as unknown as FinanceSyncSql
  ;(sql as unknown as { begin: (fn: (tx: FinanceSyncSql) => Promise<unknown>) => Promise<unknown> }).begin = fn => fn(sql)
  return { sql, queries }
}

describe('exact observation quarantine', () => {
  it('records an immutable tombstone and suppresses unresolved matching outbox rows', async () => {
    const fixture = fakeSql()
    const control = createObservationQuarantineControl(fixture.sql, 'deployment-1')
    const result = await control.create('wealthfolio#owner-a', request)
    expect(result).toEqual({
      quarantineId: '00000000-0000-4000-8000-000000000003',
      observationId: '00000000-0000-4000-8000-000000000002',
      resourceId: 'deployment-1:00000000-0000-4000-8000-000000000001:synthetic-1',
      providerVersion: request.providerVersion,
      semanticHash: request.semanticHash,
      alreadyQuarantined: false,
      suppressedOutboxRows: 3,
      deliveredOutboxRows: 1,
      uncertainOutboxRows: 1,
      createdAt: '2026-01-01T00:00:00.000Z',
    })
    expect(fixture.queries.some(query => /observation_quarantines/i.test(query))).toBe(true)
  })

  it('is idempotent for the same exact observation identity', async () => {
    const fixture = fakeSql()
    const control = createObservationQuarantineControl(fixture.sql, 'deployment-1')
    await control.create('wealthfolio#owner-a', request)
    const result = await control.create('wealthfolio#owner-a', request)
    expect(result.alreadyQuarantined).toBe(true)
  })
})
