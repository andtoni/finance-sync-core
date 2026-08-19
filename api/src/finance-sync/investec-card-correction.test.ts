import { describe, expect, it } from 'vitest'
import { createInvestecCardCurrencyCorrectionControl } from './investec-card-correction'
import { investecCardObservation } from './investec-card-observation'
import type { RuntimeConnection, SyncRuntimeDependencies } from './runtime'
import type { FinanceSyncSql } from './storage/pg'

const subject = 'urn:wealthfolio:test#owner'
const sourceResourceIncarnationId = '00000000-0000-4000-8000-000000000001'
const providerAccountId = 'provider-account'
const providerVersion = `investec:card:investec-card:${providerAccountId}:reference-R1000`
const sourcePayload = {
  accountNumber: '10000000002', centsAmount: 100000, currencyCode: 'ZAR', type: 'card',
  dateTime: '2026-08-06T12:00:00.000Z', reference: 'R1000',
  merchant: { name: 'SYNTHETIC SERVICE FEE' }, card: { display: '••••0011' },
}
const sourceObservation = investecCardObservation({
  deploymentId: 'dep-1', localOwnerId: subject, sourceResourceIncarnationId,
  providerAccountId, accountCurrency: 'ZAR', payload: sourcePayload,
})
if (!sourceObservation) throw new Error('source fixture did not normalize')

const connection = {
  deploymentId: 'dep-1', provider: 'investec', connectionId: 'connection:provider-account',
  clientId: 'client', clientSecret: 'secret', apiKey: 'key', localOwnerId: subject,
  sourceResourceIncarnationId, accountIds: [providerAccountId],
  wealthfolioAccountIds: { [providerAccountId]: 'wealthfolio-account' },
  providerGeneration: 1, receiverAccessGeneration: 1, secretReference: 'INVESTEC',
  eligibleProductNames: ['Private Bank Account'], fromDate: '2026-08-01', toDate: '2026-08-31',
} as RuntimeConnection

describe('owner-authorized Investec card currency correction', () => {
  it('appends corrected USD evidence and projects it through the normal path', async () => {
    const persisted: unknown[] = []
    const sql = (async () => [{
      observation_id: 'observation-1', quarantine_id: 'quarantine-1',
      payload: sourceObservation.payload, raw_payload: sourcePayload,
      observed_at: sourceObservation.observedAt, provider_account_id: providerAccountId,
      account_currency: 'ZAR',
    }]) as unknown as FinanceSyncSql
    const deps = {
      loadConnections: async () => [connection],
      persistAndEnqueue: async (_connection: RuntimeConnection, observations: readonly unknown[]) => {
        persisted.push(...observations)
        return 1
      },
      projectPending: async () => ({ delivered: 1, failed: 0 }),
    } as unknown as SyncRuntimeDependencies
    const control = createInvestecCardCurrencyCorrectionControl(sql, 'dep-1', deps)
    const result = await control.create(subject, {
      sourceResourceIncarnationId,
      providerVersion,
      semanticHash: sourceObservation.semanticHash,
      hashSpecificationVersion: sourceObservation.hashSpecificationVersion,
      amount: '1000.00', currency: 'USD', reason: 'owner authorized USD transaction',
    })
    expect(result).toMatchObject({ observationId: 'observation-1', quarantineId: 'quarantine-1', inserted: true, delivered: 1 })
    expect(persisted).toHaveLength(1)
    const corrected = persisted[0] as { payload: Record<string, unknown>; rawPayload: Record<string, unknown> }
    expect(corrected.payload).toMatchObject({ amount: '-1000.00', currency: 'USD', originalAmount: '1000.00', originalCurrency: 'USD' })
    expect(corrected.rawPayload).toMatchObject({ ownerCorrection: { sourceObservationId: 'observation-1', sourceQuarantineId: 'quarantine-1', correctedCurrency: 'USD' } })
  })
})
