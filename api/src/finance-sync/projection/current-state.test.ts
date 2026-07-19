import { describe, expect, it } from 'vitest'
import { assembleCurrentState, currentStateSemanticHash, type CurrentStatePayload } from './current-state'
import type { ObservationOutput } from '../probes/plaid/plaid-adapter'
import type { CanonicalDomain } from '../domain'

function observation(providerVersion: string, domain: CanonicalDomain, payload: Record<string, unknown>): ObservationOutput {
  return {
    deploymentId: 'dep-1',
    localOwnerId: 'urn:test#owner',
    sourceResourceIncarnationId: '00000000-0000-0000-0000-000000000001',
    providerVersion,
    semanticHash: 'a'.repeat(64),
    hashSpecificationVersion: 1,
    domain,
    payload: Object.freeze(payload),
    observedAt: '2026-07-18T12:00:00.000Z',
  }
}

const admitted = (providerAccountId = 'provider-1', accountId = 'wf-1', providerAssetIds?: readonly string[]) => [{ providerAccountId, accountId, ...(providerAssetIds ? { assetMappings: Object.fromEntries(providerAssetIds.map(id => [id, `wf-${id}`])) } : {}) }]
const binding = {
  providerConnectionId: 'connection-1', providerIncarnationId: '00000000-0000-0000-0000-000000000001', providerGeneration: 7,
}
const metadata = {
  providerConnectionId: '00000000-0000-0000-0000-000000000001', providerIncarnationId: '00000000-0000-0000-0000-000000000001',
  providerGeneration: 0, observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
}

describe('completed provider-run current-state assembler', () => {
  it('assembles an Investec actual current balance', () => {
    const [state] = assembleCurrentState('investec', [
      observation('investec:balance:provider-1', 'balance', { accountId: 'provider-1', currentBalance: '25000.00', currency: 'ZAR', asOfDate: '2026-07-18' }),
      observation('investec:txn:t-1', 'cash_ledger', { accountId: 'provider-1', transactionId: 't-1', amount: '-5', currency: 'ZAR' }),
    ], admitted())
    expect(state.payload).toEqual({ accountId: 'wf-1', ...metadata, asOfDate: '2026-07-18', currency: 'ZAR', cashBalance: '25000', liabilityBalance: '0', positions: [] })
  })

  it('assembles Mercury assets and positive liability magnitudes', () => {
    const observations = [
      observation('mercury:balance:cash', 'cash_ledger', { accountId: 'cash', currentBalance: '500.25', currency: 'USD' }),
      observation('mercury:balance:credit', 'liability', { accountId: 'credit', currentBalance: '-125.50', currency: 'USD' }),
    ]
    const states = assembleCurrentState('mercury', observations, [
      { providerAccountId: 'cash', accountId: 'wf-cash' },
      { providerAccountId: 'credit', accountId: 'wf-credit' },
    ])
    expect(states[0].payload).toMatchObject({ cashBalance: '500.25', liabilityBalance: '0' })
    expect(states[1].payload).toMatchObject({ cashBalance: '0', liabilityBalance: '125.5' })
  })

  it('requires and assembles Plaid balance plus liability observations', () => {
    const [state] = assembleCurrentState('plaid', [
      observation('plaid:balance:card', 'liability', { accountId: 'card', currentBalance: '420.10', currency: 'USD' }),
      observation('plaid:liability:card', 'liability', { accountId: 'card', currentBalance: '420.10', currency: 'USD', details: [] }),
    ], admitted('card', 'wf-card'))
    expect(state.payload).toMatchObject({ accountId: 'wf-card', cashBalance: '0', liabilityBalance: '420.1', positions: [] })
  })

  it('derives SnapTrade cash from total and the complete non-cash position set', () => {
    const [state] = assembleCurrentState('snaptrade', [
      observation('snaptrade:account:broker', 'balance', { accountId: 'broker', totalValue: '110.00', currency: 'USD' }),
      observation('snaptrade:holding:broker:VTI', 'holding', { accountId: 'broker', providerAssetId: 'asset-vti', symbol: 'VTI', units: '2', price: '40', value: '80', currency: 'USD', costBasis: '70', instrumentTypeTag: 'etf' }),
      observation('snaptrade:holding:broker:CASH', 'cash_ledger', { accountId: 'broker', symbol: 'CASH', units: '30', price: '1', value: '30', currency: 'USD', instrumentTypeTag: 'cash' }),
    ], admitted('broker', 'wf-broker', ['asset-vti']), binding)
    expect(state.payload).toMatchObject({ ...binding, cashBalance: '30' })
    expect(state.payload.positions).toEqual([{ symbol: 'VTI', quantity: '2', price: '40', marketValue: '80', currency: 'USD', costBasis: '70' }])
  })

  it('fails closed for a SnapTrade position without prior asset admission', () => {
    expect(() => assembleCurrentState('snaptrade', [
      observation('snaptrade:account:broker', 'balance', { accountId: 'broker', totalValue: '80', currency: 'USD' }),
      observation('snaptrade:holding:broker:UNKNOWN', 'holding', { accountId: 'broker', symbol: 'UNKNOWN', units: '1', price: '80', value: '80', currency: 'USD' }),
    ], admitted('broker', 'wf-broker'), binding)).toThrow('unmapped position')
  })

  it('preserves arbitrary decimal precision and sign without Number rounding', () => {
    const [state] = assembleCurrentState('snaptrade', [
      observation('snaptrade:account:broker', 'balance', { accountId: 'broker', totalValue: '1.000000000000000000000000000000000001', currency: 'USD' }),
      observation('snaptrade:holding:broker:TINY', 'holding', { accountId: 'broker', providerAssetId: 'asset-tiny', symbol: 'TINY', units: '-0.0000000000000000001', price: '123456789.123456789123456789', value: '0.999999999999999999999999999999999999', currency: 'USD' }),
    ], admitted('broker', 'wf-broker', ['asset-tiny']), binding)
    expect(state.payload.cashBalance).toBe('0.000000000000000000000000000000000002')
    expect(state.payload.positions[0]).toMatchObject({ quantity: '-0.0000000000000000001', price: '123456789.123456789123456789' })
  })

  it.each(['1e-8', 'NaN', 'Infinity', '', '  '])('rejects non-plain decimal input %j', value => {
    expect(() => assembleCurrentState('investec', [
      observation('investec:balance:provider-1', 'balance', { accountId: 'provider-1', currentBalance: value, currency: 'ZAR' }),
    ], admitted())).toThrow('incomplete current-state snapshot')
  })

  it('documents assets + cash - positive liability arithmetic', () => {
    const payload: CurrentStatePayload = {
      accountId: 'wf-mixed', ...binding, observedAt: '2026-07-18T12:00:00.000Z', revision: 1784376000000,
      asOfDate: '2026-07-18', currency: 'USD', cashBalance: '30', liabilityBalance: '20',
      positions: [{ symbol: 'VTI', quantity: '2', price: '40', marketValue: '80', currency: 'USD' }],
    }
    const receiverValue = Number(payload.cashBalance) + payload.positions.reduce((sum, position) => sum + Number(position.marketValue), 0) - Number(payload.liabilityBalance)
    expect(receiverValue).toBe(90)
  })

  it('refuses every partial run missing an admitted account balance', () => {
    expect(() => assembleCurrentState('investec', [
      observation('investec:txn:t-1', 'cash_ledger', { accountId: 'provider-1', transactionId: 't-1', amount: '-5', currency: 'ZAR' }),
    ], admitted())).toThrow('incomplete current-state snapshot')
  })

  it('binds the complete payload deterministically for replay identity', () => {
    const input = [observation('investec:balance:provider-1', 'balance', { accountId: 'provider-1', currentBalance: '12.50', currency: 'ZAR', asOfDate: '2026-07-18' })]
    const first = assembleCurrentState('investec', input, admitted())[0]
    const replay = assembleCurrentState('investec', [...input], [...admitted()])[0]
    expect(replay).toEqual(first)
    expect(first.semanticHash).toBe(currentStateSemanticHash(first.payload))
    expect(currentStateSemanticHash({ ...first.payload, cashBalance: '12.51' })).not.toBe(first.semanticHash)
  })
})
