import { describe, expect, it } from 'vitest'
import { capabilityMatrix, type ProbeEvidence } from './capability-matrix'

describe('G0 capability matrix', () => {
  it('never enables a domain from synthetic or sandbox evidence alone', () => {
    const evidence: ProbeEvidence[] = [
      { provider: 'plaid', accountKey: 'acct-a', domain: 'cash_ledger', mode: 'synthetic', outcome: 'proven' },
      { provider: 'plaid', accountKey: 'acct-a', domain: 'cash_ledger', mode: 'sandbox', outcome: 'proven' },
    ]

    expect(capabilityMatrix(evidence)).toEqual([{
      provider: 'plaid',
      accountKey: 'acct-a',
      domain: 'cash_ledger',
      enabled: false,
      reason: 'CONSTRAINED_LIVE_EVIDENCE_REQUIRED',
      modes: ['sandbox', 'synthetic'],
    }])
  })

  it('enables only the exact account and domain with all required evidence', () => {
    const evidence: ProbeEvidence[] = [
      { provider: 'mercury', accountKey: 'acct-a', domain: 'cash_ledger', mode: 'synthetic', outcome: 'proven' },
      { provider: 'mercury', accountKey: 'acct-a', domain: 'cash_ledger', mode: 'sandbox', outcome: 'proven' },
      { provider: 'mercury', accountKey: 'acct-a', domain: 'cash_ledger', mode: 'constrained_live', outcome: 'proven' },
      { provider: 'mercury', accountKey: 'acct-a', domain: 'balance', mode: 'synthetic', outcome: 'proven' },
    ]

    expect(capabilityMatrix(evidence)).toEqual([
      {
        provider: 'mercury', accountKey: 'acct-a', domain: 'balance', enabled: false,
        reason: 'SANDBOX_EVIDENCE_REQUIRED', modes: ['synthetic'],
      },
      {
        provider: 'mercury', accountKey: 'acct-a', domain: 'cash_ledger', enabled: true,
        reason: null, modes: ['constrained_live', 'sandbox', 'synthetic'],
      },
    ])
  })

  it('fails closed when any evidence is contradicted or inconclusive', () => {
    const evidence: ProbeEvidence[] = [
      { provider: 'snaptrade', accountKey: 'acct-a', domain: 'holding', mode: 'synthetic', outcome: 'proven' },
      { provider: 'snaptrade', accountKey: 'acct-a', domain: 'holding', mode: 'sandbox', outcome: 'proven' },
      { provider: 'snaptrade', accountKey: 'acct-a', domain: 'holding', mode: 'constrained_live', outcome: 'inconclusive' },
    ]

    expect(capabilityMatrix(evidence)[0]).toMatchObject({
      enabled: false,
      reason: 'EVIDENCE_INCONCLUSIVE',
    })
  })
})
