import { mkdtemp, readFile } from 'node:fs/promises'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { describe, expect, it } from 'vitest'
import { createConstrainedLiveRunner, type ConstrainedLiveApproval } from './constrained-live'

const now = new Date('2026-07-11T12:00:00.000Z')

function approval(overrides: Partial<ConstrainedLiveApproval> = {}): ConstrainedLiveApproval {
  return {
    approvalId: 'g0-approval-1',
    approver: 'Alexis Example',
    purpose: 'Validate pagination semantics',
    provider: 'plaid',
    accountIds: ['account-exact-1'],
    from: '2026-07-01T00:00:00.000Z',
    to: '2026-07-10T00:00:00.000Z',
    expiresAt: '2026-07-12T00:00:00.000Z',
    maxRequests: 2,
    evidenceOutputPath: 'g0/plaid-evidence.json',
    retentionDeadline: '2026-07-18T00:00:00.000Z',
    projectionEnabled: false,
    schedulingEnabled: false,
    ...overrides,
  }
}

describe('G0 constrained-live runner', () => {
  it('guards a bounded request budget and writes redacted constrained-live evidence', async () => {
    const root = await mkdtemp(join(tmpdir(), 'g0-evidence-'))
    const runner = createConstrainedLiveRunner(approval(), { now, evidenceRoot: root })

    runner.consumeRequest('account-exact-1')
    runner.consumeRequest('account-exact-1')
    expect(() => runner.consumeRequest('account-exact-1')).toThrow('request budget exhausted')
    expect(() => runner.consumeRequest('another-account')).toThrow('not allowlisted')

    await runner.writeEvidence({ outcome: 'proven', accountId: 'account-exact-1', accessToken: 'secret' })
    const written = JSON.parse(await readFile(join(root, 'g0/plaid-evidence.json'), 'utf8'))
    expect(written).toMatchObject({ mode: 'constrained_live', provider: 'plaid', approvalId: 'g0-approval-1', outcome: 'proven' })
    expect(written.accountId).toBe('[REDACTED]')
    expect(written.accessToken).toBe('[REDACTED]')
  })

  it.each([
    [{ accountIds: [] }, 'accountIds'],
    [{ accountIds: ['*'] }, 'wildcard'],
    [{ accountIds: ['account-*'] }, 'wildcard'],
    [{ from: '' }, 'bounded UTC'],
    [{ from: '2026-07-10', to: '2026-07-01T00:00:00.000Z' }, 'bounded UTC'],
    [{ expiresAt: '2026-07-11T11:59:59.000Z' }, 'expired'],
    [{ maxRequests: 0 }, 'maxRequests'],
    [{ maxRequests: 1001 }, 'maxRequests'],
    [{ evidenceOutputPath: '../private.json' }, 'evidenceOutputPath'],
    [{ evidenceOutputPath: '/tmp/private.json' }, 'evidenceOutputPath'],
    [{ retentionDeadline: '2026-07-11T11:00:00.000Z' }, 'retentionDeadline'],
    [{ projectionEnabled: true }, 'projection'],
    [{ schedulingEnabled: true }, 'scheduling'],
  ] as const)('fails closed for invalid approval %j', (overrides, message) => {
    expect(() => createConstrainedLiveRunner(approval(overrides), { now, evidenceRoot: '/safe' })).toThrow(message)
  })

  it('rejects evidence that attempts to change the recorded mode', async () => {
    const root = await mkdtemp(join(tmpdir(), 'g0-evidence-'))
    const runner = createConstrainedLiveRunner(approval(), { now, evidenceRoot: root })
    await expect(runner.writeEvidence({ mode: 'synthetic', outcome: 'proven' })).rejects.toThrow('mode')
  })
})
