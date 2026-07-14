import { describe, expect, it } from 'vitest'
import { redactProbeEvidence } from './redaction'

describe('G0 evidence redaction', () => {
  it('removes secrets, raw payloads and financial identifiers recursively', () => {
    expect(redactProbeEvidence({
      accessToken: 'secret',
      accountId: 'acct-123',
      payload: { merchant: 'private', amount: '12.34' },
      status: 'proven',
      nested: { refresh_token: 'secret-2', count: 2 },
    })).toEqual({
      accessToken: '[REDACTED]',
      accountId: '[REDACTED]',
      payload: '[REDACTED]',
      status: 'proven',
      nested: { refresh_token: '[REDACTED]', count: 2 },
    })
  })
})
