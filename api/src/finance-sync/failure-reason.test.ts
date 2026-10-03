import { describe, expect, it } from 'vitest'
import { safeFailureReason } from './failure-reason'

function throwsHere(): Error {
  return new Error('provider payload sensitive-token 10012345678 {"amount":1234.56}')
}

describe('safeFailureReason', () => {
  it('reports where the error was thrown and never its message', () => {
    const reason = safeFailureReason(throwsHere())
    expect(reason.errorClass).toBe('Error')
    expect(reason.at).toMatch(/^throwsHere@failure-reason\.test\.ts:5 < /)
    expect(JSON.stringify(reason)).not.toMatch(/sensitive|10012345678|amount|payload/)
  })

  it('keeps a numeric HTTP status', () => {
    const reason = safeFailureReason(Object.assign(new Error('investec request failed: HTTP 401'), { status: 401 }))
    expect(reason.status).toBe(401)
    expect(JSON.stringify(reason)).not.toMatch(/investec request failed/)
  })

  it('reports the SQLSTATE and schema object of a database error, never its detail', () => {
    const error = Object.assign(new Error('duplicate key value violates unique constraint "outbox_key"'), {
      name: 'PostgresError', code: '23505', table_name: 'projection_outbox', constraint_name: 'outbox_key',
      detail: 'Key (idempotency_key)=(secret-value) already exists.',
    })
    expect(safeFailureReason(error)).toMatchObject({
      errorClass: 'PostgresError', sqlState: '23505', table: 'projection_outbox', constraint: 'outbox_key',
    })
    expect(JSON.stringify(safeFailureReason(error))).not.toMatch(/secret-value|duplicate key/)
  })

  it('reports the system code behind a failed fetch', () => {
    const error = new TypeError('fetch failed', { cause: Object.assign(new Error('connect'), { code: 'ECONNREFUSED' }) })
    expect(safeFailureReason(error)).toMatchObject({ errorClass: 'TypeError', causeCode: 'ECONNREFUSED' })
  })

  it('names unknown error classes and non-errors without their content', () => {
    class ProviderSdkError extends Error { override name = 'ProviderSdkError 1234' }
    expect(safeFailureReason(new ProviderSdkError('x')).errorClass).toBe('OTHER_ERROR')
    expect(safeFailureReason('token-abc')).toEqual({ errorClass: 'STRING_ERROR' })
    expect(safeFailureReason(null)).toEqual({ errorClass: 'NULL_ERROR' })
  })
})
