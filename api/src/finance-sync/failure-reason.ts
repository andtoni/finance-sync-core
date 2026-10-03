/**
 * Bounded description of a caught failure that is safe to log.
 *
 * Provider and SDK errors can embed tokens, identifiers and financial payloads,
 * so neither the message nor the error itself is ever printed. Only fields with
 * a fixed, non-sensitive shape are kept: the standard error class, where in this
 * service's code it was thrown (function name, file and line from the stack), an
 * HTTP status, a system error code, and the PostgreSQL SQLSTATE with the schema
 * object it names.
 */
const STANDARD_ERROR_CLASSES = new Set([
  'Error', 'AggregateError', 'EvalError', 'RangeError', 'ReferenceError',
  'SyntaxError', 'TypeError', 'URIError', 'PostgresError', 'AbortError', 'TimeoutError',
])
const STACK_FRAME = /^\s+at (?:async )?(?:([A-Za-z_$][\w$.]{0,79}) )?\(?(?:.*[\\/])?([\w.-]{1,80}\.[cm]?[jt]s):(\d+):\d+\)?$/
const STACK_FRAMES = 3
const SQLSTATE = /^[0-9A-Z]{5}$/
const SCHEMA_IDENTIFIER = /^[a-z_][a-z0-9_]{0,62}$/
const SYSTEM_CODE = /^[A-Z][A-Z0-9_]{2,39}$/

export function safeFailureReason(error: unknown): Readonly<Record<string, string | number>> {
  if (!(error instanceof Error)) return { errorClass: error === null ? 'NULL_ERROR' : `${typeof error}_error`.toUpperCase() }
  const reason: Record<string, string | number> = {
    errorClass: STANDARD_ERROR_CLASSES.has(error.name) ? error.name : 'OTHER_ERROR',
  }
  const frames = (error.stack ?? '').split('\n')
    .map(line => STACK_FRAME.exec(line))
    .filter(match => match !== null)
    .slice(0, STACK_FRAMES)
    .map(match => `${match[1] ?? '(anonymous)'}@${match[2]}:${match[3]}`)
  if (frames.length > 0) reason.at = frames.join(' < ')
  const fields = error as Error & Record<string, unknown>
  if (typeof fields.status === 'number' && Number.isInteger(fields.status) && fields.status >= 100 && fields.status <= 599) {
    reason.status = fields.status
  }
  const code = typeof fields.code === 'string' ? fields.code : undefined
  if (code && SQLSTATE.test(code) && error.name === 'PostgresError') {
    reason.sqlState = code
    for (const [field, key] of [['table_name', 'table'], ['constraint_name', 'constraint'], ['column_name', 'column']] as const) {
      const value = fields[field]
      if (typeof value === 'string' && SCHEMA_IDENTIFIER.test(value)) reason[key] = value
    }
  } else if (code && SYSTEM_CODE.test(code)) {
    reason.code = code
  }
  const cause = fields.cause as { code?: unknown } | undefined
  if (cause && typeof cause.code === 'string' && SYSTEM_CODE.test(cause.code)) reason.causeCode = cause.code
  return reason
}

/** Log a fixed event name with the bounded failure reason. */
export function logFailure(event: string, error: unknown, context: Readonly<Record<string, string | number>> = {}): void {
  console.error(event, JSON.stringify({ ...context, ...safeFailureReason(error) }))
}
