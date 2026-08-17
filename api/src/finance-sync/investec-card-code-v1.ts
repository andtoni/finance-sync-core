/** Canonical Investec Card Code delivery envelope. */
export const INVESTEC_CARD_CODE_SCHEMA_VERSION = 1 as const

export type InvestecCardCodeEventType = 'after_transaction' | 'after_decline'

export interface InvestecCardCodeEnvelope {
  readonly schemaVersion: typeof INVESTEC_CARD_CODE_SCHEMA_VERSION
  readonly eventType: InvestecCardCodeEventType
  readonly transaction: Readonly<Record<string, unknown>>
  readonly providerEventTime: string
}

/** Preserve replayable evidence while removing PAN-like values from nested card payloads. */
export function sanitizeInvestecCardEnvelope(value: unknown, parentKey = ''): unknown {
  if (Array.isArray(value)) return value.map(item => sanitizeInvestecCardEnvelope(item, parentKey))
  if (!value || typeof value !== 'object') return value
  const input = value as Record<string, unknown>
  return Object.fromEntries(Object.entries(input).map(([key, item]) => {
    const panField = /(?:^pan$|cardnumber|card_number|display)/i.test(key)
      || (parentKey.toLowerCase() === 'card' && key.toLowerCase() === 'number')
    if (panField && (typeof item === 'string' || typeof item === 'number')) {
      const digits = String(item).replace(/\D/g, '')
      return [key, digits.length >= 4 ? `••••${digits.slice(-4)}` : '[REDACTED]']
    }
    return [key, sanitizeInvestecCardEnvelope(item, key)]
  }))
}

function executionHasAuthorizationFailure(value: unknown): boolean {
  if (value == null) return false
  const text = typeof value === 'string' ? value : JSON.stringify(value)
  return /(?:declin|authori[sz]ation|card\s*(?:failure|error)|transaction\s*fail)/i.test(text)
}

export function recoverInvestecCardExecution(value: unknown): InvestecCardCodeEnvelope | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined
  const row = value as Record<string, unknown>
  if (row.sandbox !== false || row.type !== 'after_transaction' || typeof row.executionId !== 'string' || !row.executionId.trim()) return undefined
  if (row.authorizationApproved === false || executionHasAuthorizationFailure(row.error) || executionHasAuthorizationFailure(row.Error) || !Array.isArray(row.logs) || typeof row.completedAt !== 'string' || !Number.isFinite(Date.parse(row.completedAt))) return undefined
  for (const log of row.logs) {
    if (!log || typeof log !== 'object' || Array.isArray(log)) continue
    const content = (log as Record<string, unknown>).content
    if (typeof content !== 'string') continue
    try {
      const logged = JSON.parse(content)
      const transaction = logged && typeof logged === 'object' && logged.schemaVersion === INVESTEC_CARD_CODE_SCHEMA_VERSION
        ? logged.transaction
        : logged
      const envelope = parseInvestecCardCodeEnvelope(logged && typeof logged === 'object' && logged.schemaVersion === INVESTEC_CARD_CODE_SCHEMA_VERSION
        ? logged
        : {
          schemaVersion: INVESTEC_CARD_CODE_SCHEMA_VERSION,
          eventType: 'after_transaction',
          transaction,
          providerEventTime: typeof (transaction as Record<string, unknown>)?.dateTime === 'string'
            ? (transaction as Record<string, unknown>).dateTime
            : '',
        })
      if (envelope?.eventType === 'after_transaction') return envelope
    } catch {
      // Ignore non-JSON diagnostic log lines; a complete transaction log is required.
    }
  }
  return undefined
}

export function parseInvestecCardCodeEnvelope(value: unknown): InvestecCardCodeEnvelope | undefined {
  if (!value || typeof value !== 'object' || Array.isArray(value)) return undefined
  const row = value as Record<string, unknown>
  if (row.schemaVersion !== INVESTEC_CARD_CODE_SCHEMA_VERSION
    || (row.eventType !== 'after_transaction' && row.eventType !== 'after_decline')
    || !row.transaction || typeof row.transaction !== 'object' || Array.isArray(row.transaction)
    || typeof row.providerEventTime !== 'string' || !Number.isFinite(Date.parse(row.providerEventTime))) return undefined
  return {
    schemaVersion: INVESTEC_CARD_CODE_SCHEMA_VERSION,
    eventType: row.eventType,
    transaction: row.transaction as Readonly<Record<string, unknown>>,
    providerEventTime: new Date(row.providerEventTime).toISOString(),
  }
}
