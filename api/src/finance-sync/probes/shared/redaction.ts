const sensitiveKey = /(?:token|secret|password|credential|account.?id|resource.?id|payload|amount|currency|merchant|description|name|email|phone|routing|iban|swift|mask)/i

export function redactProbeEvidence(value: unknown, key = ''): unknown {
  if (sensitiveKey.test(key)) return '[REDACTED]'
  if (Array.isArray(value)) return value.map((item) => redactProbeEvidence(item))
  if (value !== null && typeof value === 'object') {
    return Object.fromEntries(Object.entries(value).map(([childKey, child]) => [
      childKey,
      redactProbeEvidence(child, childKey),
    ]))
  }
  return value
}
