import type { CanonicalDomain } from '../../domain'

export type ProbeMode = 'synthetic' | 'sandbox' | 'constrained_live'
export type ProbeOutcome = 'proven' | 'inconclusive' | 'contradicted'
export type ProbeProvider = 'investec' | 'mercury' | 'plaid' | 'snaptrade'

export interface ProbeEvidence {
  readonly provider: ProbeProvider
  readonly accountKey: string
  readonly domain: CanonicalDomain
  readonly mode: ProbeMode
  readonly outcome: ProbeOutcome
}

export interface CapabilityDecision {
  readonly provider: ProbeProvider
  readonly accountKey: string
  readonly domain: CanonicalDomain
  readonly enabled: boolean
  readonly reason:
    | 'SYNTHETIC_EVIDENCE_REQUIRED'
    | 'SANDBOX_EVIDENCE_REQUIRED'
    | 'CONSTRAINED_LIVE_EVIDENCE_REQUIRED'
    | 'EVIDENCE_INCONCLUSIVE'
    | 'EVIDENCE_CONTRADICTED'
    | null
  readonly modes: readonly ProbeMode[]
}

const requiredModes: readonly ProbeMode[] = ['synthetic', 'sandbox', 'constrained_live']

export function capabilityMatrix(evidence: readonly ProbeEvidence[]): CapabilityDecision[] {
  const grouped = new Map<string, ProbeEvidence[]>()
  for (const item of evidence) {
    const key = [item.provider, item.accountKey, item.domain].join('\u001f')
    grouped.set(key, [...(grouped.get(key) ?? []), item])
  }

  return [...grouped.values()]
    .map(decide)
    .sort((a, b) => [a.provider, a.accountKey, a.domain].join('\u001f')
      .localeCompare([b.provider, b.accountKey, b.domain].join('\u001f')))
}

function decide(items: readonly ProbeEvidence[]): CapabilityDecision {
  const [first] = items
  const modes = [...new Set(items.map((item) => item.mode))].sort()
  const base = { provider: first.provider, accountKey: first.accountKey, domain: first.domain, modes }

  if (items.some((item) => item.outcome === 'contradicted')) {
    return { ...base, enabled: false, reason: 'EVIDENCE_CONTRADICTED' }
  }
  if (items.some((item) => item.outcome === 'inconclusive')) {
    return { ...base, enabled: false, reason: 'EVIDENCE_INCONCLUSIVE' }
  }
  for (const mode of requiredModes) {
    if (!items.some((item) => item.mode === mode && item.outcome === 'proven')) {
      const reason = mode === 'synthetic'
        ? 'SYNTHETIC_EVIDENCE_REQUIRED'
        : mode === 'sandbox'
          ? 'SANDBOX_EVIDENCE_REQUIRED'
          : 'CONSTRAINED_LIVE_EVIDENCE_REQUIRED'
      return { ...base, enabled: false, reason }
    }
  }
  return { ...base, enabled: true, reason: null }
}
