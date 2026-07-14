import type { CanonicalDomain } from '../domain'

export interface AuthorityEpoch {
  readonly account: string
  readonly domain: CanonicalDomain
  readonly sourceAccount: string
  readonly role: 'primary' | 'supplemental'
  readonly validFrom: string
  readonly validTo: string | null
}

export function addAuthorityEpoch(
  epochs: readonly AuthorityEpoch[],
  candidate: AuthorityEpoch,
): readonly AuthorityEpoch[] {
  const start = instant(candidate.validFrom)
  const end = candidate.validTo === null ? Number.POSITIVE_INFINITY : instant(candidate.validTo)
  if (end <= start) throw new Error('INVALID authority range')
  if (candidate.role === 'primary' && epochs.some((epoch) => epoch.role === 'primary'
    && epoch.account === candidate.account
    && epoch.domain === candidate.domain
    && overlaps(start, end, instant(epoch.validFrom), epoch.validTo === null
      ? Number.POSITIVE_INFINITY
      : instant(epoch.validTo)))) throw new Error('authority overlap')
  return Object.freeze([...epochs, Object.freeze({ ...candidate })])
}

export function selectAuthority(
  epochs: readonly AuthorityEpoch[],
  account: string,
  domain: CanonicalDomain,
  at: string,
): AuthorityEpoch | null {
  const point = instant(at)
  return epochs.find((epoch) => epoch.role === 'primary'
    && epoch.account === account
    && epoch.domain === domain
    && instant(epoch.validFrom) <= point
    && (epoch.validTo === null || point < instant(epoch.validTo))) ?? null
}

function overlaps(aStart: number, aEnd: number, bStart: number, bEnd: number): boolean {
  return aStart < bEnd && bStart < aEnd
}

function instant(value: string): number {
  if (!/^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/.test(value)) throw new Error('INVALID instant')
  const parsed = Date.parse(value)
  if (Number.isNaN(parsed)) throw new Error('INVALID instant')
  return parsed
}
