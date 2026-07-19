import { createHash } from 'node:crypto'

export const MERCHANT_CONTEXT_NORMALIZATION_VERSION = 1 as const

export type MerchantContextProvider = 'investec' | 'mercury' | 'plaid' | 'snaptrade'
export type MerchantDirection = 'inflow' | 'outflow' | null

/** Provider-neutral descriptive context. It is evidence only and never a category assignment. */
export interface MerchantContextV1 {
  readonly merchantKeyVersion: 1
  readonly normalizationVersion: typeof MERCHANT_CONTEXT_NORMALIZATION_VERSION
  readonly rawDescription: string | null
  readonly merchantDisplay: string | null
  readonly merchantKey: string | null
  readonly direction: MerchantDirection
  readonly activityType: string | null
  readonly providerHint: Readonly<Record<string, unknown>> | null
  readonly provenance: {
    readonly source: 'provider_observation'
    readonly provider: MerchantContextProvider
  }
}

function present(value: string | null | undefined): string | null {
  return typeof value === 'string' && value.trim().length > 0 ? value : null
}

function merchantKey(value: string | null): string | null {
  if (!value) return null
  const normalized = value.normalize('NFKC').toLocaleLowerCase('en-US')
    .replace(/[^\p{L}\p{N}]+/gu, ' ').trim().replace(/\s+/g, ' ')
  return normalized
    ? `v${MERCHANT_CONTEXT_NORMALIZATION_VERSION}:${createHash('sha256').update(normalized, 'utf8').digest('hex')}`
    : null
}

export function merchantContext(input: {
  readonly provider: MerchantContextProvider
  readonly rawDescription?: string | null
  readonly merchantDisplay?: string | null
  readonly amount?: number | string | null
  readonly /** Use for brokerage semantics; cash rows derive direction from amount. */ activityType?: string | null
  readonly providerHint?: Readonly<Record<string, unknown>> | null
}): MerchantContextV1 {
  const rawDescription = present(input.rawDescription)
  const merchantDisplay = present(input.merchantDisplay) ?? rawDescription
  const numericAmount = input.amount === null || input.amount === undefined ? Number.NaN : Number(input.amount)
  return Object.freeze({
    merchantKeyVersion: 1,
    normalizationVersion: MERCHANT_CONTEXT_NORMALIZATION_VERSION,
    rawDescription,
    merchantDisplay,
    merchantKey: merchantKey(merchantDisplay),
    direction: Number.isFinite(numericAmount) && numericAmount !== 0 ? (numericAmount < 0 ? 'outflow' : 'inflow') : null,
    activityType: present(input.activityType),
    providerHint: input.providerHint ? Object.freeze({ ...input.providerHint }) : null,
    provenance: Object.freeze({ source: 'provider_observation' as const, provider: input.provider }),
  })
}
