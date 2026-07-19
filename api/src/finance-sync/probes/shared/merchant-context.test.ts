import { describe, expect, it } from 'vitest'
import { merchantContext } from './merchant-context'

describe('merchant context', () => {
  it.each([
    ['plaid', 'RAW * 123', 'Raw Merchant'],
    ['investec', 'ORIGINAL DESCRIPTION', 'ORIGINAL DESCRIPTION'],
    ['mercury', 'BANK RAW', 'Counterparty'],
    ['snaptrade', 'Bought fund', 'Bought fund'],
  ] as const)('is versioned, provider-provenanced, and category-free for %s', (provider, rawDescription, merchantDisplay) => {
    const value = merchantContext({ provider, rawDescription, merchantDisplay, amount: '-1.00', providerHint: { hint: 'evidence' } })
    expect(value).toMatchObject({ normalizationVersion: 1, rawDescription, merchantDisplay, direction: 'outflow', provenance: { source: 'provider_observation', provider } })
    expect(value.merchantKey).toMatch(/^v1:[a-f0-9]{64}$/)
    expect(value).not.toHaveProperty('category')
  })

  it('preserves raw text while producing the same key for harmless display normalization', () => {
    const a = merchantContext({ provider: 'plaid', rawDescription: '  Raw  Text  ', merchantDisplay: 'ACME, Inc.' })
    const b = merchantContext({ provider: 'plaid', rawDescription: 'different original', merchantDisplay: 'acme inc' })
    expect(a.rawDescription).toBe('  Raw  Text  ')
    expect(a.merchantKey).toBe(b.merchantKey)
  })
})
