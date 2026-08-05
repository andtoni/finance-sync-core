import { describe, expect, it, vi } from 'vitest'
import { createNativeProviderOnboarding } from './server-onboarding'
import type { ProviderTransportFactories } from './provider-runtime'

const subject = 'wealthfolio#owner'
const bundles: Record<string, Readonly<Record<string, unknown>>> = {
  INVESTEC_BUNDLE: { providerConnectionId: 'investec-principal', clientId: 'id', clientSecret: 'secret', apiKey: 'key', eligibleProductNames: ['Private Bank'], fromDate: '2026-01-01', toDate: '2026-07-18' },
  MERCURY_BUNDLE: { providerConnectionId: 'mercury-principal', apiToken: 'token' },
  SNAPTRADE_BUNDLE: { providerConnectionId: 'snaptrade-principal', authMode: 'personal-oauth', clientId: 'id', consumerKey: 'key' },
}

function setup(snapAccounts: readonly Record<string, unknown>[] = [{ account: { id: 'broker-1', name: 'Brokerage', raw_type: 'taxable' }, total_value: { currency: 'USD' } }]) {
  const recordConnection = vi.fn(async () => undefined)
  const createRedirectUri = vi.fn(async () => ({ redirectUri: 'https://app.snaptrade.com/snapTrade/redeemToken?token=temporary' }))
  const factories: ProviderTransportFactories = {
    plaid: () => { throw new Error('unused') },
    investec: () => ({
      listAccounts: async () => [{ accountId: 'bank-1', productName: 'Private Bank', accountName: 'Checking', accountNumber: '12345678' }],
      getBalance: async () => ({ currency: 'zar' }), getTransactionPage: async () => ({ transactions: [] }),
    }),
    mercury: () => ({
      fetchAccounts: async () => ({ accounts: [{ id: 'cash-1', name: 'Operating', type: 'cash', currentBalance: 1, availableBalance: 1, currency: 'USD', status: 'active' }] }),
      fetchTransactions: async () => ({ transactions: [], total: 0 }),
    }),
    snaptrade: () => ({ fetchAccounts: async () => snapAccounts as never[], fetchActivities: async () => [] }),
  }
  const onboarding = createNativeProviderOnboarding({
    env: {
      FINANCE_SYNC_PROVIDER_OWNER_SUBJECT: subject,
      FINANCE_SYNC_INVESTEC_SECRET_REFERENCE: 'INVESTEC_BUNDLE', FINANCE_SYNC_MERCURY_SECRET_REFERENCE: 'MERCURY_BUNDLE',
      FINANCE_SYNC_SNAPTRADE_SECRET_REFERENCE: 'SNAPTRADE_BUNDLE',
    },
    secretStore: { available: true, resolve: async reference => bundles[reference]! },
    factories, repository: { recordConnection }, snaptrade: { createRedirectUri },
  })
  return { onboarding, recordConnection, createRedirectUri }
}

describe('native server onboarding discovery', () => {
  it('resolves named bundles only on the server and persists normalized Investec and Mercury discovery', async () => {
    const { onboarding, recordConnection } = setup()
    await expect(onboarding.discover(subject, 'investec')).resolves.toEqual({ status: 'pending' })
    await expect(onboarding.discover(subject, 'mercury')).resolves.toEqual({ status: 'pending' })
    expect(recordConnection).toHaveBeenNthCalledWith(1, expect.objectContaining({
      subject, provider: 'investec', secretReference: 'INVESTEC_BUNDLE',
      accounts: [{ providerAccountId: 'bank-1', name: 'Checking', type: 'Private Bank', currency: 'ZAR', mask: '5678', assets: [] }],
    }))
    expect(recordConnection).toHaveBeenNthCalledWith(2, expect.objectContaining({
      subject, provider: 'mercury', secretReference: 'MERCURY_BUNDLE',
      accounts: [{ providerAccountId: 'cash-1', name: 'Operating', type: 'cash', currency: 'USD', assets: [] }],
    }))
  })

  it('persists authorized SnapTrade accounts, or returns a read-only authorization redirect for retry', async () => {
    const ready = setup()
    await expect(ready.onboarding.discover(subject, 'snaptrade')).resolves.toEqual({ status: 'pending' })
    expect(ready.recordConnection).toHaveBeenCalledWith(expect.objectContaining({ provider: 'snaptrade', accounts: [expect.objectContaining({ providerAccountId: 'broker-1' })] }))

    const unauthorized = setup([])
    await expect(unauthorized.onboarding.discover(subject, 'snaptrade')).resolves.toEqual({
      status: 'link_ready', redirectUrl: 'https://app.snaptrade.com/snapTrade/redeemToken?token=temporary',
    })
    expect(unauthorized.createRedirectUri).toHaveBeenCalledOnce()
    expect(unauthorized.recordConnection).not.toHaveBeenCalled()
  })

  it('discovers only non-cash SnapTrade assets with stable provider identifiers', async () => {
    const { onboarding, recordConnection } = setup([{
      account: { id: 'broker-1', name: 'Brokerage' }, total_value: { currency: 'USD' }, positions: [
        { symbol: { id: 'provider-vti', symbol: 'VTI', name: 'Vanguard Total Stock', currency: { code: 'USD' }, type: { code: 'etf' } } },
        { symbol: { symbol: 'CASH', type: { code: 'cash' } } },
      ],
    }])
    await expect(onboarding.discover(subject, 'snaptrade')).resolves.toEqual({ status: 'pending' })
    expect(recordConnection).toHaveBeenCalledWith(expect.objectContaining({ accounts: [expect.objectContaining({
      assets: [{ providerAssetId: 'provider-vti', symbol: 'VTI', name: 'Vanguard Total Stock', currency: 'USD', instrumentType: 'etf' }],
    })] }))
  })

  it('fails discovery rather than admitting an invested SnapTrade holding without a stable provider id', async () => {
    const { onboarding, recordConnection } = setup([{
      account: { id: 'broker-1', name: 'Brokerage' }, total_value: { currency: 'USD' },
      positions: [{ symbol: { symbol: 'VTI', type: { code: 'etf' } } }],
    }])
    await expect(onboarding.discover(subject, 'snaptrade')).rejects.toThrow('stable provider asset id')
    expect(recordConnection).not.toHaveBeenCalled()
  })

  it('rejects every non-owner subject before secret resolution or provider access', async () => {
    const { onboarding, recordConnection } = setup()
    await expect(onboarding.discover('wealthfolio#other', 'investec')).rejects.toThrow('not authorized')
    expect(recordConnection).not.toHaveBeenCalled()
  })

  it('does not advertise absent secret references', () => {
    const { onboarding } = setup()
    expect(onboarding.available('investec')).toBe(true)
    expect(createNativeProviderOnboarding({
      env: {}, secretStore: { available: true, resolve: async () => ({}) },
      factories: {} as ProviderTransportFactories, repository: { recordConnection: async () => undefined },
      snaptrade: { createRedirectUri: async () => ({ redirectUri: '' }) },
    }).available('investec')).toBe(false)
  })
})
