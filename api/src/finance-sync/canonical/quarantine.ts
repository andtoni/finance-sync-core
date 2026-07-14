const DECIMAL = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/
const isoCurrencies = new Set(Intl.supportedValuesOf('currency'))

interface CashCandidate {
  readonly amount: string
  readonly accountCurrency: string | null
}

type CashQualification =
  | { readonly publishable: false; readonly reason: 'MISSING_ACCOUNT_CURRENCY' }
  | { readonly publishable: true; readonly money: { readonly amount: string; readonly currency: string } }

export function qualifyCashObservation(candidate: CashCandidate): CashQualification {
  if (!DECIMAL.test(candidate.amount) || candidate.amount === '-0') throw new Error('INVALID amount')
  if (candidate.accountCurrency === null) {
    return Object.freeze({ publishable: false, reason: 'MISSING_ACCOUNT_CURRENCY' })
  }
  const currency = candidate.accountCurrency.toUpperCase()
  if (!/^[A-Z]{3}$/.test(currency) || !isoCurrencies.has(currency)) throw new Error('UNPROVEN account currency')
  return Object.freeze({
    publishable: true,
    money: Object.freeze({ amount: candidate.amount, currency }),
  })
}
