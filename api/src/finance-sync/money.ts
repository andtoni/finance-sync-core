declare const decimalBrand: unique symbol

type DecimalString = string & { readonly [decimalBrand]: 'DecimalString' }

export type CurrencyKind = 'iso_4217' | 'provider_unofficial' | 'unknown'
export type CurrencyProvenance =
  | 'payload'
  | 'account_metadata'
  | 'contractual'
  | 'constrained_live_probe'
  | 'absent'

export interface ProvenIsoCurrency {
  readonly code: string
  readonly kind: 'iso_4217'
  readonly provenance: Exclude<CurrencyProvenance, 'absent'>
}

export interface EconomicMoney {
  readonly amount: DecimalString
  readonly currency: ProvenIsoCurrency
}

const isoCurrencies = new Set(Intl.supportedValuesOf('currency'))
const DECIMAL_PATTERN = /^-?(?:0|[1-9]\d*)(?:\.\d+)?$/

export function provenIsoCurrency(input: {
  readonly code: string
  readonly kind: CurrencyKind
  readonly provenance: CurrencyProvenance
}): ProvenIsoCurrency {
  if (input.kind !== 'iso_4217' || input.provenance === 'absent') throw new Error('UNPROVEN_ISO_CURRENCY')
  const code = input.code.toUpperCase()
  if (!/^[A-Z]{3}$/.test(code) || !isoCurrencies.has(code)) throw new Error('INVALID_ISO_CURRENCY')
  return Object.freeze({ code, kind: input.kind, provenance: input.provenance })
}

export function economicMoney(amount: string, currency: ProvenIsoCurrency): EconomicMoney {
  if (!DECIMAL_PATTERN.test(amount) || amount === '-0') throw new Error('INVALID_ECONOMIC_AMOUNT')
  return Object.freeze({ amount: amount as DecimalString, currency })
}
