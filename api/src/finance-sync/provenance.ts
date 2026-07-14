export const PROVIDERS = ['investec', 'mercury', 'plaid', 'snaptrade'] as const
export type Provider = (typeof PROVIDERS)[number]

export interface MappingProvenance {
  readonly provider: Provider
  readonly providerVersion: string | null
  readonly mappingVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
}

const VERSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:/+-]{0,127}$/
const SEMANTIC_HASH_PATTERN = /^sha256:[a-f0-9]{64}$/

export function mappingProvenance(input: {
  readonly provider: string
  readonly providerVersion: string | null
  readonly mappingVersion: string
  readonly semanticHash: string
  readonly hashSpecificationVersion: number
}): MappingProvenance {
  if (!PROVIDERS.includes(input.provider as Provider)) throw new Error('INVALID_PROVIDER')
  if (input.providerVersion !== null && !VERSION_PATTERN.test(input.providerVersion)) throw new Error('INVALID_PROVIDER_VERSION')
  if (!VERSION_PATTERN.test(input.mappingVersion)) throw new Error('INVALID_MAPPING_VERSION')
  if (!SEMANTIC_HASH_PATTERN.test(input.semanticHash)) throw new Error('INVALID_SEMANTIC_HASH')
  if (!Number.isSafeInteger(input.hashSpecificationVersion) || input.hashSpecificationVersion < 1) {
    throw new Error('INVALID_HASH_SPECIFICATION_VERSION')
  }
  return Object.freeze({ ...input, provider: input.provider as Provider })
}
