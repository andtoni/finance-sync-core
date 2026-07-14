import { describe, expect, it } from 'vitest'
import { mappingProvenance } from './provenance'

describe('Finance Sync mapping provenance', () => {
  const valid = {
    provider: 'plaid' as const,
    providerVersion: 'provider-revision-7',
    mappingVersion: 'cash-ledger/v1',
    semanticHash: `sha256:${'a'.repeat(64)}`,
    hashSpecificationVersion: 1,
  }

  it('retains provider and every version required to reproduce semantics', () => {
    const provenance = mappingProvenance(valid)
    expect(provenance).toEqual(valid)
    expect(Object.isFrozen(provenance)).toBe(true)
  })

  it('permits an absent provider version but rejects arbitrary providers and incomplete versions', () => {
    expect(mappingProvenance({ ...valid, providerVersion: null }).providerVersion).toBeNull()
    expect(() => mappingProvenance({ ...valid, provider: 'random-bank' })).toThrowError('INVALID_PROVIDER')
    expect(() => mappingProvenance({ ...valid, mappingVersion: '' })).toThrowError('INVALID_MAPPING_VERSION')
    expect(() => mappingProvenance({ ...valid, semanticHash: 'sha256:abc' })).toThrowError('INVALID_SEMANTIC_HASH')
    expect(() => mappingProvenance({ ...valid, hashSpecificationVersion: 0 })).toThrowError('INVALID_HASH_SPECIFICATION_VERSION')
  })
})
