declare const identityBrand: unique symbol

type BrandedIdentity<Name extends string> = string & { readonly [identityBrand]: Name }

export type DeploymentId = BrandedIdentity<'DeploymentId'>
export type LocalOwnerId = BrandedIdentity<'LocalOwnerId'>
export type ProviderPrincipalId = BrandedIdentity<'ProviderPrincipalId'>
export type SourceConnectionIncarnationId = BrandedIdentity<'SourceConnectionIncarnationId'>
export type SourceAccountIncarnationId = BrandedIdentity<'SourceAccountIncarnationId'>
export type ProviderResourceIncarnationId = BrandedIdentity<'ProviderResourceIncarnationId'>
export type CanonicalEntityId = BrandedIdentity<'CanonicalEntityId'>

const ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/

function identity<Name extends string>(value: string, name: Name): BrandedIdentity<Name> {
  if (!ID_PATTERN.test(value)) throw new Error(`INVALID_${name.replaceAll(/([a-z])([A-Z])/g, '$1_$2').toUpperCase()}`)
  return value as BrandedIdentity<Name>
}

export const deploymentId = (value: string): DeploymentId => identity(value, 'DeploymentId')
export const localOwnerId = (value: string): LocalOwnerId => identity(value, 'LocalOwnerId')
export const providerPrincipalId = (value: string): ProviderPrincipalId => identity(value, 'ProviderPrincipalId')
export const sourceConnectionIncarnationId = (value: string): SourceConnectionIncarnationId => identity(value, 'SourceConnectionIncarnationId')
export const sourceAccountIncarnationId = (value: string): SourceAccountIncarnationId => identity(value, 'SourceAccountIncarnationId')
export const providerResourceIncarnationId = (value: string): ProviderResourceIncarnationId => identity(value, 'ProviderResourceIncarnationId')
export const canonicalEntityId = (value: string): CanonicalEntityId => identity(value, 'CanonicalEntityId')
