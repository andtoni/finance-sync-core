
import type { CanonicalDomain } from "../domain"

export type ProviderName = "plaid" | "investec" | "mercury" | "snaptrade"

export interface ProviderRegistryEntry {
  provider: ProviderName
  supportedDomains: readonly CanonicalDomain[]
  adapterVersion: string
}

const REGISTRY: Record<ProviderName, ProviderRegistryEntry> = {
  plaid: { provider: "plaid", supportedDomains: ["cash_ledger", "liability"], adapterVersion: "g4-2026-07-11" },
  investec: { provider: "investec", supportedDomains: ["cash_ledger", "balance"], adapterVersion: "g5-2026-07-11" },
  mercury: { provider: "mercury", supportedDomains: ["cash_ledger", "balance"], adapterVersion: "g6-2026-07-11" },
  snaptrade: { provider: "snaptrade", supportedDomains: ["brokerage_activity", "holding", "security"], adapterVersion: "g7-2026-07-11" },
}

export function getProviderEntry(provider: ProviderName): ProviderRegistryEntry {
  const entry = REGISTRY[provider]
  if (!entry) throw new Error(`UNKNOWN_PROVIDER: ${provider}`)
  return entry
}

export function listRegisteredProviders(): readonly ProviderName[] {
  return Object.keys(REGISTRY) as ProviderName[]
}
