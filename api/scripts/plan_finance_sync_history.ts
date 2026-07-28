import { createHash } from 'node:crypto'

type MaybePromise<T> = T | Promise<T>
export type HistoricalSource = 'tarn' | 'lunch_money'
export type HistoricalDirection = 'inflow' | 'outflow'
export type HistoricalAgreementState = 'agreement' | 'conflict'

export interface HistoricalRow {
  readonly sourceId: string
  readonly accountRef: string
  readonly categoryRef?: string | null
  readonly effectiveAt: string
  readonly amount: string
  readonly currency: string
  readonly rawDescription: string
  /** Canonical Wealthfolio activity type. Readers must provide this; the planner never derives it from amount. */
  readonly activityType: string
  /** Explicit categorization direction. Readers must provide this; the planner never derives it from amount. */
  readonly direction: HistoricalDirection
  readonly merchantKey?: string | null
  readonly merchantKeyVersion?: number
  readonly normalizationVersion?: number
  readonly providerContext?: string | null
  readonly firstLabelDate?: string
  readonly lastLabelDate?: string
  readonly labelCount?: number
  readonly agreementState?: HistoricalAgreementState
  readonly userConfirmedCategory?: boolean
}

/** Injected readers are the only input boundary. This module has no export, DB, or HTTP client. */
export interface HistoricalReader { read(): MaybePromise<readonly HistoricalRow[]> }
export interface AccountMapping {
  readonly source: HistoricalSource
  readonly sourceAccountRef: string
  readonly canonicalAccountId: string
  readonly cutoverWatermark: string
}
export interface CategoryMapping {
  readonly source: HistoricalSource
  readonly sourceCategoryRef: string
  readonly canonicalCategoryKey: string
  readonly taxonomyId: string
  readonly categoryId: string
}
export interface HistoricalPlanOptions {
  readonly importRunId: string
  readonly accountMappings: readonly AccountMapping[]
  readonly categoryMappings: readonly CategoryMapping[]
  readonly apply?: boolean
}

export interface HistoricalMigrationManifest {
  readonly schemaVersion: 1
  readonly mode: 'dry-run'
  readonly provenance: { readonly importRunId: string; readonly generatedAt: string; readonly sources: readonly HistoricalSource[] }
  readonly cutovers: readonly AccountMapping[]
  readonly mappings: { readonly accounts: readonly AccountMapping[]; readonly categories: readonly CategoryMapping[] }
  readonly ledger: readonly {
    readonly identity: string; readonly source: HistoricalSource; readonly sourceId: string; readonly sourceAccountRef: string; readonly canonicalAccountId: string
    readonly activityType: string; readonly effectiveAt: string; readonly cutoverWatermark: string; readonly amount: string; readonly currency: string; readonly rawDescription: string
    readonly provenance: { readonly importRunId: string; readonly source: HistoricalSource }
  }[]
  readonly learningEvidence: readonly {
    readonly identity: string; readonly ledgerIdentity: string; readonly accountId: string
    readonly merchantKey: string; readonly merchantKeyVersion: number; readonly normalizationVersion: number
    readonly canonicalCategoryKey: string; readonly taxonomyId: string; readonly categoryId: string
    readonly direction: HistoricalDirection; readonly activityType: string; readonly providerContext: string | null
    readonly firstLabelDate: string; readonly lastLabelDate: string; readonly labelCount: number; readonly agreementState: HistoricalAgreementState
    readonly authority: 'user_confirmed' | 'historical_hint'
    readonly provenance: { readonly importRunId: string; readonly source: HistoricalSource; readonly sourceCategoryRef: string }
  }[]
  readonly overlapCandidates: readonly {
    readonly candidateId: string; readonly fingerprint: string
    readonly reason: 'cross_source_match' | 'at_or_after_cutover'; readonly ledgerIdentities: readonly string[]
  }[]
  readonly rollback: { readonly importRunId: string; readonly ledgerIdentities: readonly string[]; readonly learningEvidenceIdentities: readonly string[] }
}

function digest(parts: readonly string[]): string {
  return createHash('sha256').update(parts.map(part => `${Buffer.byteLength(part)}:${part}`).join(''), 'utf8').digest('hex')
}
function key(source: HistoricalSource, ref: string): string { return `${source}\u001f${ref}` }
function uniqueMap<T>(rows: readonly T[], identity: (row: T) => string, label: string): Map<string, T> {
  const out = new Map<string, T>()
  for (const row of rows) {
    const id = identity(row)
    if (!id || out.has(id)) throw new Error(`${label} collision: ${id || '<empty>'}`)
    out.set(id, row)
  }
  return out
}
function validateInstant(value: string, label: string): void {
  if (!value || Number.isNaN(Date.parse(value))) throw new Error(`invalid ${label}: ${value}`)
}
function required(value: string | null | undefined, label: string): string {
  if (!value?.trim()) throw new Error(`${label} is required`)
  return value
}
function positiveInteger(value: number | undefined, label: string): number {
  if (!Number.isSafeInteger(value) || value! < 1) throw new Error(`${label} must be a positive integer`)
  return value!
}

/** Build a deterministic, non-writing plan. `apply` is rejected until the injected sink is explicitly invoked. */
export async function planFinanceSyncHistory(
  readers: { readonly tarn: HistoricalReader; readonly lunchMoney: HistoricalReader },
  options: HistoricalPlanOptions,
): Promise<HistoricalMigrationManifest> {
  if (!options.importRunId.trim()) throw new Error('importRunId is required')
  if (options.apply === true) throw new Error('historical migration planner is dry-run only')
  const accountMappings = uniqueMap(options.accountMappings, row => key(row.source, row.sourceAccountRef), 'account mapping')
  const categoryMappings = uniqueMap(options.categoryMappings, row => key(row.source, row.sourceCategoryRef), 'category mapping')
  for (const mapping of options.accountMappings) {
    required(mapping.canonicalAccountId, 'canonicalAccountId')
    validateInstant(mapping.cutoverWatermark, 'cutover watermark')
  }
  for (const mapping of options.categoryMappings) {
    required(mapping.canonicalCategoryKey, 'canonicalCategoryKey')
    required(mapping.taxonomyId, 'taxonomyId')
    required(mapping.categoryId, 'categoryId')
  }

  const materialized: readonly [HistoricalSource, readonly HistoricalRow[]][] = [
    ['tarn', await readers.tarn.read()], ['lunch_money', await readers.lunchMoney.read()],
  ]
  const seenSourceIds = new Set<string>()
  const ledger: HistoricalMigrationManifest['ledger'][number][] = []
  const learningEvidence: HistoricalMigrationManifest['learningEvidence'][number][] = []
  const fingerprints = new Map<string, { identity: string; source: HistoricalSource }[]>()
  const cutoverOverlaps: { fingerprint: string; reason: 'at_or_after_cutover'; ledgerIdentities: string[] }[] = []

  for (const [source, rows] of materialized) for (const row of rows) {
    const sourceIdentity = key(source, row.sourceId)
    if (!row.sourceId || seenSourceIds.has(sourceIdentity)) throw new Error(`source identity collision: ${sourceIdentity}`)
    seenSourceIds.add(sourceIdentity)
    const account = accountMappings.get(key(source, row.accountRef))
    if (!account) throw new Error(`missing account mapping: ${source}/${row.accountRef}`)
    validateInstant(row.effectiveAt, 'effectiveAt')
    if (!/^-?\d+(?:\.\d+)?$/.test(row.amount)) throw new Error(`invalid amount: ${row.amount}`)
    if (!/^[A-Z]{3}$/.test(row.currency)) throw new Error(`invalid currency: ${row.currency}`)
    const activityType = required(row.activityType, 'activityType')
    if (row.direction !== 'inflow' && row.direction !== 'outflow') throw new Error(`invalid direction: ${String(row.direction)}`)
    const identity = `hist_ledger_v1_${digest([source, row.sourceId])}`
    ledger.push({
      identity, source, sourceId: row.sourceId, sourceAccountRef: row.accountRef, canonicalAccountId: account.canonicalAccountId,
      activityType, effectiveAt: row.effectiveAt, cutoverWatermark: account.cutoverWatermark, amount: row.amount,
      currency: row.currency, rawDescription: row.rawDescription, provenance: { importRunId: options.importRunId, source },
    })
    const fingerprint = digest([account.canonicalAccountId, row.effectiveAt.slice(0, 10), row.amount, row.currency])
    fingerprints.set(fingerprint, [...(fingerprints.get(fingerprint) ?? []), { identity, source }])
    if (Date.parse(row.effectiveAt) >= Date.parse(account.cutoverWatermark)) {
      cutoverOverlaps.push({ fingerprint, reason: 'at_or_after_cutover', ledgerIdentities: [identity] })
    }

    if (row.categoryRef) {
      const category = categoryMappings.get(key(source, row.categoryRef))
      if (!category) throw new Error(`missing category mapping: ${source}/${row.categoryRef}`)
      const merchantKey = required(row.merchantKey, 'merchantKey for categorized row')
      const merchantKeyVersion = positiveInteger(row.merchantKeyVersion, 'merchantKeyVersion')
      const normalizationVersion = positiveInteger(row.normalizationVersion, 'normalizationVersion')
      const firstLabelDate = required(row.firstLabelDate, 'firstLabelDate')
      const lastLabelDate = required(row.lastLabelDate, 'lastLabelDate')
      validateInstant(firstLabelDate, 'firstLabelDate')
      validateInstant(lastLabelDate, 'lastLabelDate')
      if (Date.parse(firstLabelDate) > Date.parse(lastLabelDate)) throw new Error('firstLabelDate must not be after lastLabelDate')
      const labelCount = positiveInteger(row.labelCount, 'labelCount')
      if (row.agreementState !== 'agreement' && row.agreementState !== 'conflict') throw new Error(`invalid agreementState: ${String(row.agreementState)}`)
      learningEvidence.push({
        identity: `hist_learning_v1_${digest([source, row.sourceId, row.categoryRef])}`, ledgerIdentity: identity,
        accountId: account.canonicalAccountId, merchantKey, merchantKeyVersion, normalizationVersion,
        canonicalCategoryKey: category.canonicalCategoryKey, taxonomyId: category.taxonomyId, categoryId: category.categoryId,
        direction: row.direction, activityType, providerContext: row.providerContext ?? null,
        firstLabelDate, lastLabelDate, labelCount, agreementState: row.agreementState,
        authority: row.userConfirmedCategory === true && row.agreementState === 'agreement' && labelCount >= 3 ? 'user_confirmed' : 'historical_hint',
        provenance: { importRunId: options.importRunId, source, sourceCategoryRef: row.categoryRef },
      })
    }
  }
  ledger.sort((a, b) => a.identity.localeCompare(b.identity))
  learningEvidence.sort((a, b) => a.identity.localeCompare(b.identity))
  const rawOverlapCandidates = [
    ...[...fingerprints.entries()].filter(([, rows]) => new Set(rows.map(row => row.source)).size > 1)
      .map(([fingerprint, rows]) => ({ fingerprint, reason: 'cross_source_match' as const, ledgerIdentities: rows.map(row => row.identity).sort() })),
    ...cutoverOverlaps,
  ]
  const overlapCandidates = rawOverlapCandidates.map(candidate => ({
    candidateId: `hist_candidate_v1_${digest([candidate.reason, candidate.fingerprint, ...candidate.ledgerIdentities])}`,
    ...candidate,
  })).sort((a, b) => a.candidateId.localeCompare(b.candidateId))
  const mappings = {
    accounts: [...options.accountMappings].sort((a, b) => key(a.source, a.sourceAccountRef).localeCompare(key(b.source, b.sourceAccountRef))),
    categories: [...options.categoryMappings].sort((a, b) => key(a.source, a.sourceCategoryRef).localeCompare(key(b.source, b.sourceCategoryRef))),
  }
  return Object.freeze({
    schemaVersion: 1, mode: 'dry-run', provenance: { importRunId: options.importRunId, generatedAt: new Date().toISOString(), sources: ['tarn', 'lunch_money'] as const },
    cutovers: mappings.accounts, mappings, ledger, learningEvidence, overlapCandidates,
    rollback: { importRunId: options.importRunId, ledgerIdentities: ledger.map(x => x.identity), learningEvidenceIdentities: learningEvidence.map(x => x.identity) },
  })
}

if (import.meta.main) throw new Error('inject Tarn and Lunch Money readers; live exports are intentionally unsupported')
