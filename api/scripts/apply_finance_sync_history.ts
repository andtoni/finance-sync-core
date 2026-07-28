import { createHash } from 'node:crypto'
import type { HistoricalMigrationManifest, HistoricalSource } from './plan_finance_sync_history'

type MaybePromise<T> = T | Promise<T>
type Candidate = HistoricalMigrationManifest['overlapCandidates'][number]
type Ledger = HistoricalMigrationManifest['ledger'][number]
type Evidence = HistoricalMigrationManifest['learningEvidence'][number]

export type HistoricalCandidateResolution =
  | { readonly candidateId: string; readonly action: 'exclude_all' }
  | { readonly candidateId: string; readonly action: 'keep_one'; readonly ledgerIdentity: string }

export interface HistoricalLedgerWrite {
  readonly idempotencyKey: string
  readonly historicalIdentity: string
  readonly source: HistoricalSource
  readonly sourceId: string
  readonly accountId: string
  readonly activityType: string
  readonly effectiveAt: string
  readonly cutoverWatermark: string
  readonly amount: string
  readonly currency: string
  readonly rawDescription: string
}

/** Only trustworthy manual labels enter this lane. Historical hints have no sink representation. */
export interface HistoricalLearningEvidenceWrite {
  readonly idempotencyKey: string
  readonly merchantKey: string
  readonly merchantKeyVersion: number
  readonly normalizationVersion: number
  readonly taxonomyId: string
  readonly categoryId: string
  readonly direction: 'inflow' | 'outflow'
  readonly activityType: string
  readonly providerContext: string | null
  readonly accountId: string
  readonly source: HistoricalSource
  readonly sourceCategoryRef: string
  readonly firstLabelDate: string
  readonly lastLabelDate: string
  readonly labelCount: number
  readonly agreementState: 'agreement' | 'conflict'
  readonly authority: 'user_confirmed'
  /** Reviewed-manifest binding; evidence remains valid even when its financial row was excluded. */
  readonly manifestLedgerIdentity: string
  readonly ledgerDisposition: 'selected' | 'excluded'
  readonly overlapProvenance: readonly {
    readonly candidateId: string
    readonly reason: Candidate['reason']
    readonly candidateLedgerIdentities: readonly string[]
    readonly action: HistoricalCandidateResolution['action']
    readonly selectedLedgerIdentity: string | null
    readonly excludedLedgerIdentities: readonly string[]
  }[]
}

export interface HistoricalReviewedManifest {
  readonly schemaVersion: 1
  readonly importRunId: string
  /** Immutable source coverage, including excluded ledger rows. */
  readonly sources: readonly {
    readonly source: HistoricalSource
    readonly checksum: string
    readonly ledgerCount: number
    readonly totals: readonly { readonly currency: string; readonly amount: string }[]
  }[]
  /** Every ledger field is approval-bound, including rows excluded by overlap review. */
  readonly ledger: readonly (HistoricalLedgerWrite & {
    readonly sourceAccountRef: string
    readonly provenance: { readonly importRunId: string; readonly source: HistoricalSource }
    readonly disposition: 'selected' | 'excluded'
  })[]
  /** Full reviewed evidence payloads, rather than a lossy digest-only summary. */
  readonly evidence: readonly HistoricalLearningEvidenceWrite[]
  readonly overlapResolutions: readonly HistoricalLearningEvidenceWrite['overlapProvenance'][number][]
}

interface HistoricalBatchBase {
  readonly schemaVersion: 1
  readonly subject: string
  readonly importRunId: string
  readonly idempotencyKey: string
  readonly dryRun: boolean
  readonly overlapsResolved: true
}
export type WealthfolioHistoricalBatch = HistoricalBatchBase & (
  | { readonly operation: 'stage_ledger'; readonly manifestHash: string; readonly manifest: HistoricalReviewedManifest; readonly rows: readonly HistoricalLedgerWrite[] }
  | { readonly operation: 'stage_learning_evidence'; readonly manifestHash: string; readonly manifest: HistoricalReviewedManifest; readonly rows: readonly HistoricalLearningEvidenceWrite[] }
  | { readonly operation: 'finalize'; readonly manifestHash: string; readonly manifest: HistoricalReviewedManifest }
  | { readonly operation: 'rollback' }
)
export interface WealthfolioHistoricalBatchReceipt {
  readonly dryRun: boolean
  readonly importRunId: string
  readonly validated: number
  readonly written: number
  readonly alreadyPresent: number
  readonly ledgerDeleted: number
  readonly learningEvidenceDeleted: number
  readonly checksum: string
}

/** Sole writer boundary: an implementation may only submit the strict schema-v1 historical batch. */
export interface WealthfolioHistorySink {
  writeHistoricalBatch(batch: WealthfolioHistoricalBatch): MaybePromise<WealthfolioHistoricalBatchReceipt>
}

export interface HistoricalOwnerApproval {
  readonly approvalId: string
  readonly action: 'apply_finance_sync_history'
  readonly subject: string
  readonly importRunId: string
  readonly manifestHash: string
  readonly issuedAt: string
  readonly expiresAt: string
}
export type HistoricalOwnerApprovalVerifier = (approval: HistoricalOwnerApproval, manifest: HistoricalReviewedManifest) => MaybePromise<boolean>

export interface HistoricalApplyOptions {
  readonly apply?: boolean
  readonly subject?: string
  /** Separately obtained owner evidence; caller hashes alone are not approvals. */
  readonly ownerApproval?: HistoricalOwnerApproval
  /** Trusted verifier supplied by the owner-approval authority. */
  readonly verifyOwnerApproval?: HistoricalOwnerApprovalVerifier
  readonly resolutions?: readonly HistoricalCandidateResolution[]
  readonly batchSize?: number
}
export interface HistoricalApplyResult {
  readonly mode: 'dry-run' | 'applied'
  readonly importRunId: string
  /** Exact canonical reviewed-manifest hash that owner approval must name for apply. */
  readonly manifestHash: string
  readonly ledger: { readonly manifest: number; readonly selected: number; readonly excluded: number; readonly written: number; readonly alreadyPresent: number; readonly checksum: string }
  readonly learningEvidence: { readonly manifest: number; readonly selectedManual: number; readonly excludedHints: number; readonly written: number; readonly alreadyPresent: number; readonly checksum: string }
}
export interface HistoricalRollbackResult {
  readonly mode: 'dry-run' | 'rolled-back'
  readonly importRunId: string
  readonly idempotencyKey: string
  readonly ledgerDeleted: number
  readonly learningEvidenceDeleted: number
  readonly checksum: string
}

function digest(parts: readonly string[]): string {
  return createHash('sha256').update(parts.map(part => `${Buffer.byteLength(part)}:${part}`).join(''), 'utf8').digest('hex')
}
function mappingKey(source: HistoricalSource, ref: string): string { return `${source}\u001f${ref}` }
function checksum(keys: readonly string[]): string { return digest([...keys].sort()) }
function canonicalJson(value: unknown): string {
  if (value === null) return 'null'
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(',')}]`
  if (typeof value === 'object') {
    const input = value as Readonly<Record<string, unknown>>
    return `{${Object.keys(input).sort().map(key => `${JSON.stringify(key)}:${canonicalJson(input[key])}`).join(',')}}`
  }
  return JSON.stringify(value)
}
export function historicalManifestHash(manifest: HistoricalReviewedManifest): string {
  return createHash('sha256').update(canonicalJson(manifest), 'utf8').digest('hex')
}
function requiredSubject(subject: string | undefined): string {
  if (!subject?.trim()) throw new Error('authenticated subject is required when apply is true')
  return subject
}
function uniqueBy<T>(rows: readonly T[], key: (row: T) => string, label: string): Map<string, T> {
  const out = new Map<string, T>()
  for (const row of rows) {
    const identity = key(row)
    if (!identity || out.has(identity)) throw new Error(`ambiguous ${label}: ${identity || '<empty>'}`)
    out.set(identity, row)
  }
  return out
}

function validateManifest(manifest: HistoricalMigrationManifest): Map<string, Candidate> {
  if (manifest.schemaVersion !== 1 || manifest.mode !== 'dry-run') throw new Error('unsupported historical migration manifest')
  const importRunId = manifest.provenance.importRunId
  if (!importRunId.trim() || manifest.rollback.importRunId !== importRunId) throw new Error('inconsistent importRunId')
  const accounts = uniqueBy(manifest.mappings.accounts, row => mappingKey(row.source, row.sourceAccountRef), 'account mapping')
  const categories = uniqueBy(manifest.mappings.categories, row => mappingKey(row.source, row.sourceCategoryRef), 'category mapping')
  const ledger = uniqueBy(manifest.ledger, row => row.identity, 'ledger identity')
  uniqueBy(manifest.learningEvidence, row => row.identity, 'learning evidence identity')
  const actualFingerprints = new Map<string, { identity: string; source: HistoricalSource }[]>()
  const expectedCutover = new Map<string, string>()
  for (const row of manifest.ledger) {
    if (row.provenance.importRunId !== importRunId || row.provenance.source !== row.source) throw new Error(`inconsistent ledger provenance: ${row.identity}`)
    const account = accounts.get(mappingKey(row.source, row.sourceAccountRef))
    if (!account || account.canonicalAccountId !== row.canonicalAccountId || account.cutoverWatermark !== row.cutoverWatermark) throw new Error(`missing or ambiguous account mapping for ledger: ${row.identity}`)
    if (!row.activityType.trim()) throw new Error(`missing activityType for ledger: ${row.identity}`)
    const effectiveAt = Date.parse(row.effectiveAt)
    const cutover = Date.parse(row.cutoverWatermark)
    if (Number.isNaN(effectiveAt) || Number.isNaN(cutover)) throw new Error(`invalid ledger/cutover timestamp: ${row.identity}`)
    const fingerprint = digest([row.canonicalAccountId, row.effectiveAt.slice(0, 10), row.amount, row.currency])
    actualFingerprints.set(fingerprint, [...(actualFingerprints.get(fingerprint) ?? []), { identity: row.identity, source: row.source }])
    if (effectiveAt >= cutover) expectedCutover.set(row.identity, fingerprint)
  }
  for (const row of manifest.learningEvidence) {
    if (!ledger.has(row.ledgerIdentity) || row.provenance.importRunId !== importRunId) throw new Error(`inconsistent learning evidence: ${row.identity}`)
    const category = categories.get(mappingKey(row.provenance.source, row.provenance.sourceCategoryRef))
    if (!category || category.canonicalCategoryKey !== row.canonicalCategoryKey || category.taxonomyId !== row.taxonomyId || category.categoryId !== row.categoryId) throw new Error(`missing or ambiguous category mapping for evidence: ${row.identity}`)
    if (!row.accountId.trim() || !row.activityType.trim() || !row.merchantKey.trim() || row.labelCount < 1 || !Number.isSafeInteger(row.labelCount)) throw new Error(`invalid strict evidence fields: ${row.identity}`)
    if (row.authority === 'user_confirmed' && (row.agreementState !== 'agreement' || row.labelCount < 3)) throw new Error(`authoritative evidence requires three agreeing labels: ${row.identity}`)
  }
  const candidates = uniqueBy(manifest.overlapCandidates, row => row.candidateId, 'overlap candidate')
  const cutoffCovered = new Set<string>()
  const crossCovered = new Set<string>()
  for (const candidate of manifest.overlapCandidates) {
    const ids = [...candidate.ledgerIdentities].sort()
    if (!ids.length || ids.some(id => !ledger.has(id))) throw new Error(`invalid overlap candidate: ${candidate.candidateId}`)
    const expectedId = `hist_candidate_v1_${digest([candidate.reason, candidate.fingerprint, ...ids])}`
    if (candidate.candidateId !== expectedId) throw new Error(`invalid overlap candidate identity: ${candidate.candidateId}`)
    if (candidate.reason === 'at_or_after_cutover') {
      if (ids.length !== 1 || expectedCutover.get(ids[0]) !== candidate.fingerprint) throw new Error(`invalid cutover candidate: ${candidate.candidateId}`)
      cutoffCovered.add(ids[0])
    } else {
      const actual = actualFingerprints.get(candidate.fingerprint) ?? []
      const expectedIds = actual.map(row => row.identity).sort()
      if (new Set(actual.map(row => row.source)).size < 2 || ids.join('\u001f') !== expectedIds.join('\u001f')) throw new Error(`invalid cross-source candidate: ${candidate.candidateId}`)
      crossCovered.add(candidate.fingerprint)
    }
  }
  if ([...expectedCutover.keys()].some(id => !cutoffCovered.has(id))) throw new Error('manifest omits an at-or-after-cutover candidate')
  for (const [fingerprint, rows] of actualFingerprints) if (new Set(rows.map(row => row.source)).size > 1 && !crossCovered.has(fingerprint)) throw new Error('manifest omits a cross-source overlap candidate')
  return candidates
}

function resolveLedger(manifest: HistoricalMigrationManifest, candidates: Map<string, Candidate>, resolutions: readonly HistoricalCandidateResolution[]): Set<string> {
  const byCandidate = uniqueBy(resolutions, row => row.candidateId, 'candidate resolution')
  for (const id of byCandidate.keys()) if (!candidates.has(id)) throw new Error(`resolution does not match a candidate: ${id}`)
  const unresolved = [...candidates.keys()].filter(id => !byCandidate.has(id))
  if (unresolved.length) throw new Error(`unresolved historical overlap candidates: ${unresolved.join(', ')}`)
  const excluded = new Set<string>()
  for (const candidate of candidates.values()) {
    const resolution = byCandidate.get(candidate.candidateId)!
    if (candidate.reason === 'at_or_after_cutover' && resolution.action !== 'exclude_all') throw new Error(`at-or-after-cutover candidate must be excluded: ${candidate.candidateId}`)
    if (resolution.action === 'exclude_all') for (const id of candidate.ledgerIdentities) excluded.add(id)
    else {
      if (!candidate.ledgerIdentities.includes(resolution.ledgerIdentity)) throw new Error(`kept ledger is not in candidate: ${candidate.candidateId}`)
      for (const id of candidate.ledgerIdentities) if (id !== resolution.ledgerIdentity) excluded.add(id)
    }
  }
  return excluded
}
function ledgerWrite(row: Ledger): HistoricalLedgerWrite {
  return { idempotencyKey: row.identity, historicalIdentity: row.identity, source: row.source, sourceId: row.sourceId, accountId: row.canonicalAccountId, activityType: row.activityType, effectiveAt: row.effectiveAt, cutoverWatermark: row.cutoverWatermark, amount: row.amount, currency: row.currency, rawDescription: row.rawDescription }
}
function reviewedManifest(
  manifest: HistoricalMigrationManifest,
  candidates: ReadonlyMap<string, Candidate>,
  resolutions: ReadonlyMap<string, HistoricalCandidateResolution>,
  excluded: ReadonlySet<string>,
): HistoricalReviewedManifest {
  const overlapResolutions = [...candidates.values()].map(candidate => {
    const resolution = resolutions.get(candidate.candidateId)
    if (!resolution) throw new Error(`missing reviewed resolution: ${candidate.candidateId}`)
    const selectedLedgerIdentity = resolution.action === 'keep_one' ? resolution.ledgerIdentity : null
    return {
      candidateId: candidate.candidateId,
      reason: candidate.reason,
      candidateLedgerIdentities: [...candidate.ledgerIdentities].sort(),
      action: resolution.action,
      selectedLedgerIdentity,
      excludedLedgerIdentities: candidate.ledgerIdentities.filter(identity => identity !== selectedLedgerIdentity).sort(),
    }
  }).sort((a, b) => a.candidateId.localeCompare(b.candidateId))
  const sourceSummaries = (['tarn', 'lunch_money'] as const).map(source => {
    const rows = manifest.ledger.filter(row => row.source === source).sort((a, b) => a.identity.localeCompare(b.identity))
    const totals = new Map<string, string>()
    for (const row of rows) totals.set(row.currency, addDecimal(totals.get(row.currency) ?? '0', row.amount))
    return Object.freeze({ source, checksum: createHash('sha256').update(canonicalJson(rows), 'utf8').digest('hex'), ledgerCount: rows.length,
      totals: Object.freeze([...totals].map(([currency, amount]) => Object.freeze({ currency, amount })).sort((a, b) => a.currency.localeCompare(b.currency))) })
  })
  return Object.freeze({
    schemaVersion: 1 as const, importRunId: manifest.provenance.importRunId, sources: Object.freeze(sourceSummaries),
    ledger: Object.freeze(manifest.ledger.map(row => Object.freeze({
      ...ledgerWrite(row), sourceAccountRef: row.sourceAccountRef, provenance: row.provenance,
      disposition: excluded.has(row.identity) ? 'excluded' as const : 'selected' as const,
    })).sort((a, b) => a.historicalIdentity.localeCompare(b.historicalIdentity))),
    evidence: Object.freeze([]), overlapResolutions: Object.freeze(overlapResolutions),
  })
}
function addDecimal(left: string, right: string): string {
  const parse = (value: string) => { const [whole, fraction = ''] = value.replace(/^\+/, '').split('.'); return { negative: whole.startsWith('-'), whole: (whole.replace('-', '') || '0'), fraction } }
  const a = parse(left); const b = parse(right); const scale = Math.max(a.fraction.length, b.fraction.length)
  const integer = (value: { negative: boolean; whole: string; fraction: string }) => BigInt(`${value.negative ? '-' : ''}${value.whole}${value.fraction.padEnd(scale, '0')}`)
  const total = integer(a) + integer(b); const negative = total < 0n; const digits = (negative ? -total : total).toString().padStart(scale + 1, '0')
  const whole = scale ? digits.slice(0, -scale) : digits; const fraction = scale ? digits.slice(-scale).replace(/0+$/, '') : ''
  return `${negative ? '-' : ''}${whole}${fraction ? `.${fraction}` : ''}`
}
async function validateOwnerApproval(approval: HistoricalOwnerApproval | undefined, verifier: HistoricalOwnerApprovalVerifier | undefined, subject: string, importRunId: string, manifestHash: string, manifest: HistoricalReviewedManifest): Promise<void> {
  if (!approval || !verifier || !approval.approvalId.trim() || approval.action !== 'apply_finance_sync_history'
    || approval.subject !== subject || approval.importRunId !== importRunId || approval.manifestHash !== manifestHash
    || Number.isNaN(Date.parse(approval.issuedAt)) || Number.isNaN(Date.parse(approval.expiresAt))
    || Date.parse(approval.issuedAt) > Date.now() || Date.parse(approval.expiresAt) <= Date.now()) {
    throw new Error('separately verified action-bound manifest approval is required')
  }
  if (!await verifier(approval, manifest)) throw new Error('separately verified action-bound manifest approval is required')
}

function evidenceWrite(
  row: Evidence,
  candidates: Map<string, Candidate>,
  resolutions: ReadonlyMap<string, HistoricalCandidateResolution>,
  excluded: ReadonlySet<string>,
): HistoricalLearningEvidenceWrite {
  if (row.authority !== 'user_confirmed' || row.agreementState !== 'agreement' || row.labelCount < 3) throw new Error('historical evidence is not authoritative')
  const overlapProvenance = [...candidates.values()].filter(candidate => candidate.ledgerIdentities.includes(row.ledgerIdentity)).map(candidate => {
    const resolution = resolutions.get(candidate.candidateId)
    if (!resolution) throw new Error(`missing reviewed resolution for evidence: ${row.identity}`)
    const selectedLedgerIdentity = resolution.action === 'keep_one' ? resolution.ledgerIdentity : null
    return {
      candidateId: candidate.candidateId, reason: candidate.reason,
      candidateLedgerIdentities: [...candidate.ledgerIdentities], action: resolution.action,
      selectedLedgerIdentity,
      excludedLedgerIdentities: candidate.ledgerIdentities.filter(identity => identity !== selectedLedgerIdentity),
    }
  })
  return {
    idempotencyKey: row.identity, merchantKey: row.merchantKey, merchantKeyVersion: row.merchantKeyVersion,
    normalizationVersion: row.normalizationVersion, taxonomyId: row.taxonomyId, categoryId: row.categoryId,
    direction: row.direction, activityType: row.activityType, providerContext: row.providerContext, accountId: row.accountId,
    source: row.provenance.source, sourceCategoryRef: row.provenance.sourceCategoryRef, firstLabelDate: row.firstLabelDate,
    lastLabelDate: row.lastLabelDate, labelCount: row.labelCount, agreementState: row.agreementState, authority: 'user_confirmed',
    manifestLedgerIdentity: row.ledgerIdentity, ledgerDisposition: excluded.has(row.ledgerIdentity) ? 'excluded' : 'selected',
    overlapProvenance,
  }
}
function validateReceipt(receipt: WealthfolioHistoricalBatchReceipt, importRunId: string, expectedCount: number, expectedChecksum: string): void {
  if (receipt.importRunId !== importRunId || receipt.validated !== expectedCount || receipt.checksum !== expectedChecksum || receipt.written + receipt.alreadyPresent !== expectedCount) throw new Error('invalid Wealthfolio historical batch receipt')
}
async function writeBatches<T extends HistoricalLedgerWrite | HistoricalLearningEvidenceWrite>(
  rows: readonly T[], operation: 'stage_ledger' | 'stage_learning_evidence', batchSize: number,
  subject: string, importRunId: string, manifest: HistoricalReviewedManifest, manifestHash: string,
  sink: WealthfolioHistorySink,
): Promise<{ written: number; alreadyPresent: number }> {
  let written = 0; let alreadyPresent = 0
  for (let offset = 0; offset < rows.length; offset += batchSize) {
    const batchRows = rows.slice(offset, offset + batchSize)
    const keys = batchRows.map(row => row.idempotencyKey)
    const expectedChecksum = checksum(keys)
    const idempotencyKey = `hist_batch_v1_${digest([importRunId, operation, ...keys])}`
    const base = { schemaVersion: 1 as const, subject, importRunId, idempotencyKey, dryRun: false, overlapsResolved: true as const, manifestHash, manifest }
    const batch: WealthfolioHistoricalBatch = operation === 'stage_ledger' ? { ...base, operation, rows: batchRows as readonly HistoricalLedgerWrite[] } : { ...base, operation, rows: batchRows as readonly HistoricalLearningEvidenceWrite[] }
    const receipt = await sink.writeHistoricalBatch(batch)
    validateReceipt(receipt, importRunId, batchRows.length, expectedChecksum)
    written += receipt.written; alreadyPresent += receipt.alreadyPresent
  }
  return { written, alreadyPresent }
}

/** Validates and reconciles by default; emits strict batches only with the explicit `apply: true` gate. */
export async function applyFinanceSyncHistory(manifest: HistoricalMigrationManifest, sink: WealthfolioHistorySink, options: HistoricalApplyOptions = {}): Promise<HistoricalApplyResult> {
  const candidates = validateManifest(manifest)
  const excluded = resolveLedger(manifest, candidates, options.resolutions ?? [])
  const batchSize = options.batchSize ?? 100
  if (!Number.isSafeInteger(batchSize) || batchSize < 1) throw new Error('batchSize must be a positive integer')
  const importRunId = manifest.provenance.importRunId
  const ledgerRows = manifest.ledger.filter(row => !excluded.has(row.identity)).map(ledgerWrite)
  const resolutions = uniqueBy(options.resolutions ?? [], row => row.candidateId, 'candidate resolution')
  const evidenceRows = manifest.learningEvidence.filter(row => row.authority === 'user_confirmed')
    .map(row => evidenceWrite(row, candidates, resolutions, excluded))
  const manifestWithoutEvidence = reviewedManifest(manifest, candidates, resolutions, excluded)
  const canonicalManifest: HistoricalReviewedManifest = Object.freeze({
    ...manifestWithoutEvidence,
    evidence: Object.freeze(evidenceRows.map(row => Object.freeze(row)).sort((a, b) => a.idempotencyKey.localeCompare(b.idempotencyKey))),
  })
  const manifestHash = historicalManifestHash(canonicalManifest)
  let ledgerReceipt = { written: 0, alreadyPresent: 0 }; let evidenceReceipt = { written: 0, alreadyPresent: 0 }
  if (options.apply === true) {
    const subject = requiredSubject(options.subject)
    await validateOwnerApproval(options.ownerApproval, options.verifyOwnerApproval, subject, importRunId, manifestHash, canonicalManifest)
    // Evidence is accepted and receipt-validated before any financial ledger batch is sent.
    evidenceReceipt = await writeBatches(evidenceRows, 'stage_learning_evidence', batchSize, subject, importRunId, canonicalManifest, manifestHash, sink)
    ledgerReceipt = await writeBatches(ledgerRows, 'stage_ledger', batchSize, subject, importRunId, canonicalManifest, manifestHash, sink)
    const idempotencyKey = `hist_finalize_v1_${digest([importRunId, manifestHash])}`
    const finalize = await sink.writeHistoricalBatch({ schemaVersion: 1, subject, importRunId, idempotencyKey, dryRun: false, overlapsResolved: true, operation: 'finalize', manifestHash, manifest: canonicalManifest })
    validateReceipt(finalize, importRunId, ledgerRows.length + evidenceRows.length, checksum([...ledgerRows, ...evidenceRows].map(row => row.idempotencyKey)))
  }
  return { mode: options.apply === true ? 'applied' : 'dry-run', importRunId, manifestHash,
    ledger: { manifest: manifest.ledger.length, selected: ledgerRows.length, excluded: excluded.size, ...ledgerReceipt, checksum: checksum(ledgerRows.map(row => row.idempotencyKey)) },
    learningEvidence: { manifest: manifest.learningEvidence.length, selectedManual: evidenceRows.length, excludedHints: manifest.learningEvidence.length - evidenceRows.length, ...evidenceReceipt, checksum: checksum(evidenceRows.map(row => row.idempotencyKey)) },
  }
}

export async function rollbackFinanceSyncHistory(importRunId: string, sink: WealthfolioHistorySink, options: { readonly apply?: boolean; readonly subject?: string } = {}): Promise<HistoricalRollbackResult> {
  if (!importRunId.trim()) throw new Error('importRunId is required')
  const idempotencyKey = `hist_rollback_v1_${digest([importRunId])}`
  let deleted = { ledgerDeleted: 0, learningEvidenceDeleted: 0 }; let resultChecksum = digest([importRunId, 'dry-run'])
  if (options.apply === true) {
    const receipt = await sink.writeHistoricalBatch({ schemaVersion: 1, subject: requiredSubject(options.subject), importRunId, idempotencyKey, dryRun: false, overlapsResolved: true, operation: 'rollback' })
    if (receipt.importRunId !== importRunId || receipt.validated !== 0 || receipt.written !== 0 || receipt.alreadyPresent !== 0) throw new Error('invalid Wealthfolio rollback receipt')
    deleted = { ledgerDeleted: receipt.ledgerDeleted, learningEvidenceDeleted: receipt.learningEvidenceDeleted }; resultChecksum = receipt.checksum
  }
  if (!Number.isSafeInteger(deleted.ledgerDeleted) || deleted.ledgerDeleted < 0 || !Number.isSafeInteger(deleted.learningEvidenceDeleted) || deleted.learningEvidenceDeleted < 0) throw new Error('invalid Wealthfolio rollback receipt')
  return { mode: options.apply === true ? 'rolled-back' : 'dry-run', importRunId, idempotencyKey, ...deleted, checksum: resultChecksum }
}

export type HistoricalFetch = (input: string, init: { readonly method: 'POST'; readonly headers: Readonly<Record<string, string>>; readonly body: string }) => Promise<{ readonly ok: boolean; readonly status: number; json(): Promise<unknown> }>
/** Secret-safe adapter: both independent credentials are requested per call and never retained, serialized, or logged. */
export class HttpWealthfolioHistorySink implements WealthfolioHistorySink {
  constructor(
    private readonly fetch: HistoricalFetch,
    private readonly baseUrl: string,
    private readonly tokenProvider: () => MaybePromise<string>,
    private readonly importerAuthorityProvider: () => MaybePromise<string>,
  ) {
    if (!baseUrl.trim()) throw new Error('Wealthfolio base URL is required')
  }
  async writeHistoricalBatch(batch: WealthfolioHistoricalBatch): Promise<WealthfolioHistoricalBatchReceipt> {
    const token = await this.tokenProvider()
    if (!token.trim()) throw new Error('Wealthfolio authentication token is required')
    if (!this.importerAuthorityProvider) throw new Error('dedicated historical importer authority is required')
    const importerAuthority = await this.importerAuthorityProvider()
    if (!importerAuthority.trim()) throw new Error('dedicated historical importer authority is required')
    const response = await this.fetch(`${this.baseUrl.replace(/\/$/, '')}/internal/history`, { method: 'POST', headers: { authorization: `Bearer ${token}`, 'x-wealthfolio-importer-authority': `Bearer ${importerAuthority}`, 'content-type': 'application/json', 'idempotency-key': batch.idempotencyKey }, body: JSON.stringify(batch) })
    if (!response.ok) throw new Error(`Wealthfolio historical sink rejected request (${response.status})`)
    return await response.json() as WealthfolioHistoricalBatchReceipt
  }
}

if (import.meta.main) throw new Error('inject an authenticated WealthfolioHistorySink; standalone writes are intentionally unsupported')
