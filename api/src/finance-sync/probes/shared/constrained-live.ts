import { mkdir, writeFile } from 'node:fs/promises'
import { dirname, isAbsolute, resolve, sep } from 'node:path'
import type { ProbeProvider } from './capability-matrix'
import { redactProbeEvidence } from './redaction'

const MAX_G0_REQUESTS = 1_000
const utcInstant = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d{3})?Z$/

export interface ConstrainedLiveApproval {
  readonly approvalId: string
  readonly approver: string
  readonly purpose: string
  readonly provider: ProbeProvider
  readonly accountIds: readonly string[]
  readonly from: string
  readonly to: string
  readonly expiresAt: string
  readonly maxRequests: number
  readonly evidenceOutputPath: string
  readonly retentionDeadline: string
  readonly projectionEnabled: boolean
  readonly schedulingEnabled: boolean
}

export interface ConstrainedLiveRunner {
  consumeRequest(accountId: string): void
  writeEvidence(evidence: Readonly<Record<string, unknown>>): Promise<void>
}

export function createConstrainedLiveRunner(
  approval: ConstrainedLiveApproval,
  options: { readonly now: Date; readonly evidenceRoot: string },
): ConstrainedLiveRunner {
  validateApproval(approval, options.now)
  const outputPath = safeOutputPath(options.evidenceRoot, approval.evidenceOutputPath)
  let requestsRemaining = approval.maxRequests

  return Object.freeze({
    consumeRequest(accountId: string): void {
      if (!approval.accountIds.includes(accountId)) throw new Error('G0 account is not allowlisted')
      if (requestsRemaining <= 0) throw new Error('G0 request budget exhausted')
      requestsRemaining -= 1
    },
    async writeEvidence(evidence: Readonly<Record<string, unknown>>): Promise<void> {
      if ('mode' in evidence && evidence.mode !== 'constrained_live') {
        throw new Error('G0 evidence mode must be constrained_live')
      }
      const record = redactProbeEvidence({
        ...evidence,
        mode: 'constrained_live',
        provider: approval.provider,
        approvalId: approval.approvalId,
        retentionDeadline: approval.retentionDeadline,
      })
      await mkdir(dirname(outputPath), { recursive: true })
      await writeFile(outputPath, `${JSON.stringify(record, null, 2)}\n`, { encoding: 'utf8', flag: 'wx' })
    },
  })
}

function validateApproval(approval: ConstrainedLiveApproval, now: Date): void {
  if (!approval.approvalId.trim() || !approval.approver.trim() || !approval.purpose.trim()) {
    throw new Error('G0 explicit approval record is incomplete')
  }
  if (approval.accountIds.length === 0) throw new Error('G0 accountIds must not be empty')
  if (approval.accountIds.some((id) => !id.trim() || id.includes('*') || id.includes('?'))) {
    throw new Error('G0 wildcard or empty accountIds are prohibited')
  }
  const from = parseUtc(approval.from)
  const to = parseUtc(approval.to)
  if (from === null || to === null || from >= to) throw new Error('G0 requires a bounded UTC from/to window')
  const expiry = parseUtc(approval.expiresAt)
  if (expiry === null || expiry <= now.getTime()) throw new Error('G0 approval is expired or has invalid expiry')
  const retention = parseUtc(approval.retentionDeadline)
  if (retention === null || retention <= now.getTime()) throw new Error('G0 retentionDeadline must be a future UTC instant')
  if (!Number.isSafeInteger(approval.maxRequests) || approval.maxRequests < 1 || approval.maxRequests > MAX_G0_REQUESTS) {
    throw new Error(`G0 maxRequests must be between 1 and ${MAX_G0_REQUESTS}`)
  }
  if (approval.projectionEnabled) throw new Error('G0 production projection must be disabled')
  if (approval.schedulingEnabled) throw new Error('G0 production scheduling must be disabled')
  safeOutputPath('/g0-evidence-root', approval.evidenceOutputPath)
}

function parseUtc(value: string): number | null {
  if (!utcInstant.test(value)) return null
  const parsed = Date.parse(value)
  return Number.isFinite(parsed) ? parsed : null
}

function safeOutputPath(root: string, relativePath: string): string {
  if (!relativePath.trim() || isAbsolute(relativePath)) throw new Error('G0 evidenceOutputPath must be relative')
  const rootPath = resolve(root)
  const outputPath = resolve(rootPath, relativePath)
  if (!outputPath.startsWith(`${rootPath}${sep}`)) throw new Error('G0 evidenceOutputPath path traversal is prohibited')
  return outputPath
}
