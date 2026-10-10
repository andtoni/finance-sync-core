// Outbound queue drainer. A persistent in-process loop pulls a Cloudflare Queue HTTP pull
// consumer, hands each message to an injected handler, then acknowledges it. The service only
// makes outbound requests. The queue is a latency optimisation, not a correctness dependency:
// if it is unreachable the loop backs off and scheduled synchronisation remains the backstop.
// `fetch` and the message handler are injected so tests can drive the loop.

/** The envelope the worker enqueues. */
export interface QueueEnvelope {
  source: string
  dedup_key: string
  payload: unknown
  received_at: string
}

export interface PulledMessage {
  lease_id: string
  id: string
  attempts: number
  /** Decoded message body — the `QueueEnvelope` when it parses, else the raw value. */
  envelope: QueueEnvelope | unknown
}

export interface DrainerConfig {
  accountId: string
  queueId: string
  /** CF_QUEUE_PULL_TOKEN — the pull-consumer API token. */
  pullToken: string
  apiBase?: string // default https://api.cloudflare.com/client/v4
  batchSize?: number // default 50 (CF max 100)
  visibilityTimeoutMs?: number // default 30_000
  /** Idle sleep when a pull came back empty (~10–15 s, the "seconds" path). */
  idlePollMs?: number // default 12_000
  /** Initial backoff after a pull/ack error (full-jitter, capped at maxBackoffMs). */
  baseBackoffMs?: number // default 1_000
  maxBackoffMs?: number // default 30_000
}

export interface DrainerDeps {
  fetchImpl: typeof fetch
  /**
   * Handle ONE pulled message. Returns true → ack, false → retry (re-deliver).
   * In production this records `webhook_events` (dedup) and calls
   * `ingest(envelope.source, …)`. A throw is treated as a retry, never an ack.
   */
  handle: (msg: PulledMessage) => boolean | Promise<boolean>
  log?: (level: 'info' | 'warn' | 'error', msg: string, extra?: unknown) => void
  sleep?: (ms: number) => Promise<void>
  /** Jitter source in [0,1); injectable for deterministic tests. Default Math.random. */
  random?: () => number
  /** Liveness hook — called after each SUCCESSFUL drain pass (drives the DMS heartbeat). */
  onPass?: (r: DrainResult) => void | Promise<void>
  /** Liveness hook — called when a pull/ack pass THROWS (drives the pull-error alert). */
  onError?: (err: unknown) => void | Promise<void>
}

export interface DrainResult {
  pulled: number
  acked: number
  retried: number
}

const DEFAULTS = {
  apiBase: 'https://api.cloudflare.com/client/v4',
  batchSize: 50,
  visibilityTimeoutMs: 30_000,
  idlePollMs: 12_000,
  baseBackoffMs: 1_000,
  maxBackoffMs: 30_000,
}

function decodeBody(raw: unknown): QueueEnvelope | unknown {
  if (raw && typeof raw === 'object') return raw // already deserialised
  if (typeof raw !== 'string') return raw
  const tryJson = (s: string): unknown => {
    try {
      return JSON.parse(s)
    } catch {
      return undefined
    }
  }
  const direct = tryJson(raw)
  if (direct !== undefined) return direct
  // CF may return the body base64-encoded (`May be base64 encoded`).
  try {
    const decoded =
      typeof atob === 'function' ? atob(raw) : Buffer.from(raw, 'base64').toString('utf8')
    const parsed = tryJson(decoded)
    return parsed !== undefined ? parsed : decoded
  } catch {
    return raw
  }
}

function authHeaders(cfg: DrainerConfig): Record<string, string> {
  return { 'content-type': 'application/json', authorization: `Bearer ${cfg.pullToken}` }
}

function queueUrl(cfg: DrainerConfig, op: 'pull' | 'ack'): string {
  const base = cfg.apiBase ?? DEFAULTS.apiBase
  return `${base}/accounts/${cfg.accountId}/queues/${cfg.queueId}/messages/${op}`
}

/** Pull one batch. Returns [] when the queue is empty; throws on a non-2xx pull. */
export async function pullBatch(cfg: DrainerConfig, deps: DrainerDeps): Promise<PulledMessage[]> {
  const res = await deps.fetchImpl(queueUrl(cfg, 'pull'), {
    method: 'POST',
    headers: authHeaders(cfg),
    body: JSON.stringify({
      visibility_timeout_ms: cfg.visibilityTimeoutMs ?? DEFAULTS.visibilityTimeoutMs,
      batch_size: cfg.batchSize ?? DEFAULTS.batchSize,
    }),
  })
  if (!res.ok) throw new Error(`queue pull failed: ${res.status}`)
  const json = (await res.json()) as {
    success?: boolean
    result?: { messages?: Array<{ body: unknown; id: string; attempts?: number; lease_id: string }> }
  }
  const messages = json.result?.messages ?? []
  return messages.map((m) => ({
    lease_id: m.lease_id,
    id: m.id,
    attempts: m.attempts ?? 0,
    envelope: decodeBody(m.body),
  }))
}

/** Ack/retry a set of leases in one call. No-op when both lists are empty. */
export async function ackBatch(
  cfg: DrainerConfig,
  deps: DrainerDeps,
  acks: string[],
  retries: string[],
): Promise<void> {
  if (acks.length === 0 && retries.length === 0) return
  const res = await deps.fetchImpl(queueUrl(cfg, 'ack'), {
    method: 'POST',
    headers: authHeaders(cfg),
    body: JSON.stringify({
      acks: acks.map((lease_id) => ({ lease_id })),
      retries: retries.map((lease_id) => ({ lease_id })),
    }),
  })
  if (!res.ok) throw new Error(`queue ack failed: ${res.status}`)
}

/**
 * One drain pass: pull a batch, handle each message, then ack the successes and
 * retry the failures in a single ack call. A handler throw → retry (never ack),
 * and the other messages in the batch are unaffected.
 */
export async function drainOnce(cfg: DrainerConfig, deps: DrainerDeps): Promise<DrainResult> {
  const messages = await pullBatch(cfg, deps)
  const acks: string[] = []
  const retries: string[] = []
  for (const msg of messages) {
    let ok = false
    try {
      ok = await deps.handle(msg)
    } catch (err) {
      deps.log?.('error', `drain handler threw for ${msg.lease_id}; will retry`, err)
      ok = false
    }
    if (ok) acks.push(msg.lease_id)
    else retries.push(msg.lease_id)
  }
  await ackBatch(cfg, deps, acks, retries)
  return { pulled: messages.length, acked: acks.length, retried: retries.length }
}

export interface DrainerHandle {
  /** Request loop shutdown; resolves via `done` once the in-flight pass returns. */
  stop(): void
  /** Resolves when the loop has fully exited. */
  done: Promise<void>
}

const defaultSleep = (ms: number) => new Promise<void>((r) => setTimeout(r, ms))

/**
 * Start the persistent loop. Drains back-to-back while messages keep arriving
 * (the "seconds" path under load); sleeps `idlePollMs` when a pull is empty; and
 * applies full-jitter exponential backoff (capped) when a pull/ack errors — so a
 * Cloudflare outage degrades to idle, never a hot error loop. Returns a handle
 * whose `stop()` ends the loop after the current pass.
 */
export function startDrainer(cfg: DrainerConfig, deps: DrainerDeps): DrainerHandle {
  const sleep = deps.sleep ?? defaultSleep
  const random = deps.random ?? Math.random
  const idle = cfg.idlePollMs ?? DEFAULTS.idlePollMs
  const base = cfg.baseBackoffMs ?? DEFAULTS.baseBackoffMs
  const maxBackoff = cfg.maxBackoffMs ?? DEFAULTS.maxBackoffMs
  let stopped = false
  let failures = 0

  const done = (async () => {
    while (!stopped) {
      try {
        const r = await drainOnce(cfg, deps)
        failures = 0
        // Liveness: a completed pass is a live drainer — feeds the DMS heartbeat +
        // clears any pull-error alert. Best-effort (never let it break the loop).
        try {
          await deps.onPass?.(r)
        } catch (e) {
          deps.log?.('warn', 'drain onPass hook threw (ignored)', e)
        }
        if (stopped) break
        // Empty pull → idle wait; non-empty → loop immediately to stay tight.
        if (r.pulled === 0) await sleep(idle)
      } catch (err) {
        failures++
        try {
          await deps.onError?.(err)
        } catch (e) {
          deps.log?.('warn', 'drain onError hook threw (ignored)', e)
        }
        const cap = Math.min(maxBackoff, base * 2 ** (failures - 1))
        const wait = Math.floor(random() * cap) // full jitter
        deps.log?.('warn', `drain pass failed (#${failures}); backing off ${wait}ms`, err)
        if (stopped) break
        await sleep(wait)
      }
    }
  })()

  return {
    stop() {
      stopped = true
    },
    done,
  }
}
