import type { DrainerHandle } from '../drain/drainer'

// With the drainer's 1 s base / 30 s cap full-jitter backoff, 20 consecutive
// failures take about four minutes: a short Cloudflare blip does not flip health,
// a sustained outage does.
export const WEBHOOK_DRAIN_FAILED_PASS_LIMIT = 20

export type WebhookDrainState = 'disabled' | 'ok' | 'failing'

export function webhookDrainState(
  drainer: Pick<DrainerHandle, 'consecutiveFailedPasses'> | undefined,
): WebhookDrainState {
  if (!drainer) return 'disabled'
  return drainer.consecutiveFailedPasses() >= WEBHOOK_DRAIN_FAILED_PASS_LIMIT ? 'failing' : 'ok'
}

export interface FinanceSyncHealth {
  readonly ok: boolean
  readonly service: 'finance-sync'
  readonly mode: 'disabled' | 'enabled'
  readonly databaseReady: boolean
  readonly providerReady: boolean
  readonly webhookDrain: WebhookDrainState
}

export function financeSyncHealth(
  databaseReady: boolean,
  cycleReady: boolean,
  connectionCount: number,
  cycleFailed: boolean,
  webhookDrain: WebhookDrainState,
): FinanceSyncHealth {
  const enabled = connectionCount > 0
  return {
    ok: databaseReady && !cycleFailed && (!enabled || cycleReady) && webhookDrain !== 'failing',
    service: 'finance-sync',
    mode: enabled ? 'enabled' : 'disabled',
    databaseReady,
    providerReady: enabled && cycleReady,
    webhookDrain,
  }
}

/**
 * Service-health outcome of one sync cycle. Only failures make a cycle fail:
 * a Plaid Item condition is reported per Item for its owner and never does.
 */
export function syncCycleOutcome(result: { readonly connections: number; readonly failures: number }): {
  readonly cycleReady: boolean
  readonly cycleFailed: boolean
  readonly event: 'FINANCE_SYNC_DISABLED' | 'FINANCE_SYNC_CYCLE_OK' | 'FINANCE_SYNC_CYCLE_FAILED'
} {
  const cycleReady = result.connections > 0 && result.failures === 0
  return {
    cycleReady,
    cycleFailed: result.failures > 0,
    event: result.connections === 0 && result.failures === 0 ? 'FINANCE_SYNC_DISABLED'
      : cycleReady ? 'FINANCE_SYNC_CYCLE_OK' : 'FINANCE_SYNC_CYCLE_FAILED',
  }
}

export function financeSyncHealthResponse(
  databaseReady: boolean,
  cycleReady: boolean,
  connectionCount: number,
  cycleFailed: boolean,
  webhookDrain: WebhookDrainState,
): { readonly status: number; readonly body: FinanceSyncHealth } {
  const body = financeSyncHealth(databaseReady, cycleReady, connectionCount, cycleFailed, webhookDrain)
  return { status: body.ok ? 200 : 503, body }
}
