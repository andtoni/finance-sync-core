export interface FinanceSyncHealth {
  readonly ok: boolean
  readonly service: 'finance-sync'
  readonly mode: 'disabled' | 'enabled'
  readonly databaseReady: boolean
  readonly providerReady: boolean
}

export function financeSyncHealth(
  databaseReady: boolean,
  cycleReady: boolean,
  connectionCount: number,
  cycleFailed = false,
): FinanceSyncHealth {
  const enabled = connectionCount > 0
  return {
    ok: databaseReady && !cycleFailed && (!enabled || cycleReady),
    service: 'finance-sync',
    mode: enabled ? 'enabled' : 'disabled',
    databaseReady,
    providerReady: enabled && cycleReady,
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
): { readonly status: number; readonly body: FinanceSyncHealth } {
  const body = financeSyncHealth(databaseReady, cycleReady, connectionCount, cycleFailed)
  return { status: body.ok ? 200 : 503, body }
}
