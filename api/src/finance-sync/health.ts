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
