export interface FinanceSyncServiceConfig {
  readonly databaseUrl: string
  readonly deploymentId: string
  readonly healthPort: number
  readonly intervalMs: number
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  const value = env[name]
  if (!value) throw new Error(`${name} is required`)
  return value
}

function positiveInteger(value: string | undefined, fallback: number, name: string): number {
  if (value === undefined) return fallback
  const parsed = Number(value)
  if (!Number.isSafeInteger(parsed) || parsed <= 0) throw new Error(`${name} must be a positive integer`)
  return parsed
}

export function parseServiceConfig(env: NodeJS.ProcessEnv): FinanceSyncServiceConfig {
  return {
    databaseUrl: required(env, 'FINANCE_SYNC_DATABASE_URL'),
    deploymentId: required(env, 'FINANCE_SYNC_DEPLOYMENT_ID'),
    healthPort: positiveInteger(env.FINANCE_SYNC_HEALTH_PORT, 8080, 'FINANCE_SYNC_HEALTH_PORT'),
    intervalMs: positiveInteger(env.FINANCE_SYNC_INTERVAL_MS, 30_000, 'FINANCE_SYNC_INTERVAL_MS'),
  }
}
