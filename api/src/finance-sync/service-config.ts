import { readFileSync } from 'node:fs'
import { readExclusiveInput } from './secret-input'

export interface FinanceSyncServiceConfig {
  readonly databaseUrl: string
  readonly deploymentId: string
  readonly healthPort: number
  readonly intervalMs: number
  readonly controlToken: string
}

function required(env: NodeJS.ProcessEnv, name: string): string {
  return readExclusiveInput(env, name, true) as string
}

function controlToken(env: NodeJS.ProcessEnv): string {
  if (env.FINANCE_SYNC_CONTROL_TOKEN !== undefined) throw new Error('FINANCE_SYNC_CONTROL_TOKEN is not accepted; use FINANCE_SYNC_CONTROL_TOKEN_FILE')
  const path = env.FINANCE_SYNC_CONTROL_TOKEN_FILE
  if (!path) throw new Error('FINANCE_SYNC_CONTROL_TOKEN_FILE is required')
  let value: string
  try { value = readFileSync(path, 'utf8').trim() } catch { throw new Error('FINANCE_SYNC_CONTROL_TOKEN_FILE could not be read') }
  if (!value) throw new Error('FINANCE_SYNC_CONTROL_TOKEN_FILE must contain a token')
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
    controlToken: controlToken(env),
  }
}
