import { describe, expect, it } from 'vitest'
import { parseServiceConfig } from './service-config'

describe('Finance Sync service configuration', () => {
  const required = {
    FINANCE_SYNC_DATABASE_URL: 'postgres://finance-sync',
    FINANCE_SYNC_DEPLOYMENT_ID: 'production-1',
  }

  it('uses safe long-running service defaults', () => {
    expect(parseServiceConfig(required)).toMatchObject({
      databaseUrl: required.FINANCE_SYNC_DATABASE_URL,
      deploymentId: required.FINANCE_SYNC_DEPLOYMENT_ID,
      healthPort: 8080,
      intervalMs: 30_000,
    })
  })

  it.each(['FINANCE_SYNC_DATABASE_URL', 'FINANCE_SYNC_DEPLOYMENT_ID'] as const)('requires %s', (key) => {
    const env = { ...required }
    delete env[key]
    expect(() => parseServiceConfig(env)).toThrow(`${key} is required`)
  })

  it.each(['0', '-1', 'not-a-number', '1.5'])('rejects invalid interval %s', (interval) => {
    expect(() => parseServiceConfig({ ...required, FINANCE_SYNC_INTERVAL_MS: interval })).toThrow('FINANCE_SYNC_INTERVAL_MS must be a positive integer')
  })
})
