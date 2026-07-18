import { mkdtempSync, rmSync, writeFileSync } from 'node:fs'
import { tmpdir } from 'node:os'
import { join } from 'node:path'
import { afterAll, describe, expect, it } from 'vitest'
import { parseServiceConfig } from './service-config'

describe('Finance Sync service configuration', () => {
  const root = mkdtempSync(join(tmpdir(), 'finance-sync-config-'))
  const tokenFile = join(root, 'control-token')
  writeFileSync(tokenFile, 'dedicated-control-token\n', { mode: 0o600 })
  afterAll(() => rmSync(root, { recursive: true, force: true }))
  const required = {
    FINANCE_SYNC_DATABASE_URL: 'postgres://finance-sync',
    FINANCE_SYNC_DEPLOYMENT_ID: 'production-1',
    FINANCE_SYNC_CONTROL_TOKEN_FILE: tokenFile,
  }

  it('uses safe long-running service defaults', () => {
    expect(parseServiceConfig(required)).toMatchObject({
      databaseUrl: required.FINANCE_SYNC_DATABASE_URL,
      deploymentId: required.FINANCE_SYNC_DEPLOYMENT_ID,
      healthPort: 8080,
      intervalMs: 30_000,
      controlToken: 'dedicated-control-token',
    })
  })

  it.each(['FINANCE_SYNC_DATABASE_URL', 'FINANCE_SYNC_DEPLOYMENT_ID', 'FINANCE_SYNC_CONTROL_TOKEN_FILE'] as const)('requires %s', (key) => {
    const env = { ...required }
    delete env[key]
    expect(() => parseServiceConfig(env)).toThrow(`${key} is required`)
  })

  it('rejects an inline control token', () => {
    expect(() => parseServiceConfig({ ...required, FINANCE_SYNC_CONTROL_TOKEN: 'wrong-channel' })).toThrow('is not accepted')
  })

  it.each(['0', '-1', 'not-a-number', '1.5'])('rejects invalid interval %s', (interval) => {
    expect(() => parseServiceConfig({ ...required, FINANCE_SYNC_INTERVAL_MS: interval })).toThrow('FINANCE_SYNC_INTERVAL_MS must be a positive integer')
  })
})
