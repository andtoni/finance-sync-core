/** Finance Sync durable runner: read-only provider ingestion, canonical persistence, bridge outbox projection. */
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { createRuntimeDependencies } from './runtime-deps'
import { runConfiguredSyncCycle } from './runtime'
import { connectFinanceSync } from './storage/pg'
import { migrate } from './storage/migrate'
import { parseServiceConfig } from './service-config'
import { financeSyncHealth } from './health'
import {
  createPlaidConnectTransport,
  createSnapTradeConnectTransport,
  FinanceSyncConnectControl,
} from './connect-control'
import { createFinanceSyncHttpHandler } from './connect-http'
import { PostgresConnectSessionStore } from './connect-session-store'
import { createInfisicalPlaidTokenWriter, createInfisicalProviderSecretReader } from './infisical-secret-store'
import { PostgresOnboardingRepository } from './onboarding-repository'
import { createNativeProviderOnboarding } from './server-onboarding'
import { createProviderTransportFactories } from './provider-runtime'

interface CliArgs { help?: boolean; once?: boolean }
function parseArgs(argv = process.argv): CliArgs { return { help: argv.includes('--help') || argv.includes('-h'), once: argv.includes('--once') } }
async function main() {
  const args = parseArgs()
  if (args.help) { console.log(`Finance Sync Runner

Runs admitted read-only provider accounts from PostgreSQL to canonical observations and a bridge outbox.
Provider credentials are resolved through scoped server-side Infisical machine identities; environment metadata is not runtime authority.
--once runs one complete sync cycle and exits.`); return }
  const config = parseServiceConfig(process.env); const sql = connectFinanceSync(config.databaseUrl)
  const secretReader = createInfisicalProviderSecretReader(process.env)
  const tokenWriter = createInfisicalPlaidTokenWriter(process.env)
  const deps = createRuntimeDependencies(sql, config.deploymentId, process.env, undefined, undefined, secretReader)
  const onboarding = new PostgresOnboardingRepository(sql, config.deploymentId)
  const snaptrade = createSnapTradeConnectTransport(process.env)
  const nativeOnboarding = createNativeProviderOnboarding({
    env: process.env, secretStore: secretReader, factories: createProviderTransportFactories(process.env), repository: onboarding, snaptrade,
  })
  const connect = new FinanceSyncConnectControl({
    deploymentId: config.deploymentId,
    connections: () => deps.loadConnections(config.deploymentId),
    plaid: createPlaidConnectTransport({
      env: process.env, secretReader,
      secretReference: process.env.FINANCE_SYNC_PLAID_CONNECT_SECRET_REFERENCE ?? '',
    }),
    snaptrade,
    secretStore: tokenWriter,
    nativeOnboarding,
    sessionStore: new PostgresConnectSessionStore(sql),
    onboarding,
    onPlaidConnected: input => onboarding.recordConnection({
      subject: input.subject, provider: 'plaid', providerConnectionId: input.itemId,
      secretReference: input.secretReference, accounts: input.accounts,
    }),
  })
  let databaseReady = false; let cycleReady = false; let connectionCount = 0; let stopping = false; let timer: ReturnType<typeof setTimeout> | undefined
  const cycle = async () => {
    try {
      const result = await runConfiguredSyncCycle(config.deploymentId, deps)
      connectionCount = result.connections
      cycleReady = result.connections > 0 && result.failures === 0
      if (result.connections === 0) console.log('FINANCE_SYNC_DISABLED')
      else if (cycleReady) console.log('FINANCE_SYNC_CYCLE_OK')
      else console.error('FINANCE_SYNC_CYCLE_FAILED')
    } catch {
      cycleReady = false
      // Never print provider/transport errors: SDK errors can contain secrets or financial payloads.
      console.error('FINANCE_SYNC_CYCLE_FAILED')
    }
  }
  const replay = async () => {
    try { await sql`SELECT 1`; databaseReady = true }
    catch { databaseReady = false; console.error('FINANCE_SYNC_DATABASE_NOT_READY') }
  }
  try {
    console.log('FINANCE_SYNC_MIGRATING'); await migrate(sql, resolve(import.meta.dirname, '../finance-sync-migrations')); await replay(); await cycle()
    if (args.once) { if (!databaseReady || (connectionCount > 0 && !cycleReady)) process.exitCode = 1; return }
    const handler = createFinanceSyncHttpHandler({
      controlToken: config.controlToken,
      connect,
      health: () => { const body = financeSyncHealth(databaseReady, cycleReady, connectionCount); return { status: body.ok ? 200 : 503, body } },
    })
    const server = createServer((request, response) => { void handler(request, response) })
    await new Promise<void>((ok, fail) => { server.once('error', fail); server.listen(config.healthPort, '0.0.0.0', ok) })
    console.log('FINANCE_SYNC_HEALTH_LISTENING')
    const schedule = () => { timer = setTimeout(async () => { await replay(); await cycle(); if (!stopping) schedule() }, config.intervalMs) }; schedule()
    await new Promise<void>(done => { const stop = () => { if (stopping) return; stopping = true; if (timer) clearTimeout(timer); server.close(() => done()) }; process.once('SIGTERM', stop); process.once('SIGINT', stop) })
  } finally { if (timer) clearTimeout(timer); await sql.end() }
}
main().catch(() => { console.error('FINANCE_SYNC_RUNNER_FAILED'); process.exit(1) })
