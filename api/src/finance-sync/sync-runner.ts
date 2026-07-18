/** Finance Sync durable runner: read-only provider ingestion, canonical persistence, bridge outbox projection. */
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { createRuntimeDependencies } from './runtime-deps'
import { runConfiguredSyncCycle } from './runtime'
import { connectFinanceSync } from './storage/pg'
import { migrate } from './storage/migrate'
import { parseServiceConfig } from './service-config'

interface CliArgs { help?: boolean; once?: boolean }
function parseArgs(argv = process.argv): CliArgs { return { help: argv.includes('--help') || argv.includes('-h'), once: argv.includes('--once') } }
async function main() {
  const args = parseArgs()
  if (args.help) { console.log(`Finance Sync Runner

Runs configured read-only providers to canonical observations and a bridge outbox.
FINANCE_SYNC_CONNECTIONS_JSON is optional; without it, the runner performs migrations/readiness only.
--once runs one complete sync cycle and exits.`); return }
  const config = parseServiceConfig(process.env); const sql = connectFinanceSync(config.databaseUrl)
  const deps = createRuntimeDependencies(sql, config.deploymentId); let databaseReady = false; let cycleReady = false; let stopping = false; let timer: ReturnType<typeof setTimeout> | undefined
  const cycle = async () => {
    try {
      const result = await runConfiguredSyncCycle(config.deploymentId, deps)
      cycleReady = result.failures === 0
      if (cycleReady) console.log('Finance Sync cycle completed:', result)
      else console.error('Finance Sync cycle completed with failures:', result)
    } catch {
      cycleReady = false
      // Never print provider/transport errors: SDK errors can contain secrets or financial payloads.
      console.error('Finance Sync cycle failed')
    }
  }
  const replay = async () => {
    try { await sql`SELECT 1`; databaseReady = true }
    catch { databaseReady = false; console.error('Finance Sync database readiness failed') }
  }
  try {
    console.log('Running Finance Sync migrations...'); await migrate(sql, resolve(import.meta.dirname, '../finance-sync-migrations')); await replay(); await cycle()
    if (args.once) { if (!databaseReady || !cycleReady) process.exitCode = 1; return }
    const server = createServer((request, response) => { if (request.url !== '/health') return void response.writeHead(404).end(); const ready = databaseReady && cycleReady; response.writeHead(ready ? 200 : 503, { 'content-type': 'application/json' }).end(JSON.stringify({ ok: ready, service: 'finance-sync' })) })
    await new Promise<void>((ok, fail) => { server.once('error', fail); server.listen(config.healthPort, '0.0.0.0', ok) })
    console.log(`Finance Sync health endpoint listening on :${config.healthPort}`)
    const schedule = () => { timer = setTimeout(async () => { await replay(); await cycle(); if (!stopping) schedule() }, config.intervalMs) }; schedule()
    await new Promise<void>(done => { const stop = () => { if (stopping) return; stopping = true; if (timer) clearTimeout(timer); server.close(() => done()) }; process.once('SIGTERM', stop); process.once('SIGINT', stop) })
  } finally { if (timer) clearTimeout(timer); await sql.end() }
}
main().catch(() => { console.error('Finance Sync runner failed'); process.exit(1) })
