/** Finance Sync durable runner: read-only provider ingestion, canonical persistence, bridge outbox projection. */
import { createServer } from 'node:http'
import { resolve } from 'node:path'
import { assertBridgeConfiguration, createRuntimeDependencies, linkZelleTransferPairs, reclassifyPlaidCardRefunds, reconcileInvestecCardLifecycles, retireUnmatchedInvestecCardAuthorizations } from './runtime-deps'
import { runConfiguredSyncCycle, type PlaidReissuedAccount } from './runtime'
import { connectFinanceSync } from './storage/pg'
import { migrate } from './storage/migrate'
import { parseServiceConfig } from './service-config'
import { financeSyncHealthResponse, syncCycleOutcome, webhookDrainState } from './health'
import { logFailure } from './failure-reason'
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
import { createFixtureConnectControl, fixtureModeEnabled } from './fixture-runtime'
import { replayQuarantinedInvestecCards, reprojectRetainedInvestecCards, startInvestecCardDrainer } from './investec-card-drainer'
import { createObservationQuarantineControl } from './observation-quarantine'
import { createInvestecCardCurrencyCorrectionControl } from './investec-card-correction'
import { createInvestecCardLifecycleLinkControl } from './investec-card-lifecycle-link'
import { createInvestecTerminalRevisionRecoveryControl } from './investec-terminal-revision-recovery'
import type { DrainerHandle } from '../drain/drainer'

interface CliArgs { help?: boolean; once?: boolean }
function parseArgs(argv = process.argv): CliArgs { return { help: argv.includes('--help') || argv.includes('-h'), once: argv.includes('--once') } }
async function main() {
  const args = parseArgs()
  if (args.help) { console.log(`Finance Sync Runner

Runs admitted read-only provider accounts from PostgreSQL to canonical observations and a bridge outbox.
Provider credentials are resolved through scoped server-side Infisical machine identities; environment metadata is not runtime authority.
--once runs one complete sync cycle and exits.`); return }
  const config = parseServiceConfig(process.env); const sql = connectFinanceSync(config.databaseUrl)
  let deps: ReturnType<typeof createRuntimeDependencies> | undefined
  let connect: FinanceSyncConnectControl | undefined
  let webhookDrainer: DrainerHandle | undefined
  let databaseReady = false; let cycleReady = false; let cycleFailed = false; let connectionCount = 0; let stopping = false; let timer: ReturnType<typeof setTimeout> | undefined
  // Plaid Items whose last completed cycle needed their owner, with the fixed
  // reason code. Derived from the provider on every cycle, so nothing durable
  // has to remember it.
  const plaidItemConditions = new Map<string, string | undefined>()
  // Admissions the last completed cycle ingested under a re-issued account id,
  // by Wealthfolio account id. Derived on every cycle like the conditions.
  const plaidReissuedAccounts = new Map<string, PlaidReissuedAccount>()
  let retainedCardReplayNeeded = true
  let quarantinedCardReplayNeeded = true
  let plaidCardRefundReclassificationNeeded = true
  const cycle = async () => {
    if (!config.providersEnabled) {
      connectionCount = 0
      cycleReady = false
      cycleFailed = false
      console.log('FINANCE_SYNC_RUNTIME_DISABLED')
      return
    }
    cycleFailed = false
    try {
      const result = await runConfiguredSyncCycle(config.deploymentId, deps!, {
        providersEnabled: config.providersEnabled,
        projectionEnabled: config.projectionEnabled,
      })
      connectionCount = result.connections
      plaidItemConditions.clear()
      for (const condition of result.plaidItemConditions) plaidItemConditions.set(condition.providerConnectionId, condition.reason)
      plaidReissuedAccounts.clear()
      for (const account of result.plaidReissuedAccounts) plaidReissuedAccounts.set(account.wealthfolioAccountId, account)
      const outcome = syncCycleOutcome(result)
      cycleReady = outcome.cycleReady
      cycleFailed = outcome.cycleFailed
      if (outcome.event === 'FINANCE_SYNC_CYCLE_FAILED') console.error(outcome.event)
      else console.log(outcome.event)
    } catch (error) {
      cycleReady = false
      cycleFailed = true
      // Never print provider/transport errors whole: SDK errors can contain secrets or financial payloads.
      logFailure('FINANCE_SYNC_CYCLE_FAILED', error)
    }
  }
  const reprojectRetainedCards = async () => {
    if (!retainedCardReplayNeeded) return
    if (!deps || !config.projectionEnabled) {
      retainedCardReplayNeeded = false
      return
    }
    try {
      const reprojectedCards = await reprojectRetainedInvestecCards(sql, config.deploymentId, deps)
      retainedCardReplayNeeded = false
      if (reprojectedCards > 0) console.log('FINANCE_SYNC_RETAINED_CARD_REPROJECTION_OK')
    } catch (error) {
      // Keep the health server and normal retry loop available when the bridge
      // is transiently unavailable or another outbox row is unresolved.
      logFailure('FINANCE_SYNC_RETAINED_CARD_REPROJECTION_FAILED', error)
    }
  }
  const replayQuarantinedCards = async () => {
    if (!quarantinedCardReplayNeeded) return
    if (!deps || !config.projectionEnabled) {
      quarantinedCardReplayNeeded = false
      return
    }
    try {
      const replayedCards = await replayQuarantinedInvestecCards(sql, config.deploymentId, deps)
      quarantinedCardReplayNeeded = false
      if (replayedCards > 0) console.log('FINANCE_SYNC_CARD_QUARANTINE_REPLAY_OK')
    } catch (error) {
      logFailure('FINANCE_SYNC_CARD_QUARANTINE_REPLAY_FAILED', error)
    }
  }
  const sweepInvestecCardLifecycles = async () => {
    if (!deps || !config.projectionEnabled) return
    try {
      const connections = await deps.loadConnections(config.deploymentId)
      let swept = 0
      let retired = 0
      for (const connection of connections) {
        if (connection.provider !== 'investec') continue
        swept += await reconcileInvestecCardLifecycles(sql, config.deploymentId, connection, deps)
        retired += await retireUnmatchedInvestecCardAuthorizations(sql, config.deploymentId, connection, deps)
        const projection = await deps.projectPending(connection)
        if (projection.failed > 0) throw new Error('Investec lifecycle sweep projection is unresolved')
      }
      if (retired > 0) console.log('FINANCE_SYNC_INVESTEC_CARD_AUTHORIZATION_RETIRED', JSON.stringify({ retirements: retired }))
      if (swept > 0) console.log('FINANCE_SYNC_INVESTEC_CARD_LIFECYCLE_SWEEP_OK')
    } catch (error) {
      cycleReady = false
      cycleFailed = true
      logFailure('FINANCE_SYNC_INVESTEC_CARD_LIFECYCLE_SWEEP_FAILED', error)
    }
  }
  const linkZelleTransfers = async () => {
    if (!deps || !config.projectionEnabled) return
    try {
      const connections = await deps.loadConnections(config.deploymentId)
      const linked = await linkZelleTransferPairs(sql, config.deploymentId, connections, deps)
      if (linked > 0) console.log('FINANCE_SYNC_PLAID_ZELLE_PAIRS_LINKED', JSON.stringify({ pairs: linked }))
    } catch (error) {
      cycleReady = false
      cycleFailed = true
      logFailure('FINANCE_SYNC_PLAID_ZELLE_LINK_FAILED', error)
    }
  }
  const reclassifyCardRefunds = async () => {
    if (!plaidCardRefundReclassificationNeeded) return
    if (!deps || !config.projectionEnabled) {
      plaidCardRefundReclassificationNeeded = false
      return
    }
    try {
      const reclassified = await reclassifyPlaidCardRefunds(sql, config.deploymentId, await deps.loadConnections(config.deploymentId), deps)
      plaidCardRefundReclassificationNeeded = false
      if (reclassified > 0) console.log('FINANCE_SYNC_PLAID_CARD_REFUNDS_RECLASSIFIED', JSON.stringify({ refunds: reclassified }))
    } catch (error) {
      logFailure('FINANCE_SYNC_PLAID_CARD_REFUND_RECLASSIFICATION_FAILED', error)
    }
  }
  const replay = async () => {
    try { await sql`SELECT 1`; databaseReady = true }
    catch { databaseReady = false; console.error('FINANCE_SYNC_DATABASE_NOT_READY') }
  }
  try {
    console.log('FINANCE_SYNC_MIGRATING'); await migrate(sql, resolve(import.meta.dirname, '../finance-sync-migrations')); await replay()
    if (config.providersEnabled) {
      if (!databaseReady) throw new Error('database is required before provider control is initialized')
      const fixture = fixtureModeEnabled(process.env)
      // Validate bridge delivery before constructing onboarding. The control
      // plane must never be available while projection is misconfigured.
      assertBridgeConfiguration(process.env, fixture)
      if (fixture) {
        // Fixture mode uses the synthetic adapters only. Do not initialize the
        // production onboarding/Infisical control plane or attempt provider calls.
        deps = createRuntimeDependencies(sql, config.deploymentId, process.env)
        connect = createFixtureConnectControl(sql, config.deploymentId, () => deps!.loadConnections(config.deploymentId))
      } else {
        const secretReader = createInfisicalProviderSecretReader(process.env)
        const tokenWriter = createInfisicalPlaidTokenWriter(process.env)
        deps = createRuntimeDependencies(sql, config.deploymentId, process.env, undefined, undefined, secretReader)
        const onboarding = new PostgresOnboardingRepository(sql, config.deploymentId)
        const snaptrade = createSnapTradeConnectTransport(process.env)
        const nativeOnboarding = createNativeProviderOnboarding({
          env: process.env, secretStore: secretReader, factories: createProviderTransportFactories(process.env), repository: onboarding, snaptrade,
        })
        connect = new FinanceSyncConnectControl({
          deploymentId: config.deploymentId,
          connections: () => deps!.loadConnections(config.deploymentId),
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
            secretReference: input.secretReference, recovery: input.recovery, accounts: input.accounts,
          }),
          onPlaidConnectedTransactional: (input, tx) => onboarding.recordConnectionTransaction(tx as import('./storage/pg').FinanceSyncSql, {
            subject: input.subject, provider: 'plaid', providerConnectionId: input.itemId,
            secretReference: input.secretReference, recovery: input.recovery, accounts: input.accounts,
          }),
          onPlaidAccountsUpdated: input => onboarding.updateConnectionAccounts(input),
          plaidItemConditions,
          plaidReissuedAccounts,
        })
      }
    }
    await cycle()
    await linkZelleTransfers()
    await reclassifyCardRefunds()
    await reprojectRetainedCards()
    await replayQuarantinedCards()
    await sweepInvestecCardLifecycles()
    if (args.once) { if (!databaseReady || cycleFailed || (connectionCount > 0 && !cycleReady)) process.exitCode = 1; return }
    const lifecycleLinkEnabled = process.env.FINANCE_SYNC_INVESTEC_CARD_LIFECYCLE_LINK_ENABLED === 'true'
    const terminalRecoveryEnabled = process.env.FINANCE_SYNC_INVESTEC_TERMINAL_REVISION_RECOVERY_ENABLED === 'true'
    const handler = createFinanceSyncHttpHandler({
      controlToken: config.controlToken,
      providersEnabled: config.providersEnabled,
      connect,
      quarantine: createObservationQuarantineControl(sql, config.deploymentId),
      investecCardCorrection: deps ? createInvestecCardCurrencyCorrectionControl(sql, config.deploymentId, deps) : undefined,
      investecCardLifecycleLink: deps && lifecycleLinkEnabled
        ? createInvestecCardLifecycleLinkControl(sql, config.deploymentId, deps)
        : undefined,
      investecTerminalRevisionRecovery: terminalRecoveryEnabled && deps?.terminalRevisionResolver
        ? createInvestecTerminalRevisionRecoveryControl(sql, config.deploymentId, deps.terminalRevisionResolver)
        : undefined,
      health: () => financeSyncHealthResponse(databaseReady, cycleReady, connectionCount, cycleFailed, webhookDrainState(webhookDrainer)),
    })
    const server = createServer((request, response) => { void handler(request, response) })
    await new Promise<void>((ok, fail) => { server.once('error', fail); server.listen(config.healthPort, '0.0.0.0', ok) })
    console.log('FINANCE_SYNC_HEALTH_LISTENING')
    if (deps) {
      webhookDrainer = startInvestecCardDrainer(sql, config.deploymentId, deps, process.env)
      console.log(webhookDrainer ? 'FINANCE_SYNC_WEBHOOK_DRAINER_STARTED' : 'FINANCE_SYNC_WEBHOOK_DRAINER_DISABLED')
    }
    const schedule = () => { timer = setTimeout(async () => { await replay(); await cycle(); await linkZelleTransfers(); await reclassifyCardRefunds(); await reprojectRetainedCards(); await replayQuarantinedCards(); await sweepInvestecCardLifecycles(); if (!stopping) schedule() }, config.intervalMs) }
    if (config.schedulesEnabled) schedule()
    await new Promise<void>(done => { const stop = () => { if (stopping) return; stopping = true; if (timer) clearTimeout(timer); webhookDrainer?.stop(); server.close(() => done()) }; process.once('SIGTERM', stop); process.once('SIGINT', stop) })
  } finally { if (timer) clearTimeout(timer); webhookDrainer?.stop(); await webhookDrainer?.done; await sql.end() }
}
main().catch(() => { console.error('FINANCE_SYNC_RUNNER_FAILED'); process.exit(1) })
