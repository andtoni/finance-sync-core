import { afterAll, beforeAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { migrate } from './storage/migrate'
import { PostgresOnboardingRepository } from './onboarding-repository'
import { createNativeProviderOnboarding } from './server-onboarding'
import { loadDurableRuntimeConnections } from './durable-connections'
import { runRuntimeProvider, type ProviderTransportFactories } from './provider-runtime'
import { assembleCurrentState, currentStateSemanticHash } from './projection/current-state'
import { wealthfolioEmptyAccountAttestation } from './connect-control'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
const runId = `${process.pid}-${Date.now()}`
const deploymentId = `snaptrade-cash-${runId}`
const subject = 'wealthfolio#snaptrade-cash-owner'
const providerAccountId = 'dummy-cash-account'
const secretReference = 'SNAPTRADE_DUMMY_BUNDLE'
const bundle = { providerConnectionId: `dummy-${runId}`, clientId: 'dummy-client', consumerKey: 'dummy-key', userId: 'dummy-user', userSecret: 'dummy-secret', activitiesStartDate: '2026-07-18', activitiesEndDate: '2026-07-18' }
let sql: ReturnType<typeof postgres>

const snaptrade = () => ({
  fetchAccounts: async () => [{ account: { id: providerAccountId, name: 'Dummy Cash', raw_type: 'brokerage' }, positions: [], total_value: { value: '42.00', currency: 'USD' } }],
  fetchActivities: async () => [],
})
const factories: ProviderTransportFactories = {
  plaid: () => { throw new Error('unexpected Plaid use') },
  investec: () => { throw new Error('unexpected Investec use') },
  mercury: () => { throw new Error('unexpected Mercury use') },
  snaptrade,
}

suite('cash-only dummy SnapTrade onboarding with PostgreSQL', () => {
  beforeAll(async () => {
    sql = postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
  })

  it('discovers, attests, admits, durable-loads, and emits a stable empty-position current state without network or secrets', async () => {
    const repository = new PostgresOnboardingRepository(sql, deploymentId)
    const onboarding = createNativeProviderOnboarding({
      env: { FINANCE_SYNC_PROVIDER_OWNER_SUBJECT: subject, FINANCE_SYNC_SNAPTRADE_SECRET_REFERENCE: secretReference },
      secretStore: { available: true, resolve: async reference => {
        expect(reference).toBe(secretReference)
        return bundle
      } },
      factories,
      repository,
      snaptrade: { createRedirectUri: async () => { throw new Error('unexpected redirect') } },
    })
    await expect(onboarding.discover(subject, 'snaptrade')).resolves.toEqual({ status: 'pending' })
    const [discovered] = await repository.listPending(subject, 'snaptrade')
    expect(discovered).toMatchObject({ providerAccountId, assets: [] })

    const wealthfolioAccountId = 'wf-dummy-cash'
    const databaseStateDigest = wealthfolioEmptyAccountAttestation({
      authenticatedSubject: subject, provider: 'snaptrade', connectionId: discovered.connectionId,
      providerAccountId, wealthfolioAccountId, receiverAccessGeneration: 1,
    })
    await repository.admit({ subject, provider: 'snaptrade', connectionId: discovered.connectionId,
      providerAccountId, wealthfolioAccountId, databaseStateDigest, providerGeneration: 1, receiverAccessGeneration: 1,
      baselineKind: 'empty', assetMappings: [] })

    const [runtime] = await loadDurableRuntimeConnections(sql, deploymentId, { resolve: async () => bundle })
    expect(runtime).toMatchObject({ provider: 'snaptrade', accountIds: [providerAccountId], assetMappings: {} })
    const run = await runRuntimeProvider(runtime, null, factories)
    const [state] = assembleCurrentState('snaptrade', run.observations, [{ providerAccountId,
      accountId: wealthfolioAccountId, assetMappings: runtime.provider === 'snaptrade' ? runtime.assetMappings : {} }], {
      providerConnectionId: runtime.connectionId, providerIncarnationId: runtime.sourceResourceIncarnationId,
      providerGeneration: runtime.providerGeneration,
    })
    expect(state.payload).toMatchObject({ accountId: wealthfolioAccountId, cashBalance: '42', positions: [] })
    expect(state.semanticHash).toBe(currentStateSemanticHash(state.payload))
    expect(JSON.stringify({ payload: state.payload, semanticHash: state.semanticHash })).toBe(
      JSON.stringify({ payload: state.payload, semanticHash: currentStateSemanticHash(state.payload) }))

    // Relinking the same immutable provider account advances the provider fence,
    // revokes generation 1, and atomically re-establishes the same Wealthfolio mapping.
    await expect(onboarding.discover(subject, 'snaptrade')).resolves.toEqual({ status: 'pending' })
    const [relinked] = await repository.listPending(subject, 'snaptrade')
    expect(relinked).toMatchObject({ providerAccountId, providerGeneration: 2, assets: [] })
    await repository.admit({ subject, provider: 'snaptrade', connectionId: relinked.connectionId,
      providerAccountId, wealthfolioAccountId, databaseStateDigest, providerGeneration: 2,
      receiverAccessGeneration: 1, baselineKind: 'relink', priorProviderGeneration: 1, assetMappings: [] })
    const [relinkedRuntime] = await loadDurableRuntimeConnections(sql, deploymentId, { resolve: async () => bundle })
    expect(relinkedRuntime).toMatchObject({ provider: 'snaptrade', providerGeneration: 2,
      wealthfolioAccountIds: { [providerAccountId]: wealthfolioAccountId } })
  })
})

afterAll(async () => { await sql?.end() })
