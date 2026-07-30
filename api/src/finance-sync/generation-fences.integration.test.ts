import { afterAll, describe, expect, it } from 'vitest'
import postgres from 'postgres'
import { resolve } from 'node:path'
import { migrate } from './storage/migrate'
import { PostgresOnboardingRepository } from './onboarding-repository'
import { loadDurableRuntimeConnections } from './durable-connections'
import { createRuntimeDependencies } from './runtime-deps'

const url = process.env.FINANCE_SYNC_TEST_DATABASE_URL
const suite = url ? describe : describe.skip
const runId = `${process.pid}-${Date.now()}`
const deploymentId = `generation-fences-${runId}`
const subject = 'urn:wealthfolio:test#generation-owner'
const providerConnectionId = `generation-provider-${runId}`
const providerAccountId = 'provider-account'
const wealthfolioAccountId = 'wealthfolio-account'
const secretReference = 'GENERATION_TEST_SECRET'
const digest = `sha256:${'a'.repeat(64)}`
let sql: ReturnType<typeof postgres> | undefined

const account = {
  providerAccountId,
  name: 'Generation account',
  type: 'checking',
  currency: 'USD',
  assets: [],
} as const

suite('provider generation durable fences with PostgreSQL', () => {
  it('binds admission generation, fences old cursors/outbox, and supersedes prior pending work on rotation', async () => {
    sql = postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
    const repository = new PostgresOnboardingRepository(sql, deploymentId)

    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId, secretReference, accounts: [account],
    })
    const [pending] = await repository.listPending(subject, 'plaid')
    await repository.admit({
      subject, provider: 'plaid', connectionId: pending.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 1,
      receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })
    const [generationOne] = await loadDurableRuntimeConnections(sql, deploymentId, {
      resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }),
    })
    expect(generationOne.providerGeneration).toBe(1)
    const [storedAdmission] = await sql`
      SELECT provider_generation FROM provider_account_admissions
      WHERE deployment_id = ${deploymentId} AND connection_id = ${pending.connectionId}::uuid
    `
    expect(storedAdmission.provider_generation).toBe(1)

    await sql`INSERT INTO sync_checkpoints
      (deployment_id, connection_id, provider_generation, provider, cursor)
      VALUES (${deploymentId}, ${generationOne.connectionId}, 1, 'plaid', 'generation-one-cursor')`
    const idempotencyKey = `generation-work-${runId}`
    await sql`INSERT INTO projection_outbox
      (deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload)
      VALUES (${deploymentId}, ${subject}, ${generationOne.connectionId}, 1, ${idempotencyKey},
        ${sql.json({ idempotencyKey, subject })})`

    // Advance twice without admitting generation two. A current-1 check would
    // accept generation two, but the revoked admission is durably bound to one.
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId, secretReference, accounts: [account],
    })
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId, secretReference, accounts: [account],
    })
    const [generationThreePending] = await repository.listPending(subject, 'plaid')
    expect(generationThreePending.providerGeneration).toBe(3)
    await expect(repository.admit({
      subject, provider: 'plaid', connectionId: generationThreePending.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 3,
      receiverAccessGeneration: 1, baselineKind: 'relink', priorProviderGeneration: 2, assetMappings: [],
    })).rejects.toThrow(/relink baseline does not match a revoked admission/)
    await repository.admit({
      subject, provider: 'plaid', connectionId: generationThreePending.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 3,
      receiverAccessGeneration: 1, baselineKind: 'relink', priorProviderGeneration: 1, assetMappings: [],
    })

    const [generationThree] = await loadDurableRuntimeConnections(sql, deploymentId, {
      resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }),
    })
    expect(generationThree.providerGeneration).toBe(3)
    const dependencies = createRuntimeDependencies(sql, deploymentId, {})
    await expect(dependencies.loadCheckpoint(generationThree)).resolves.toBeNull()
    await dependencies.saveCheckpoint(generationThree, 'generation-three-cursor')
    const checkpoints = await sql`
      SELECT provider_generation, cursor FROM sync_checkpoints
      WHERE deployment_id = ${deploymentId} AND connection_id = ${generationOne.connectionId}
      ORDER BY provider_generation
    `
    expect(checkpoints).toEqual([
      { provider_generation: 1, cursor: 'generation-one-cursor' },
      { provider_generation: 3, cursor: 'generation-three-cursor' },
    ])

    const [superseded] = await sql`
      SELECT provider_generation, state, dead_letter_reason FROM projection_outbox
      WHERE deployment_id = ${deploymentId} AND idempotency_key = ${idempotencyKey}
    `
    expect(superseded).toEqual({
      provider_generation: 1,
      state: 'dead_letter',
      dead_letter_reason: 'PROVIDER_GENERATION_SUPERSEDED',
    })
    await expect(sql`INSERT INTO projection_outbox
      (deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload)
      VALUES (${deploymentId}, ${subject}, ${generationThree.connectionId}, 3, ${idempotencyKey},
        ${sql.json({ idempotencyKey, subject })})`).resolves.toHaveLength(0)
  })

  it('rebases earlier active admissions and rejected outbox work when a later account advances receiver access', async () => {
    const rebindDeployment = `${deploymentId}-receiver-rebind`
    const repository = new PostgresOnboardingRepository(sql!, rebindDeployment)
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: `${providerConnectionId}-first`,
      secretReference: 'RECEIVER_REBIND_PLAID', accounts: [account],
    })
    const [first] = await repository.listPending(subject, 'plaid')
    await repository.admit({
      subject, provider: 'plaid', connectionId: first.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 1,
      receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })
    const oldKey = `receiver-rebind-${runId}`
    await sql!`INSERT INTO projection_outbox
      (deployment_id, local_owner_id, connection_id, provider_generation, idempotency_key, payload,
       state, attempts, last_error, dead_lettered_at, dead_letter_reason)
      VALUES (${rebindDeployment}, ${subject}, ${first.connectionId}, 1, ${oldKey},
        ${sql!.json({ idempotencyKey: oldKey, subject, accountId: wealthfolioAccountId, accessGeneration: 1 })},
        'dead_letter', 3, 'BRIDGE_HTTP_REJECTED', now(), 'BRIDGE_HTTP_REJECTED')`

    const secondProviderAccountId = 'provider-account-second'
    const secondWealthfolioAccountId = 'wealthfolio-account-second'
    await repository.recordConnection({
      subject, provider: 'mercury', providerConnectionId: `${providerConnectionId}-second`,
      secretReference: 'RECEIVER_REBIND_MERCURY', accounts: [{ ...account, providerAccountId: secondProviderAccountId }],
    })
    const [second] = await repository.listPending(subject, 'mercury')
    await repository.admit({
      subject, provider: 'mercury', connectionId: second.connectionId,
      providerAccountId: secondProviderAccountId, wealthfolioAccountId: secondWealthfolioAccountId,
      databaseStateDigest: digest, providerGeneration: 1, receiverAccessGeneration: 2,
      baselineKind: 'empty', assetMappings: [],
    })

    const generations = await sql!`
      SELECT wealthfolio_account_id, receiver_access_generation
      FROM provider_account_admissions
      WHERE deployment_id = ${rebindDeployment} AND revoked_at IS NULL
      ORDER BY wealthfolio_account_id`
    expect(generations).toEqual([
      { wealthfolio_account_id: wealthfolioAccountId, receiver_access_generation: 2 },
      { wealthfolio_account_id: secondWealthfolioAccountId, receiver_access_generation: 2 },
    ])
    const [rebound] = await sql!`
      SELECT state, attempts, last_error, dead_letter_reason,
        (payload->>'accessGeneration')::int AS access_generation
      FROM projection_outbox WHERE deployment_id = ${rebindDeployment} AND idempotency_key = ${oldKey}`
    expect(rebound).toEqual({
      state: 'pending', attempts: 0, last_error: null,
      dead_letter_reason: null, access_generation: 2,
    })
  })

  it('rolls back a post-write onboarding failure without replacing the old active slot or generation', async () => {
    const failureDeployment = `${deploymentId}-post-write`
    const failureProviderConnection = `${providerConnectionId}-post-write`
    const repository = new PostgresOnboardingRepository(sql!, failureDeployment)
    await repository.recordConnection({ subject, provider: 'plaid', providerConnectionId: failureProviderConnection, secretReference: 'PLAID_SLOT_A', accounts: [account] })
    const [pending] = await repository.listPending(subject, 'plaid')
    await repository.admit({ subject, provider: 'plaid', connectionId: pending.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 1, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [] })
    // The writer has already put the candidate bundle in PLAID_SLOT_B. Force a
    // database failure after recordConnection rotates its in-transaction row.
    await expect(repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: failureProviderConnection, secretReference: 'PLAID_SLOT_B',
      accounts: [account, { ...account, providerAccountId: '' }],
    })).rejects.toThrow()
    const [active] = await sql!`SELECT secret_reference, provider_generation, state
      FROM provider_connections WHERE deployment_id = ${failureDeployment} AND local_owner_id = ${subject}`
    expect(active).toEqual({ secret_reference: 'PLAID_SLOT_A', provider_generation: 1, state: 'active' })
  })
})

afterAll(async () => { await sql?.end() })
