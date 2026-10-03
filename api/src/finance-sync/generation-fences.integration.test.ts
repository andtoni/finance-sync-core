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

  it('discovers new accounts on an existing Plaid Item without rotating admitted accounts', async () => {
    const updateDeployment = `${deploymentId}-plaid-update`
    const updateProviderConnectionId = `${providerConnectionId}-plaid-update`
    const updateSecretReference = 'PLAID_UPDATE_SLOT'
    const repository = new PostgresOnboardingRepository(sql!, updateDeployment)
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: updateProviderConnectionId,
      secretReference: updateSecretReference, accounts: [account],
    })
    const [existing] = await repository.listPending(subject, 'plaid')
    await repository.admit({
      subject, provider: 'plaid', connectionId: existing.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 1,
      receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })

    const newProviderAccountId = 'provider-account-new'
    await repository.updateConnectionAccounts({
      subject, providerConnectionId: updateProviderConnectionId, secretReference: updateSecretReference,
      providerGeneration: 1,
      accounts: [account, { ...account, providerAccountId: newProviderAccountId, name: 'New account' }],
    })

    const pending = await repository.listPending(subject, 'plaid')
    expect(pending).toEqual([expect.objectContaining({
      providerAccountId: newProviderAccountId, providerGeneration: 1, name: 'New account',
    })])
    const [state] = await sql!`
      SELECT pc.provider_generation, paa.revoked_at, count(dpa.provider_account_id)::int AS discovered
      FROM provider_connections pc
      JOIN provider_account_admissions paa ON paa.connection_id = pc.connection_id
      JOIN discovered_provider_accounts dpa ON dpa.connection_id = pc.connection_id
      WHERE pc.deployment_id = ${updateDeployment} AND pc.local_owner_id = ${subject}
      GROUP BY pc.provider_generation, paa.revoked_at`
    expect(state).toEqual({ provider_generation: 1, revoked_at: null, discovered: 2 })
  })

  it('recovers the same Plaid Item without revoking its active admissions', async () => {
    const recoveryDeployment = `${deploymentId}-recovery`
    const recoveryProviderConnection = `${providerConnectionId}-recovery`
    const repository = new PostgresOnboardingRepository(sql!, recoveryDeployment)
    await repository.recordConnection({ subject, provider: 'plaid', providerConnectionId: recoveryProviderConnection, secretReference: 'PLAID_RECOVERY_SLOT', accounts: [account] })
    const [pending] = await repository.listPending(subject, 'plaid')
    await repository.admit({ subject, provider: 'plaid', connectionId: pending.connectionId, providerAccountId,
      wealthfolioAccountId, databaseStateDigest: digest, providerGeneration: 1, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [] })
    await repository.recordConnection({ subject, provider: 'plaid', providerConnectionId: recoveryProviderConnection, secretReference: 'PLAID_RECOVERY_SLOT', recovery: true, accounts: [account] })
    const [state] = await sql!`
      SELECT pc.provider_connection_id, pc.secret_reference, pc.provider_generation, pc.state, paa.revoked_at
      FROM provider_connections pc JOIN provider_account_admissions paa ON paa.connection_id = pc.connection_id
      WHERE pc.deployment_id = ${recoveryDeployment} AND pc.local_owner_id = ${subject}`
    expect(state).toEqual({ provider_connection_id: recoveryProviderConnection, secret_reference: 'PLAID_RECOVERY_SLOT', provider_generation: 1, state: 'active', revoked_at: null })
    await expect(repository.recordConnection({ subject, provider: 'plaid', providerConnectionId: recoveryProviderConnection, secretReference: 'PLAID_OTHER_SLOT', recovery: true, accounts: [account] })).rejects.toThrow(/recovery secret reference/)
    const [unchanged] = await sql!`
      SELECT provider_generation, secret_reference, state
      FROM provider_connections
      WHERE deployment_id = ${recoveryDeployment} AND local_owner_id = ${subject}`
    expect(unchanged).toEqual({ provider_generation: 1, secret_reference: 'PLAID_RECOVERY_SLOT', state: 'active' })
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
      VALUES (${rebindDeployment}, ${subject}, ${`${first.connectionId}:${providerAccountId}`}, 1, ${oldKey},
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

suite('already-connected provider accounts are not offered again', () => {
  it('hides a pending account whose mask is already admitted through another Item', async () => {
    sql = postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
    const repository = new PostgresOnboardingRepository(sql, deploymentId)
    const masked = { ...account, mask: '0029' }

    // First Item: the account is discovered and admitted.
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: `item-a-${runId}`, secretReference: 'PLAID_SLOT_A', accounts: [masked],
    })
    const [first] = await repository.listPending(subject, 'plaid')
    expect(first.mask).toBe('0029')
    await repository.admit({
      subject, provider: 'plaid', connectionId: first.connectionId, providerAccountId,
      wealthfolioAccountId: `wealthfolio-account-mask-${runId}`, databaseStateDigest: digest,
      providerGeneration: first.providerGeneration,
      receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
    })

    // Second Item from the same institution exposes the same bank account plus a
    // genuinely new one. Only the new account may be offered: admitting the
    // duplicate cannot succeed (it already exists in Wealthfolio, and a relink
    // is only monotonic inside the Item that owns it).
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: `item-b-${runId}`, secretReference: 'PLAID_SLOT_B',
      accounts: [masked, { ...masked, providerAccountId: 'provider-account-new', name: 'New account', mask: '0088' }],
    })
    const pending = await repository.listPending(subject, 'plaid')
    expect(pending.map(entry => entry.mask)).toEqual(['0088'])
  })
})

suite('connected Plaid Items are labelled by their admitted accounts only', () => {
  it('lists the names and masks the runtime syncs, scoped to the subject and the active generation', async () => {
    sql ??= postgres(url!, { prepare: false })
    await migrate(sql, resolve(import.meta.dirname, '../../finance-sync-migrations'))
    const labelDeployment = `${deploymentId}-labels`
    const repository = new PostgresOnboardingRepository(sql, labelDeployment)
    const itemA = `label-item-a-${runId}`
    const itemB = `label-item-b-${runId}`
    const admitAll = async (wealthfolioPrefix: string) => {
      for (const entry of await repository.listPending(subject, 'plaid')) {
        if (entry.providerAccountId === 'provider-account-unadmitted') continue
        await repository.admit({
          subject, provider: 'plaid', connectionId: entry.connectionId, providerAccountId: entry.providerAccountId,
          wealthfolioAccountId: `${wealthfolioPrefix}-${entry.providerAccountId}`, databaseStateDigest: digest,
          providerGeneration: entry.providerGeneration, receiverAccessGeneration: 1, baselineKind: 'empty', assetMappings: [],
        })
      }
    }

    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: itemA, secretReference: 'PLAID_LABEL_SLOT_A',
      accounts: [
        { ...account, providerAccountId: 'provider-account-checking', name: 'Checking', mask: '1111' },
        { ...account, providerAccountId: 'provider-account-savings', name: 'Savings' },
        { ...account, providerAccountId: 'provider-account-unadmitted', name: 'Never admitted', mask: '9999' },
      ],
    })
    await repository.recordConnection({
      subject, provider: 'plaid', providerConnectionId: itemB, secretReference: 'PLAID_LABEL_SLOT_B',
      accounts: [{ ...account, providerAccountId: 'provider-account-card', name: 'Rewards Card', mask: '2222' }],
    })
    await admitAll('wealthfolio-label')

    const listed = await repository.listConnectedAccounts(subject, 'plaid')
    expect(listed).toEqual([
      { providerConnectionId: itemA, name: 'Checking', mask: '1111' },
      { providerConnectionId: itemA, name: 'Savings', mask: null },
      { providerConnectionId: itemB, name: 'Rewards Card', mask: '2222' },
    ])
    // Exactly the Items the durable runtime (and therefore the update route) can target.
    const runtime = await loadDurableRuntimeConnections(sql, labelDeployment, {
      resolve: async () => ({ clientId: 'test-client', clientSecret: 'test-secret', accessToken: 'test-token' }),
    })
    expect([...new Set(runtime.map(connection => connection.providerConnectionId))].sort())
      .toEqual([...new Set(listed.map(entry => entry.providerConnectionId))].sort())
    await expect(repository.listConnectedAccounts('urn:wealthfolio:test#another-owner', 'plaid')).resolves.toEqual([])
    await expect(repository.listConnectedAccounts(subject, 'snaptrade')).resolves.toEqual([])

    // A disconnected Item is no longer listed.
    const [connection] = await sql<{ connection_id: string }[]>`
      SELECT connection_id::text FROM provider_connections
      WHERE deployment_id = ${labelDeployment} AND provider_connection_id = ${itemB}`
    await expect(repository.revoke(subject, 'plaid', connection.connection_id)).resolves.toBe(true)
    expect((await repository.listConnectedAccounts(subject, 'plaid')).map(entry => entry.providerConnectionId))
      .toEqual([itemA, itemA])
  })
})

afterAll(async () => { await sql?.end() })
