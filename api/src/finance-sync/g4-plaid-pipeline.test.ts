
import { describe, expect, it } from "vitest"
import { runPlaidSync } from "./probes/plaid/plaid-adapter"
import { appendObservation, canonicalRevision } from "./observations/lifecycle"
import { addAuthorityEpoch, selectAuthority } from "./authority/epochs"
import { plaidResourceKind } from "./probes/plaid/plaid-adapter"

function mockPlaidTransport() {
  return {
    syncTransactions: async (_token: string, cursor?: string) => {
      if (!cursor) {
        return {
          added: [
            { transaction_id: "plaid-txn-1", account_id: "acc-1", amount: 42.50, date: "2026-07-11", name: "GITHUB.COM" },
          ],
          modified: [], removed: [],
          accounts: [{
            account_id: "acc-1", name: "Checking", type: "depository", subtype: "checking",
            balances: { current: 5000, iso_currency_code: "USD" },
          }],
          next_cursor: "cursor-complete", has_more: false,
        }
      }
      return { added: [], modified: [], removed: [], accounts: [], next_cursor: cursor, has_more: false }
    },
  }
}

describe("G4 integration: Plaid → observations → authority → revisions", () => {
  it("produces observations that feed into the canonical lifecycle", async () => {
    const syncResult = await runPlaidSync({
      accessToken: "access-sandbox-int",
      deploymentId: "g4-int-test",
      localOwnerId: "alexis",
      sourceResourceIncarnationId: "plaid:item-1:inc-1",
      accountIds: ["acc-1"],
    }, mockPlaidTransport())

    expect(syncResult.observations.length).toBe(1)
    const obs = syncResult.observations[0]

    // Can construct lifecycle observation identity
    const lifecycleIdentity = {
      deploymentId: obs.deploymentId as any,
      localOwnerId: obs.localOwnerId as any,
      canonicalEntityId: `plaid:txn:${(obs.payload as any).transactionId}` as any,
      providerResourceIncarnationId: obs.sourceResourceIncarnationId as any,
    }

    // Append to observation list (idempotent)
    const observations = appendObservation([], {
      identity: lifecycleIdentity,
      observedAt: obs.observedAt,
      providerVersion: obs.providerVersion,
      rawHash: obs.semanticHash,
      payload: obs.payload as Record<string, unknown>,
    })
    expect(observations.length).toBe(1)

    // Create canonical revision
    const revision = canonicalRevision(null, lifecycleIdentity, obs.semanticHash, "g4-plaid-v1")
    expect(revision.revision).toBe(1)
    expect(revision.semanticHash).toBe(obs.semanticHash)

    // Same observation doesn't produce new revision
    const revision2 = canonicalRevision(revision, lifecycleIdentity, obs.semanticHash, "g4-plaid-v1")
    expect(revision2.revision).toBe(1) // no change = same revision

    // Different hash produces new revision
    const revision3 = canonicalRevision(revision, lifecycleIdentity, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", "g4-plaid-v1")
    expect(revision3.revision).toBe(2)

    console.log(`Plaid txn ${(obs.payload as any).transactionId} → revision ${revision.revision} → domain ${obs.domain}`)
  })

  it("Plaid domain integrates with authority epoch model", () => {
    const domain = plaidResourceKind("depository")
    expect(domain).toBe("cash_ledger")

    const epochs = addAuthorityEpoch([], {
      account: "test-acc-1",
      domain: "cash_ledger",
      sourceAccount: "plaid:item-1:acc-1",
      role: "primary",
      validFrom: "2026-07-01T00:00:00.000Z",
      validTo: null,
    })
    expect(epochs.length).toBe(1)

    const epoch = selectAuthority(epochs, "test-acc-1", "cash_ledger", "2026-07-11T12:00:00.000Z")
    expect(epoch).not.toBeNull()
    expect(epoch!.sourceAccount).toBe("plaid:item-1:acc-1")

    // Overlapping authority rejected
    expect(() => addAuthorityEpoch(epochs, {
      account: "test-acc-1", domain: "cash_ledger", sourceAccount: "plaid:item-2:acc-1",
      role: "primary", validFrom: "2026-06-01T00:00:00.000Z", validTo: null,
    })).toThrow("authority overlap")
  })
})
