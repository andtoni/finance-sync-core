import { describe, expect, it } from "vitest";
import { createRuntimeDependencies, type SyncCycleAuditEntry } from "./runtime-deps";
import type { FinanceSyncSql } from "./storage/pg";
import type { RuntimeConnection } from "./runtime";

const MINIMAL_VALID_CONNECTION: RuntimeConnection = {
  provider: "plaid",
  connectionId: "item-1",
  deploymentId: "dep-1",
  accessToken: "access-sandbox-abc123",
  localOwnerId: "owner-uuid",
  sourceResourceIncarnationId: "00000000-0000-0000-0000-000000000001",
  accountIds: ["acc-1"],
  wealthfolioAccountIds: { "acc-1": "wf-acc-1" },
  accessGeneration: 1,
};

const noopSql = {} as FinanceSyncSql;

// ── Mock tagged-template SQL (postgres compat) for projectPending tests ──
function mockSql(cfg: {
  pendingRows?: Array<{ id: string; connection_id?: string; payload: unknown; attempts?: number }>;
}) {
  const recordedUpdates: string[] = [];
  const fn = function (
    strings: TemplateStringsArray | readonly string[],
    ..._values: unknown[]
  ): Promise<Record<string, unknown>[]> {
    const sqlText = Array.isArray(strings) ? strings.join("?") : String(strings);
    if (/SELECT.*(projection_outbox|attempts)/i.test(sqlText)) {
      return Promise.resolve(
        (cfg.pendingRows ?? []).map((r) => ({
          id: r.id,
          connection_id: r.connection_id ?? "conn-default",
          payload: r.payload,
          attempts: r.attempts ?? 0,
        })),
      );
    }
    if (/UPDATE.*projection_outbox/i.test(sqlText)) {
      recordedUpdates.push(sqlText);
    }
    return Promise.resolve([]);
  };
  (fn as unknown as Record<string, unknown>).json = (v: unknown) => JSON.stringify(v);
  return {
    sql: fn as unknown as FinanceSyncSql,
    updates: () => [...recordedUpdates],
  };
}

describe("G8 fail-closed startup — createRuntimeDependencies", () => {
  it("succeeds without credential validation when no connections configured", () => {
    const deps = createRuntimeDependencies(noopSql, "dep-1", {});
    expect(deps).toBeDefined();
    expect(typeof deps.loadConnections).toBe("function");
  });

  it("succeeds when FINANCE_SYNC_CONNECTIONS_JSON is empty array", () => {
    const deps = createRuntimeDependencies(noopSql, "dep-1", {
      FINANCE_SYNC_CONNECTIONS_JSON: "[]",
    });
    expect(deps).toBeDefined();
  });

  it("succeeds when FINANCE_SYNC_CONNECTIONS_JSON is set to empty string", () => {
    const deps = createRuntimeDependencies(noopSql, "dep-1", {
      FINANCE_SYNC_CONNECTIONS_JSON: "",
    });
    expect(deps).toBeDefined();
  });

  it("throws at creation time when connections configured but FINANCE_SYNC_PLAID_URL missing", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([MINIMAL_VALID_CONNECTION]),
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "Plaid endpoint and credentials are required for configured connections",
    );
  });

  it("throws at creation time when connections configured but FINANCE_SYNC_PLAID_CLIENT_ID missing", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([MINIMAL_VALID_CONNECTION]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "secret-1",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "Plaid endpoint and credentials are required for configured connections",
    );
  });

  it("throws at creation time when connections configured but FINANCE_SYNC_PLAID_SECRET missing", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([MINIMAL_VALID_CONNECTION]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "Plaid endpoint and credentials are required for configured connections",
    );
  });

  it("throws at creation time when connections configured but FINANCE_SYNC_BRIDGE_URL missing", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([MINIMAL_VALID_CONNECTION]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "FINANCE_SYNC_BRIDGE_URL is required for configured connections",
    );
  });

  it("succeeds at creation time when all required credentials present", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([
        MINIMAL_VALID_CONNECTION,
        { ...MINIMAL_VALID_CONNECTION, connectionId: "item-2" },
      ]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    const deps = createRuntimeDependencies(noopSql, "dep-1", env);
    expect(deps).toBeDefined();
  });

  it("returns configured connections from loadConnections", async () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([MINIMAL_VALID_CONNECTION]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    const deps = createRuntimeDependencies(noopSql, "dep-1", env);
    const conns = await deps.loadConnections("dep-1");
    expect(conns).toHaveLength(1);
    expect(conns[0].connectionId).toBe("item-1");
    expect(conns[0].deploymentId).toBe("dep-1");
  });

  // === G8 cross-deployment fail-closed invariant tests ===

  it("throws when a connection is missing deploymentId", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([
        { ...MINIMAL_VALID_CONNECTION, deploymentId: undefined },
      ]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "connection deploymentId is required",
    );
  });

  it("throws when a connection has deploymentId not matching createRuntimeDependencies parameter", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([
        { ...MINIMAL_VALID_CONNECTION, deploymentId: "other-deployment" },
      ]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "connection deploymentId mismatch",
    );
  });

  it("throws when one connection has deploymentId mismatch among valid ones", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([
        MINIMAL_VALID_CONNECTION,
        { ...MINIMAL_VALID_CONNECTION, connectionId: "item-2", deploymentId: "wrong-dep" },
      ]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    expect(() => createRuntimeDependencies(noopSql, "dep-1", env)).toThrow(
      "connection deploymentId mismatch",
    );
  });

  it("succeeds when all connections have correct deploymentId matching the parameter", () => {
    const env = {
      FINANCE_SYNC_CONNECTIONS_JSON: JSON.stringify([
        MINIMAL_VALID_CONNECTION,
        { ...MINIMAL_VALID_CONNECTION, connectionId: "item-2" },
      ]),
      FINANCE_SYNC_PLAID_URL: "https://plaid.example.com",
      FINANCE_SYNC_PLAID_CLIENT_ID: "client-1",
      FINANCE_SYNC_PLAID_SECRET: "secret-1",
      FINANCE_SYNC_BRIDGE_URL: "https://bridge.example.com",
    };
    const deps = createRuntimeDependencies(noopSql, "dep-1", env);
    expect(deps).toBeDefined();
  });
});

// ── G8 projection dead-letter vertical slice ───────────────────────────
describe("G8 projection dead-letter — projectPending", () => {
  it("dead-letters an outbox item when attempts reach MAX_PROJECTION_DELIVERY_ATTEMPTS", async () => {
    const pendingId = "00000000-0000-0000-0000-000000000001";
    const pendingPayload = { idempotencyKey: "key-exhausted", subject: "owner", kind: "ActivityUpsert", accessGeneration: 1 };

    const { sql, updates } = mockSql({
      pendingRows: [{ id: pendingId, payload: pendingPayload, attempts: 2 }],
    });

    const deps = createRuntimeDependencies(sql, "dep-1", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending();

    expect(result.delivered).toBe(0);
    expect(result.failed).toBe(1);

    const deadUpdate = updates().find((u) => /dead_letter/i.test(u));
    expect(deadUpdate).toBeDefined();
    expect(deadUpdate).toContain("dead_letter");
    expect(deadUpdate).toContain("dead_letter_reason");
  });

  it("does NOT dead-letter an outbox item below the retry threshold", async () => {
    const pendingId = "00000000-0000-0000-0000-000000000002";
    const pendingPayload = { idempotencyKey: "key-retry", subject: "owner", kind: "ActivityUpsert", accessGeneration: 1 };

    const { sql, updates } = mockSql({
      pendingRows: [{ id: pendingId, payload: pendingPayload, attempts: 1 }],
    });

    const deps = createRuntimeDependencies(sql, "dep-1", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending();

    expect(result.delivered).toBe(0);
    expect(result.failed).toBe(1);

    const deadUpdate = updates().find((u) => /dead_letter/i.test(u));
    expect(deadUpdate).toBeUndefined();

    const retryUpdate = updates().find((u) => /attempts/i.test(u));
    expect(retryUpdate).toBeDefined();
  });
});

// ── G8 auditability vertical slice — syncCycleAudit ────────────────────
describe("G8 auditability — syncCycleAudit", () => {
  it("returns empty syncCycleAudit when no pending rows exist", async () => {
    const { sql } = mockSql({ pendingRows: [] });

    const deps = createRuntimeDependencies(sql, "dep-1", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    expect(result.syncCycleAudit).toEqual([]);
  });

  it("returns empty syncCycleAudit when bridge URL is not configured", async () => {
    const { sql } = mockSql({
      pendingRows: [{ id: "id-1", connection_id: "conn-1", payload: {}, attempts: 0 }],
    });

    const deps = createRuntimeDependencies(sql, "dep-1", {});

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    expect(result.delivered).toBe(0);
    expect(result.failed).toBe(0);
    expect(result.syncCycleAudit).toEqual([]);
  });

  it("emits dead_letter audit entry with deploymentId, connectionId, outcome, count when retries exhausted", async () => {
    const { sql } = mockSql({
      pendingRows: [
        { id: "id-dl-1", connection_id: "conn-plaid-1", payload: { idempotencyKey: "k1" }, attempts: 2 },
      ],
    });

    const deps = createRuntimeDependencies(sql, "dep-g8", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    expect(result.syncCycleAudit).toHaveLength(1);
    expect(result.syncCycleAudit[0].deploymentId).toBe("dep-g8");
    expect(result.syncCycleAudit[0].connectionId).toBe("conn-plaid-1");
    expect(result.syncCycleAudit[0].outcome).toBe("dead_letter");
    expect(result.syncCycleAudit[0].count).toBe(1);
  });

  it("emits retry audit entry when delivery fails below retry threshold", async () => {
    const { sql } = mockSql({
      pendingRows: [
        { id: "id-rt-1", connection_id: "conn-plaid-2", payload: { idempotencyKey: "k2" }, attempts: 1 },
      ],
    });

    const deps = createRuntimeDependencies(sql, "dep-g8", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    expect(result.syncCycleAudit).toHaveLength(1);
    expect(result.syncCycleAudit[0].deploymentId).toBe("dep-g8");
    expect(result.syncCycleAudit[0].connectionId).toBe("conn-plaid-2");
    expect(result.syncCycleAudit[0].outcome).toBe("retry");
    expect(result.syncCycleAudit[0].count).toBe(1);
  });

  it("aggregates counts per connectionId per outcome across multiple rows", async () => {
    const { sql } = mockSql({
      pendingRows: [
        { id: "id-a-1", connection_id: "conn-shared", payload: { idempotencyKey: "k3" }, attempts: 2 },
        { id: "id-a-2", connection_id: "conn-shared", payload: { idempotencyKey: "k4" }, attempts: 2 },
        { id: "id-a-3", connection_id: "conn-other", payload: { idempotencyKey: "k5" }, attempts: 1 },
      ],
    });

    const deps = createRuntimeDependencies(sql, "dep-g8", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    // conn-shared has 2 dead_letter items, conn-other has 1 retry
    expect(result.syncCycleAudit).toHaveLength(2);

    const sharedDl = result.syncCycleAudit.find(
      (e) => e.connectionId === "conn-shared" && e.outcome === "dead_letter",
    );
    expect(sharedDl).toBeDefined();
    expect(sharedDl!.count).toBe(2);

    const otherRetry = result.syncCycleAudit.find(
      (e) => e.connectionId === "conn-other" && e.outcome === "retry",
    );
    expect(otherRetry).toBeDefined();
    expect(otherRetry!.count).toBe(1);
  });

  it("audit entries contain ONLY redacted fields — no tokens, payloads, account identifiers, or financial amounts", async () => {
    const { sql } = mockSql({
      pendingRows: [
        { id: "id-redact-1", connection_id: "conn-redact", payload: { idempotencyKey: "k6" }, attempts: 2 },
      ],
    });

    const deps = createRuntimeDependencies(sql, "dep-redact", {
      FINANCE_SYNC_BRIDGE_URL: "http://127.0.0.1:1",
    });

    const result = await deps.projectPending() as { delivered: number; failed: number; syncCycleAudit: SyncCycleAuditEntry[] };

    const audits = result.syncCycleAudit;
    expect(audits).toHaveLength(1);
    const entry = audits[0];

    // Allowed fields
    expect(entry).toHaveProperty("deploymentId");
    expect(entry).toHaveProperty("connectionId");
    expect(entry).toHaveProperty("outcome");
    expect(entry).toHaveProperty("count");

    // Forbidden sensitive fields — MUST NOT exist
    const forbidden = ["accessToken", "accountIds", "wealthfolioAccountIds", "payload", "amount", "localOwnerId", "sourceResourceIncarnationId"] as const;
    for (const key of forbidden) {
      expect(entry).not.toHaveProperty(key);
    }

    // Value types
    expect(typeof entry.deploymentId).toBe("string");
    expect(typeof entry.connectionId).toBe("string");
    expect(["delivered", "retry", "dead_letter"]).toContain(entry.outcome);
    expect(typeof entry.count).toBe("number");
    expect(entry.count).toBeGreaterThan(0);
  });
});
