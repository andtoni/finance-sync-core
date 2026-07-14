import { describe, expect, it } from "vitest";
import { HouseholdG0, buildAuthorisationVisibilityMatrix } from "./model";

describe("disposable G0-H household authorisation probe", () => {
  it("requires the intended authenticated subject to accept a one-time invitation", () => {
    const g0 = new HouseholdG0();
    const invite = g0.invite("alexis", "jordan");
    expect(() => g0.acceptInvite("alexis", invite.token)).toThrow("not found");
    g0.acceptInvite("jordan", invite.token);
    expect(() => g0.acceptInvite("jordan", invite.token)).toThrow("not found");
    expect(g0.scope("jordan").membershipGeneration).toBe(1);
  });

  it("derives scope from the authenticated subject and reveals only owned or explicitly shared accounts", () => {
    const g0 = HouseholdG0.active();
    expect(g0.hasExplicitAccess("alexis", "alexis-sole")).toBe(true);
    expect(g0.hasExplicitAccess("jordan", "alexis-sole")).toBe(false);
    expect(g0.listAccounts("alexis", { requestedSubject: "jordan" }).map(x => x.id)).toEqual([
      "alexis-private", "alexis-sole", "joint", "unknown",
    ]);
    expect(g0.listAccounts("jordan").map(x => x.id)).toEqual(["jordan-private", "jordan-sole", "joint"]);
    expect(g0.getAccount("jordan", "alexis-private")).toEqual({ status: 404 });
    g0.share("alexis", "alexis-sole");
    expect(g0.hasExplicitAccess("jordan", "alexis-sole")).toBe(true);
    expect(g0.listAccounts("jordan").map(x => x.id)).toContain("alexis-sole");
  });

  it("does not infer visibility from economic or credential ownership", () => {
    const g0 = HouseholdG0.active();
    g0.removeInitialAccessForProbe("alexis", "unknown");
    expect(g0.getAccount("alexis", "unknown")).toEqual({ status: 404 });
  });

  it("keeps private overlays private and permits visible shared-overlay edits with CAS", () => {
    const g0 = HouseholdG0.active();
    g0.share("alexis", "alexis-sole");
    expect(g0.activity("jordan", "a-alexis")?.overlays.map(x => x.id)).toEqual(["shared-note"]);
    expect(g0.activity("alexis", "a-alexis")?.overlays.map(x => x.id)).toEqual(["private-note", "shared-note"]);
    expect(() => g0.editSharedOverlay("jordan", "a-alexis", "shared-note", 0, "updated")).toThrow("stale revision");
    expect(g0.editSharedOverlay("jordan", "a-alexis", "shared-note", 1, "updated").revision).toBe(2);
  });

  it("uses one exclusive attribution value without inferring unknown activity", () => {
    const g0 = HouseholdG0.active();
    g0.share("alexis", "alexis-sole");
    expect(g0.activities("alexis").map(x => x.attribution)).toEqual([
      "alexis", "jordan", "household", "unassigned",
    ]);
    expect(g0.activities("alexis")).toHaveLength(4);
  });

  it("fences stale access generations and scopes receipts to the projection subject", () => {
    const g0 = HouseholdG0.active();
    const grant = g0.share("alexis", "alexis-sole");
    const receipt = g0.project("alexis", "jordan", "alexis-sole", grant.accessGeneration);
    expect(g0.queryReceipt("jordan", receipt.id)?.projectionSubject).toBe("jordan");
    expect(g0.queryReceipt("alexis", receipt.id)).toBeUndefined();
    g0.revoke("alexis", "alexis-sole");
    expect(() => g0.project("alexis", "jordan", "alexis-sole", grant.accessGeneration)).toThrow("stale access generation");
    const regrant = g0.share("alexis", "alexis-sole");
    expect(regrant.accessGeneration).toBeGreaterThan(grant.accessGeneration);
    expect(g0.queryReceipt("jordan", receipt.id)).toBeUndefined();
  });

  it("fences membership generations across leave and rejoin, and dissolution denies all household sharing", () => {
    const g0 = HouseholdG0.active();
    g0.share("alexis", "alexis-sole");
    const oldMembership = g0.scope("jordan").membershipGeneration;
    g0.leave("jordan");
    expect(g0.getAccount("jordan", "alexis-sole")).toEqual({ status: 404 });
    const invite = g0.invite("alexis", "jordan");
    g0.acceptInvite("jordan", invite.token);
    expect(g0.scope("jordan").membershipGeneration).toBeGreaterThan(oldMembership);
    expect(g0.getAccount("jordan", "alexis-sole")).toEqual({ status: 404 });
    g0.share("alexis", "alexis-sole");
    g0.dissolve("alexis");
    expect(g0.getAccount("jordan", "alexis-sole")).toEqual({ status: 404 });
    expect(() => g0.share("alexis", "alexis-sole")).toThrow("not found");
  });

  it("generates a stable exhaustive machine-readable matrix", () => {
    const matrix = buildAuthorisationVisibilityMatrix();
    expect(matrix.schemaVersion).toBe("g0-h/1");
    expect(matrix.subjects).toEqual(["alexis", "jordan"]);
    expect(matrix.rows.length).toBeGreaterThanOrEqual(40);
    expect(JSON.stringify(matrix)).toBe(JSON.stringify(buildAuthorisationVisibilityMatrix()));
  });

  it("covers the remaining disposable H-SQL semantics", () => {
    const g0 = HouseholdG0.active();
    const expiring = g0.invite("alexis", "jordan", 10);
    expect(() => g0.acceptInvite("jordan", expiring.token, 11)).toThrow("expired");
    expect(g0.discoverAccount("alexis", "new-account", "unknown").visibleTo).toEqual(["alexis"]);
    expect(() => g0.share("alexis", "liability", false)).toThrow("liability disclosure required");
    g0.share("alexis", "alexis-sole");
    const attribution = g0.updateAttribution("jordan", "a-alexis", 0, "household");
    expect(attribution.revision).toBe(1);
    expect(() => g0.updateAttribution("alexis", "a-alexis", 0, "alexis")).toThrow("stale revision");
    expect(g0.attributionHistory("a-alexis")).toHaveLength(2);
    expect(g0.settlePending("alexis", "pending-1", "posted-1").preservedChildren).toEqual(["category", "event", "ignore", "note", "review", "split"]);
    expect(g0.correctProvider("a-alexis", true).needsReview).toBe(true);
    const before = g0.snapshot();
    expect(() => g0.revokeAtomic("alexis", "alexis-sole", true)).toThrow("injected fault");
    expect(g0.snapshot()).toEqual(before);
    expect(g0.runFanout(["jordan", "alexis"], "jordan")).toEqual({ alexis: "committed", jordan: "rolled_back" });
    expect(g0.verifyForeignLink("other-household", "alexis")).toBe(false);
    expect(HouseholdG0.reorderedEventDigest(["share", "revoke", "share"])).toBe(HouseholdG0.reorderedEventDigest(["revoke", "share", "share"]));
  });
});
