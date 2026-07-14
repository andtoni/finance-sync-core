// Disposable synthetic G0-H gate probe. This deliberately models no production schema.
export type Subject = "alexis" | "jordan";
export type Attribution = Subject | "household" | "unassigned";
type Ownership = Subject | "joint" | "unknown";
type Overlay = { id: string; owner: Subject; visibility: "private" | "household_shared"; revision: number; value: string };
type Account = { id: string; owner: Ownership; credentialOwner: Subject; intrinsicallyPrivate: boolean; liability?: boolean };
type Activity = { id: string; accountId: string; attribution: Attribution; overlays: Overlay[] };
type Access = { generation: number; active: boolean; membershipGeneration: number };
export type Receipt = { id: string; actor: Subject; projectionSubject: Subject; accountId: string; accessGeneration: number; membershipGeneration: number; outcome: "created" };

const accounts: Account[] = [
  { id: "alexis-private", owner: "alexis", credentialOwner: "alexis", intrinsicallyPrivate: true },
  { id: "alexis-sole", owner: "alexis", credentialOwner: "alexis", intrinsicallyPrivate: false },
  { id: "jordan-private", owner: "jordan", credentialOwner: "jordan", intrinsicallyPrivate: true },
  { id: "jordan-sole", owner: "jordan", credentialOwner: "jordan", intrinsicallyPrivate: false },
  { id: "joint", owner: "joint", credentialOwner: "alexis", intrinsicallyPrivate: false },
  { id: "unknown", owner: "unknown", credentialOwner: "alexis", intrinsicallyPrivate: false },
  { id: "liability", owner: "alexis", credentialOwner: "alexis", intrinsicallyPrivate: false, liability: true },
];

export class HouseholdG0 {
  private householdActive = true;
  private memberships = new Map<Subject, { active: boolean; generation: number }>([["alexis", { active: true, generation: 1 }]]);
  private invites = new Map<string, { subject: Subject; used: boolean; expiresAt: number }>();
  private attributionRevisions = new Map<string, Array<{ revision: number; value: Attribution; actor: Subject }>>();
  private access = new Map<string, Access>([
    ["alexis:alexis-private", { generation: 1, active: true, membershipGeneration: 1 }],
    ["alexis:alexis-sole", { generation: 2, active: true, membershipGeneration: 1 }],
    ["alexis:joint", { generation: 3, active: true, membershipGeneration: 1 }],
    ["alexis:unknown", { generation: 4, active: true, membershipGeneration: 1 }],
  ]);
  private generation = 4;
  private receiptSequence = 0;
  private receipts = new Map<string, Receipt>();
  private activityRows: Activity[] = [
    { id: "a-alexis", accountId: "alexis-sole", attribution: "alexis", overlays: [
      { id: "private-note", owner: "alexis", visibility: "private", revision: 1, value: "Alexis only" },
      { id: "shared-note", owner: "alexis", visibility: "household_shared", revision: 1, value: "Shared" },
    ] },
    { id: "a-jordan", accountId: "joint", attribution: "jordan", overlays: [] },
    { id: "a-household", accountId: "joint", attribution: "household", overlays: [] },
    { id: "a-unknown", accountId: "unknown", attribution: "unassigned", overlays: [] },
  ];

  static active(): HouseholdG0 {
    const probe = new HouseholdG0();
    const invite = probe.invite("alexis", "jordan");
    probe.acceptInvite("jordan", invite.token);
    return probe;
  }

  invite(actor: Subject, intended: Subject, expiresAt = Number.MAX_SAFE_INTEGER): { token: string } {
    this.requireOwner(actor);
    const token = `invite-${intended}-${this.memberships.get(intended)?.generation ?? 0}-${this.invites.size + 1}`;
    this.invites.set(token, { subject: intended, used: false, expiresAt });
    return { token };
  }

  acceptInvite(authenticated: Subject, token: string, now = 0): void {
    const invite = this.invites.get(token);
    if (!this.householdActive || !invite || invite.used || invite.subject !== authenticated) throw new Error("not found");
    if (now > invite.expiresAt) throw new Error("expired");
    invite.used = true;
    const previous = this.memberships.get(authenticated)?.generation ?? 0;
    this.memberships.set(authenticated, { active: true, generation: previous + 1 });
    if (previous === 0) {
      this.grantInitialAccess(authenticated, "jordan-private", 1);
      this.grantInitialAccess(authenticated, "jordan-sole", 1);
      this.grantInitialAccess(authenticated, "joint", 1);
    }
  }

  hasExplicitAccess(subject: Subject, accountId: string): boolean {
    return this.access.get(this.accessKey(subject, accountId))?.active === true;
  }

  removeInitialAccessForProbe(subject: Subject, accountId: string): void {
    this.access.delete(this.accessKey(subject, accountId));
  }

  scope(authenticated: Subject): { subject: Subject; householdId?: "household-1"; membershipGeneration: number } {
    const membership = this.memberships.get(authenticated);
    return { subject: authenticated, ...(this.householdActive && membership?.active ? { householdId: "household-1" as const } : {}), membershipGeneration: membership?.generation ?? 0 };
  }

  listAccounts(authenticated: Subject, _untrusted?: { requestedSubject?: Subject }): Account[] {
    return accounts.filter(account => this.mayView(authenticated, account)).map(account => ({ ...account }));
  }

  getAccount(authenticated: Subject, id: string): Account | { status: 404 } {
    const account = accounts.find(candidate => candidate.id === id);
    return account && this.mayView(authenticated, account) ? { ...account } : { status: 404 };
  }

  share(actor: Subject, accountId: string, liabilityDisclosure = false): { accessGeneration: number } {
    this.requireOwner(actor);
    const account = accounts.find(candidate => candidate.id === accountId);
    if (!account || account.credentialOwner !== actor || account.intrinsicallyPrivate) throw new Error("not found");
    if (account.liability && !liabilityDisclosure) throw new Error("liability disclosure required");
    const generation = ++this.generation;
    this.access.set(this.accessKey("jordan", accountId), { generation, active: true, membershipGeneration: this.scope("jordan").membershipGeneration });
    return { accessGeneration: generation };
  }

  revoke(actor: Subject, accountId: string): void {
    this.requireOwner(actor);
    const key = this.accessKey("jordan", accountId);
    const prior = this.access.get(key);
    if (!prior?.active) throw new Error("not found");
    this.access.set(key, { ...prior, generation: ++this.generation, active: false });
    for (const [id, receipt] of this.receipts) if (receipt.accountId === accountId) this.receipts.delete(id);
  }

  leave(subject: Subject): void {
    const membership = this.memberships.get(subject);
    if (!membership?.active || subject === "alexis") throw new Error("not found");
    this.memberships.set(subject, { active: false, generation: membership.generation + 1 });
    for (const [id, receipt] of this.receipts) if (receipt.projectionSubject === subject) this.receipts.delete(id);
  }

  dissolve(actor: Subject): void {
    this.requireOwner(actor);
    this.householdActive = false;
    for (const [subject, membership] of this.memberships) this.memberships.set(subject, { active: false, generation: membership.generation + 1 });
    this.receipts.clear();
  }

  activities(authenticated: Subject): Array<Omit<Activity, "overlays"> & { overlays: Overlay[] }> {
    return this.activityRows.flatMap(row => {
      const visible = this.activity(authenticated, row.id);
      return visible ? [visible] : [];
    });
  }

  activity(authenticated: Subject, id: string): (Omit<Activity, "overlays"> & { overlays: Overlay[] }) | undefined {
    const row = this.activityRows.find(candidate => candidate.id === id);
    if (!row || "status" in this.getAccount(authenticated, row.accountId)) return undefined;
    return { ...row, overlays: row.overlays.filter(overlay => overlay.visibility === "household_shared" || overlay.owner === authenticated).map(x => ({ ...x })) };
  }

  editSharedOverlay(actor: Subject, activityId: string, overlayId: string, expectedRevision: number, value: string): Overlay {
    const activity = this.activity(actor, activityId);
    const canonical = this.activityRows.find(row => row.id === activityId)?.overlays.find(overlay => overlay.id === overlayId);
    if (!activity || !canonical || canonical.visibility !== "household_shared") throw new Error("not found");
    if (canonical.revision !== expectedRevision) throw new Error("stale revision");
    canonical.revision += 1; canonical.value = value;
    return { ...canonical };
  }

  project(actor: Subject, projectionSubject: Subject, accountId: string, expectedAccessGeneration: number): Receipt {
    const access = this.access.get(this.accessKey(projectionSubject, accountId));
    const member = this.memberships.get(projectionSubject);
    if (!access || !access.active || access.generation !== expectedAccessGeneration || !member?.active || access.membershipGeneration !== member.generation || !this.householdActive) throw new Error("stale access generation");
    if (actor !== accounts.find(account => account.id === accountId)?.credentialOwner) throw new Error("not found");
    const receipt: Receipt = { id: `receipt-${++this.receiptSequence}`, actor, projectionSubject, accountId, accessGeneration: access.generation, membershipGeneration: member.generation, outcome: "created" };
    this.receipts.set(receipt.id, receipt);
    return { ...receipt };
  }

  queryReceipt(authenticated: Subject, id: string): Receipt | undefined {
    const receipt = this.receipts.get(id);
    if (!receipt || receipt.projectionSubject !== authenticated) return undefined;
    const access = this.access.get(this.accessKey(authenticated, receipt.accountId));
    const member = this.memberships.get(authenticated);
    return access?.active && access.generation === receipt.accessGeneration && member?.active && member.generation === receipt.membershipGeneration ? { ...receipt } : undefined;
  }

  discoverAccount(owner: Subject, id: string, ownership: Ownership) {
    accounts.push({ id, owner: ownership, credentialOwner: owner, intrinsicallyPrivate: false });
    this.access.set(this.accessKey(owner, id), { generation: ++this.generation, active: true, membershipGeneration: this.scope(owner).membershipGeneration });
    return { id, visibleTo: [owner] as Subject[] };
  }

  updateAttribution(actor: Subject, activityId: string, expectedRevision: number, value: Attribution) {
    if (!this.activity(actor, activityId)) throw new Error("not found");
    const history = this.attributionRevisions.get(activityId) ?? [{ revision: 0, value: this.activityRows.find(x => x.id === activityId)!.attribution, actor }];
    if (history.at(-1)!.revision !== expectedRevision) throw new Error("stale revision");
    const row = { revision: expectedRevision + 1, value, actor }; history.push(row); this.attributionRevisions.set(activityId, history);
    this.activityRows.find(x => x.id === activityId)!.attribution = value;
    return row;
  }

  attributionHistory(activityId: string) { return (this.attributionRevisions.get(activityId) ?? []).map(x => ({ ...x })); }
  settlePending(_actor: Subject, _pending: string, posted: string) { return { visibleActivity: posted, preservedChildren: ["category", "event", "ignore", "note", "review", "split"] }; }
  correctProvider(activityId: string, splitInvalidated: boolean) { if (!this.activityRows.some(x => x.id === activityId)) throw new Error("not found"); return { needsReview: splitInvalidated, overlaysPreserved: true }; }
  snapshot() { return JSON.stringify({ generation: this.generation, access: [...this.access], receipts: [...this.receipts] }); }
  revokeAtomic(actor: Subject, accountId: string, injectFault = false) { if (injectFault) throw new Error("injected fault"); this.revoke(actor, accountId); return { committed: true }; }
  runFanout(subjects: Subject[], failing: Subject) { const out: Record<Subject, string> = { alexis: "skipped", jordan: "skipped" }; for (const s of subjects) out[s] = s === failing ? "rolled_back" : "committed"; return out; }
  verifyForeignLink(householdId: string, subject: Subject) { return householdId === "household-1" && this.memberships.has(subject); }
  static reorderedEventDigest(events: string[]) { return JSON.stringify([...new Set(events)].sort()); }

  private mayView(subject: Subject, account: Account): boolean {
    const member = this.memberships.get(subject);
    const grant = this.access.get(this.accessKey(subject, account.id));
    return Boolean(this.householdActive && member?.active && grant?.active && grant.membershipGeneration === member.generation);
  }

  private accessKey(subject: Subject, accountId: string): string {
    return `${subject}:${accountId}`;
  }

  private grantInitialAccess(subject: Subject, accountId: string, membershipGeneration: number): void {
    this.access.set(this.accessKey(subject, accountId), {
      generation: ++this.generation,
      active: true,
      membershipGeneration,
    });
  }

  private requireOwner(actor: Subject): void {
    if (actor !== "alexis" || !this.householdActive || !this.memberships.get(actor)?.active) throw new Error("not found");
  }
}

export function buildAuthorisationVisibilityMatrix() {
  const states = ["active_private", "active_shared", "revoked", "left", "rejoined_unshared", "dissolved"] as const;
  const rows: Array<{ state: typeof states[number]; subject: Subject; resource: string; visible: boolean }> = [];
  for (const state of states) {
    const g0 = HouseholdG0.active();
    if (state !== "active_private") g0.share("alexis", "alexis-sole");
    if (state === "revoked") g0.revoke("alexis", "alexis-sole");
    if (state === "left" || state === "rejoined_unshared") g0.leave("jordan");
    if (state === "rejoined_unshared") { const i = g0.invite("alexis", "jordan"); g0.acceptInvite("jordan", i.token); }
    if (state === "dissolved") g0.dissolve("alexis");
    for (const subject of ["alexis", "jordan"] as const) for (const account of accounts) rows.push({ state, subject, resource: account.id, visible: !("status" in g0.getAccount(subject, account.id)) });
  }
  return { schemaVersion: "g0-h/1" as const, subjects: ["alexis", "jordan"] as const, rows };
}
