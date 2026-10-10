# finance-sync

A headless service that reads transactions and balances from financial providers, records them as
immutable facts, reconciles them, and delivers the result to a downstream ledger with a receipt for
every write. It is read-only towards providers: it never initiates payments, transfers or trades.

This repository is the reconciliation core, published from a private working repository. Deployment
configuration, the user interface and other modules are not included, commit messages are reduced to
their subject lines, and test data has been replaced with synthetic values.

Built with AI coding agents (Claude Code, Codex and Pi) that I direct. Every change in the private
working repository goes through a pull request gated by CI and security scanning.

## What it does

- **Provider adapters** for Investec, Mercury, Plaid and SnapTrade behind one probe interface
  (`api/src/finance-sync/probes`).
- **Immutable observations.** Every provider response is stored as an append-only observation in
  PostgreSQL; canonical revisions are derived from observations and never edited in place
  (`observations`, `canonical`, `storage`).
- **Lifecycle reconciliation.** Pending card authorisations are linked to their settlements, retired
  when the provider drops them, and corrected when a provider re-issues identifiers.
- **Receipt-bound projection.** Changes are delivered through an outbox to a receiver that returns a
  receipt bound to the exact operation; delivery is idempotent, generation-fenced and replayable, with a dead-letter lane for
  terminal failures (`projection`).
- **Fail-closed runtime.** Each secret is accepted from an environment variable or a file but never
  both, the control token from a file only, provider origins are allow-listed, and a provider outage
  degrades one connection without stopping the service.

## Run the checks

```bash
cd api
bun install
bun run typecheck
bun run test
```

Integration tests that need PostgreSQL are skipped unless a database is configured.

## Layout

| Path | Contents |
|---|---|
| `api/src/finance-sync` | Service source and tests |
| `api/finance-sync-migrations` | PostgreSQL schema migrations |
| `api/scripts` | History planning and apply tools |

## Companion

The receiver side of the projection lives in a fork of Wealthfolio:
[andtoni/wealthfolio-fork](https://github.com/andtoni/wealthfolio-fork).
