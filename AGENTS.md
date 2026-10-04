# AGENTS.md - go-outbox

Guide for AI agents working in this repository. Pair with `CLAUDE.md` (the working agreement and
hook-enforced rules). Keep this file current when the build, layout, or public API changes.

## What this is

A Go library: a transactional outbox for PostgreSQL. `Enqueue` writes an event row through the
caller's go-postgres transaction; a `Relay` claims committed rows (`FOR UPDATE SKIP LOCKED` plus a
lease), publishes them through a `Publisher`, and marks them sent, retried or dead. Delivery is at
least once, keyed by an idempotency key that consumers deduplicate on (`Dedupe`).

Before changing it, understand the claim query in `queries.go`: the lease, the attempt count used
as a fencing token, and the per-aggregate head-of-line rule are what keep relays from double
claiming and keep aggregates in order. The README's "How the relay works" is the contract.

## Using go-outbox

- Entry points: `outbox.New` (table name, logger, metrics), `Outbox.Migrate` (or `Outbox.SQL`),
  `Outbox.Enqueue` inside the caller's transaction, `Outbox.NewRelay(...).Run(ctx)`.
- `Enqueue` must get the caller's transaction, not the pool: with the pool the row commits on its
  own and the outbox guarantee is gone.
- Publishers must return nil only once the broker has the message; the RabbitMQ adapter forces
  publisher confirms for that reason.
- The schema is versioned by the embedded `migrations/NNNN_*.sql` files. Never edit a released
  migration; add the next number.

## Layout

- Root package `outbox`: `outbox.go` (Outbox, Message, Enqueue, Dedupe, Stats, Redrive, Purge),
  `relay.go` (Relay and its options), `queries.go` (all SQL), `migrate.go` and `migrations/`,
  `publisher.go`, `metrics.go`, `errors.go`, `backoff.go`.
- `rabbitmq/`: the go-rabbitmq `Publisher` adapter, kept out of the root so the core has no AMQP
  import.
- `internal/testenv/`: integration-only helpers that start PostgreSQL and RabbitMQ with
  testcontainers.

## Build, test, lint

- Build: `task build`
- Test: `task test` (hermetic unit tests); `task test-integration` (needs Docker:
  testcontainers starts PostgreSQL and RabbitMQ; CI runs it in `job-go-integration.yaml`)
- Lint: `task lint`
- License headers: `task license` (check), `task license:fix` (inject)

## Logging

Follow the logging rules in `CLAUDE.md`. In short:

- Log generously: entry and exit of significant operations, decisions and branches, retries, state
  changes, external calls (target, duration, outcome), and every error with its context.
- Levels: `trace` for step-by-step detail, `debug` for flow, `info` for lifecycle, `warn` and
  `error` for problems. The environment filters the volume, so err on the side of too much.
- Environments: local dev `trace` with `LOG_FORMAT=console` (never JSON), dev cluster `debug`,
  qa/staging `info`, production `error`. Every cluster environment logs JSON. Set levels through
  `LOG_LEVEL` and `LOG_FORMAT`, never in code; local settings live in the run target or
  `.env.example`.
- Never log secrets, tokens, or personal data, not even at `trace`. Log an opaque or keyed ID.

## Conventions and gotchas

- See `CLAUDE.md` for the branch/commit/PR rules; they are enforced by the git hooks in
  `.claude/hooks` (run `bash .claude/hooks/install.sh` once per clone).
- Open every PR as a draft. CI skips drafts, so run the full checks locally, push once they pass,
  and mark the PR ready when the work is finished; see CLAUDE.md "CI and Actions minutes".
- Integration tests use real PostgreSQL and RabbitMQ, never mocks of pgx or amqp; fake only the
  package's own `Publisher` interface.
