# go-outbox 📮

> 🧭 Transactional outbox for Go on PostgreSQL: enqueue events inside your transaction and relay them at least once to RabbitMQ or any publisher.

A service that writes to its database and then publishes an event has two ways to get it wrong: the
publish fails after the commit and the event is lost, or the event goes out for work that was then
rolled back. `go-outbox` closes that gap. The event is a row written in **your** transaction, so
it exists only if the transaction commits, and a relay ships committed rows to the broker
afterwards, retrying until it succeeds.

## ✨ Highlights

- 🔒 **Commits with your data** — `Enqueue` writes through the go-postgres transaction you already have; a rollback leaves nothing to publish.
- 🔁 **At least once, with a key** — every message carries an idempotency key on every attempt, and `Dedupe` gives consumers exactly-once processing in their own transaction.
- 🏃 **Many relays, no double claims** — rows are claimed with `FOR UPDATE SKIP LOCKED` under a lease, so relays scale out across processes.
- ⏳ **Retry, then dead-letter** — capped, jittered exponential backoff; a dead-letter state after N attempts or a permanent error; `Redrive` to try again.
- 🧵 **Ordered per aggregate** — messages that share an aggregate key go out one at a time, in enqueue order.
- 🔔 **Wakes on commit** — LISTEN/NOTIFY wakes idle relays the moment a transaction commits, with polling as the fallback.
- 🛑 **Graceful shutdown** — in-flight publishes finish and are recorded; unstarted claims are handed back at once.
- 🗄️ **Embedded, versioned migration** — with your own table name, or the SQL for your own migration tool.
- 🐇 **RabbitMQ adapter** — on go-rabbitmq with publisher confirms, in a sub-package so the core never imports AMQP.
- 📈 **Observable** — logs through go-log, a `Metrics` hook for counters, and `Stats` for gauges and lag.

## 📦 Install

```bash
go get github.com/Bugs5382/go-outbox
```

It builds on [go-postgres](https://github.com/Bugs5382/go-postgres) (pool and transactions),
[go-log](https://github.com/Bugs5382/go-log) (logging) and, for the adapter,
[go-rabbitmq](https://github.com/Bugs5382/go-rabbitmq). PostgreSQL 13 or later.

## 🚀 Usage

Create the tables once at start-up, then enqueue inside the transaction that makes the change:

```go
ob, err := outbox.New(outbox.WithTable("orders_outbox"), outbox.WithLogger(log.NewLogger("orders")))
if err != nil {
	return err
}
if err := ob.Migrate(ctx, db); err != nil { // db is a *postgres.DB
	return err
}

err = db.RunInTx(ctx, func(tx pgx.Tx) error {
	if _, err := tx.Exec(ctx, `INSERT INTO orders (id, total) VALUES ($1, $2)`, id, total); err != nil {
		return err
	}
	_, err := ob.Enqueue(ctx, tx, outbox.Message{
		Topic:        "order.created",
		AggregateKey: "order-" + id,
		Payload:      body,
		ContentType:  "application/json",
	})
	return err
})
```

`Enqueue` returns the message's idempotency key. Leave `Message.Key` empty to get a random UUID,
or set it from the request (a client's idempotency token, say) so a replay is caught: a key that
is already in the table returns `ErrDuplicateKey`, and the transaction stays usable. `RunInTx` may
retry the whole function on a serialization failure, which is safe: the retried transaction
enqueues again and the rolled-back attempt left nothing behind.

Run a relay beside the service, or in a worker of its own:

```go
conn, err := rabbitmq.Connect(ctx, amqpURL)
if err != nil {
	return err
}
pub := outboxrabbitmq.New(conn, "events") // github.com/Bugs5382/go-outbox/rabbitmq

relay := ob.NewRelay(db, pub)
go func() { _ = relay.Run(ctx) }() // returns nil once ctx ends and the relay has drained
```

## 🧾 Consuming: deduplicate on the key

Delivery is at least once, so a consumer sees a message again when a relay stopped between
publishing it and recording it. The RabbitMQ adapter sends the idempotency key as the AMQP
message id. `Dedupe` records the key in the consumer's own transaction and reports whether it is
new; if the handler fails and rolls back, the record goes with it and the redelivery is processed:

```go
handler := func(ctx context.Context, d rabbitmq.Delivery) error {
	return db.RunInTx(ctx, func(tx pgx.Tx) error {
		fresh, err := ob.Dedupe(ctx, tx, "invoicing", d.MessageID)
		if err != nil || !fresh {
			return err // a duplicate is acknowledged and skipped
		}
		return createInvoice(ctx, tx, d.Body)
	})
}
```

Each consumer name keeps its own record. A consumer in another service can use its own `Outbox`
(and `Migrate`) just for `Dedupe`.

## ⚙️ How the relay works

Each cycle the relay claims up to a batch of **due** rows in one statement: pending, past their
backoff, not under a live lease and, for rows with an aggregate key, the oldest pending row of
that aggregate. `FOR UPDATE SKIP LOCKED` lets concurrent relays pass over each other's rows, and
the claim stamps a lease and bumps the attempt count. The relay then publishes outside any
transaction, so no connection or lock is held across the network call, and records each
outcome:

| Publish result | Row becomes |
|---|---|
| success | `sent` |
| error, attempts left | `pending`, due again after the backoff |
| error on the last attempt, or `outbox.Permanent(err)` | `dead`, with the last error kept |
| the relay stopped before marking it | still `pending`; claimable again when the lease runs out |

The attempt count fences every update: a relay whose lease ran out and whose row was reclaimed
cannot overwrite the new owner's state. The lease (`WithLease`, default 30s) must comfortably
exceed the time to publish one batch, since a lease that expires mid-publish lets another relay
send the same rows again.

**Wake-up.** `Enqueue` queues a `NOTIFY` on a channel named after the table, which PostgreSQL
delivers only on commit. Each relay holds a `LISTEN` on a connection it takes out of the pool,
and reconnects with backoff when it drops. Polling (`WithPollInterval`, default 2s) still runs:
it picks up backoff expiries, delayed messages (`Message.Delay`), expired leases and anything
committed while the listener was down. Behind a pooler in transaction mode, which cannot hold a
`LISTEN`, use `WithoutNotify()`.

**Ordering.** Only the oldest pending row of an aggregate can be claimed, so its successors wait
while it backs off. A row that becomes a dead letter stops blocking its aggregate; the ones after
it are then published, and `Redrive` sends the dead one later, out of order. Rows without an
aggregate key are claimed in id order but published concurrently (`WithConcurrency`, default 4).

**Shutdown.** When `Run`'s context ends the relay claims nothing more. Publishes already under
way get the drain timeout (`WithDrainTimeout`, default 10s) to finish, and their outcome is
recorded. Rows it had claimed but not started are released at once, with their attempt count
restored, so another relay picks them up without waiting for the lease.

## 🎛 Options

| Option | Default | What it sets |
|---|---|---|
| `WithTable(name)` | `outbox` | Table name, optionally `schema.name`; also names the inbox, migration table and notify channel |
| `WithLogger(l)` | no-op | go-log `Logger` for the outbox and its relays |
| `WithMetrics(m)` | no-op | `Metrics` hook for relay events |
| `WithBatchSize(n)` | 100 | Rows claimed per cycle |
| `WithConcurrency(n)` | 4 | Rows published at the same time |
| `WithLease(d)` | 30s | How long a claim holds its rows |
| `WithPollInterval(d)` | 2s | Idle poll interval |
| `WithMaxAttempts(n)` | 10 | Attempts before a dead letter |
| `WithBackoff(min, max)` | 1s, 5m | Retry delay range |
| `WithDrainTimeout(d)` | 10s | Time in-flight publishes get on shutdown |
| `WithoutNotify()` | notify on | Polling only |

The first three are `outbox.Option`s for `New`; the rest are `RelayOption`s for `NewRelay`.

## 🗄️ Schema and operations

`Migrate` is safe on every start and from several processes at once: it runs under an advisory
lock in one transaction and records applied versions in `<table>_migrations`. It creates:

- `<table>`: the outbox, with a unique idempotency key, `pending`/`sent`/`dead` status, attempts,
  backoff and lease times, the last error, and partial indexes for the claim query;
- `<table>_inbox`: the consumer and key pairs `Dedupe` records.

To run the schema through your own migration tool instead, take the statements from `ob.SQL()`.

| Call | Use |
|---|---|
| `Stats(ctx, q)` | Pending, in-flight, sent and dead counts, and the age of the oldest pending row |
| `Redrive(ctx, q, keys...)` | Return dead letters (all, or the listed keys) to pending with fresh attempts |
| `Purge(ctx, q, age)` | Delete sent rows older than `age`; run it on a schedule |

## 🐇 RabbitMQ publisher

`outboxrabbitmq.New(conn, exchange, opts...)` publishes each record to `exchange` with `Topic`
as the routing key, the idempotency key as the message id, `ContentType`, the record's headers,
the enqueue time as the timestamp, and two headers of its own: `x-outbox-attempt` and, when set,
`x-outbox-aggregate-key`. Publisher confirms are always on and messages are persistent, so a row
is marked sent only after the broker has it. Pass `rabbitmq.WithMandatory()` to have an
unroutable message come back as an error, and so be retried, instead of being dropped.

## 📈 Logs and metrics

Lifecycle goes to `info`, cycles and publishes to `debug`, per-message detail to `trace`,
retries and listener drops to `warn`, and dead letters and bookkeeping failures to `error`.
Payloads are never logged; messages are identified by key and topic. `Metrics` receives
`Claimed`, `Published` (with the duration), `Failed`, `DeadLettered` and `Released`; embed
`outbox.NopMetrics` in an implementation to stay compatible as the interface grows.

## 🛠 Develop

```bash
task build              # go build ./...
task test               # go test ./... (unit tests, no services)
task test-integration   # integration tests; testcontainers starts PostgreSQL and RabbitMQ in Docker
task lint               # gofmt check + golangci-lint + yamllint
task license            # check MIT headers (golic)
```

## ⚖️ License

MIT (c) 2026 Shane
