// Package outbox is a transactional outbox for PostgreSQL: a service
// publishes an event if and only if the database transaction that caused it
// commits.
//
// # Writing
//
// Enqueue writes a Message to the outbox table through the caller's own
// transaction, the one from go-postgres's RunInTx or RunInTxQuerier. The row
// commits or rolls back with the business change, so a rolled-back
// transaction never publishes anything and a committed one always does.
// Enqueue also queues a NOTIFY, which PostgreSQL delivers only on commit.
//
// # Relaying
//
// A Relay, built with NewRelay, claims due rows in batches with
// FOR UPDATE SKIP LOCKED and stamps each with a lease, then publishes them
// through a Publisher outside any transaction and marks each one sent. Any
// number of relays, in any number of processes, can drain one table: SKIP
// LOCKED and the lease keep two relays from claiming the same row. A relay
// sleeps between cycles until a notification or the poll interval wakes it.
//
// # Guarantees
//
//   - At least once. A relay that dies between publishing and marking leaves
//     its rows leased; when the lease runs out another relay publishes them
//     again. Every attempt carries the same idempotency key (Message.Key), and
//     consumers deduplicate on it, with Dedupe or their own store.
//   - Retries. A failed publish is retried after an exponential, jittered
//     backoff. After the configured number of attempts, or at once for an
//     error wrapped with Permanent, the row becomes a dead letter. Redrive
//     puts dead letters back.
//   - Ordering. Messages that share an AggregateKey are published one at a
//     time in enqueue order: a row is claimable only while no older row of
//     its aggregate is still pending. A dead letter stops blocking its
//     aggregate. Messages without an AggregateKey have no ordering.
//   - Graceful shutdown. When Run's context ends the relay stops claiming,
//     lets in-flight publishes finish within the drain timeout and records
//     them, and releases the rows it had claimed but not started.
//
// # Schema
//
// Migrate creates the outbox table, an inbox table for Dedupe, and a table
// that records applied schema versions, all named after WithTable (default
// "outbox"). SQL returns the same statements for an external migration tool.
// PostgreSQL 13 or later is required.
//
// # Publishers and observability
//
// Publisher is a single method, so any broker fits. The rabbitmq
// sub-package is a ready one for RabbitMQ on go-rabbitmq, with publisher
// confirms. The outbox logs through a go-log Logger (WithLogger) and reports
// relay events to a Metrics hook (WithMetrics); Stats gives counts and lag
// for gauges and health checks.
package outbox

/*
MIT License

Copyright (c) 2026 Shane

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
*/
