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

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"strings"
	"time"

	log "github.com/Bugs5382/go-log"
	postgres "github.com/Bugs5382/go-postgres"
)

// DefaultTable is the outbox table name used when WithTable is not given.
const DefaultTable = "outbox"

// Message is one event to publish once the transaction that enqueues it
// commits.
type Message struct {
	// Key is the idempotency key. It is unique per outbox table, travels with
	// every delivery attempt, and is what consumers deduplicate on. Leave it
	// empty to have a random UUID generated.
	Key string
	// Topic says where the message goes; the RabbitMQ publisher uses it as
	// the routing key. Required.
	Topic string
	// AggregateKey scopes ordering: messages that share one are published
	// one at a time, in enqueue order. Empty means no ordering constraint.
	AggregateKey string
	// Payload is the message body, stored as bytes.
	Payload []byte
	// ContentType describes Payload, such as application/json.
	ContentType string
	// Headers are carried to the publisher unchanged.
	Headers map[string]string
	// Delay holds the message back for this long after the enqueue.
	Delay time.Duration
}

// Outbox writes messages to an outbox table, inside the caller's
// transaction, and builds relays that drain that table. An Outbox holds no
// connection and is safe for concurrent use.
type Outbox struct {
	t       tableNames
	q       queries
	logger  log.Logger
	metrics Metrics
}

// Option configures an Outbox.
type Option func(*config)

type config struct {
	table   string
	logger  log.Logger
	metrics Metrics
}

// WithTable sets the outbox table name, optionally schema-qualified
// ("audit.events"). Each part is lowercase letters, digits and underscores,
// starts with a letter or underscore, and is at most 50 characters. The inbox
// and migration tables are named after it, with _inbox and _migrations
// suffixes, and the notification channel is the table name.
func WithTable(name string) Option { return func(c *config) { c.table = name } }

// WithLogger sets the go-log logger the outbox and its relays write to. The
// default discards everything.
func WithLogger(l log.Logger) Option { return func(c *config) { c.logger = l } }

// WithMetrics sets the hook that receives relay events. The default discards
// them.
func WithMetrics(m Metrics) Option { return func(c *config) { c.metrics = m } }

// New builds an Outbox. It returns ErrInvalidTable when the table name is not
// a plain, optionally schema-qualified, lowercase identifier.
func New(opts ...Option) (*Outbox, error) {
	c := config{table: DefaultTable, logger: log.Nop(), metrics: NopMetrics{}}
	for _, opt := range opts {
		opt(&c)
	}
	t, err := parseTable(c.table)
	if err != nil {
		return nil, err
	}
	return &Outbox{t: t, q: buildQueries(t), logger: c.logger.With(log.F("outbox", t.full)), metrics: c.metrics}, nil
}

// Table returns the outbox table name as configured.
func (o *Outbox) Table() string { return o.t.full }

// Enqueue writes msg to the outbox through tx and returns its idempotency
// key. Pass the transaction that makes the change the message announces
// (from postgres.DB.RunInTx or RunInTxQuerier): the row, and the wake-up
// notification for idle relays, then exist only if that transaction commits.
//
// A key that is already in the table returns ErrDuplicateKey without
// aborting tx, so the caller can treat a replayed request as done. A message
// without a Topic returns ErrInvalidMessage.
func (o *Outbox) Enqueue(ctx context.Context, tx postgres.Querier, msg Message) (string, error) {
	if msg.Topic == "" {
		return "", fmt.Errorf("%w: topic is required", ErrInvalidMessage)
	}
	if msg.Delay < 0 {
		return "", fmt.Errorf("%w: negative delay", ErrInvalidMessage)
	}
	payload := msg.Payload
	if payload == nil {
		payload = []byte{}
	}
	headers := msg.Headers
	if headers == nil {
		headers = map[string]string{}
	}

	var key string
	err := tx.QueryRow(ctx, o.q.enqueue,
		msg.Key, msg.Topic, msg.AggregateKey, payload, msg.ContentType, headers,
		msg.Delay.Milliseconds(), o.t.channel,
	).Scan(&key)
	if errors.Is(err, postgres.ErrNoRows) {
		o.logger.Debug("enqueue skipped a duplicate key", log.F("key", msg.Key), log.F("topic", msg.Topic))
		return "", fmt.Errorf("%w: %s", ErrDuplicateKey, msg.Key)
	}
	if err != nil {
		o.logger.Error(err, "enqueue failed", log.F("topic", msg.Topic))
		return "", fmt.Errorf("outbox: enqueue: %w", err)
	}
	log.Trace(o.logger, "enqueued", log.F("key", key), log.F("topic", msg.Topic), log.F("aggregate", msg.AggregateKey))
	return key, nil
}

// Dedupe is the consumer half of exactly-once processing on top of
// at-least-once delivery. It records that consumer has handled the message
// with key and reports whether this is the first time. Call it in the same
// transaction as the consumer's own writes: when it returns false the message
// was already handled and can be acknowledged without doing anything; when
// the transaction rolls back, the record goes with it and a redelivery is
// fresh again. Each consumer name keeps its own record of keys.
func (o *Outbox) Dedupe(ctx context.Context, tx postgres.Querier, consumer, key string) (bool, error) {
	if consumer == "" || key == "" {
		return false, fmt.Errorf("%w: consumer and key are required", ErrInvalidMessage)
	}
	tag, err := tx.Exec(ctx, o.q.dedupe, consumer, key)
	if err != nil {
		return false, fmt.Errorf("outbox: dedupe: %w", err)
	}
	fresh := tag.RowsAffected() == 1
	log.Trace(o.logger, "dedupe", log.F("consumer", consumer), log.F("key", key), log.F("fresh", fresh))
	return fresh, nil
}

// Stats counts the outbox rows by state.
type Stats struct {
	// Pending rows wait to be published, including rows backing off after
	// a failed attempt. Rows a relay holds right now are in InFlight instead.
	Pending int64
	// InFlight rows are claimed by a relay under an unexpired lease.
	InFlight int64
	// Sent rows were published and are kept until Purge removes them.
	Sent int64
	// Dead rows ran out of attempts or failed permanently.
	Dead int64
	// OldestPending is the age of the oldest pending or in-flight row, the
	// relay's lag. Zero when nothing is waiting.
	OldestPending time.Duration
}

// Stats reports the outbox counts, for a gauge or a health check.
func (o *Outbox) Stats(ctx context.Context, q postgres.Querier) (Stats, error) {
	var s Stats
	var oldest float64
	err := q.QueryRow(ctx, o.q.stats).Scan(&s.Pending, &s.InFlight, &s.Sent, &s.Dead, &oldest)
	if err != nil {
		return Stats{}, fmt.Errorf("outbox: stats: %w", err)
	}
	s.OldestPending = time.Duration(oldest * float64(time.Second))
	return s, nil
}

// Redrive returns dead letters to pending with a fresh attempt count, so a
// relay publishes them again. With no keys it redrives every dead letter.
// It reports how many rows it changed.
func (o *Outbox) Redrive(ctx context.Context, q postgres.Querier, keys ...string) (int64, error) {
	var (
		tag postgres.CommandTag
		err error
	)
	if len(keys) == 0 {
		tag, err = q.Exec(ctx, o.q.redriveAll)
	} else {
		tag, err = q.Exec(ctx, o.q.redriveKeys, keys)
	}
	if err != nil {
		return 0, fmt.Errorf("outbox: redrive: %w", err)
	}
	if tag.RowsAffected() > 0 {
		if _, err := q.Exec(ctx, o.q.notify, o.t.channel); err != nil {
			return 0, fmt.Errorf("outbox: redrive notify: %w", err)
		}
	}
	o.logger.Info("redrove dead letters", log.F("rows", tag.RowsAffected()))
	return tag.RowsAffected(), nil
}

// Purge deletes sent rows older than age and reports how many it removed.
// Pending and dead rows are never purged. Run it on a schedule; the relay
// does not delete anything by itself.
func (o *Outbox) Purge(ctx context.Context, q postgres.Querier, age time.Duration) (int64, error) {
	tag, err := q.Exec(ctx, o.q.purge, age.Milliseconds())
	if err != nil {
		return 0, fmt.Errorf("outbox: purge: %w", err)
	}
	o.logger.Debug("purged sent rows", log.F("rows", tag.RowsAffected()), log.F("older_than", age.String()))
	return tag.RowsAffected(), nil
}

var identPart = regexp.MustCompile(`^[a-z_][a-z0-9_]{0,49}$`)

type tableNames struct {
	full       string
	schema     string
	base       string
	table      string
	inbox      string
	migrations string
	channel    string
}

func parseTable(name string) (tableNames, error) {
	parts := strings.Split(name, ".")
	if len(parts) > 2 {
		return tableNames{}, fmt.Errorf("%w: %q", ErrInvalidTable, name)
	}
	for _, p := range parts {
		if !identPart.MatchString(p) {
			return tableNames{}, fmt.Errorf("%w: %q", ErrInvalidTable, name)
		}
	}
	t := tableNames{full: name, base: parts[len(parts)-1], channel: name}
	prefix := ""
	if len(parts) == 2 {
		t.schema = parts[0]
		prefix = quote(parts[0]) + "."
	}
	t.table = prefix + quote(t.base)
	t.inbox = prefix + quote(t.base+"_inbox")
	t.migrations = prefix + quote(t.base+"_migrations")
	return t, nil
}

func quote(ident string) string { return `"` + ident + `"` }
