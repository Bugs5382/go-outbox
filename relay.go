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
	"fmt"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	log "github.com/Bugs5382/go-log"
	postgres "github.com/Bugs5382/go-postgres"
)

const (
	defaultBatchSize    = 100
	defaultConcurrency  = 4
	defaultLease        = 30 * time.Second
	defaultPollInterval = 2 * time.Second
	defaultMaxAttempts  = 10
	defaultMinBackoff   = time.Second
	defaultMaxBackoff   = 5 * time.Minute
	defaultDrainTimeout = 10 * time.Second
	bookkeepingTimeout  = 5 * time.Second
	maxErrorText        = 2000
)

// Relay moves committed messages from the outbox to a Publisher. Each cycle
// it claims a batch of due rows under a lease (FOR UPDATE SKIP LOCKED, so any
// number of relays can run against one table without claiming the same row),
// publishes them, and marks each one sent, scheduled for a retry, or dead.
//
// Delivery is at least once: a relay that stops between publishing a row and
// marking it leaves the row leased, and once the lease runs out another
// relay publishes it again with the same idempotency key.
type Relay struct {
	ob        *Outbox
	db        *postgres.DB
	pub       Publisher
	cfg       relayConfig
	logger    log.Logger
	listening atomic.Bool
}

// RelayOption configures a Relay.
type RelayOption func(*relayConfig)

type relayConfig struct {
	batchSize    int
	concurrency  int
	lease        time.Duration
	pollInterval time.Duration
	maxAttempts  int
	minBackoff   time.Duration
	maxBackoff   time.Duration
	drainTimeout time.Duration
	notify       bool
}

// WithBatchSize sets how many rows one cycle claims (default 100).
func WithBatchSize(n int) RelayOption {
	return func(c *relayConfig) {
		if n > 0 {
			c.batchSize = n
		}
	}
}

// WithConcurrency sets how many claimed rows are published at the same time
// (default 4). Rows of one aggregate are never in flight together, whatever
// the setting.
func WithConcurrency(n int) RelayOption {
	return func(c *relayConfig) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// WithLease sets how long a claim holds its rows (default 30s). It must
// comfortably exceed the time to publish one batch: a lease that runs out
// mid-publish lets another relay send the same rows again. After a crash, the
// lease is also how long the crashed relay's rows wait to be reclaimed.
func WithLease(d time.Duration) RelayOption {
	return func(c *relayConfig) {
		if d > 0 {
			c.lease = d
		}
	}
}

// WithPollInterval sets how often an idle relay looks for due rows (default
// 2s). Notifications wake it sooner; polling is what finds retries whose
// backoff has passed, delayed messages, expired leases, and rows committed
// while the notification connection was down.
func WithPollInterval(d time.Duration) RelayOption {
	return func(c *relayConfig) {
		if d > 0 {
			c.pollInterval = d
		}
	}
}

// WithMaxAttempts sets how many publish attempts a row gets before it is
// dead-lettered (default 10).
func WithMaxAttempts(n int) RelayOption {
	return func(c *relayConfig) {
		if n > 0 {
			c.maxAttempts = n
		}
	}
}

// WithBackoff sets the retry delay range (default 1s to 5m). The delay
// doubles per attempt from minDelay, is capped at maxDelay, and is jittered
// into the upper half of that value.
func WithBackoff(minDelay, maxDelay time.Duration) RelayOption {
	return func(c *relayConfig) {
		if minDelay > 0 && maxDelay >= minDelay {
			c.minBackoff, c.maxBackoff = minDelay, maxDelay
		}
	}
}

// WithDrainTimeout bounds how long Run waits, after its context ends, for
// in-flight publishes to finish (default 10s).
func WithDrainTimeout(d time.Duration) RelayOption {
	return func(c *relayConfig) {
		if d > 0 {
			c.drainTimeout = d
		}
	}
}

// WithoutNotify turns off the LISTEN/NOTIFY wake-up, leaving polling only.
// Use it behind a connection pooler in transaction mode, which cannot hold
// a LISTEN.
func WithoutNotify() RelayOption {
	return func(c *relayConfig) { c.notify = false }
}

// NewRelay builds a relay that drains this outbox through db and sends
// through pub. It does nothing until Run or RunOnce.
func (o *Outbox) NewRelay(db *postgres.DB, pub Publisher, opts ...RelayOption) *Relay {
	cfg := relayConfig{
		batchSize:    defaultBatchSize,
		concurrency:  defaultConcurrency,
		lease:        defaultLease,
		pollInterval: defaultPollInterval,
		maxAttempts:  defaultMaxAttempts,
		minBackoff:   defaultMinBackoff,
		maxBackoff:   defaultMaxBackoff,
		drainTimeout: defaultDrainTimeout,
		notify:       true,
	}
	for _, opt := range opts {
		opt(&cfg)
	}
	return &Relay{ob: o, db: db, pub: pub, cfg: cfg, logger: o.logger.With(log.F("component", "relay"))}
}

// Listening reports whether the relay currently holds its LISTEN
// subscription. It is false with WithoutNotify, before Run starts, and while
// the notification connection is being re-established.
func (r *Relay) Listening() bool { return r.listening.Load() }

// Run drains the outbox until ctx ends, then shuts down gracefully: it stops
// claiming, lets publishes already under way finish (up to the drain
// timeout) and records their outcome, and releases claimed rows it had not
// started so another relay can take them at once. It returns nil after a
// shutdown. Errors along the way are logged and retried, never returned.
func (r *Relay) Run(ctx context.Context) error {
	r.logger.Info("relay started",
		log.F("batch_size", r.cfg.batchSize), log.F("concurrency", r.cfg.concurrency),
		log.F("lease", r.cfg.lease.String()), log.F("poll_interval", r.cfg.pollInterval.String()),
		log.F("max_attempts", r.cfg.maxAttempts), log.F("notify", r.cfg.notify))

	wake := make(chan struct{}, 1)
	var listener sync.WaitGroup
	if r.cfg.notify {
		listener.Add(1)
		ready := make(chan struct{})
		go func() {
			defer listener.Done()
			r.listen(ctx, wake, ready)
		}()
		select {
		case <-ready:
		case <-ctx.Done():
		}
	}
	defer listener.Wait()

	failures := 0
	for {
		n, err := r.RunOnce(ctx)
		if ctx.Err() != nil {
			r.logger.Info("relay stopped")
			return nil
		}
		wait := r.cfg.pollInterval
		switch {
		case err != nil:
			failures++
			wait = min(backoff(failures, r.cfg.minBackoff, r.cfg.maxBackoff), r.cfg.pollInterval)
			r.logger.Warn("relay cycle failed", log.F("error", err.Error()), log.F("retry_in", wait.String()))
		case n == r.cfg.batchSize:
			failures = 0
			continue
		default:
			failures = 0
		}
		timer := time.NewTimer(wait)
		select {
		case <-ctx.Done():
			timer.Stop()
			r.logger.Info("relay stopped")
			return nil
		case <-wake:
			log.Trace(r.logger, "woken by notification")
		case <-timer.C:
		}
		timer.Stop()
	}
}

// RunOnce runs a single cycle: it claims up to one batch of due rows,
// publishes them and records each outcome. It returns how many rows it
// claimed. Run calls it in a loop; call it yourself to drive the relay from
// a scheduler or a test. When ctx ends part way, rows not yet started are
// released and rows in flight finish within the drain timeout.
func (r *Relay) RunOnce(ctx context.Context) (int, error) {
	start := time.Now()
	records, err := r.claim(ctx)
	if err != nil {
		return 0, err
	}
	if len(records) == 0 {
		log.Trace(r.logger, "nothing due")
		return 0, nil
	}
	r.ob.metrics.Claimed(len(records))
	r.logger.Debug("claimed", log.F("rows", len(records)))

	work, cancelWork := r.workContext(ctx)
	defer cancelWork()

	sem := make(chan struct{}, r.cfg.concurrency)
	var wg sync.WaitGroup
	var unstarted []Record
	for i, rec := range records {
		acquired := false
		select {
		case sem <- struct{}{}:
			acquired = true
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			if acquired {
				<-sem
			}
			unstarted = records[i:]
			break
		}
		wg.Add(1)
		go func(rec Record) {
			defer wg.Done()
			defer func() { <-sem }()
			r.deliver(work, rec)
		}(rec)
	}
	wg.Wait()
	if len(unstarted) > 0 {
		r.release(ctx, unstarted)
	}
	r.logger.Debug("cycle done", log.F("claimed", len(records)), log.F("released", len(unstarted)),
		log.F("took", time.Since(start).String()))
	return len(records), nil
}

// workContext keeps in-flight publishes alive past ctx for the drain
// timeout, so a shutdown does not cut a publish off half way.
func (r *Relay) workContext(ctx context.Context) (context.Context, context.CancelFunc) {
	work, cancel := context.WithCancel(context.WithoutCancel(ctx))
	stop := context.AfterFunc(ctx, func() {
		t := time.AfterFunc(r.cfg.drainTimeout, cancel)
		context.AfterFunc(work, func() { t.Stop() })
	})
	return work, func() {
		stop()
		cancel()
	}
}

func (r *Relay) claim(ctx context.Context) ([]Record, error) {
	rows, err := r.db.Pool().Query(ctx, r.ob.q.claim, r.cfg.batchSize, r.cfg.lease.Milliseconds())
	if err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	defer rows.Close()
	var out []Record
	for rows.Next() {
		var rec Record
		if err := rows.Scan(&rec.ID, &rec.Key, &rec.Topic, &rec.AggregateKey, &rec.Payload,
			&rec.ContentType, &rec.Headers, &rec.Attempt, &rec.CreatedAt); err != nil {
			return nil, fmt.Errorf("outbox: claim scan: %w", err)
		}
		out = append(out, rec)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("outbox: claim: %w", err)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out, nil
}

func (r *Relay) deliver(ctx context.Context, rec Record) {
	l := r.logger.With(log.F("key", rec.Key), log.F("topic", rec.Topic), log.F("attempt", rec.Attempt))
	log.Trace(l, "publishing")
	start := time.Now()
	err := r.pub.Publish(ctx, rec)
	took := time.Since(start)

	book, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer cancel()

	if err == nil {
		r.ob.metrics.Published(rec.Topic, rec.Attempt, took)
		if _, err := r.db.Pool().Exec(book, r.ob.q.markSent, rec.ID); err != nil {
			// Published but not recorded: the lease runs out and the row is
			// sent again, which consumers deduplicate by key.
			l.Error(err, "published but could not mark the row sent; it will be redelivered")
			return
		}
		l.Debug("published", log.F("took", took.String()))
		return
	}

	r.ob.metrics.Failed(rec.Topic, rec.Attempt, err)
	reason := truncate(err.Error())
	if IsPermanent(err) || rec.Attempt >= r.cfg.maxAttempts {
		if _, uerr := r.db.Pool().Exec(book, r.ob.q.dead, rec.ID, rec.Attempt, reason); uerr != nil {
			l.Error(uerr, "could not dead-letter the row", log.F("cause", reason))
			return
		}
		r.ob.metrics.DeadLettered(rec.Topic, rec.Attempt)
		l.Error(err, "dead-lettered", log.F("permanent", IsPermanent(err)))
		return
	}
	wait := backoff(rec.Attempt, r.cfg.minBackoff, r.cfg.maxBackoff)
	if _, uerr := r.db.Pool().Exec(book, r.ob.q.retry, rec.ID, rec.Attempt, wait.Milliseconds(), reason); uerr != nil {
		l.Error(uerr, "could not schedule the retry; the lease will expire instead", log.F("cause", reason))
		return
	}
	l.Warn("publish failed, will retry", log.F("error", reason), log.F("retry_in", wait.String()))
}

func (r *Relay) release(ctx context.Context, recs []Record) {
	ids := make([]int64, len(recs))
	attempts := make([]int, len(recs))
	for i, rec := range recs {
		ids[i], attempts[i] = rec.ID, rec.Attempt
	}
	book, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
	defer cancel()
	tag, err := r.db.Pool().Exec(book, r.ob.q.release, ids, attempts)
	if err != nil {
		r.logger.Warn("could not release unstarted claims; their lease will expire instead",
			log.F("rows", len(recs)), log.F("error", err.Error()))
		return
	}
	r.ob.metrics.Released(int(tag.RowsAffected()))
	r.logger.Info("released unstarted claims on shutdown", log.F("rows", tag.RowsAffected()))
}

// listen holds a LISTEN on the outbox channel and turns each notification
// into a wake-up. It reconnects with backoff until ctx ends; polling covers
// any gap. ready is closed after the first attempt, so Run's first cycle
// starts with the subscription in place whenever it can be.
func (r *Relay) listen(ctx context.Context, wake chan<- struct{}, ready chan struct{}) {
	var once sync.Once
	signalReady := func() { once.Do(func() { close(ready) }) }
	defer signalReady()

	failures := 0
	for ctx.Err() == nil {
		err := r.listenOnce(ctx, wake, signalReady)
		r.listening.Store(false)
		signalReady()
		if ctx.Err() != nil {
			return
		}
		failures++
		wait := backoff(failures, r.cfg.minBackoff, max(r.cfg.pollInterval, r.cfg.minBackoff))
		r.logger.Warn("notification listener down; polling until it is back",
			log.F("error", errString(err)), log.F("retry_in", wait.String()))
		select {
		case <-ctx.Done():
			return
		case <-time.After(wait):
		}
	}
}

func (r *Relay) listenOnce(ctx context.Context, wake chan<- struct{}, ready func()) error {
	pc, err := r.db.Pool().Acquire(ctx)
	if err != nil {
		return err
	}
	// The connection leaves the pool for good: a LISTEN must not leak into
	// other users of the pool.
	conn := pc.Hijack()
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.WithoutCancel(ctx), bookkeepingTimeout)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()
	if _, err := conn.Exec(ctx, "LISTEN "+quote(r.ob.t.channel)); err != nil {
		return err
	}
	r.listening.Store(true)
	r.logger.Debug("listening for notifications", log.F("channel", r.ob.t.channel))
	ready()
	for {
		if _, err := conn.WaitForNotification(ctx); err != nil {
			if ctx.Err() != nil {
				return nil
			}
			return err
		}
		select {
		case wake <- struct{}{}:
		default:
		}
	}
}

func truncate(s string) string {
	if len(s) > maxErrorText {
		return s[:maxErrorText]
	}
	return s
}

func errString(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
