//go:build integration

package outbox_test

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
	"sync"
	"sync/atomic"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/internal/testenv"
	"github.com/jackc/pgx/v5"
)

func TestCommittedTransactionPublishes(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	bus := testenv.NewBus(t)

	keys := enqueue(t, db, ob, outbox.Message{
		Topic:       "order.created",
		Payload:     []byte(`{"order":1}`),
		ContentType: "application/json",
	})
	runRelay(t, ob.NewRelay(db, busPublisher(t, bus)))

	d := bus.Next(t, 10*time.Second)
	if d.MessageID != keys[0] {
		t.Errorf("message id = %q, want the idempotency key %q", d.MessageID, keys[0])
	}
	if d.RoutingKey != "order.created" || string(d.Body) != `{"order":1}` || d.ContentType != "application/json" {
		t.Errorf("delivery = %q %q %q", d.RoutingKey, d.Body, d.ContentType)
	}
	testenv.Eventually(t, 5*time.Second, "the row to be marked sent", func() bool {
		s := stats(t, db, ob)
		return s.Sent == 1 && s.Pending == 0
	})
}

func TestRolledBackTransactionNeverPublishes(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	bus := testenv.NewBus(t)

	errAbort := errors.New("business rule failed")
	err := db.RunInTx(ctx, func(tx pgx.Tx) error {
		if _, err := ob.Enqueue(ctx, tx, outbox.Message{Topic: "order.created", Key: "rolled-back"}); err != nil {
			return err
		}
		return errAbort
	})
	if !errors.Is(err, errAbort) {
		t.Fatalf("RunInTx = %v, want the abort error", err)
	}
	marker := enqueue(t, db, ob, outbox.Message{Topic: "order.created", Key: "committed"})

	runRelay(t, ob.NewRelay(db, busPublisher(t, bus), outbox.WithPollInterval(50*time.Millisecond)))

	if d := bus.Next(t, 10*time.Second); d.MessageID != marker[0] {
		t.Fatalf("first delivery = %q, want the committed message", d.MessageID)
	}
	bus.None(t, time.Second)
	if s := stats(t, db, ob); s.Pending+s.InFlight+s.Sent+s.Dead != 1 {
		t.Errorf("stats = %+v, want only the committed row", s)
	}
}

func TestCrashBetweenPublishAndMarkRedeliversSameKey(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	bus := testenv.NewBus(t)
	real := busPublisher(t, bus)
	keys := enqueue(t, db, ob, outbox.Message{Topic: "invoice.issued", Payload: []byte("inv-1")})

	published := make(chan struct{})
	hang := make(chan struct{})
	var once sync.Once
	crashing := outbox.PublisherFunc(func(ctx context.Context, r outbox.Record) error {
		if err := real.Publish(ctx, r); err != nil {
			return err
		}
		once.Do(func() { close(published) })
		<-hang
		return nil
	})
	runRelay(t, ob.NewRelay(db, crashing, outbox.WithLease(time.Second), outbox.WithoutNotify()))
	t.Cleanup(func() { close(hang) })

	<-published
	first := bus.Next(t, 10*time.Second)

	runRelay(t, ob.NewRelay(db, real, outbox.WithLease(time.Second), outbox.WithPollInterval(100*time.Millisecond)))
	second := bus.Next(t, 15*time.Second)

	if first.MessageID != keys[0] || second.MessageID != keys[0] {
		t.Fatalf("message ids = %q, %q; want both %q", first.MessageID, second.MessageID, keys[0])
	}

	var fresh []bool
	for _, d := range []string{first.MessageID, second.MessageID} {
		err := db.RunInTx(ctx, func(tx pgx.Tx) error {
			ok, err := ob.Dedupe(ctx, tx, "billing", d)
			fresh = append(fresh, ok)
			return err
		})
		if err != nil {
			t.Fatalf("Dedupe: %v", err)
		}
	}
	if !fresh[0] || fresh[1] {
		t.Errorf("Dedupe = %v, want the first delivery fresh and the redelivery a duplicate", fresh)
	}
	testenv.Eventually(t, 5*time.Second, "the row to be marked sent", func() bool {
		return stats(t, db, ob).Sent == 1
	})
}

func TestConcurrentRelaysNeverDoubleClaim(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)

	const total = 400
	var want []string
	for batch := 0; batch < total/50; batch++ {
		var msgs []outbox.Message
		for i := 0; i < 50; i++ {
			msgs = append(msgs, outbox.Message{Topic: "load", Payload: []byte(fmt.Sprint(batch*50 + i))})
		}
		want = append(want, enqueue(t, db, ob, msgs...)...)
	}

	pub := &recorder{delay: time.Millisecond}
	for i := 0; i < 4; i++ {
		runRelay(t, ob.NewRelay(db, pub,
			outbox.WithBatchSize(25),
			outbox.WithConcurrency(4),
			outbox.WithLease(time.Minute),
			outbox.WithPollInterval(20*time.Millisecond),
		))
	}

	testenv.Eventually(t, 60*time.Second, "every row to be sent", func() bool {
		return stats(t, db, ob).Sent == total
	})
	seen := map[string]int{}
	for _, r := range pub.snapshot() {
		seen[r.Key]++
	}
	for _, k := range want {
		if seen[k] != 1 {
			t.Errorf("key %s published %d times, want exactly once", k, seen[k])
		}
	}
	if len(seen) != total {
		t.Errorf("published %d distinct keys, want %d", len(seen), total)
	}
}

func TestRetriesEndInDeadLetter(t *testing.T) {
	db := testenv.DB(t)
	m := &metricsRecorder{}
	ob := newOutbox(t, db, outbox.WithMetrics(m))
	keys := enqueue(t, db, ob, outbox.Message{Topic: "mail.send"})

	var attempts []int
	var mu sync.Mutex
	failing := outbox.PublisherFunc(func(_ context.Context, r outbox.Record) error {
		mu.Lock()
		defer mu.Unlock()
		attempts = append(attempts, r.Attempt)
		return errBrokerDown
	})
	runRelay(t, ob.NewRelay(db, failing,
		outbox.WithMaxAttempts(3),
		outbox.WithBackoff(10*time.Millisecond, 20*time.Millisecond),
		outbox.WithPollInterval(20*time.Millisecond),
	))

	testenv.Eventually(t, 10*time.Second, "the row to be dead-lettered", func() bool {
		return stats(t, db, ob).Dead == 1
	})
	time.Sleep(200 * time.Millisecond)
	mu.Lock()
	got := append([]int(nil), attempts...)
	mu.Unlock()
	if fmt.Sprint(got) != "[1 2 3]" {
		t.Errorf("attempts = %v, want [1 2 3]", got)
	}
	m.mu.Lock()
	if m.failed != 3 || m.dead != 1 || m.published != 0 {
		t.Errorf("metrics failed=%d dead=%d published=%d, want 3, 1, 0", m.failed, m.dead, m.published)
	}
	m.mu.Unlock()

	n, err := ob.Redrive(testenv.Context(t), db.Querier(), keys[0])
	if err != nil || n != 1 {
		t.Fatalf("Redrive = %d, %v; want 1, nil", n, err)
	}
	if s := stats(t, db, ob); s.Dead != 0 || s.Pending+s.InFlight != 1 {
		t.Errorf("after redrive stats = %+v, want the row pending again", s)
	}
}

func TestPermanentErrorDeadLettersAtOnce(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	enqueue(t, db, ob, outbox.Message{Topic: "mail.send"})

	var calls atomic.Int32
	pub := outbox.PublisherFunc(func(context.Context, outbox.Record) error {
		calls.Add(1)
		return outbox.Permanent(errors.New("payload rejected"))
	})
	runRelay(t, ob.NewRelay(db, pub, outbox.WithMaxAttempts(10), outbox.WithPollInterval(20*time.Millisecond)))

	testenv.Eventually(t, 10*time.Second, "the row to be dead-lettered", func() bool {
		return stats(t, db, ob).Dead == 1
	})
	if calls.Load() != 1 {
		t.Errorf("publisher called %d times, want 1", calls.Load())
	}
}

func TestShutdownDrainsCleanly(t *testing.T) {
	db := testenv.DB(t)
	m := &metricsRecorder{}
	ob := newOutbox(t, db, outbox.WithMetrics(m))

	const total = 30
	var msgs []outbox.Message
	for i := 0; i < total; i++ {
		msgs = append(msgs, outbox.Message{Topic: "report.ready", Payload: []byte(fmt.Sprint(i))})
	}
	enqueue(t, db, ob, msgs...)

	started := make(chan struct{}, total)
	slow := &recorder{delay: 200 * time.Millisecond}
	pub := outbox.PublisherFunc(func(ctx context.Context, r outbox.Record) error {
		started <- struct{}{}
		return slow.Publish(ctx, r)
	})
	relay := ob.NewRelay(db, pub,
		outbox.WithBatchSize(10),
		outbox.WithConcurrency(2),
		outbox.WithLease(time.Minute),
		outbox.WithDrainTimeout(5*time.Second),
	)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- relay.Run(ctx) }()
	<-started
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("Run = %v, want nil on shutdown", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after shutdown")
	}

	s := stats(t, db, ob)
	if s.InFlight != 0 {
		t.Errorf("%d rows still leased after shutdown, want 0", s.InFlight)
	}
	if int(s.Sent) != slow.count() {
		t.Errorf("sent = %d but %d publishes completed: every finished publish must be recorded", s.Sent, slow.count())
	}
	if s.Sent == 0 || s.Sent == total || s.Sent+s.Pending != total {
		t.Errorf("stats = %+v, want shutdown to stop part way with nothing lost", s)
	}
	m.mu.Lock()
	if m.released == 0 {
		t.Errorf("no claims released on shutdown")
	}
	m.mu.Unlock()

	fast := &recorder{}
	runRelay(t, ob.NewRelay(db, fast))
	testenv.Eventually(t, 10*time.Second, "the rest to be sent", func() bool {
		return stats(t, db, ob).Sent == total
	})
	seen := map[string]int{}
	for _, r := range append(slow.snapshot(), fast.snapshot()...) {
		seen[r.Key]++
	}
	for k, n := range seen {
		if n != 1 {
			t.Errorf("key %s published %d times", k, n)
		}
	}
	for _, r := range fast.snapshot() {
		if r.Attempt != 1 {
			t.Errorf("released row %s came back as attempt %d, want 1", r.Key, r.Attempt)
		}
	}
}

func TestOrderingPerAggregateKey(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)

	var msgs []outbox.Message
	for i := 0; i < 5; i++ {
		for _, agg := range []string{"a", "b", "c"} {
			msgs = append(msgs, outbox.Message{Topic: "seq", AggregateKey: agg, Payload: []byte(fmt.Sprint(i))})
		}
	}
	enqueue(t, db, ob, msgs...)

	var failedOnce atomic.Bool
	pub := &recorder{fail: func(r outbox.Record) error {
		if r.AggregateKey == "a" && string(r.Payload) == "0" && failedOnce.CompareAndSwap(false, true) {
			return errBrokerDown
		}
		return nil
	}}
	runRelay(t, ob.NewRelay(db, pub,
		outbox.WithConcurrency(8),
		outbox.WithBatchSize(50),
		outbox.WithBackoff(50*time.Millisecond, 50*time.Millisecond),
		outbox.WithPollInterval(20*time.Millisecond),
	))

	testenv.Eventually(t, 15*time.Second, "every row to be sent", func() bool {
		return stats(t, db, ob).Sent == int64(len(msgs))
	})
	order := map[string]string{}
	for _, r := range pub.snapshot() {
		order[r.AggregateKey] += string(r.Payload)
	}
	for _, agg := range []string{"a", "b", "c"} {
		if order[agg] != "01234" {
			t.Errorf("aggregate %s published in order %q, want 01234", agg, order[agg])
		}
	}
}

func TestNotifyWakesIdleRelay(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	pub := &recorder{}
	relay := ob.NewRelay(db, pub, outbox.WithPollInterval(time.Hour))
	runRelay(t, relay)

	testenv.Eventually(t, 10*time.Second, "the relay to listen", relay.Listening)
	enqueue(t, db, ob, outbox.Message{Topic: "wake"})
	testenv.Eventually(t, 5*time.Second, "the notification to wake the relay", func() bool {
		return pub.count() == 1
	})
}

func TestPollingFallbackWithoutNotify(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	pub := &recorder{}
	relay := ob.NewRelay(db, pub, outbox.WithoutNotify(), outbox.WithPollInterval(100*time.Millisecond))
	runRelay(t, relay)

	time.Sleep(200 * time.Millisecond)
	enqueue(t, db, ob, outbox.Message{Topic: "poll"})
	testenv.Eventually(t, 5*time.Second, "polling to pick the row up", func() bool {
		return pub.count() == 1
	})
	if relay.Listening() {
		t.Error("Listening() = true with notifications turned off")
	}
}

func TestDelayedMessageWaits(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	pub := &recorder{}
	relay := ob.NewRelay(db, pub)
	enqueue(t, db, ob, outbox.Message{Topic: "later", Delay: time.Second})

	if n, err := relay.RunOnce(ctx); err != nil || n != 0 {
		t.Fatalf("RunOnce before the delay = %d, %v; want 0, nil", n, err)
	}
	time.Sleep(1200 * time.Millisecond)
	if n, err := relay.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("RunOnce after the delay = %d, %v; want 1, nil", n, err)
	}
	if pub.count() != 1 {
		t.Errorf("published %d, want 1", pub.count())
	}
}
