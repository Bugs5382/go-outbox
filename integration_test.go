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
	"sync"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/internal/testenv"
	outboxrabbitmq "github.com/Bugs5382/go-outbox/rabbitmq"
	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"
)

func TestMain(m *testing.M) { testenv.Main(m, true) }

func newOutbox(t *testing.T, db *postgres.DB, opts ...outbox.Option) *outbox.Outbox {
	t.Helper()
	opts = append([]outbox.Option{outbox.WithTable(testenv.Name(t, "outbox"))}, opts...)
	ob, err := outbox.New(opts...)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ob.Migrate(testenv.Context(t), db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return ob
}

func enqueue(t *testing.T, db *postgres.DB, ob *outbox.Outbox, msgs ...outbox.Message) []string {
	t.Helper()
	ctx := testenv.Context(t)
	var keys []string
	err := db.RunInTx(ctx, func(tx pgx.Tx) error {
		keys = keys[:0]
		for _, m := range msgs {
			key, err := ob.Enqueue(ctx, tx, m)
			if err != nil {
				return err
			}
			keys = append(keys, key)
		}
		return nil
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	return keys
}

func busPublisher(t *testing.T, bus *testenv.Bus) *outboxrabbitmq.Publisher {
	t.Helper()
	p := outboxrabbitmq.New(bus.Conn, bus.Exchange)
	t.Cleanup(func() { _ = p.Close() })
	return p
}

// runRelay runs r until the test ends and checks that Run then returns nil.
func runRelay(t *testing.T, r *outbox.Relay) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()
	t.Cleanup(func() {
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run returned %v after shutdown", err)
			}
		case <-time.After(30 * time.Second):
			t.Errorf("Run did not return after shutdown")
		}
	})
}

func stats(t *testing.T, db *postgres.DB, ob *outbox.Outbox) outbox.Stats {
	t.Helper()
	s, err := ob.Stats(testenv.Context(t), db.Querier())
	if err != nil {
		t.Fatalf("Stats: %v", err)
	}
	return s
}

// recorder is a Publisher that keeps every record it was handed, in order.
type recorder struct {
	mu      sync.Mutex
	records []outbox.Record
	fail    func(outbox.Record) error
	delay   time.Duration
}

func (r *recorder) Publish(ctx context.Context, rec outbox.Record) error {
	if r.delay > 0 {
		select {
		case <-time.After(r.delay):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if r.fail != nil {
		if err := r.fail(rec); err != nil {
			return err
		}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.records = append(r.records, rec)
	return nil
}

func (r *recorder) snapshot() []outbox.Record {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]outbox.Record(nil), r.records...)
}

func (r *recorder) count() int { return len(r.snapshot()) }

// metricsRecorder counts the relay's metrics events.
type metricsRecorder struct {
	outbox.NopMetrics
	mu        sync.Mutex
	claimed   int
	published int
	failed    int
	dead      int
	released  int
}

func (m *metricsRecorder) Claimed(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.claimed += n
}

func (m *metricsRecorder) Published(string, int, time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.published++
}

func (m *metricsRecorder) Failed(string, int, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failed++
}

func (m *metricsRecorder) DeadLettered(string, int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.dead++
}

func (m *metricsRecorder) Released(n int) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.released += n
}

var errBrokerDown = errors.New("broker down")
