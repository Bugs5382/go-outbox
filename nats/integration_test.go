//go:build integration

package nats_test

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
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/internal/testenv"
	outboxnats "github.com/Bugs5382/go-outbox/nats"
	postgres "github.com/Bugs5382/go-postgres"
	"github.com/jackc/pgx/v5"
	"github.com/nats-io/nats.go/jetstream"
)

func TestMain(m *testing.M) { testenv.Main(m, false) }

func newOutbox(t *testing.T, db *postgres.DB) *outbox.Outbox {
	t.Helper()
	ob, err := outbox.New(outbox.WithTable(testenv.Name(t, "outbox")))
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := ob.Migrate(testenv.Context(t), db); err != nil {
		t.Fatalf("Migrate: %v", err)
	}
	return ob
}

func enqueue(t *testing.T, db *postgres.DB, ob *outbox.Outbox, msgs ...outbox.Message) {
	t.Helper()
	ctx := testenv.Context(t)
	err := db.RunInTx(ctx, func(tx pgx.Tx) error {
		for _, m := range msgs {
			if _, err := ob.Enqueue(ctx, tx, m); err != nil {
				return err
			}
		}
		return nil
	})
	if err != nil {
		t.Fatalf("enqueue: %v", err)
	}
}

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

func streamMessages(t *testing.T, st jetstream.Stream, count int) []struct {
	Aggregate string
	N         int
} {
	t.Helper()
	ctx := testenv.Context(t)
	var out []struct {
		Aggregate string
		N         int
	}
	for seq := uint64(1); len(out) < count; seq++ {
		msg, err := st.GetMsg(ctx, seq)
		if err != nil {
			t.Fatalf("read sequence %d: %v", seq, err)
		}
		var body struct {
			Aggregate string
			N         int
		}
		if err := json.Unmarshal(msg.Data, &body); err != nil {
			t.Fatal(err)
		}
		out = append(out, body)
	}
	return out
}

func TestRelayPublishesToJetStreamInAggregateOrder(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	e := newEnv(t)
	st := e.stream(t, "FLEET", "sf.fleet.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."))

	const aggregates, perAggregate = 3, 20
	var msgs []outbox.Message
	for n := 1; n <= perAggregate; n++ {
		for a := 0; a < aggregates; a++ {
			agg := fmt.Sprintf("member:%d", a)
			body, _ := json.Marshal(map[string]any{"Aggregate": agg, "N": n})
			msgs = append(msgs, outbox.Message{Topic: "fleet.rank.changed", AggregateKey: agg, Payload: body, ContentType: "application/json"})
		}
	}
	for i := 0; i < len(msgs); i += 10 {
		end := min(i+10, len(msgs))
		enqueue(t, db, ob, msgs[i:end]...)
	}
	runRelay(t, ob.NewRelay(db, pub, outbox.WithPollInterval(50*time.Millisecond)))
	runRelay(t, ob.NewRelay(db, pub, outbox.WithPollInterval(50*time.Millisecond))) // a second relay: rows are claimed with SKIP LOCKED

	total := aggregates * perAggregate
	testenv.Eventually(t, 20*time.Second, "every row to be published", func() bool {
		info, err := st.Info(context.Background())
		return err == nil && int(info.State.Msgs) == total
	})
	last := map[string]int{}
	for _, m := range streamMessages(t, st, total) {
		if m.N != last[m.Aggregate]+1 {
			t.Errorf("%s: message %d came after %d: order broken", m.Aggregate, m.N, last[m.Aggregate])
		}
		last[m.Aggregate] = m.N
	}
}

// lostAck publishes the first attempt of every message and then reports a failure, as if the
// acknowledgement had been lost on the way back.
type lostAck struct {
	inner outbox.Publisher
	mu    sync.Mutex
	seen  map[string]bool
}

func (l *lostAck) Publish(ctx context.Context, r outbox.Record) error {
	if err := l.inner.Publish(ctx, r); err != nil {
		return err
	}
	l.mu.Lock()
	first := !l.seen[r.Key]
	l.seen[r.Key] = true
	l.mu.Unlock()
	if first {
		return errors.New("simulated lost acknowledgement")
	}
	return nil
}

func TestRetryAfterALostAckIsDroppedByTheDuplicateWindow(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	e := newEnv(t)
	st := e.stream(t, "OPS", "sf.ops.>")
	pub := &lostAck{inner: outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf.")), seen: map[string]bool{}}

	enqueue(t, db, ob, outbox.Message{Topic: "ops.notice.posted", Payload: []byte(`{"Aggregate":"n","N":1}`)})
	runRelay(t, ob.NewRelay(db, pub, outbox.WithPollInterval(50*time.Millisecond), outbox.WithBackoff(50*time.Millisecond, 100*time.Millisecond)))

	testenv.Eventually(t, 20*time.Second, "the row to be sent after the retry", func() bool {
		s, err := ob.Stats(context.Background(), db.Querier())
		return err == nil && s.Sent == 1
	})
	info, err := st.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Errorf("messages in the stream = %d, want 1: the retry carried the same key and must be dropped", info.State.Msgs)
	}
}
