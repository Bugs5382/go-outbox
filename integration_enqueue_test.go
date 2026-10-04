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
	"errors"
	"regexp"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/internal/testenv"
	"github.com/jackc/pgx/v5"
)

func TestEnqueueGeneratesKeyWhenMissing(t *testing.T) {
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	keys := enqueue(t, db, ob, outbox.Message{Topic: "t"}, outbox.Message{Topic: "t", Key: "given"})

	uuid := regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
	if !uuid.MatchString(keys[0]) {
		t.Errorf("generated key %q is not a UUID", keys[0])
	}
	if keys[1] != "given" {
		t.Errorf("key = %q, want the caller's key", keys[1])
	}
}

func TestEnqueueDuplicateKeyKeepsTransactionUsable(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)

	err := db.RunInTx(ctx, func(tx pgx.Tx) error {
		if _, err := ob.Enqueue(ctx, tx, outbox.Message{Topic: "t", Key: "same"}); err != nil {
			return err
		}
		if _, err := ob.Enqueue(ctx, tx, outbox.Message{Topic: "t", Key: "same"}); !errors.Is(err, outbox.ErrDuplicateKey) {
			t.Errorf("second Enqueue = %v, want ErrDuplicateKey", err)
		}
		_, err := ob.Enqueue(ctx, tx, outbox.Message{Topic: "t", Key: "other"})
		return err
	})
	if err != nil {
		t.Fatalf("transaction after a duplicate key: %v", err)
	}
	if s := stats(t, db, ob); s.Pending != 2 {
		t.Errorf("pending = %d, want 2", s.Pending)
	}
}

func TestEnqueueRejectsMessageWithoutTopic(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	_, err := ob.Enqueue(ctx, db.Querier(), outbox.Message{Payload: []byte("x")})
	if !errors.Is(err, outbox.ErrInvalidMessage) {
		t.Errorf("Enqueue = %v, want ErrInvalidMessage", err)
	}
}

func TestMigrateIsIdempotentWithCustomTable(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	if _, err := db.Pool().Exec(ctx, `CREATE SCHEMA IF NOT EXISTS audit`); err != nil {
		t.Fatal(err)
	}

	for _, table := range []string{testenv.Name(t, "app_events"), "audit." + testenv.Name(t, "events")} {
		ob, err := outbox.New(outbox.WithTable(table))
		if err != nil {
			t.Fatalf("New(%q): %v", table, err)
		}
		if ob.Table() != table {
			t.Errorf("Table() = %q, want %q", ob.Table(), table)
		}
		for i := 0; i < 2; i++ {
			if err := ob.Migrate(ctx, db); err != nil {
				t.Fatalf("Migrate %q run %d: %v", table, i+1, err)
			}
		}
		var outboxTable, inboxTable *string
		err = db.Pool().QueryRow(ctx, `SELECT to_regclass($1)::text, to_regclass($1 || '_inbox')::text`, table).
			Scan(&outboxTable, &inboxTable)
		if err != nil || outboxTable == nil || inboxTable == nil {
			t.Errorf("tables for %q: outbox %v, inbox %v, err %v", table, outboxTable, inboxTable, err)
		}
		enqueue(t, db, ob, outbox.Message{Topic: "t"})
	}
}

func TestDedupeRollsBackWithTheConsumerTransaction(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)

	errHandler := errors.New("handler failed")
	err := db.RunInTx(ctx, func(tx pgx.Tx) error {
		if ok, err := ob.Dedupe(ctx, tx, "mailer", "k1"); err != nil || !ok {
			t.Errorf("first Dedupe = %v, %v; want true", ok, err)
		}
		return errHandler
	})
	if !errors.Is(err, errHandler) {
		t.Fatalf("RunInTx = %v", err)
	}
	for i, want := range []bool{true, false} {
		ok, err := ob.Dedupe(ctx, db.Querier(), "mailer", "k1")
		if err != nil || ok != want {
			t.Errorf("Dedupe %d = %v, %v; want %v", i+1, ok, err, want)
		}
	}
	if ok, _ := ob.Dedupe(ctx, db.Querier(), "auditor", "k1"); !ok {
		t.Error("a second consumer must see the key as fresh")
	}
}

func TestPurgeRemovesOldSentRows(t *testing.T) {
	ctx := testenv.Context(t)
	db := testenv.DB(t)
	ob := newOutbox(t, db)
	enqueue(t, db, ob, outbox.Message{Topic: "t"}, outbox.Message{Topic: "t"})

	relay := ob.NewRelay(db, &recorder{}, outbox.WithBatchSize(1))
	if n, err := relay.RunOnce(ctx); err != nil || n != 1 {
		t.Fatalf("RunOnce = %d, %v", n, err)
	}
	time.Sleep(50 * time.Millisecond)
	n, err := ob.Purge(ctx, db.Querier(), 10*time.Millisecond)
	if err != nil || n != 1 {
		t.Fatalf("Purge = %d, %v; want 1 sent row removed", n, err)
	}
	if s := stats(t, db, ob); s.Sent != 0 || s.Pending != 1 {
		t.Errorf("stats after purge = %+v, want the pending row kept", s)
	}
}
