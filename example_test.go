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
	"time"

	log "github.com/Bugs5382/go-log"
	outbox "github.com/Bugs5382/go-outbox"
	outboxrabbitmq "github.com/Bugs5382/go-outbox/rabbitmq"
	postgres "github.com/Bugs5382/go-postgres"
	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	"github.com/jackc/pgx/v5"
)

func Example() {
	ctx := context.Background()
	db, err := postgres.New(ctx, "postgres://app:secret@localhost:5432/app")
	if err != nil {
		return
	}
	defer db.Close()

	ob, err := outbox.New(outbox.WithTable("app_outbox"), outbox.WithLogger(log.NewLogger("orders")))
	if err != nil {
		return
	}
	if err := ob.Migrate(ctx, db); err != nil {
		return
	}

	// The order and its event commit together, or not at all.
	_ = db.RunInTx(ctx, func(tx pgx.Tx) error {
		if _, err := tx.Exec(ctx, `INSERT INTO orders (id) VALUES ($1)`, 42); err != nil {
			return err
		}
		_, err := ob.Enqueue(ctx, tx, outbox.Message{
			Topic:        "order.created",
			AggregateKey: "order-42",
			Payload:      []byte(`{"id":42}`),
			ContentType:  "application/json",
		})
		return err
	})

	conn, err := rabbitmq.Connect(ctx, "amqp://app:secret@localhost:5672/")
	if err != nil {
		return
	}
	defer func() { _ = conn.Close() }()
	pub := outboxrabbitmq.New(conn, "events")
	defer func() { _ = pub.Close() }()

	relay := ob.NewRelay(db, pub, outbox.WithMaxAttempts(8))
	runCtx, stop := context.WithTimeout(ctx, time.Minute)
	defer stop()
	_ = relay.Run(runCtx)
}

func ExampleOutbox_Dedupe() {
	ctx := context.Background()
	db, err := postgres.New(ctx, "postgres://app:secret@localhost:5432/app")
	if err != nil {
		return
	}
	defer db.Close()
	ob, _ := outbox.New()

	handle := func(ctx context.Context, d rabbitmq.Delivery) error {
		return db.RunInTx(ctx, func(tx pgx.Tx) error {
			fresh, err := ob.Dedupe(ctx, tx, "invoicing", d.MessageID)
			if err != nil || !fresh {
				return err
			}
			_, err = tx.Exec(ctx, `INSERT INTO invoices (order_id) VALUES ($1)`, string(d.Body))
			return err
		})
	}
	_ = handle
}

func ExamplePermanent() {
	pub := outbox.PublisherFunc(func(ctx context.Context, r outbox.Record) error {
		if len(r.Payload) == 0 {
			return outbox.Permanent(errors.New("empty payload"))
		}
		return nil
	})
	_ = pub
}
