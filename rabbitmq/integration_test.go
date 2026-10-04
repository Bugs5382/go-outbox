//go:build integration

package rabbitmq_test

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
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/internal/testenv"
	outboxrabbitmq "github.com/Bugs5382/go-outbox/rabbitmq"
	gorabbitmq "github.com/Bugs5382/go-rabbitmq"
)

func TestMain(m *testing.M) { testenv.Main(m, true) }

func TestPublishCarriesTheRecord(t *testing.T) {
	bus := testenv.NewBus(t)
	pub := outboxrabbitmq.New(bus.Conn, bus.Exchange)
	defer func() { _ = pub.Close() }()

	err := pub.Publish(context.Background(), outbox.Record{
		Message: outbox.Message{
			Key:          "key-1",
			Topic:        "user.created",
			AggregateKey: "user-7",
			Payload:      []byte(`{"id":7}`),
			ContentType:  "application/json",
			Headers:      map[string]string{"tenant": "example"},
		},
		ID:        42,
		Attempt:   2,
		CreatedAt: time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC),
	})
	if err != nil {
		t.Fatalf("Publish: %v", err)
	}

	d := bus.Next(t, 10*time.Second)
	if d.MessageID != "key-1" || d.RoutingKey != "user.created" || string(d.Body) != `{"id":7}` || d.ContentType != "application/json" {
		t.Errorf("delivery = id %q, key %q, body %q, type %q", d.MessageID, d.RoutingKey, d.Body, d.ContentType)
	}
	if !d.Timestamp.Equal(time.Date(2026, 1, 2, 3, 4, 5, 0, time.UTC)) {
		t.Errorf("timestamp = %s, want the enqueue time", d.Timestamp)
	}
	for k, want := range map[string]any{
		"tenant":                          "example",
		outboxrabbitmq.HeaderAggregateKey: "user-7",
		outboxrabbitmq.HeaderAttempt:      int64(2),
	} {
		if got := d.Headers[k]; got != want {
			t.Errorf("header %s = %#v, want %#v", k, got, want)
		}
	}
}

func TestPublishUnroutableWithMandatory(t *testing.T) {
	bus := testenv.NewBus(t)
	empty := testenv.Name(t, "empty")
	if err := bus.Conn.DeclareExchange(context.Background(), gorabbitmq.ExchangeConfig{Name: empty}); err != nil {
		t.Fatal(err)
	}
	pub := outboxrabbitmq.New(bus.Conn, empty, gorabbitmq.WithMandatory())
	defer func() { _ = pub.Close() }()

	err := pub.Publish(context.Background(), outbox.Record{Message: outbox.Message{Key: "k", Topic: "nobody.listens"}})
	if !errors.Is(err, gorabbitmq.ErrUnroutable) {
		t.Errorf("Publish = %v, want ErrUnroutable", err)
	}
}
