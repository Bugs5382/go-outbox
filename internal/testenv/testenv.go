//go:build integration

// Package testenv starts the real PostgreSQL and RabbitMQ servers the
// integration tests run against, through testcontainers, and gives each test
// its own database pool and its own exchange and queue.
package testenv

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
	"os"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	postgres "github.com/Bugs5382/go-postgres"
	rabbitmq "github.com/Bugs5382/go-rabbitmq"
	"github.com/testcontainers/testcontainers-go"
	tcpostgres "github.com/testcontainers/testcontainers-go/modules/postgres"
	tcrabbitmq "github.com/testcontainers/testcontainers-go/modules/rabbitmq"
)

const (
	postgresImage = "postgres:17-alpine"
	rabbitmqImage = "rabbitmq:4-management-alpine"
)

var (
	postgresDSN string
	rabbitmqURL string
	seq         atomic.Int64
)

// Main starts the containers, runs the package's tests and stops the
// containers again. Call it from TestMain.
func Main(m *testing.M, withRabbitMQ bool) {
	os.Exit(run(m, withRabbitMQ))
}

func run(m *testing.M, withRabbitMQ bool) int {
	ctx := context.Background()

	pg, err := tcpostgres.Run(ctx, postgresImage,
		tcpostgres.WithDatabase("outbox"),
		tcpostgres.WithUsername("outbox"),
		tcpostgres.WithPassword("outbox"),
		tcpostgres.BasicWaitStrategies(),
	)
	defer terminate(pg)
	if err != nil {
		fmt.Fprintln(os.Stderr, "start postgres:", err)
		return 1
	}
	if postgresDSN, err = pg.ConnectionString(ctx, "sslmode=disable"); err != nil {
		fmt.Fprintln(os.Stderr, "postgres dsn:", err)
		return 1
	}

	if withRabbitMQ {
		mq, err := tcrabbitmq.Run(ctx, rabbitmqImage,
			tcrabbitmq.WithAdminUsername("outbox"),
			tcrabbitmq.WithAdminPassword("outbox"),
		)
		defer terminate(mq)
		if err != nil {
			fmt.Fprintln(os.Stderr, "start rabbitmq:", err)
			return 1
		}
		if rabbitmqURL, err = mq.AmqpURL(ctx); err != nil {
			fmt.Fprintln(os.Stderr, "rabbitmq url:", err)
			return 1
		}
	}

	return m.Run()
}

func terminate(c testcontainers.Container) {
	if err := testcontainers.TerminateContainer(c); err != nil {
		fmt.Fprintln(os.Stderr, "stop container:", err)
	}
}

// Context returns a context that ends with the test, bounded by a generous
// timeout so a hung test fails instead of stalling the run.
func Context(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	t.Cleanup(cancel)
	return ctx
}

// DB returns a pool on the shared PostgreSQL server, closed with the test.
func DB(t *testing.T) *postgres.DB {
	t.Helper()
	db, err := postgres.New(context.Background(), postgresDSN, postgres.WithMaxConns(20))
	if err != nil {
		t.Fatalf("connect postgres: %v", err)
	}
	t.Cleanup(db.Close)
	return db
}

// Name returns an identifier unique to this test run, safe as a table,
// exchange or queue name.
func Name(t *testing.T, prefix string) string {
	t.Helper()
	return fmt.Sprintf("%s_%d_%d", prefix, time.Now().UnixNano()%1_000_000, seq.Add(1))
}

// Bus is a RabbitMQ exchange with one queue bound to every routing key, and
// a consumer that collects what arrives.
type Bus struct {
	Conn     *rabbitmq.Conn
	Exchange string
	got      chan rabbitmq.Delivery
}

// NewBus declares a fresh topic exchange and a queue bound to it, and waits
// until the consumer is receiving.
func NewBus(t *testing.T) *Bus {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())

	conn, err := rabbitmq.Connect(ctx, rabbitmqURL)
	if err != nil {
		cancel()
		t.Fatalf("connect rabbitmq: %v", err)
	}
	b := &Bus{Conn: conn, Exchange: strings.ReplaceAll(Name(t, "events"), "_", "."), got: make(chan rabbitmq.Delivery, 1024)}
	if err := conn.DeclareExchange(ctx, rabbitmq.ExchangeConfig{Name: b.Exchange}); err != nil {
		cancel()
		t.Fatalf("declare exchange: %v", err)
	}

	queue := b.Exchange + ".all"
	cons := conn.NewConsumer(rabbitmq.ConsumerConfig{
		Queue:    rabbitmq.QueueConfig{Name: queue, Exclusive: true, AutoDelete: true}.Transient(),
		Bindings: []rabbitmq.BindingConfig{{Queue: queue, Exchange: b.Exchange, RoutingKey: "#"}},
	}, func(_ context.Context, d rabbitmq.Delivery) error {
		b.got <- d
		return nil
	})
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = cons.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
		_ = conn.Close()
	})

	deadline := time.Now().Add(30 * time.Second)
	for !cons.Ready() {
		if time.Now().After(deadline) {
			t.Fatalf("consumer never became ready: %+v", cons.Status())
		}
		time.Sleep(20 * time.Millisecond)
	}
	return b
}

// Next waits for the next delivery.
func (b *Bus) Next(t *testing.T, within time.Duration) rabbitmq.Delivery {
	t.Helper()
	select {
	case d := <-b.got:
		return d
	case <-time.After(within):
		t.Fatalf("no delivery within %s", within)
		return rabbitmq.Delivery{}
	}
}

// None asserts nothing arrives for the given time.
func (b *Bus) None(t *testing.T, wait time.Duration) {
	t.Helper()
	select {
	case d := <-b.got:
		t.Fatalf("unexpected delivery: id %q, routing key %q", d.MessageID, d.RoutingKey)
	case <-time.After(wait):
	}
}

// Eventually polls cond until it holds or the time runs out.
func Eventually(t *testing.T, within time.Duration, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(within)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out after %s waiting for %s", within, what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}
