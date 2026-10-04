// Package rabbitmq is the outbox Publisher for RabbitMQ, built on
// github.com/Bugs5382/go-rabbitmq. It lives in its own package so that
// importing the outbox does not pull in an AMQP client.
//
// Each record is published to one exchange with the record's Topic as the
// routing key, its idempotency key as the AMQP message id, and publisher
// confirms on, so the relay marks a row sent only once the broker has
// acknowledged it.
package rabbitmq

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

	outbox "github.com/Bugs5382/go-outbox"
	gorabbitmq "github.com/Bugs5382/go-rabbitmq"
	amqp "github.com/rabbitmq/amqp091-go"
)

// Headers the publisher adds to every message, beside the record's own.
const (
	// HeaderAggregateKey carries Message.AggregateKey when it is set.
	HeaderAggregateKey = "x-outbox-aggregate-key"
	// HeaderAttempt carries the delivery attempt, starting at 1. A value
	// above 1 means the message may have been delivered before.
	HeaderAttempt = "x-outbox-attempt"
)

// Publisher is an outbox.Publisher backed by a go-rabbitmq publisher.
type Publisher struct {
	pub *gorabbitmq.Publisher
}

var _ outbox.Publisher = (*Publisher)(nil)

// New builds a Publisher for exchange on conn. Publisher confirms are always
// on and messages are persistent. opts go to go-rabbitmq unchanged: add
// gorabbitmq.WithMandatory() to have a message that no queue is bound to
// receive come back as an error (and so be retried) instead of being dropped
// by the broker, or gorabbitmq.WithExchangeDeclare to declare the exchange.
func New(conn *gorabbitmq.Conn, exchange string, opts ...gorabbitmq.PublisherOption) *Publisher {
	opts = append([]gorabbitmq.PublisherOption{gorabbitmq.WithConfirms(), gorabbitmq.WithPersistentDefault(true)}, opts...)
	return &Publisher{pub: conn.NewPublisher(exchange, opts...)}
}

// Publish sends r and returns once the broker has confirmed it. A nack, a
// lost confirm or a returned message is an error, and the relay retries it.
func (p *Publisher) Publish(ctx context.Context, r outbox.Record) error {
	headers := amqp.Table{HeaderAttempt: int64(r.Attempt)}
	for k, v := range r.Headers {
		headers[k] = v
	}
	if r.AggregateKey != "" {
		headers[HeaderAggregateKey] = r.AggregateKey
	}
	opts := []gorabbitmq.PublishOption{
		gorabbitmq.WithMessageID(r.Key),
		gorabbitmq.WithHeaders(headers),
		gorabbitmq.WithPersistent(true),
		withTimestamp(r),
	}
	if r.ContentType != "" {
		opts = append(opts, gorabbitmq.WithContentType(r.ContentType))
	}
	if err := p.pub.Publish(ctx, r.Topic, r.Payload, opts...); err != nil {
		return fmt.Errorf("rabbitmq: publish %s: %w", r.Key, err)
	}
	return nil
}

// Close closes the underlying go-rabbitmq publisher. It does not close the
// connection.
func (p *Publisher) Close() error { return p.pub.Close() }

func withTimestamp(r outbox.Record) gorabbitmq.PublishOption {
	return func(m *amqp.Publishing) {
		if !r.CreatedAt.IsZero() {
			m.Timestamp = r.CreatedAt
		}
	}
}
