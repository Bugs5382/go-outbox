// Package nats is the outbox Publisher for NATS JetStream, built on
// github.com/nats-io/nats.go. It lives in its own package so that importing
// the outbox does not pull in a NATS client.
//
// Each record is published to the subject prefix + Topic, with its
// idempotency key as the Nats-Msg-Id header, so the stream's duplicate window
// drops a retried attempt, and the relay marks a row sent only once JetStream
// has acknowledged the message.
package nats

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
	"strconv"
	"strings"
	"time"
	"unicode"

	outbox "github.com/Bugs5382/go-outbox"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Headers the publisher adds to every message, beside the record's own. The
// names match the RabbitMQ adapter's.
const (
	// HeaderAggregateKey carries Message.AggregateKey when it is set.
	HeaderAggregateKey = "x-outbox-aggregate-key"
	// HeaderAttempt carries the delivery attempt, starting at 1. A value
	// above 1 means the message may have been delivered before.
	HeaderAttempt = "x-outbox-attempt"

	headerMsgID       = "Nats-Msg-Id"
	headerContentType = "Content-Type"

	defaultTimeout = 5 * time.Second
)

// ErrInvalidSubject is returned (wrapped, and marked permanent) when prefix
// plus topic is not a usable subject: it is empty, has an empty token,
// whitespace, or a wildcard.
var ErrInvalidSubject = errors.New("nats: invalid subject")

// Publisher is an outbox.Publisher backed by a JetStream context.
type Publisher struct {
	js      jetstream.JetStream
	prefix  string
	timeout time.Duration
}

var _ outbox.Publisher = (*Publisher)(nil)

// Option configures a Publisher.
type Option func(*Publisher)

// WithSubjectPrefix puts prefix in front of every topic to make the subject.
// Include the trailing dot: WithSubjectPrefix("sf.") turns the topic
// "fleet.rank.changed" into "sf.fleet.rank.changed".
func WithSubjectPrefix(prefix string) Option {
	return func(p *Publisher) { p.prefix = prefix }
}

// WithTimeout bounds how long one publish waits for the JetStream
// acknowledgement. The default is 5 seconds.
func WithTimeout(d time.Duration) Option {
	return func(p *Publisher) {
		if d > 0 {
			p.timeout = d
		}
	}
}

// New builds a Publisher on js.
func New(js jetstream.JetStream, opts ...Option) *Publisher {
	p := &Publisher{js: js, timeout: defaultTimeout}
	for _, o := range opts {
		o(p)
	}
	return p
}

// Publish sends r and returns once JetStream has acknowledged it. A repeat of
// a key that is still inside the stream's duplicate window counts as
// delivered. A subject that is not valid, a subject with no stream, and a
// payload over the server's limit are permanent errors; a lost connection, a
// timeout and a cancelled context are retryable, and the relay retries them.
func (p *Publisher) Publish(ctx context.Context, r outbox.Record) error {
	subject, err := p.subject(r.Topic)
	if err != nil {
		return outbox.Permanent(err)
	}
	msg := &natsgo.Msg{Subject: subject, Data: r.Payload, Header: natsgo.Header{}}
	for k, v := range r.Headers {
		msg.Header.Set(k, v)
	}
	msg.Header.Set(headerMsgID, r.Key)
	msg.Header.Set(HeaderAttempt, strconv.Itoa(r.Attempt))
	if r.AggregateKey != "" {
		msg.Header.Set(HeaderAggregateKey, r.AggregateKey)
	}
	if r.ContentType != "" {
		msg.Header.Set(headerContentType, r.ContentType)
	}

	ctx, cancel := context.WithTimeout(ctx, p.timeout)
	defer cancel()
	if _, err := p.js.PublishMsg(ctx, msg); err != nil {
		err = fmt.Errorf("nats: publish %s: %w", r.Key, err)
		if errors.Is(err, jetstream.ErrNoStreamResponse) || errors.Is(err, jetstream.ErrStreamNotFound) || errors.Is(err, natsgo.ErrMaxPayload) {
			return outbox.Permanent(err)
		}
		return err
	}
	return nil
}

func (p *Publisher) subject(topic string) (string, error) {
	if topic == "" {
		return "", fmt.Errorf("%w: empty topic", ErrInvalidSubject)
	}
	subject := p.prefix + topic
	for _, token := range strings.Split(subject, ".") {
		if token == "" {
			return "", fmt.Errorf("%w: %q has an empty token", ErrInvalidSubject, subject)
		}
	}
	for _, c := range subject {
		if unicode.IsSpace(c) || unicode.IsControl(c) || c == '*' || c == '>' {
			return "", fmt.Errorf("%w: %q has a space, control character or wildcard", ErrInvalidSubject, subject)
		}
	}
	return subject, nil
}

// StreamSpec describes a stream for EnsureStream.
type StreamSpec struct {
	// Name is the stream name. Required.
	Name string
	// Subjects are the subjects the stream captures. Required.
	Subjects []string
	// DuplicateWindow is how long the stream remembers Nats-Msg-Id values.
	// Zero uses the server default (two minutes).
	DuplicateWindow time.Duration
	// Retention is the retention policy. The zero value is limits.
	Retention jetstream.RetentionPolicy
}

// EnsureStream creates the stream when it does not exist and leaves an
// existing stream exactly as it is: it never reconfigures one. It is off
// unless you call it, so a service whose streams are managed elsewhere never
// touches them.
func (p *Publisher) EnsureStream(ctx context.Context, spec StreamSpec) error {
	if spec.Name == "" || len(spec.Subjects) == 0 {
		return errors.New("nats: a stream needs a name and subjects")
	}
	_, err := p.js.Stream(ctx, spec.Name)
	if err == nil {
		return nil
	}
	if !errors.Is(err, jetstream.ErrStreamNotFound) {
		return fmt.Errorf("nats: look up stream %s: %w", spec.Name, err)
	}
	_, err = p.js.CreateStream(ctx, jetstream.StreamConfig{
		Name:       spec.Name,
		Subjects:   spec.Subjects,
		Duplicates: spec.DuplicateWindow,
		Retention:  spec.Retention,
		Storage:    jetstream.FileStorage,
	})
	if err != nil && !errors.Is(err, jetstream.ErrStreamNameAlreadyInUse) {
		return fmt.Errorf("nats: create stream %s: %w", spec.Name, err)
	}
	return nil
}
