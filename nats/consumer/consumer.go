// Package consumer is the consuming side of go-outbox/nats: a durable JetStream
// consumer that replicas share (the queue group), acknowledges with a backoff
// schedule, sends messages it cannot process to a dead letter subject in the same
// stream, hands out resume tokens (stream:sequence), and drains cleanly on cancel.
//
// It pairs with the publisher: the idempotency key the outbox publishes as
// Nats-Msg-Id is Message.Key, so a handler can deduplicate with outbox.Dedupe in
// its own transaction.
package consumer

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
	"sync"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// Headers the consumer adds to a dead letter, beside the message's own.
const (
	// HeaderOriginalSubject is the subject the message was published on.
	HeaderOriginalSubject = "x-outbox-original-subject"
	// HeaderAttempts is how many times it was delivered before giving up.
	HeaderAttempts = "x-outbox-attempts"
	// HeaderReason is why it was given up on (the last error).
	HeaderReason = "x-outbox-dead-letter-reason"
	// HeaderStreamSequence is the sequence of the original message in the stream.
	HeaderStreamSequence = "x-outbox-original-sequence"

	headerMsgID = "Nats-Msg-Id"

	defaultAckWait      = 30 * time.Second
	defaultMaxDeliver   = 5
	defaultDrainTimeout = 30 * time.Second
)

// Config describes one durable consumer. Every replica that runs a consumer with the same
// Stream and Durable shares one consumer and splits the messages between them.
type Config struct {
	// Stream is the stream to read. Required.
	Stream string
	// Durable is the consumer name and the queue group. Required.
	Durable string
	// FilterSubjects are the subjects to receive. At least one.
	FilterSubjects []string
	// DLQSubject is where messages that cannot be processed go. It must be captured by the same
	// stream. Empty means they are terminated and dropped.
	DLQSubject string
	// MaxDeliver is the most deliveries of one message before it goes to the DLQ. Default 5.
	MaxDeliver int
	// Backoff are the delays before each retry: the first retry waits Backoff[0], and the last
	// value is reused. Empty means retry at once.
	Backoff []time.Duration
	// AckWait is how long the handler has before the server redelivers. Default 30 seconds.
	AckWait time.Duration
	// MaxAckPending bounds the messages in flight across all replicas. Default 1000.
	MaxAckPending int
	// DrainTimeout is how long Run waits for handlers in flight after cancel. Default 30 seconds.
	DrainTimeout time.Duration
}

// Message is one delivery.
type Message struct {
	Subject string
	Data    []byte
	Headers map[string][]string
	// Key is the idempotency key the publisher set (Nats-Msg-Id).
	Key string
	// Attempt is the delivery count, starting at 1.
	Attempt int
	// Stream and StreamSequence say where the message sits in the stream.
	Stream         string
	StreamSequence uint64
	Timestamp      time.Time
}

// ResumeToken is stream:sequence. A client that kept the token of the last message it saw can
// continue after it with Resume.
func (m Message) ResumeToken() string {
	return m.Stream + ":" + strconv.FormatUint(m.StreamSequence, 10)
}

// Handler processes one message. Returning nil acknowledges it. Returning an error retries it
// with the backoff schedule; an error wrapped with outbox.Permanent, or the last attempt, sends
// it to the dead letter subject.
type Handler func(ctx context.Context, m Message) error

// Metrics receives what the consumer does. Embed NopMetrics to stay compatible as it grows.
type Metrics interface {
	Received(subject string)
	Acked(subject string, took time.Duration)
	Retried(subject string, attempt int)
	DeadLettered(subject string, attempts int)
}

// NopMetrics ignores everything.
type NopMetrics struct{}

// Received implements Metrics.
func (NopMetrics) Received(string) {}

// Acked implements Metrics.
func (NopMetrics) Acked(string, time.Duration) {}

// Retried implements Metrics.
func (NopMetrics) Retried(string, int) {}

// DeadLettered implements Metrics.
func (NopMetrics) DeadLettered(string, int) {}

// Option configures a Consumer.
type Option func(*Consumer)

// WithMetrics sets the metrics receiver.
func WithMetrics(m Metrics) Option { return func(c *Consumer) { c.metrics = m } }

// WithConcurrency runs up to n handlers at once (default 1: messages are handled one at a time).
func WithConcurrency(n int) Option {
	return func(c *Consumer) {
		if n > 0 {
			c.concurrency = n
		}
	}
}

// Consumer reads one durable consumer.
type Consumer struct {
	js          jetstream.JetStream
	cfg         Config
	cons        jetstream.Consumer
	metrics     Metrics
	concurrency int
}

// New creates or updates the durable consumer and returns a Consumer for it.
func New(js jetstream.JetStream, cfg Config, opts ...Option) (*Consumer, error) {
	if cfg.Stream == "" || cfg.Durable == "" || len(cfg.FilterSubjects) == 0 {
		return nil, errors.New("consumer: a stream, a durable name and at least one filter subject are required")
	}
	if cfg.MaxDeliver <= 0 {
		cfg.MaxDeliver = defaultMaxDeliver
	}
	if cfg.AckWait <= 0 {
		cfg.AckWait = defaultAckWait
	}
	if cfg.DrainTimeout <= 0 {
		cfg.DrainTimeout = defaultDrainTimeout
	}
	if cfg.MaxAckPending <= 0 {
		cfg.MaxAckPending = 1000
	}
	c := &Consumer{js: js, cfg: cfg, metrics: NopMetrics{}, concurrency: 1}
	for _, o := range opts {
		o(c)
	}
	jc := jetstream.ConsumerConfig{
		Durable: cfg.Durable, AckPolicy: jetstream.AckExplicitPolicy, MaxDeliver: cfg.MaxDeliver,
		AckWait: cfg.AckWait, MaxAckPending: cfg.MaxAckPending,
	}
	if len(cfg.FilterSubjects) == 1 {
		jc.FilterSubject = cfg.FilterSubjects[0]
	} else {
		jc.FilterSubjects = cfg.FilterSubjects
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	cons, err := js.CreateOrUpdateConsumer(ctx, cfg.Stream, jc)
	if err != nil {
		return nil, fmt.Errorf("consumer: create %s on %s: %w", cfg.Durable, cfg.Stream, err)
	}
	c.cons = cons
	return c, nil
}

// Run delivers messages to h until ctx is cancelled, then stops fetching and waits (up to
// DrainTimeout) for the handlers in flight. It returns nil after a clean drain.
func (c *Consumer) Run(ctx context.Context, h Handler) error {
	var wg sync.WaitGroup
	sem := make(chan struct{}, c.concurrency)
	// Handlers keep their own context so a cancel of Run lets the ones in flight finish.
	hctx, hcancel := context.WithCancel(context.WithoutCancel(ctx))
	defer hcancel()

	cc, err := c.cons.Consume(func(msg jetstream.Msg) {
		sem <- struct{}{}
		wg.Add(1)
		go func() {
			defer func() { <-sem; wg.Done() }()
			c.handle(hctx, h, msg)
		}()
	})
	if err != nil {
		return fmt.Errorf("consumer: consume %s: %w", c.cfg.Durable, err)
	}
	<-ctx.Done()
	cc.Stop()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
		return nil
	case <-time.After(c.cfg.DrainTimeout):
		hcancel()
		<-done
		return fmt.Errorf("consumer: drain timed out after %s", c.cfg.DrainTimeout)
	}
}

func (c *Consumer) handle(ctx context.Context, h Handler, msg jetstream.Msg) {
	md, err := msg.Metadata()
	if err != nil {
		_ = msg.Term()
		return
	}
	m := Message{Subject: msg.Subject(), Data: msg.Data(), Headers: msg.Headers(), Key: msg.Headers().Get(headerMsgID), Attempt: int(md.NumDelivered),
		Stream: md.Stream, StreamSequence: md.Sequence.Stream, Timestamp: md.Timestamp}
	c.metrics.Received(m.Subject)
	start := time.Now()
	herr := h(ctx, m)
	if herr == nil {
		_ = msg.Ack()
		c.metrics.Acked(m.Subject, time.Since(start))
		return
	}
	if outbox.IsPermanent(herr) || m.Attempt >= c.cfg.MaxDeliver {
		if c.deadLetter(ctx, msg, m, herr) {
			c.metrics.DeadLettered(m.Subject, m.Attempt)
			return
		}
		// The dead letter could not be written: try again later instead of losing the message.
		_ = msg.NakWithDelay(c.delay(m.Attempt))
		return
	}
	c.metrics.Retried(m.Subject, m.Attempt)
	_ = msg.NakWithDelay(c.delay(m.Attempt))
}

func (c *Consumer) delay(attempt int) time.Duration {
	if len(c.cfg.Backoff) == 0 {
		return 0
	}
	return c.cfg.Backoff[min(max(attempt-1, 0), len(c.cfg.Backoff)-1)]
}

// deadLetter writes the message to the DLQ subject and terminates it. It reports whether the
// message is dealt with.
func (c *Consumer) deadLetter(ctx context.Context, msg jetstream.Msg, m Message, cause error) bool {
	if c.cfg.DLQSubject == "" {
		_ = msg.Term()
		return true
	}
	out := &natsgo.Msg{Subject: c.cfg.DLQSubject, Data: m.Data, Header: natsgo.Header{}}
	for k, v := range m.Headers {
		out.Header[k] = append([]string(nil), v...)
	}
	// One dead letter per original message, even if this runs twice.
	out.Header.Set(headerMsgID, "dlq-"+m.Stream+"-"+strconv.FormatUint(m.StreamSequence, 10))
	out.Header.Set(HeaderOriginalSubject, m.Subject)
	out.Header.Set(HeaderAttempts, strconv.Itoa(m.Attempt))
	out.Header.Set(HeaderReason, cause.Error())
	out.Header.Set(HeaderStreamSequence, strconv.FormatUint(m.StreamSequence, 10))
	pctx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := c.js.PublishMsg(pctx, out); err != nil {
		return false
	}
	_ = msg.Term()
	return true
}

// ParseToken splits a resume token into the stream and the sequence of the last message seen.
func ParseToken(token string) (stream string, sequence uint64, err error) {
	stream, seq, ok := strings.Cut(token, ":")
	if !ok || stream == "" {
		return "", 0, fmt.Errorf("consumer: %q is not a resume token (stream:sequence)", token)
	}
	sequence, err = strconv.ParseUint(seq, 10, 64)
	if err != nil || sequence == 0 {
		return "", 0, fmt.Errorf("consumer: %q is not a resume token (stream:sequence)", token)
	}
	return stream, sequence, nil
}

// Resume returns an ordered consumer that delivers the messages after the one the token names,
// in order, for the given subjects. It is for a client that reconnects and wants what it missed;
// use Consume on the result. The stream must still hold the message after the token.
func Resume(ctx context.Context, js jetstream.JetStream, token string, filterSubjects ...string) (jetstream.Consumer, error) {
	stream, seq, err := ParseToken(token)
	if err != nil {
		return nil, err
	}
	cons, err := js.OrderedConsumer(ctx, stream, jetstream.OrderedConsumerConfig{
		FilterSubjects: filterSubjects, DeliverPolicy: jetstream.DeliverByStartSequencePolicy, OptStartSeq: seq + 1,
	})
	if err != nil {
		return nil, fmt.Errorf("consumer: resume %s after %d: %w", stream, seq, err)
	}
	return cons, nil
}
