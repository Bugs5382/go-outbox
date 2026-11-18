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
	"errors"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	outboxnats "github.com/Bugs5382/go-outbox/nats"
	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// startServer runs an embedded JetStream server on addr ("127.0.0.1:0" for a free port) with the
// file store in dir.
func startServer(t *testing.T, dir, addr string) *server.Server {
	t.Helper()
	host, portText, err := net.SplitHostPort(addr)
	if err != nil {
		t.Fatal(err)
	}
	port, _ := strconv.Atoi(portText)
	if port == 0 {
		port = -1
	}
	s, err := server.NewServer(&server.Options{Host: host, Port: port, JetStream: true, StoreDir: dir, NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatalf("new server: %v", err)
	}
	go s.Start()
	if !s.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	return s
}

type env struct {
	srv *server.Server
	nc  *natsgo.Conn
	js  jetstream.JetStream
	dir string
}

func newEnv(t *testing.T) *env {
	t.Helper()
	dir := t.TempDir()
	srv := startServer(t, dir, "127.0.0.1:0")
	nc, err := natsgo.Connect(srv.ClientURL(), natsgo.MaxReconnects(-1), natsgo.ReconnectWait(100*time.Millisecond))
	if err != nil {
		t.Fatalf("connect: %v", err)
	}
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close(); srv.Shutdown(); srv.WaitForShutdown() })
	return &env{srv: srv, nc: nc, js: js, dir: dir}
}

func (e *env) stream(t *testing.T, name string, subjects ...string) jetstream.Stream {
	t.Helper()
	s, err := e.js.CreateStream(context.Background(), jetstream.StreamConfig{Name: name, Subjects: subjects, Duplicates: time.Minute})
	if err != nil {
		t.Fatalf("create stream: %v", err)
	}
	return s
}

func record(key, topic string) outbox.Record {
	return outbox.Record{Message: outbox.Message{Key: key, Topic: topic, Payload: []byte(`{"n":1}`)}, ID: 1, Attempt: 1, CreatedAt: time.Now()}
}

func TestPublishIsAckedAndCarriesTheRecord(t *testing.T) {
	e := newEnv(t)
	st := e.stream(t, "FLEET", "sf.fleet.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."))

	r := record("key-1", "fleet.rank.changed")
	r.AggregateKey = "member:42"
	r.ContentType = "application/json"
	r.Attempt = 3
	r.Headers = map[string]string{"x-trace": "abc"}
	if err := pub.Publish(context.Background(), r); err != nil {
		t.Fatalf("Publish = %v", err)
	}

	msg, err := st.GetLastMsgForSubject(context.Background(), "sf.fleet.rank.changed")
	if err != nil {
		t.Fatalf("message not in the stream: %v", err)
	}
	if string(msg.Data) != `{"n":1}` {
		t.Errorf("data = %q", msg.Data)
	}
	want := map[string]string{
		"Nats-Msg-Id":                 "key-1",
		"Content-Type":                "application/json",
		outboxnats.HeaderAggregateKey: "member:42",
		outboxnats.HeaderAttempt:      "3",
		"x-trace":                     "abc",
	}
	for k, v := range want {
		if got := msg.Header.Get(k); got != v {
			t.Errorf("header %s = %q, want %q", k, got, v)
		}
	}
}

func TestDuplicateKeyIsDroppedByTheStream(t *testing.T) {
	e := newEnv(t)
	st := e.stream(t, "FLEET", "sf.fleet.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."))
	for i := 0; i < 3; i++ {
		r := record("same-key", "fleet.rank.changed")
		r.Attempt = i + 1
		if err := pub.Publish(context.Background(), r); err != nil {
			t.Fatalf("attempt %d: %v", i+1, err)
		}
	}
	info, err := st.Info(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if info.State.Msgs != 1 {
		t.Errorf("messages in the stream = %d, want 1 (the duplicate window drops the retries)", info.State.Msgs)
	}
}

func TestNoStreamForTheSubjectIsPermanent(t *testing.T) {
	e := newEnv(t)
	pub := outboxnats.New(e.js, outboxnats.WithTimeout(time.Second))
	err := pub.Publish(context.Background(), record("k", "orphan.event"))
	if err == nil || !outbox.IsPermanent(err) {
		t.Fatalf("Publish = %v, want a permanent error", err)
	}
}

func TestInvalidTopicsArePermanent(t *testing.T) {
	e := newEnv(t)
	pub := outboxnats.New(e.js)
	for _, topic := range []string{"", "has space", "a..b", ".lead", "trail.", "wild.*", "all.>", "tab\tbed"} {
		err := pub.Publish(context.Background(), record("k-"+topic, topic))
		if !errors.Is(err, outboxnats.ErrInvalidSubject) || !outbox.IsPermanent(err) {
			t.Errorf("topic %q: Publish = %v, want a permanent ErrInvalidSubject", topic, err)
		}
	}
}

func TestPrefixAndTopicBecomeTheSubject(t *testing.T) {
	e := newEnv(t)
	st := e.stream(t, "OPS", "sf.ops.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."))
	if err := pub.Publish(context.Background(), record("k", "ops.notice.posted")); err != nil {
		t.Fatal(err)
	}
	if _, err := st.GetLastMsgForSubject(context.Background(), "sf.ops.notice.posted"); err != nil {
		t.Errorf("not published on sf.ops.notice.posted: %v", err)
	}
}

func TestOversizedPayloadIsPermanent(t *testing.T) {
	e := newEnv(t)
	e.stream(t, "BIG", "big.>")
	pub := outboxnats.New(e.js)
	r := record("big", "big.thing")
	r.Payload = []byte(strings.Repeat("x", 2<<20))
	err := pub.Publish(context.Background(), r)
	if err == nil || !outbox.IsPermanent(err) {
		t.Fatalf("Publish = %v, want a permanent error", err)
	}
}

func TestConnectionLossIsRetryableAndRecovers(t *testing.T) {
	e := newEnv(t)
	e.stream(t, "FLEET", "sf.fleet.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."), outboxnats.WithTimeout(500*time.Millisecond))
	addr := e.srv.Addr().String()

	e.srv.Shutdown()
	e.srv.WaitForShutdown()
	err := pub.Publish(context.Background(), record("down", "fleet.rank.changed"))
	if err == nil || outbox.IsPermanent(err) {
		t.Fatalf("Publish while down = %v, want a retryable error", err)
	}

	e.srv = startServer(t, e.dir, addr)
	deadline := time.Now().Add(15 * time.Second)
	for {
		if err = pub.Publish(context.Background(), record("down", "fleet.rank.changed")); err == nil {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("Publish after the restart = %v", err)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func TestCancelledContextIsRetryable(t *testing.T) {
	e := newEnv(t)
	e.stream(t, "FLEET", "sf.fleet.>")
	pub := outboxnats.New(e.js, outboxnats.WithSubjectPrefix("sf."))
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := pub.Publish(ctx, record("c", "fleet.rank.changed"))
	if err == nil || outbox.IsPermanent(err) {
		t.Fatalf("Publish = %v, want a retryable error", err)
	}
}

func TestEnsureStreamCreatesOnceAndNeverReconfigures(t *testing.T) {
	e := newEnv(t)
	pub := outboxnats.New(e.js)
	ctx := context.Background()
	spec := outboxnats.StreamSpec{Name: "SF_FLEET", Subjects: []string{"sf.fleet.>"}, DuplicateWindow: 2 * time.Minute, Retention: jetstream.LimitsPolicy}
	if err := pub.EnsureStream(ctx, spec); err != nil {
		t.Fatalf("EnsureStream = %v", err)
	}
	st, err := e.js.Stream(ctx, "SF_FLEET")
	if err != nil {
		t.Fatal(err)
	}
	info, _ := st.Info(ctx)
	if info.Config.Duplicates != 2*time.Minute || len(info.Config.Subjects) != 1 || info.Config.Subjects[0] != "sf.fleet.>" {
		t.Errorf("config = %+v", info.Config)
	}

	changed := spec
	changed.DuplicateWindow = time.Hour
	changed.Subjects = []string{"other.>"}
	if err := pub.EnsureStream(ctx, changed); err != nil {
		t.Fatalf("second EnsureStream = %v", err)
	}
	info, _ = st.Info(ctx)
	if info.Config.Duplicates != 2*time.Minute || info.Config.Subjects[0] != "sf.fleet.>" {
		t.Errorf("an existing stream was reconfigured: %+v", info.Config)
	}
}
