package consumer_test

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
	"sync/atomic"
	"testing"
	"time"

	outbox "github.com/Bugs5382/go-outbox"
	"github.com/Bugs5382/go-outbox/nats/consumer"
	"github.com/nats-io/nats-server/v2/server"
	natsgo "github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

type env struct {
	js jetstream.JetStream
	st jetstream.Stream
}

func newEnv(t *testing.T) env {
	t.Helper()
	srv, err := server.NewServer(&server.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	go srv.Start()
	if !srv.ReadyForConnections(10 * time.Second) {
		t.Fatal("server not ready")
	}
	nc, err := natsgo.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { nc.Close(); srv.Shutdown(); srv.WaitForShutdown() })
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	st, err := js.CreateStream(context.Background(), jetstream.StreamConfig{Name: "EVENTS", Subjects: []string{"sf.>", "dlq.>"}})
	if err != nil {
		t.Fatal(err)
	}
	return env{js: js, st: st}
}

func (e env) publish(t *testing.T, subject, key, body string) {
	t.Helper()
	msg := &natsgo.Msg{Subject: subject, Data: []byte(body), Header: natsgo.Header{}}
	msg.Header.Set("Nats-Msg-Id", key)
	if _, err := e.js.PublishMsg(context.Background(), msg); err != nil {
		t.Fatal(err)
	}
}

func config(durable string) consumer.Config {
	return consumer.Config{
		Stream: "EVENTS", Durable: durable, FilterSubjects: []string{"sf.test.>"}, DLQSubject: "dlq.test",
		MaxDeliver: 3, Backoff: []time.Duration{50 * time.Millisecond, 100 * time.Millisecond}, AckWait: 2 * time.Second,
	}
}

func run(t *testing.T, e env, cfg consumer.Config, h consumer.Handler) (stop func()) {
	t.Helper()
	c, err := consumer.New(e.js, cfg)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- c.Run(ctx, h) }()
	stopped := false
	stop = func() {
		if stopped {
			return
		}
		stopped = true
		cancel()
		select {
		case err := <-done:
			if err != nil {
				t.Errorf("Run = %v", err)
			}
		case <-time.After(10 * time.Second):
			t.Error("Run did not return")
		}
	}
	t.Cleanup(stop)
	return stop
}

func eventually(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(10 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestHandlerSeesTheMessageAndAcks(t *testing.T) {
	e := newEnv(t)
	got := make(chan consumer.Message, 1)
	run(t, e, config("a"), func(_ context.Context, m consumer.Message) error { got <- m; return nil })
	e.publish(t, "sf.test.one", "key-1", "hello")
	select {
	case m := <-got:
		if m.Subject != "sf.test.one" || string(m.Data) != "hello" || m.Key != "key-1" || m.Attempt != 1 || m.StreamSequence == 0 {
			t.Errorf("message = %+v", m)
		}
		if tok := m.ResumeToken(); tok != "EVENTS:"+itoa(m.StreamSequence) {
			t.Errorf("token = %q", tok)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("no message")
	}
	eventually(t, "the ack", func() bool {
		ci, err := e.st.Consumer(context.Background(), "a")
		if err != nil {
			return false
		}
		info, _ := ci.Info(context.Background())
		return info.NumAckPending == 0 && info.NumPending == 0
	})
}

func itoa(n uint64) string {
	if n == 0 {
		return "0"
	}
	var b []byte
	for ; n > 0; n /= 10 {
		b = append([]byte{byte('0' + n%10)}, b...)
	}
	return string(b)
}

func TestAnErrorRedeliversWithTheBackoff(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	var times []time.Time
	var attempts []int
	run(t, e, config("b"), func(_ context.Context, m consumer.Message) error {
		mu.Lock()
		defer mu.Unlock()
		times, attempts = append(times, time.Now()), append(attempts, m.Attempt)
		if len(times) < 3 {
			return errors.New("not yet")
		}
		return nil
	})
	e.publish(t, "sf.test.retry", "k", "x")
	eventually(t, "three deliveries", func() bool { mu.Lock(); defer mu.Unlock(); return len(times) == 3 })
	mu.Lock()
	defer mu.Unlock()
	if attempts[0] != 1 || attempts[1] != 2 || attempts[2] != 3 {
		t.Errorf("attempts = %v", attempts)
	}
	if d := times[1].Sub(times[0]); d < 45*time.Millisecond {
		t.Errorf("first retry after %s, want at least the first backoff step (50ms)", d)
	}
	if d := times[2].Sub(times[1]); d < 95*time.Millisecond {
		t.Errorf("second retry after %s, want at least the second backoff step (100ms)", d)
	}
}

func dlqMessages(t *testing.T, e env) []*jetstream.RawStreamMsg {
	t.Helper()
	var out []*jetstream.RawStreamMsg
	for seq := uint64(1); ; seq++ {
		m, err := e.st.GetMsg(context.Background(), seq)
		if err != nil {
			return out
		}
		if m.Subject == "dlq.test" {
			out = append(out, m)
		}
	}
}

func TestReachingMaxDeliverSendsTheMessageToTheDLQSubject(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	run(t, e, config("c"), func(context.Context, consumer.Message) error { calls.Add(1); return errors.New("always") })
	e.publish(t, "sf.test.poison", "k", "bad")
	eventually(t, "a dead letter", func() bool { return len(dlqMessages(t, e)) == 1 })
	m := dlqMessages(t, e)[0]
	if string(m.Data) != "bad" || m.Header.Get(consumer.HeaderOriginalSubject) != "sf.test.poison" || m.Header.Get(consumer.HeaderAttempts) != "3" || m.Header.Get(consumer.HeaderReason) == "" {
		t.Errorf("dead letter = %q headers %v", m.Data, m.Header)
	}
	if calls.Load() != 3 {
		t.Errorf("handler calls = %d, want MaxDeliver (3)", calls.Load())
	}
	time.Sleep(300 * time.Millisecond)
	if calls.Load() != 3 {
		t.Errorf("the message came back after the DLQ: %d calls", calls.Load())
	}
}

func TestAPermanentErrorGoesStraightToTheDLQ(t *testing.T) {
	e := newEnv(t)
	var calls atomic.Int32
	run(t, e, config("d"), func(context.Context, consumer.Message) error {
		calls.Add(1)
		return outbox.Permanent(errors.New("cannot be processed"))
	})
	e.publish(t, "sf.test.bad", "k", "x")
	eventually(t, "a dead letter", func() bool { return len(dlqMessages(t, e)) == 1 })
	if calls.Load() != 1 {
		t.Errorf("handler calls = %d, want 1", calls.Load())
	}
	if m := dlqMessages(t, e)[0]; m.Header.Get(consumer.HeaderAttempts) != "1" {
		t.Errorf("attempts header = %q", m.Header.Get(consumer.HeaderAttempts))
	}
}

func TestReplicasWithTheSameDurableSplitTheMessages(t *testing.T) {
	e := newEnv(t)
	var mu sync.Mutex
	seen := map[string]int{}
	byReplica := [2]int{}
	for i := range byReplica {
		i := i
		run(t, e, config("g"), func(_ context.Context, m consumer.Message) error {
			mu.Lock()
			defer mu.Unlock()
			seen[m.Key]++
			byReplica[i]++
			return nil
		})
	}
	const total = 40
	for i := 0; i < total; i++ {
		e.publish(t, "sf.test.n", "key-"+itoa(uint64(i)), "x")
	}
	eventually(t, "all messages", func() bool { mu.Lock(); defer mu.Unlock(); return len(seen) == total })
	mu.Lock()
	defer mu.Unlock()
	for k, n := range seen {
		if n != 1 {
			t.Errorf("%s was handled %d times", k, n)
		}
	}
	if byReplica[0]+byReplica[1] != total {
		t.Errorf("handled %v, want %d in all", byReplica, total)
	}
}

func TestResumeStartsAfterTheToken(t *testing.T) {
	e := newEnv(t)
	for i := 0; i < 5; i++ {
		e.publish(t, "sf.test.r", "r-"+itoa(uint64(i)), "m"+itoa(uint64(i)))
	}
	// the token of the third message
	third, err := e.st.GetMsg(context.Background(), 3)
	if err != nil {
		t.Fatal(err)
	}
	token := "EVENTS:" + itoa(third.Sequence)
	stream, seq, err := consumer.ParseToken(token)
	if err != nil || stream != "EVENTS" || seq != 3 {
		t.Fatalf("ParseToken = %q %d %v", stream, seq, err)
	}
	for _, bad := range []string{"", "EVENTS", "EVENTS:x", ":5", "EVENTS:0"} {
		if _, _, err := consumer.ParseToken(bad); err == nil {
			t.Errorf("ParseToken(%q) = nil error", bad)
		}
	}
	cons, err := consumer.Resume(context.Background(), e.js, token, "sf.test.>")
	if err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var got []string
	cc, err := cons.Consume(func(m jetstream.Msg) { mu.Lock(); defer mu.Unlock(); got = append(got, string(m.Data())) })
	if err != nil {
		t.Fatal(err)
	}
	defer cc.Stop()
	eventually(t, "the two later messages", func() bool { mu.Lock(); defer mu.Unlock(); return len(got) == 2 })
	mu.Lock()
	defer mu.Unlock()
	if got[0] != "m3" || got[1] != "m4" {
		t.Errorf("resumed with %v, want m3 then m4", got)
	}
}

func TestDrainWaitsForTheHandlerInFlight(t *testing.T) {
	e := newEnv(t)
	started, release := make(chan struct{}), make(chan struct{})
	var finished atomic.Bool
	stop := run(t, e, config("h"), func(context.Context, consumer.Message) error {
		close(started)
		<-release
		finished.Store(true)
		return nil
	})
	e.publish(t, "sf.test.slow", "k", "x")
	<-started
	stopped := make(chan struct{})
	go func() { stop(); close(stopped) }()
	select {
	case <-stopped:
		t.Fatal("Run returned while a handler was still working")
	case <-time.After(300 * time.Millisecond):
	}
	close(release)
	select {
	case <-stopped:
	case <-time.After(10 * time.Second):
		t.Fatal("Run did not return after the handler finished")
	}
	if !finished.Load() {
		t.Error("the handler did not finish")
	}
}

func TestConfigIsChecked(t *testing.T) {
	e := newEnv(t)
	for name, c := range map[string]consumer.Config{
		"no stream":  {Durable: "x", FilterSubjects: []string{"a"}},
		"no durable": {Stream: "EVENTS", FilterSubjects: []string{"a"}},
		"no filter":  {Stream: "EVENTS", Durable: "x"},
	} {
		if _, err := consumer.New(e.js, c); err == nil {
			t.Errorf("%s: New = nil error", name)
		}
	}
}
