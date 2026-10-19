package ca

import (
	"context"
	"github.com/wakonig/cago/internal/wire"
	"sync"
	"testing"
	"time"
)

func eventually(t *testing.T, fn func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !fn() {
		if time.Now().After(deadline) {
			t.Fatal("condition not reached")
		}
		time.Sleep(time.Millisecond)
	}
}
func subscribeTest(t *testing.T, ch *Channel, ctx context.Context, opts ...RequestOption) *Subscription {
	t.Helper()
	s, err := ch.Subscribe(ctx, opts...)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { s.Cancel() })
	return s
}
func putTest(t *testing.T, ch *Channel, ctx context.Context, v float64) {
	t.Helper()
	value, _ := NewValue(v)
	if err := ch.PutNotify(ctx, value); err != nil {
		t.Fatal(err)
	}
}

func TestSharedMonitorSnapshotAndIndependentBuffers(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	slow := subscribeTest(t, ch, ctx, WithBuffer(1))
	initial := event(t, slow)
	fast := subscribeTest(t, ch, ctx, WithBuffer(8))
	replay := event(t, fast)
	if initial.Value.Float64() != replay.Value.Float64() || !initial.ReceivedAt.Equal(replay.ReceivedAt) {
		t.Fatal("late subscriber did not receive cached snapshot")
	}
	if cap(slow.events) != 1 || cap(fast.events) != 8 {
		t.Fatal("buffers were shared")
	}
	for v := 2.0; v <= 8; v++ {
		putTest(t, ch, ctx, v)
		if got := event(t, fast).Value.Float64(); got != v {
			t.Fatalf("fast consumer got %g want %g", got, v)
		}
	}
	if got := event(t, slow).Value.Float64(); got != 8 {
		t.Fatalf("slow consumer got %g", got)
	}
	if server.subCount.Load() != 1 {
		t.Fatal("matching subscribers created multiple IOC monitors")
	}
	slow.Cancel()
	if server.unsubCount.Load() != 0 {
		t.Fatal("cancelled shared monitor while fast consumer remained")
	}
	putTest(t, ch, ctx, 9)
	if event(t, fast).Value.Float64() != 9 {
		t.Fatal("remaining consumer stopped")
	}
	fast.Cancel()
	eventually(t, func() bool { return server.unsubCount.Load() == 1 })
}

func TestSharedMonitorDifferentRequestsStaySeparate(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	configs := [][]RequestOption{nil, {WithCount(1)}, {WithMask(LogEvent)}, {WithType(Double)}, {WithMetadata(Time)}}
	for _, cfg := range configs {
		s := subscribeTest(t, ch, ctx, cfg...)
		event(t, s)
	}
	if got := server.subCount.Load(); got != 5 {
		t.Fatalf("different requests shared IOC monitors: %d", got)
	}
	again := subscribeTest(t, ch, ctx, WithMetadata(Time), WithBuffer(16))
	if e := event(t, again); e.Value.Type() != 20 {
		t.Fatal("wrong metadata replay")
	}
	if server.subCount.Load() != 5 {
		t.Fatal("local buffer size prevented sharing")
	}
}

func TestSharedMonitorBeforeFirstSnapshot(t *testing.T) {
	server := newTestIOC(t)
	server.suppressEvents.Store(true)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	a := subscribeTest(t, ch, ctx)
	b := subscribeTest(t, ch, ctx)
	eventually(t, func() bool { return server.subCount.Load() == 1 })
	if len(a.events) != 0 || len(b.events) != 0 {
		t.Fatal("invented initial snapshot")
	}
	server.suppressEvents.Store(false)
	putTest(t, ch, ctx, 7)
	if event(t, a).Value.Float64() != 7 || event(t, b).Value.Float64() != 7 {
		t.Fatal("first update did not reach all consumers")
	}
}

func TestSharedMonitorReconnectDropsCachedSnapshot(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	a := subscribeTest(t, ch, ctx)
	event(t, a)
	server.suppressEvents.Store(true)
	server.mu.Lock()
	server.value = 9
	server.mu.Unlock()
	server.disconnect()
	eventually(t, func() bool { return server.subCount.Load() >= 2 })
	b := subscribeTest(t, ch, ctx)
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if len(a.events) != 0 || len(b.events) != 0 {
		t.Fatal("replayed stale snapshot after disconnect")
	}
	if server.subCount.Load() != 2 {
		t.Fatal("reconnect did not preserve shared monitor")
	}
	server.suppressEvents.Store(false)
	putTest(t, ch, ctx, 10)
	if event(t, a).Value.Float64() != 10 || event(t, b).Value.Float64() != 10 {
		t.Fatal("fresh reconnect update missing")
	}
}

func TestSharedMonitorLastCancelAndRejoin(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	old := subscribeTest(t, ch, ctx)
	event(t, old)
	m := old.monitor
	m.mu.Lock()
	conn := m.conn
	m.mu.Unlock()
	conn.gate <- struct{}{}
	old.cancel()
	<-old.Done()
	result := make(chan *Subscription, 1)
	fail := make(chan error, 1)
	go func() {
		s, err := ch.Subscribe(ctx)
		if err != nil {
			fail <- err
			return
		}
		result <- s
	}()
	m.mu.Lock()
	closing := m.closing
	m.mu.Unlock()
	if !closing {
		<-conn.gate
		t.Fatal("last cancellation did not stop monitor")
	}
	<-conn.gate
	var next *Subscription
	select {
	case next = <-result:
	case err := <-fail:
		t.Fatal(err)
	case <-ctx.Done():
		t.Fatal("rejoin blocked")
	}
	defer next.Cancel()
	event(t, next)
	if next.monitor == m {
		t.Fatal("joined a retired monitor")
	}
	eventually(t, func() bool { return server.unsubCount.Load() == 1 })
	if server.subCount.Load() != 2 {
		t.Fatal("monitor not recreated once")
	}
}

func TestSharedMonitorConcurrentSubscribers(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	ch := client.Channel("test")
	var wg sync.WaitGroup
	subs := make([]*Subscription, 32)
	for i := range subs {
		wg.Add(1)
		go func() {
			defer wg.Done()
			s, err := ch.Subscribe(ctx)
			if err != nil {
				t.Error(err)
				return
			}
			subs[i] = s
		}()
	}
	wg.Wait()
	for _, s := range subs {
		if s == nil {
			t.Fatal("missing consumer")
		}
		event(t, s)
	}
	if server.subCount.Load() != 1 {
		t.Fatal("concurrent callers created multiple monitors")
	}
	for _, s := range subs {
		wg.Add(1)
		go func() { defer wg.Done(); s.Cancel() }()
	}
	wg.Wait()
	eventually(t, func() bool { return server.unsubCount.Load() == 1 })
	ch.monitorMu.Lock()
	n := len(ch.monitors)
	ch.monitorMu.Unlock()
	if n != 0 {
		t.Fatal("retired monitor retained in registry")
	}
}

func TestSharedMonitorErrorsFanOutOnce(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	handle := subscribeTest(t, ch, ctx)
	event(t, handle)
	second := subscribeTest(t, ch, ctx)
	event(t, second)
	a, err := client.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	b, err := client.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	<-a
	<-b
	handle.monitor.mu.Lock()
	id := handle.monitor.id
	handle.monitor.mu.Unlock()
	raw, _ := wire.Encode(wire.Frame{Command: wire.EventAdd, P2: id})
	payload := append(raw[:16:16], []byte("failure\x00")...)
	server.mu.Lock()
	var peer *testPeer
	for p := range server.peers {
		peer = p
		break
	}
	server.mu.Unlock()
	peer.send(wire.Frame{Command: wire.Error, P2: 0, Payload: payload})
	for _, errs := range []<-chan error{handle.Errors(), second.Errors(), client.Errors()} {
		select {
		case err := <-errs:
			if err == nil {
				t.Fatal("missing fan-out error")
			}
		case <-ctx.Done():
			t.Fatal("error fan-out blocked")
		}
	}
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if len(client.errors) != 0 || len(handle.errors) != 0 || len(second.errors) != 0 {
		t.Fatal("duplicate monitor error")
	}
}

func TestSharedMonitorFailureDropsCachedSnapshot(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	first := subscribeTest(t, ch, ctx)
	event(t, first)
	server.suppressEvents.Store(true)
	m := first.monitor
	m.mu.Lock()
	conn, id := m.conn, m.id
	m.mu.Unlock()
	m.reject(conn, id, ErrDisconnected)
	select {
	case <-first.Errors():
	case <-ctx.Done():
		t.Fatal("missing monitor error")
	}
	eventually(t, func() bool { return server.subCount.Load() == 2 })
	second := subscribeTest(t, ch, ctx)
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if len(second.events) != 0 {
		t.Fatal("replayed cached value after monitor failure")
	}
	server.suppressEvents.Store(false)
	putTest(t, ch, ctx, 6)
	if event(t, first).Value.Float64() != 6 || event(t, second).Value.Float64() != 6 {
		t.Fatal("missing fresh snapshot")
	}
}
