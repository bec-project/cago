package cago

import (
	"context"
	"testing"
	"time"
)

func TestMonitorActivationQueueTimeoutBackoff(t *testing.T) {
	server := newTestIOC(t)
	c, err := New(context.Background(), Options{SearchAddresses: []string{server.tcp.Addr().String()}, DisableRepeater: true, ContextTimeout: 50 * time.Millisecond, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: time.Second, InactivityTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("matrix")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	b := ch.binding
	ch.mu.Unlock()
	b.conn.gate <- struct{}{}
	before := b.conn.ids.Load()
	sub, err := ch.Subscribe(ctx)
	if err != nil {
		<-b.conn.gate
		t.Fatal(err)
	}
	time.Sleep(250 * time.Millisecond)
	attempts := b.conn.ids.Load() - before
	errorsQueued := len(sub.errors)
	<-b.conn.gate
	sub.Cancel()
	t.Logf("during 250ms gate contention with 50ms send timeout: activation attempts=%d, queued errors=%d", attempts, errorsQueued)
	if attempts > 3 {
		t.Fatalf("activation retries have no failure backoff: %d attempts", attempts)
	}
}

func TestMonitorGenerationDiscardsStaleRestart(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	cfg, _ := requestConfig(nil)
	sourceCtx, stop := context.WithCancel(ctx)
	m := &sharedMonitor{ch: ch, ctx: sourceCtx, cancel: stop, done: make(chan struct{}), restart: make(chan struct{}, 1), cfg: cfg, key: monitorKey{typ: Native, mask: cfg.mask}, members: make(map[*Subscription]struct{})}
	m.restart <- struct{}{}
	go m.run()
	defer func() { stop(); <-m.done }()
	eventually(t, func() bool {
		m.mu.Lock()
		defer m.mu.Unlock()
		return m.latest != nil
	})
	time.Sleep(30 * time.Millisecond)
	if got := server.unsubCount.Load(); got != 0 {
		t.Fatalf("stale restart cancelled new generation: %d cancellations", got)
	}
	m.mu.Lock()
	latest := m.latest
	m.mu.Unlock()
	if latest == nil {
		t.Fatal("new generation did not deliver a value")
	}
}

func TestDisconnectInterruptsMonitorRetryBackoff(t *testing.T) {
	server := newTestIOC(t)
	server.reject.Store(1)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	sub, err := ch.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	for attempt := 0; attempt < 4; attempt++ {
		select {
		case <-sub.Errors():
		case <-ctx.Done():
			t.Fatal("rejection not observed")
		}
		if attempt < 3 {
			server.reject.Store(1)
		}
	}
	eventually(t, func() bool { return server.unsubCount.Load() >= 4 })
	ch.mu.Lock()
	old := ch.binding
	ch.mu.Unlock()
	started := time.Now()
	server.disconnect()
	eventually(t, func() bool { ch.mu.Lock(); defer ch.mu.Unlock(); return ch.binding != nil && ch.binding != old })
	t.Logf("channel restored after %s; monitor additions=%d", time.Since(started), server.subCount.Load())
	deadline := time.Now().Add(200 * time.Millisecond)
	for server.subCount.Load() < 5 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if server.subCount.Load() < 5 {
		t.Fatalf("fresh binding remains without monitor while old generation backoff runs: %s elapsed", time.Since(started))
	}
}
