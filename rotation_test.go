package cago

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

func rotationClient(t *testing.T, server *testIOC, drain time.Duration) *Client {
	t.Helper()
	c, err := New(context.Background(), Options{SearchAddresses: []string{server.tcp.Addr().String()}, DisableRepeater: true, ContextTimeout: 200 * time.Millisecond, RotationDrainTimeout: drain, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: time.Second, InactivityTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func rotationBinding(t *testing.T, ch *Channel) *binding {
	t.Helper()
	ch.mu.Lock()
	b := ch.binding
	ch.mu.Unlock()
	if b == nil {
		t.Fatal("no connected binding")
	}
	return b
}

func seedCircuitIDs(b *binding, n uint64) {
	b.conn.mu.Lock()
	b.conn.ids.Store(n)
	b.conn.mu.Unlock()
}

func waitRotation(t *testing.T, conn *circuit) {
	t.Helper()
	eventually(t, func() bool { conn.mu.Lock(); defer conn.mu.Unlock(); return conn.rotating })
}

func pausedReply(t *testing.T) (*testReplyPause, func()) {
	t.Helper()
	pause := &testReplyPause{entered: make(chan wire.Frame, 1), release: make(chan struct{})}
	var once sync.Once
	release := func() { once.Do(func() { close(pause.release) }) }
	t.Cleanup(release)
	return pause, release
}

func readAsync(ctx context.Context, ch *Channel) <-chan ReadResult {
	result := make(chan ReadResult, 1)
	go func() { v, err := ch.Get(ctx); result <- ReadResult{Value: v, Err: err} }()
	return result
}

func readReply(t *testing.T, ctx context.Context, result <-chan ReadResult) ReadResult {
	t.Helper()
	select {
	case got := <-result:
		return got
	case <-ctx.Done():
		t.Fatal("read did not finish")
		return ReadResult{Err: ctx.Err()}
	}
}

func TestIdentifierRotationRestoresSharedSubscriptions(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	a := subscribeTest(t, ch, ctx)
	first := event(t, a)
	b := subscribeTest(t, ch, ctx)
	event(t, b)
	old := rotationBinding(t, ch)
	channelIDs := client.channelIDs.Load()
	seedCircuitIDs(old, uint64(^uint32(0)))
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal(err)
	}
	replacement := rotationBinding(t, ch)
	if replacement.conn == old.conn {
		t.Fatal("exhausted connection was reused")
	}
	for _, sub := range []*Subscription{a, b} {
		snapshot := event(t, sub)
		if !snapshot.ReceivedAt.After(first.ReceivedAt) {
			t.Fatal("old cached snapshot survived rotation")
		}
	}
	if a.monitor != b.monitor {
		t.Fatal("local sharing was lost")
	}
	a.monitor.mu.Lock()
	id := a.monitor.id
	a.monitor.mu.Unlock()
	// Even a matching ID and shape from the retired source must be ignored.
	value, _ := NewValue(99.0)
	a.monitor.deliver(old.conn, wire.Frame{Command: wire.EventAdd, Type: uint16(Double), Count: 1, P1: 1, P2: id, Payload: value.Bytes()})
	for _, sub := range []*Subscription{a, b} {
		select {
		case e := <-sub.Events():
			t.Fatalf("old source delivered %v", e.Value.Slice())
		default:
		}
	}
	value, _ = NewValue(7.0)
	if err := ch.PutNotify(ctx, value); err != nil {
		t.Fatal(err)
	}
	for _, sub := range []*Subscription{a, b} {
		if got := event(t, sub).Value.Float64(); got != 7 {
			t.Fatal(got)
		}
	}
	if client.channelIDs.Load() != channelIDs {
		t.Fatal("I/O consumed channel identifiers")
	}
	if replacement.conn.ids.Load() > 16 {
		t.Fatal("new connection did not reset its identifiers")
	}
}

func TestIdentifierRotationDrainsAdmittedOperations(t *testing.T) {
	for _, write := range []bool{false, true} {
		name := "read"
		if write {
			name = "write"
		}
		t.Run(name, func(t *testing.T) {
			server := newTestIOC(t)
			client := rotationClient(t, server, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ch := client.Channel("test")
			if err := ch.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			old := rotationBinding(t, ch)
			seedCircuitIDs(old, uint64(^uint32(0))-1)
			pause, release := pausedReply(t)
			var reads <-chan ReadResult
			var writes chan error
			if write {
				server.writePause.Store(pause)
				writes = make(chan error, 1)
				v, _ := NewValue(7.0)
				go func() { writes <- ch.PutNotify(ctx, v) }()
			} else {
				server.readPause.Store(pause)
				reads = readAsync(ctx, ch)
			}
			select {
			case <-pause.entered:
			case <-ctx.Done():
				t.Fatal("operation did not reach IOC")
			}
			next := readAsync(ctx, ch)
			waitRotation(t, old.conn)
			select {
			case <-old.conn.done:
				t.Fatal("rotation interrupted an admitted operation")
			default:
			}
			release()
			if write {
				select {
				case err := <-writes:
					if err != nil {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("write did not finish")
				}
			} else {
				if got := readReply(t, ctx, reads); got.Err != nil {
					t.Fatal(got.Err)
				}
			}
			got := readReply(t, ctx, next)
			if got.Err != nil {
				t.Fatal(got.Err)
			}
			want := 1.25
			if write {
				want = 7
				if server.writes.Load() != 1 {
					t.Fatal("write was replayed")
				}
			}
			if got.Value.Float64() != want {
				t.Fatal(got.Value.Slice())
			}
			if rotationBinding(t, ch).conn == old.conn {
				t.Fatal("read did not move to replacement connection")
			}
		})
	}
}

func TestIdentifierRotationTimeoutDoesNotReplayWrite(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, 30*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0))-1)
	pause, release := pausedReply(t)
	server.writePause.Store(pause)
	written := make(chan error, 1)
	v, _ := NewValue(7.0)
	go func() { written <- ch.PutNotify(ctx, v) }()
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("write not received")
	}
	next := readAsync(ctx, ch)
	waitRotation(t, old.conn)
	select {
	case err := <-written:
		if !errors.Is(err, ErrDisconnected) {
			t.Fatalf("uncertain write returned %v", err)
		}
	case <-ctx.Done():
		t.Fatal("drain timeout did not terminate write")
	}
	got := readReply(t, ctx, next)
	if got.Err != nil || got.Value.Float64() != 7 {
		t.Fatalf("replacement read: %+v", got)
	}
	release() // Its late completion remains on the closed old connection.
	if server.writes.Load() != 1 {
		t.Fatalf("uncertain write replayed %d times", server.writes.Load())
	}
}

func TestIdentifierRotationWaitsForQueuedPlainWrite(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0)))
	old.conn.gate <- struct{}{}
	written := make(chan error, 1)
	v, _ := NewValue(8.0)
	go func() { written <- ch.Put(ctx, v) }()
	eventually(t, func() bool { old.conn.mu.Lock(); defer old.conn.mu.Unlock(); return old.conn.active == 1 })
	next := readAsync(ctx, ch)
	waitRotation(t, old.conn)
	select {
	case <-old.conn.done:
		<-old.conn.gate
		t.Fatal("queued plain write was not drained")
	default:
	}
	<-old.conn.gate
	select {
	case err := <-written:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("plain write did not finish")
	}
	got := readReply(t, ctx, next)
	if got.Err != nil || got.Value.Float64() != 8 {
		t.Fatalf("plain write lost: %+v", got)
	}
	if server.writes.Load() != 1 {
		t.Fatal("plain write was replayed")
	}
}

func TestIdentifierRotationOrdersPlainWriteBeforeNewRead(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0)))
	pause, release := pausedReply(t)
	server.applyPause.Store(pause)
	value, _ := NewValue(8.0)
	if err := ch.Put(ctx, value); err != nil {
		t.Fatal(err)
	}
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("plain write did not reach IOC")
	}
	next := readAsync(ctx, ch)
	waitRotation(t, old.conn)
	select {
	case got := <-next:
		t.Fatalf("replacement read overtook old plain write: value=%v err=%v", got.Value.Slice(), got.Err)
	case <-time.After(50 * time.Millisecond):
	}
	release()
	got := readReply(t, ctx, next)
	if got.Err != nil || got.Value.Float64() != 8 {
		t.Fatalf("ordered read: %+v", got)
	}
	if server.writes.Load() != 1 {
		t.Fatal("plain write replayed")
	}
}

func TestIdentifierRotationConcurrentRequests(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0))-1)
	const n = 24
	start := make(chan struct{})
	results := make(chan error, n)
	for range n {
		go func() { <-start; _, err := ch.Get(ctx); results <- err }()
	}
	close(start)
	for range n {
		select {
		case err := <-results:
			if err != nil {
				t.Fatal(err)
			}
		case <-ctx.Done():
			t.Fatal("concurrent requests stalled")
		}
	}
	replacement := rotationBinding(t, ch)
	if replacement.conn == old.conn {
		t.Fatal("connection did not rotate")
	}
	replacement.conn.mu.Lock()
	active, pending := replacement.conn.active, len(replacement.conn.requests)
	replacement.conn.mu.Unlock()
	if active != 0 || pending != 0 {
		t.Fatalf("unfinished operations: active=%d pending=%d", active, pending)
	}
}

func TestCloseInterruptsIdentifierRotationDrain(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0))-1)
	pause, _ := pausedReply(t)
	server.readPause.Store(pause)
	first := readAsync(ctx, ch)
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("request not received")
	}
	next := readAsync(ctx, ch)
	waitRotation(t, old.conn)
	closed := make(chan struct{})
	go func() { client.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("Close waited for drain timeout")
	}
	if readReply(t, ctx, first).Err == nil || readReply(t, ctx, next).Err == nil {
		t.Fatal("closed client reported successful reads")
	}
}

func TestIdentifierRotationIgnoresHeartbeatDuringBarrier(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0)))
	pause, release := pausedReply(t)
	server.echoPause.Store(pause)
	next := readAsync(ctx, ch)
	select {
	case f := <-pause.entered:
		if f.P2 != rotationEchoTag {
			t.Fatalf("unexpected Echo: %+v", f)
		}
	case <-ctx.Done():
		t.Fatal("barrier was not sent")
	}
	server.mu.Lock()
	var peer *testPeer
	for p := range server.peers {
		if p.net.RemoteAddr().String() == old.conn.socket.LocalAddr().String() {
			peer = p
			break
		}
	}
	server.mu.Unlock()
	if peer == nil {
		t.Fatal("old peer not found")
	}
	peer.send(wire.Frame{Command: wire.Echo})
	peer.send(wire.Frame{Command: wire.Echo, P2: rotationEchoTag, Count: 1})
	select {
	case got := <-next:
		t.Fatalf("unmatched Echo released barrier: %+v", got)
	case <-time.After(30 * time.Millisecond):
	}
	release()
	if got := readReply(t, ctx, next); got.Err != nil {
		t.Fatal(got.Err)
	}
}

func TestIdentifierRotationBarrierTimeout(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, 30*time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0)))
	pause, _ := pausedReply(t)
	server.echoPause.Store(pause)
	next := readAsync(ctx, ch)
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("barrier was not sent")
	}
	if got := readReply(t, ctx, next); got.Err != nil {
		t.Fatal(got.Err)
	}
	if rotationBinding(t, ch).conn == old.conn {
		t.Fatal("missing barrier reply prevented rotation forever")
	}
}

func TestIdentifierRotationWaitingOperationsRespectContext(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Hour)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0))-1)
	pause, _ := pausedReply(t)
	server.readPause.Store(pause)
	first := readAsync(ctx, ch)
	select {
	case <-pause.entered:
	case <-ctx.Done():
		t.Fatal("request not received")
	}
	next := readAsync(ctx, ch)
	waitRotation(t, old.conn)
	v, _ := NewValue(9.0)
	for name, op := range map[string]func(context.Context) error{
		"get":        func(ctx context.Context) error { _, err := ch.Get(ctx); return err },
		"put":        func(ctx context.Context) error { return ch.Put(ctx, v) },
		"put-notify": func(ctx context.Context) error { return ch.PutNotify(ctx, v) },
		"connect":    ch.Connect,
		"subscribe": func(ctx context.Context) error {
			sub, err := ch.Subscribe(ctx)
			if sub != nil {
				sub.Cancel()
			}
			return err
		},
	} {
		short, stop := context.WithTimeout(ctx, 10*time.Millisecond)
		if err := op(short); !errors.Is(err, context.DeadlineExceeded) {
			t.Errorf("%s: %v", name, err)
		}
		stop()
	}
	client.Close()
	if readReply(t, ctx, first).Err == nil || readReply(t, ctx, next).Err == nil {
		t.Fatal("closed reads succeeded")
	}
	if server.writes.Load() != 0 {
		t.Fatal("waiting cancelled writes reached IOC")
	}
}

func TestIdentifierRotationCanRepeat(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	for range 5 {
		old := rotationBinding(t, ch)
		seedCircuitIDs(old, uint64(^uint32(0)))
		if _, err := ch.Get(ctx); err != nil {
			t.Fatal(err)
		}
		if rotationBinding(t, ch).conn == old.conn {
			t.Fatal("exhausted connection was reused")
		}
	}
	if client.channelIDs.Load() != 1 {
		t.Fatal("rotations consumed channel IDs")
	}
}

func TestCancelledOperationsDoNotTriggerRotation(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	old := rotationBinding(t, ch)
	seedCircuitIDs(old, uint64(^uint32(0)))
	stopped, stop := context.WithCancel(ctx)
	stop()
	v, _ := NewValue(7.0)
	for name, op := range map[string]func() error{
		"get":        func() error { _, err := ch.Get(stopped); return err },
		"put":        func() error { return ch.Put(stopped, v) },
		"put-notify": func() error { return ch.PutNotify(stopped, v) },
		"connect":    func() error { return ch.Connect(stopped) },
		"subscribe": func() error {
			sub, err := ch.Subscribe(stopped)
			if sub != nil {
				sub.Cancel()
			}
			return err
		},
	} {
		if err := op(); !errors.Is(err, context.Canceled) {
			t.Errorf("%s: %v", name, err)
		}
	}
	old.conn.mu.Lock()
	rotated := old.conn.rotating
	old.conn.mu.Unlock()
	if rotated {
		t.Fatal("already-cancelled operation triggered connection rotation")
	}
	if server.writes.Load() != 0 {
		t.Fatal("cancelled write reached IOC")
	}
}
