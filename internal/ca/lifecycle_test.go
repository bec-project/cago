package ca

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"reflect"
	"sync"
	"testing"
	"time"
)

func TestCircuitReadTimeoutTracksInactivity(t *testing.T) {
	for _, progress := range []bool{false, true} {
		t.Run(fmt.Sprint(progress), func(t *testing.T) {
			a, peer := net.Pipe()
			c := &Client{opts: Options{InactivityTimeout: 100 * time.Millisecond, MaxArrayBytes: 512}, circuits: make(map[string]*dialEntry)}
			s := newCircuit(c, a, "", "")
			result := make(chan response, 1)
			s.requests[1] = pendingResponse{command: wire.ReadNotify, result: result}
			finished := make(chan struct{})
			go func() { s.readLoop(); close(finished) }()
			var writerDone chan struct{}
			t.Cleanup(func() {
				peer.Close()
				a.Close()
				<-finished
				if writerDone != nil {
					<-writerDone
				}
			})
			if !progress {
				select {
				case <-s.done:
				case <-time.After(time.Second):
					t.Fatal("idle socket did not time out")
				}
				return
			}
			writerDone = make(chan struct{})
			go func() {
				defer close(writerDone)
				// The complete frame takes at least 160ms; each read progresses
				// well within the 100ms inactivity interval.
				frame, _ := wire.Encode(wire.Frame{Command: wire.Echo, Payload: make([]byte, 128)})
				for i := 0; i < len(frame); i += 16 {
					if _, err := peer.Write(frame[i : i+16]); err != nil {
						return
					}
					time.Sleep(20 * time.Millisecond)
				}
				frame, _ = wire.Encode(wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 1, P1: 1, P2: 1, Payload: make([]byte, 8)})
				peer.Write(frame)
			}()
			select {
			case reply := <-result:
				if reply.err != nil {
					t.Fatal(reply.err)
				}
			case <-s.done:
				t.Fatal("continuously progressing frame was timed out")
			case <-time.After(time.Second):
				t.Fatal("response was not delivered")
			}
		})
	}
}

type writeSignalConn struct {
	net.Conn
	writing chan struct{}
	once    sync.Once
}

func (c *writeSignalConn) Write(p []byte) (int, error) {
	c.once.Do(func() { close(c.writing) })
	return c.Conn.Write(p)
}

func TestBindingDisconnectIsNotCallerCancel(t *testing.T) {
	for _, callerCancelled := range []bool{false, true} {
		a, peer := net.Pipe()
		ctx, cancel := context.WithCancel(context.Background())
		c := &Client{opts: Options{ContextTimeout: time.Second, MaxArrayBytes: 512}, circuits: make(map[string]*dialEntry)}
		socket := &writeSignalConn{Conn: a, writing: make(chan struct{})}
		s := newCircuit(c, socket, "", "")
		b := &binding{ctx: ctx, cancel: cancel, conn: s, native: Double, count: 1, lost: make(chan struct{})}
		ch := &Channel{client: c, binding: b, changed: make(chan struct{})}
		caller, stop := context.WithTimeout(context.Background(), time.Second)
		result := make(chan error, 1)
		v, _ := NewValue(1.0)
		go func() { result <- ch.PutNotify(caller, v) }()
		select {
		case <-socket.writing:
		case <-caller.Done():
			t.Fatal("write did not begin")
		}
		want := ErrDisconnected
		if callerCancelled {
			stop()
			want = context.Canceled
		}
		b.disconnect()
		if err := <-result; !errors.Is(err, want) {
			t.Fatalf("caller cancelled=%v: got %v, want %v", callerCancelled, err, want)
		}
		stop()
		a.Close()
		peer.Close()
	}
}

func TestHeartbeatQueueTimeoutPreservesCircuit(t *testing.T) {
	a, peer := net.Pipe()
	defer a.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{ctx: ctx, opts: Options{ContextTimeout: 5 * time.Millisecond, EchoInterval: time.Millisecond, CircuitIdleTimeout: time.Hour}, circuits: make(map[string]*dialEntry)}
	s := newCircuit(c, a, "", "")
	s.gate <- struct{}{}
	finished := make(chan struct{})
	go func() { s.heartbeat(); close(finished) }()
	select {
	case <-s.done:
		t.Fatal("queued Echo timeout retired circuit without attempting a write")
	case <-time.After(30 * time.Millisecond):
	}
	cancel()
	<-finished
	<-s.gate
}

func TestReconnectStatsCountSuccessfulRestoration(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	b := ch.binding
	ch.mu.Unlock()
	b.conn.fail(ErrDisconnected)
	deadline := time.Now().Add(time.Second)
	for c.Stats().Reconnects != 1 {
		if time.Now().After(deadline) {
			t.Fatal("successful restoration was not counted")
		}
		time.Sleep(time.Millisecond)
	}
	if err := ch.Close(); err != nil {
		t.Fatal(err)
	}
	if got := c.Stats().Reconnects; got != 1 {
		t.Fatalf("normal closure counted as reconnect: %d", got)
	}
}

func TestRejectedChannelCreationBacksOff(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel()
	ch := c.Channel("rejected")
	if err := ch.Connect(ctx); err == nil {
		t.Fatal("rejected channel connected")
	}
	ch.Close()
	if attempts := s.createCount.Load(); attempts > 8 {
		t.Fatalf("rejected channel retried without backoff: %d attempts", attempts)
	}
}

func TestClearTimeoutRetiresCircuit(t *testing.T) {
	s := newTestIOC(t)
	c, err := New(context.Background(), Options{SearchAddresses: []string{s.tcp.Addr().String()}, DisableRepeater: true, ContextTimeout: 30 * time.Millisecond, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: time.Second, InactivityTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	healthy := c.Channel("healthy")
	closing := c.Channel("closing")
	if err = healthy.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err = closing.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	closing.mu.Lock()
	b := closing.binding
	closing.mu.Unlock()
	b.conn.gate <- struct{}{}
	if err = closing.Close(); err != nil {
		t.Fatal(err)
	}
	<-b.conn.gate
	select {
	case <-b.conn.done:
	default:
		t.Fatal("failed Clear did not retire the circuit")
	}
	if _, err = healthy.Get(ctx); err != nil {
		t.Fatalf("healthy channel did not reconnect: %v", err)
	}

}
func TestQueuedWriteCannotPassChannelClear(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("closed-target")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	healthy := c.Channel("healthy")
	if err := healthy.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	b := ch.binding
	ch.mu.Unlock()
	b.conn.gate <- struct{}{}
	before := b.conn.ids.Load()
	value, _ := NewValue(77.0)
	result := make(chan error, 1)
	go func() { result <- ch.PutNotify(ctx, value) }()
	deadline := time.Now().Add(time.Second)
	for b.conn.ids.Load() == before {
		if time.Now().After(deadline) {
			<-b.conn.gate
			t.Fatal("write did not capture binding")
		}
		time.Sleep(time.Millisecond)
	}
	closed := make(chan struct{})
	go func() { ch.Close(); close(closed) }()
	select {
	case <-b.lost:
	case <-ctx.Done():
		<-b.conn.gate
		t.Fatal("Clear did not disconnect binding")
	}
	<-b.conn.gate
	<-closed
	writeErr := <-result
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	s.mu.Lock()
	got := s.value
	s.mu.Unlock()
	t.Logf("Clear count=%d; queued PutNotify result=%v; IOC value=%g", s.clears.Load(), writeErr, got)
	if got == 77 {
		t.Fatal("queued write transmitted after channel was cleared")
	}
	if writeErr == nil {
		t.Fatal("closed-channel write reported success")
	}
}

// recordedCommandConn observes writes without replacing a live circuit socket.
type recordedCommandConn struct {
	net.Conn
	commands []uint16
}

func (c *recordedCommandConn) Write(p []byte) (int, error) {
	c.commands = append(c.commands, binary.BigEndian.Uint16(p))
	return c.Conn.Write(p)
}

func TestMonitorCancellationAfterClearPreservesCircuit(t *testing.T) {
	a, peer := net.Pipe()
	defer a.Close()
	defer peer.Close()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	client := &Client{ctx: context.Background(), opts: Options{ContextTimeout: time.Second}, circuits: make(map[string]*dialEntry)}
	socket := &recordedCommandConn{Conn: a}
	conn := newCircuit(client, socket, "", "")
	b := &binding{ctx: ctx, cancel: cancel, conn: conn, sid: 1, cid: 2, lost: make(chan struct{})}
	ch := &Channel{client: client}
	m := &sharedMonitor{ch: ch, binding: b, conn: conn, id: 3}
	readerDone := make(chan struct{})
	go func() {
		defer close(readerDone)
		for {
			if _, err := wire.Read(peer, 512); err != nil {
				return
			}
		}
	}()
	defer func() { peer.Close(); <-readerDone }()
	// Channel.Close cancels the binding's parent before sending Clear. The
	// lost signal is closed afterward, leaving teardown a window to run here.
	cancel()
	if err := conn.send(context.Background(), wire.Frame{Command: wire.Clear, P1: b.sid, P2: b.cid}); err != nil {
		t.Fatal(err)
	}
	m.deactivate(b, 3, Double, true)
	if !reflect.DeepEqual(socket.commands, []uint16{wire.Clear}) {
		t.Fatalf("monitor sent a command after Clear: %v", socket.commands)
	}
	select {
	case <-conn.done:
		t.Fatal("channel cleanup retired the shared circuit")
	default:
	}
}
