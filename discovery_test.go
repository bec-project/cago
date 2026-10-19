package cago

import (
	"context"
	"errors"
	"fmt"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"sync"
	"testing"
	"time"
)

func TestRepeaterDNSCancellation(t *testing.T) {
	original := net.DefaultResolver
	started := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		once.Do(func() { close(started) })
		select {
		case <-release:
			return nil, errors.New("released")
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}}
	defer func() { net.DefaultResolver = original; close(release) }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		r, e := StartRepeater(ctx, "review-repeater.invalid:0")
		if r != nil {
			r.Close()
		}
		done <- e
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("resolver did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err == nil {
			t.Fatal("cancelled repeater startup succeeded")
		}
	case <-time.After(200 * time.Millisecond):
		t.Fatal("StartRepeater ignored cancellation during DNS")
	}
}
func TestTCPNameserverIsolation(t *testing.T) {
	for _, stalledFirst := range []bool{true, false} {
		t.Run(fmt.Sprint(stalledFirst), func(t *testing.T) {
			server := newTestIOC(t)
			addresses := []string{"127.0.0.1:1", server.tcp.Addr().String()}
			if !stalledFirst {
				addresses[0], addresses[1] = addresses[1], addresses[0]
			}
			c, err := New(context.Background(), Options{SearchTransport: TCP, SearchAddresses: addresses, DisableRepeater: true, ContextTimeout: time.Second})
			if err != nil {
				t.Fatal(err)
			}
			defer c.Close()
			// A cached nameserver whose receive window is full: writes stop progressing.
			socket, peer := net.Pipe()
			defer peer.Close()
			conn := newCircuit(c, socket, "127.0.0.1:1/0", "127.0.0.1:1")
			ready := make(chan struct{})
			close(ready)
			c.mu.Lock()
			c.circuits[conn.key] = &dialEntry{ready: ready, conn: conn}
			c.mu.Unlock()
			ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
			defer cancel()
			if _, err := c.Channel("test").Get(ctx); err != nil {
				t.Fatalf("stalled nameserver blocked healthy endpoint: %v", err)
			}
		})
	}
}

func TestRepeaterAlreadyCancelled(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	r, err := StartRepeater(ctx, "127.0.0.1:0")
	if r != nil {
		r.Close()
		t.Fatal("cancelled startup returned repeater")
	}
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want cancellation", err)
	}
}

func TestTCPDiscoveryReleasesPendingAtShutdown(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{ctx: ctx}
	s := &tcpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
	// Model Close winning just before a worker selects its initial wake.
	cancel()
	ch := &Channel{id: 1, ctx: ctx}
	s.pending[ch.id] = ch
	// Only cancellation is ready, making termination deterministic.
	s.run()
	s.mu.Lock()
	remaining := len(s.pending)
	s.mu.Unlock()
	if remaining != 0 {
		t.Fatalf("shutdown retained %d queued closed Channels", remaining)
	}
	s.enqueue(ch)
	s.mu.Lock()
	remaining = len(s.pending)
	s.mu.Unlock()
	if remaining != 0 {
		t.Fatal("post-shutdown enqueue retains Channel")
	}
}

func TestBeaconCacheBoundsAndExpiry(t *testing.T) {
	c := &Client{}
	for i := uint32(1); i <= 2*maxBeaconStates; i++ {
		c.beaconAnomaly(wire.Frame{P1: 1, P2: i, Type: 5064}, "127.0.0.1")
	}
	if len(c.beacons) != maxBeaconStates {
		t.Fatalf("beacon cache size %d", len(c.beacons))
	}
	key := "0.0.0.1:5064"
	old := c.beacons[key]
	old.seen = time.Now().Add(-3 * time.Minute)
	c.beacons[key] = old
	c.nextBeaconSweep = time.Time{}
	c.beaconAnomaly(wire.Frame{P1: 2, P2: 99999, Type: 5064}, "127.0.0.1")
	if _, ok := c.beacons[key]; ok {
		t.Fatal("expired beacon retained")
	}
	// Routine traffic must not rescan the entire cache between scheduled sweeps.
	key = "0.0.0.2:5064"
	old = c.beacons[key]
	old.seen = time.Now().Add(-3 * time.Minute)
	c.beacons[key] = old
	c.beaconAnomaly(wire.Frame{P1: 3, P2: 99999, Type: 5064}, "127.0.0.1")
	if _, ok := c.beacons[key]; !ok {
		t.Fatal("cache swept on every packet")
	}
}
