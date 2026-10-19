package ca

import (
	"context"
	"fmt"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"sync"
	"testing"
	"time"
)

type pausedCloseConn struct {
	net.Conn
	entered, release chan struct{}
	once             sync.Once
}

func (p *pausedCloseConn) Close() error {
	p.once.Do(func() { close(p.entered); <-p.release })
	return p.Conn.Close()
}
func TestIdleRetirementRejectsNewBindings(t *testing.T) {
	server := newTestIOC(t)
	client, err := New(context.Background(), Options{SearchAddresses: []string{server.tcp.Addr().String()}, DisableRepeater: true, ContextTimeout: time.Second, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: 10 * time.Millisecond, InactivityTimeout: time.Second, CircuitIdleTimeout: time.Millisecond})
	if err != nil {
		t.Fatal(err)
	}
	nc, err := net.Dial("tcp4", server.tcp.Addr().String())
	if err != nil {
		client.Close()
		t.Fatal(err)
	}
	socket := &pausedCloseConn{Conn: nc, entered: make(chan struct{}), release: make(chan struct{})}
	var release sync.Once
	resume := func() { release.Do(func() { close(socket.release) }) }
	defer func() { resume(); client.Close() }()
	address := server.tcp.Addr().String()
	key := address + "/0"
	conn := newCircuit(client, socket, key, address)
	ready := make(chan struct{})
	close(ready)
	client.mu.Lock()
	client.circuits[key] = &dialEntry{conn: conn, ready: ready}
	client.mu.Unlock()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := conn.send(ctx, wire.Frame{Command: wire.Version, Count: wire.Minor}); err != nil {
		t.Fatal(err)
	}
	client.start(conn.readLoop)
	conn.mu.Lock()
	conn.idleSince = time.Now().Add(-time.Second)
	conn.mu.Unlock()
	client.start(conn.heartbeat)
	select {
	case <-socket.entered:
	case <-ctx.Done():
		t.Fatal("retirement did not start")
	}
	ch := client.Channel("newly-active")
	result := make(chan error, 1)
	go func() {
		value, err := ch.Get(ctx)
		if err == nil && value.Float64() != 1.25 {
			err = fmt.Errorf("unexpected value %v", value.Slice())
		}
		result <- err
	}()
	select {
	case err := <-result:
		t.Fatalf("new request used a retiring circuit: %v", err)
	case <-time.After(50 * time.Millisecond):
	}
	conn.mu.Lock()
	attached := len(conn.bindings)
	conn.mu.Unlock()
	if attached != 0 {
		t.Fatal("new binding attached after idle retirement decision")
	}
	resume()
	select {
	case err := <-result:
		if err != nil {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("new request did not reconnect")
	}
	ch.mu.Lock()
	replacement := ch.binding
	ch.mu.Unlock()
	if replacement == nil || replacement.conn == conn {
		t.Fatal("request did not use a fresh circuit")
	}
}
