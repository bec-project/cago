package ca

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"runtime"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// delayedCreateReturnConn models a writer being descheduled after the socket
// accepted Create while the independent receiver handles creation and loss.
type delayedCreateReturnConn struct {
	net.Conn
	conn         *circuit
	disconnected chan struct{}
	resume       chan struct{}
	clears       atomic.Int32
	once         sync.Once
}

func (c *delayedCreateReturnConn) Write(p []byte) (int, error) {
	if len(p) >= 16 && binary.BigEndian.Uint16(p[:2]) == wire.Clear {
		c.clears.Add(1)
	}
	n, err := c.Conn.Write(p)
	if err == nil && len(p) >= 16 && binary.BigEndian.Uint16(p[:2]) == wire.Create {
		c.conn.mu.Lock()
		b := c.conn.bindings[binary.BigEndian.Uint32(p[8:12])]
		c.conn.mu.Unlock()
		<-b.lost
		c.once.Do(func() { close(c.disconnected) })
		<-c.resume
	}
	return n, err
}

func TestCreateAndImmediateDisconnectStayChannelLocal(t *testing.T) {
	for _, command := range []uint16{wire.CreateFail, wire.Disconnect} {
		for _, finish := range []string{"open", "cancel", "deadline"} {
			t.Run(fmt.Sprintf("command_%d_%s", command, finish), func(t *testing.T) {
				ctx, cancel := context.WithCancel(context.Background())
				local, peer := net.Pipe()
				socket := &delayedCreateReturnConn{Conn: local, disconnected: make(chan struct{}), resume: make(chan struct{})}
				client := &Client{ctx: ctx, opts: Options{SearchTransport: TCP, MaxConcurrentCreates: 4, ContextTimeout: time.Second, InactivityTimeout: time.Second, SearchInterval: time.Second, MaxSearchPeriod: time.Second}, channels: make(map[channelKey]*Channel), byID: make(map[uint32]*Channel), circuits: make(map[string]*dialEntry)}
				if finish == "deadline" {
					client.opts.ContextTimeout = 100 * time.Millisecond
				}
				const address = "127.0.0.1:5064"
				conn := newCircuit(client, socket, address+"/0", address)
				socket.conn = conn
				conn.peerMinor.Store(13)
				ready := make(chan struct{})
				close(ready)
				client.circuits[conn.key] = &dialEntry{conn: conn, ready: ready}
				healthy := &binding{lost: make(chan struct{})}
				conn.bindings[9000] = healthy
				readDone := make(chan struct{})
				go func() { conn.readLoop(); close(readDone) }()
				serverDone := make(chan struct{})
				go func() {
					defer close(serverDone)
					request, err := wire.Read(peer, 1024)
					if err != nil {
						return
					}
					created, _ := wire.Encode(wire.Frame{Command: wire.Create, Type: uint16(Double), Count: 1, P1: request.P1, P2: 7})
					lost, _ := wire.Encode(wire.Frame{Command: command, P1: request.P1})
					if _, err = peer.Write(append(created, lost...)); err != nil {
						return
					}
					for {
						_, err := wire.Read(peer, 1024)
						if err != nil {
							return
						}
					}
				}()
				ch := client.Channel("target")
				ch.results <- address
				var resume sync.Once
				defer func() {
					resume.Do(func() { close(socket.resume) })
					cancel()
					local.Close()
					peer.Close()
					ch.Close()
					client.wg.Wait()
					<-readDone
					<-serverDone
				}()
				select {
				case <-socket.disconnected:
				case <-time.After(time.Second):
					t.Fatal("receiver did not consume Create+disconnect")
				}
				if finish == "cancel" {
					ch.cancel()
				} else if finish == "deadline" {
					// Let the Create deadline become ready alongside the already
					// consumed reply and channel loss before Write returns.
					time.Sleep(2 * client.opts.ContextTimeout)
				}
				resume.Do(func() { close(socket.resume) })
				if finish != "cancel" {
					select {
					case update := <-ch.updates:
						if update.Info.State == Connected {
							t.Error("published a binding already known to be disconnected")
						}
					case <-time.After(time.Second):
						t.Fatal("channel did not finish its handshake")
					}
				}
				ch.Close()
				select {
				case <-healthy.lost:
					t.Error("one channel's disconnect retired the shared circuit")
				default:
				}
				if socket.clears.Load() != 0 {
					t.Error("sent Clear for a server-retired SID")
				}
			})
		}
	}
}

func TestPutRejectsUnterminatedDecodedStrings(t *testing.T) {
	for _, count := range []uint32{1, 2} {
		for _, notify := range []bool{false, true} {
			t.Run(fmt.Sprintf("count_%d_notify_%v", count, notify), func(t *testing.T) {
				payload := bytes.Repeat([]byte{'x'}, int(count)*40)
				if count == 2 {
					payload[0] = 0
				}
				v, err := DecodeValue(String, count, payload)
				if err != nil {
					t.Fatal("reads must preserve full STRING slots", err)
				}
				var sent atomic.Bool
				ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
					sent.Store(true)
					return wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2}
				})
				if notify {
					err = ch.PutNotify(ch.ctx, v)
				} else {
					err = ch.Put(ch.ctx, v)
				}
				if err == nil || sent.Load() {
					t.Fatalf("unterminated STRING was sent: err=%v sent=%v", err, sent.Load())
				}
			})
		}
	}
}

type clearRecordingConn struct {
	net.Conn
	clears atomic.Int32
}

func (c *clearRecordingConn) Write(p []byte) (int, error) {
	if len(p) >= 16 && binary.BigEndian.Uint16(p[:2]) == wire.Clear {
		c.clears.Add(1)
	}
	return c.Conn.Write(p)
}

func TestQueuedClearSkipsLaterServerDisconnect(t *testing.T) {
	for _, command := range []uint16{wire.CreateFail, wire.Disconnect} {
		t.Run(fmt.Sprint(command), func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			local, peer := net.Pipe()
			socket := &clearRecordingConn{Conn: local}
			client := &Client{ctx: ctx, opts: Options{SearchTransport: TCP, MaxConcurrentCreates: 4, ContextTimeout: time.Second, InactivityTimeout: time.Second, SearchInterval: time.Second, MaxSearchPeriod: time.Second}, channels: make(map[channelKey]*Channel), byID: make(map[uint32]*Channel), circuits: make(map[string]*dialEntry)}
			const address = "127.0.0.1:5064"
			conn := newCircuit(client, socket, address+"/0", address)
			conn.peerMinor.Store(13)
			ready := make(chan struct{})
			close(ready)
			client.circuits[conn.key] = &dialEntry{conn: conn, ready: ready}
			healthy := &binding{lost: make(chan struct{})}
			conn.bindings[9000] = healthy
			readDone := make(chan struct{})
			go func() { conn.readLoop(); close(readDone) }()
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				request, err := wire.Read(peer, 1024)
				if err != nil {
					return
				}
				created, _ := wire.Encode(wire.Frame{Command: wire.Create, Type: uint16(Double), Count: 1, P1: request.P1, P2: 7})
				if _, err = peer.Write(created); err != nil {
					return
				}
				for {
					if _, err = wire.Read(peer, 1024); err != nil {
						return
					}
				}
			}()
			ch := client.Channel("target")
			ch.results <- address
			gateHeld := false
			defer func() {
				if gateHeld {
					<-conn.gate
				}
				cancel()
				local.Close()
				peer.Close()
				ch.Close()
				client.wg.Wait()
				<-readDone
				<-serverDone
			}()
			connectCtx, stop := context.WithTimeout(ctx, time.Second)
			defer stop()
			if err := ch.Connect(connectCtx); err != nil {
				t.Fatal(err)
			}
			ch.mu.Lock()
			b := ch.binding
			ch.mu.Unlock()
			conn.gate <- struct{}{}
			gateHeld = true
			closed := make(chan struct{})
			go func() { ch.Close(); close(closed) }()
			deadline := time.Now().Add(500 * time.Millisecond)
			stack := make([]byte, 1<<20)
			for {
				// Observe the queued send itself, so loss cannot accidentally
				// precede the outer alive check and hide the gate race.
				n := runtime.Stack(stack, true)
				if bytes.Contains(stack[:n], []byte("github.com/wakonig/cago/internal/ca.(*circuit).sendBound")) {
					break
				}
				if time.Now().After(deadline) {
					t.Fatal("Clear did not queue behind the socket gate")
				}
				runtime.Gosched()
			}
			lost, _ := wire.Encode(wire.Frame{Command: command, P1: ch.id})
			if _, err := peer.Write(lost); err != nil {
				t.Fatal(err)
			}
			select {
			case <-b.lost:
			case <-connectCtx.Done():
				t.Fatal("receiver did not consume the channel disconnect")
			}
			<-conn.gate
			gateHeld = false
			<-closed
			if socket.clears.Load() != 0 {
				t.Fatal("queued Clear transmitted a server-retired SID")
			}
			select {
			case <-healthy.lost:
				t.Fatal("skipped Clear retired the shared circuit")
			default:
			}
		})
	}
}
