package ca

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

// scriptedProtocolChannel owns both ends of a protocol connection so tests can
// inspect requests and return responses that the normal IOC fixture never sends.
func scriptedProtocolChannel(t *testing.T, capacity uint32, reply func(wire.Frame) wire.Frame) (*Channel, <-chan wire.Frame) {
	t.Helper()
	local, remote := net.Pipe()
	ctx, cancel := context.WithCancel(context.Background())
	c := &Client{ctx: ctx, opts: Options{ContextTimeout: time.Second, InactivityTimeout: time.Second, MaxArrayBytes: 512}, circuits: make(map[string]*dialEntry), errors: make(chan error, 32)}
	conn := newCircuit(c, local, "scripted", "127.0.0.1:5064")
	conn.peerMinor.Store(13)
	bctx, bcancel := context.WithCancel(ctx)
	b := &binding{ctx: bctx, cancel: bcancel, conn: conn, cid: 1, sid: 2, native: Double, count: capacity, lost: make(chan struct{})}
	ch := &Channel{client: c, ctx: ctx, name: "scripted", binding: b, info: ChannelInfo{State: Connected}, changed: make(chan struct{})}
	requests := make(chan wire.Frame, 8)
	readerDone, serverDone := make(chan struct{}), make(chan struct{})
	go func() { defer close(readerDone); conn.readLoop() }()
	go func() {
		defer close(serverDone)
		for {
			f, err := wire.Read(remote, 4096)
			if err != nil {
				return
			}
			select {
			case requests <- f:
			case <-ctx.Done():
				return
			}
			data, err := wire.Encode(reply(f))
			if err != nil {
				t.Errorf("encode scripted response: %v", err)
				return
			}
			if _, err := remote.Write(data); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		cancel()
		b.disconnect()
		conn.fail(ErrClosed)
		remote.Close()
		<-readerDone
		<-serverDone
	})
	return ch, requests
}

func TestAlarmAcknowledgementWireSequence(t *testing.T) {
	for _, transient := range []bool{false, true} {
		t.Run(map[bool]string{false: "persistent", true: "transient"}[transient], func(t *testing.T) {
			ch, requests := scriptedProtocolChannel(t, 1, func(f wire.Frame) wire.Frame {
				return wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := ch.Acknowledge(ctx, transient, 2); err != nil {
				t.Fatal(err)
			}
			wantTransient := uint16(0)
			if transient {
				wantTransient = 1
			}
			for _, want := range []struct {
				typ   DataType
				value uint16
			}{{PutACKT, wantTransient}, {PutACKS, 2}} {
				f := <-requests
				if f.Command != wire.WriteNotify || f.Type != uint16(want.typ) || f.Count != 1 || f.P1 != 2 || binary.BigEndian.Uint16(f.Payload) != want.value {
					t.Fatalf("acknowledgement request: %+v; want type=%d value=%d", f, want.typ, want.value)
				}
			}
		})
	}
}

func TestAlarmAcknowledgementStopsAfterRejectedTransient(t *testing.T) {
	ch, requests := scriptedProtocolChannel(t, 1, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.WriteNotify, P1: 47 << 3, P2: f.P2}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Acknowledge(ctx, true, 2); !errors.Is(err, ErrNoWrite) {
		t.Fatalf("got %v, want write rejection", err)
	}
	if f := <-requests; f.Type != uint16(PutACKT) {
		t.Fatalf("first request type=%d", f.Type)
	}
	select {
	case f := <-requests:
		t.Fatalf("sent second acknowledgement after rejection: %+v", f)
	default:
	}
}

func TestInvalidRequestsPreserveConnectedChannel(t *testing.T) {
	ch, requests := scriptedProtocolChannel(t, 1024, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 1, P1: 1, P2: f.P2, Payload: make([]byte, 8)}
	})
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	for _, tt := range []struct {
		name string
		opts []RequestOption
		want error
	}{
		{"bad_options", []RequestOption{WithBuffer(0)}, nil},
		{"incompatible_metadata", []RequestOption{WithType(STSACKString), WithMetadata(Time)}, ErrBadType},
		{"exceeds_capacity", []RequestOption{WithCount(1025)}, ErrBadCount},
	} {
		t.Run(tt.name, func(t *testing.T) {
			_, err := ch.Get(ctx, tt.opts...)
			if err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	empty, err := NewValue([]float64{})
	if err != nil {
		t.Fatal(err)
	}
	large, err := NewValue(make([]byte, 1025))
	if err != nil {
		t.Fatal(err)
	}
	metadata, err := DecodeValue(DataType(Status)*7+Double, 1, make([]byte, 16))
	if err != nil {
		t.Fatal(err)
	}
	for _, tt := range []struct {
		name  string
		value Value
		want  error
	}{
		{"empty", empty, ErrBadCount},
		{"metadata", metadata, ErrBadType},
		{"exceeds_capacity", large, ErrBadCount},
	} {
		t.Run(tt.name, func(t *testing.T) {
			if err := ch.Put(ctx, tt.value); err == nil || tt.want != nil && !errors.Is(err, tt.want) {
				t.Fatalf("got %v, want %v", err, tt.want)
			}
		})
	}
	select {
	case f := <-requests:
		t.Fatalf("invalid request reached server: %+v", f)
	default:
	}
	if _, err := ch.Get(ctx, WithCount(1)); err != nil {
		t.Fatalf("healthy request after validation errors: %v", err)
	}
}

func TestProtocolErrorCompletesRequestsWithServerDiagnostic(t *testing.T) {
	for _, operation := range []string{"read", "write"} {
		t.Run(operation, func(t *testing.T) {
			ch, _ := scriptedProtocolChannel(t, 1, func(f wire.Frame) wire.Frame {
				original, err := wire.Encode(f)
				if err != nil {
					t.Error(err)
				}
				return wire.Frame{Command: wire.Error, P2: 46 << 3, Payload: append(original[:16:16], []byte("access changed\x00")...)}
			})
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			command := wire.ReadNotify
			var err error
			if operation == "read" {
				_, err = ch.Get(ctx)
			} else {
				command = wire.WriteNotify
				value, valueErr := NewValue(1.0)
				if valueErr != nil {
					t.Fatal(valueErr)
				}
				err = ch.PutNotify(ctx, value)
			}
			var protocol *ProtocolError
			if !errors.As(err, &protocol) || protocol.Command != command || protocol.Message != "access changed" || !errors.Is(err, ErrNoRead) {
				t.Fatalf("lost server diagnostic: %v", err)
			}
			select {
			case reported := <-ch.client.Errors():
				if !errors.Is(reported, ErrNoRead) {
					t.Fatalf("asynchronous diagnostic: %v", reported)
				}
			case <-ctx.Done():
				t.Fatal("server diagnostic was not reported")
			}
		})
	}
}

func TestChannelIdentityPriorityAccessAndClosure(t *testing.T) {
	srv := newTestIOC(t)
	c := testClient(t, srv, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("identity", WithPriority(99))
	if ch.Name() != "identity" || c.Channel("identity", WithPriority(99)) != ch {
		t.Fatal("name/priority did not identify a shared handle")
	}
	other := c.Channel("identity", WithPriority(0))
	if other == ch {
		t.Fatal("different priorities shared a handle")
	}
	for _, channel := range []*Channel{ch, other} {
		if err := channel.Connect(ctx); err != nil {
			t.Fatal(err)
		}
	}
	info := ch.Info()
	if info.Name != "identity" || info.Priority != 99 || info.NativeType != Double || info.Count != 1 || info.Access != ReadAccess|WriteAccess || info.Host == "" || info.State != Connected {
		t.Fatalf("incomplete connected information: %+v", info)
	}
	if c.Stats().Circuits != 2 {
		t.Fatal("priorities shared a virtual circuit")
	}
	select {
	case change := <-ch.StateChanges():
		if change.Info != info || change.At.IsZero() || change.Err != nil {
			t.Fatalf("connected state change: %+v", change)
		}
	case <-ctx.Done():
		t.Fatal("missing connected state change")
	}
	srv.mu.Lock()
	peers := make([]*testPeer, 0, len(srv.peers))
	for peer := range srv.peers {
		peers = append(peers, peer)
	}
	srv.mu.Unlock()
	for _, peer := range peers {
		peer.mu.Lock()
		_, ownsChannel := peer.channels[ch.id]
		peer.mu.Unlock()
		if ownsChannel {
			peer.send(wire.Frame{Command: wire.Access, P1: ch.id, P2: uint32(ReadAccess)})
		}
	}
	select {
	case change := <-ch.StateChanges():
		if change.Info.Access != ReadAccess || ch.Info().Access != ReadAccess {
			t.Fatalf("access change was lost: %+v", change)
		}
	case <-ctx.Done():
		t.Fatal("missing access state change")
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-c.Done():
	default:
		t.Fatal("Close returned before Done closed")
	}
	if ch.Info().State != Closed {
		t.Fatal("client shutdown did not close channel")
	}
	if err := c.Channel("after-close").Connect(ctx); !errors.Is(err, ErrClosed) {
		t.Fatalf("new handle on closed client: %v", err)
	}
}

func TestInvalidChannelHandlesAreClosedAndUnregistered(t *testing.T) {
	c := testClient(t, newTestIOC(t), UDP)
	for _, tt := range []struct {
		name string
		opts []ChannelOption
	}{
		{"", nil},
		{strings.Repeat("x", 501), nil},
		{"embedded\x00nul", nil},
		{"bad-priority", []ChannelOption{WithPriority(100)}},
	} {
		ch := c.Channel(tt.name, tt.opts...)
		if err := ch.Connect(context.Background()); err == nil {
			t.Fatal("invalid handle connected")
		}
		if ch.Info().State != Closed {
			t.Fatal("invalid handle was not closed")
		}
		if err := ch.Close(); err != nil {
			t.Fatal(err)
		}
		if _, ok := <-ch.StateChanges(); ok {
			t.Fatal("invalid handle retained an open event stream")
		}
	}
	if c.Stats().Channels != 0 {
		t.Fatal("invalid handles were retained by the client")
	}
	c.channelIDs.Store(uint64(^uint32(0)))
	if err := c.Channel("exhausted").Connect(context.Background()); !errors.Is(err, ErrIDExhausted) {
		t.Fatalf("exhausted client returned %v", err)
	}
}
