package cago

import (
	"context"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"testing"
	"time"
)

func responseReviewChannel(t *testing.T, reply func(wire.Frame) wire.Frame, limits ...int) *Channel {
	limit := 16384
	if len(limits) != 0 {
		limit = limits[0]
	}
	a, b := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	c := &Client{opts: Options{ContextTimeout: time.Second, InactivityTimeout: time.Second, MaxArrayBytes: limit}, ctx: ctx, circuits: make(map[string]*dialEntry)}
	s := newCircuit(c, a, "", "127.0.0.1:5064")
	s.peerMinor.Store(13)
	ch := &Channel{client: c, ctx: ctx, binding: &binding{conn: s, native: Double, count: 100, lost: make(chan struct{})}, changed: make(chan struct{})}
	go s.readLoop()
	go func() { f, _ := wire.Read(b, 16384); out, _ := wire.Encode(reply(f)); b.Write(out) }()
	t.Cleanup(func() { cancel(); a.Close(); b.Close() })
	return ch
}
func TestWrongCommandDoesNotAcknowledgePut(t *testing.T) {
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame { return wire.Frame{Command: wire.ReadNotify, P1: 1, P2: f.P2} })
	v, _ := NewValue(1.0)
	if err := ch.PutNotify(ch.ctx, v); err == nil {
		t.Fatal("PutNotify succeeded on ReadNotify response")
	}
}
func TestGetUsesReturnedCount(t *testing.T) {
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 2, P1: 1, P2: f.P2, Payload: make([]byte, 16)}
	})
	v, err := ch.Get(ch.ctx, WithCount(1))
	if err != nil || v.Count() != 2 {
		t.Fatalf("returned count=%d err=%v", v.Count(), err)
	}
}
func TestGetUsesReturnedType(t *testing.T) {
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(Long), Count: 1, P1: 1, P2: f.P2, Payload: make([]byte, 4)}
	})
	if v, err := ch.Get(ch.ctx); err != nil || v.Type() != Long {
		t.Fatalf("returned type=%d err=%v", v.Type(), err)
	}
}

func TestReadProtocolCountLimit(t *testing.T) {
	s := &circuit{}
	for _, minor := range []uint32{8, 13} {
		s.peerMinor.Store(minor)
		maxBytes := uint64(0xfffffff0)
		if minor < 9 {
			maxBytes = 16384
		}
		for _, typ := range []DataType{Double, 34} {
			maximum := uint32((maxBytes - uint64(valueOffset(typ)) - uint64(typ.Size())) / uint64(typ.Size()))
			if err := s.validateReadSize(typ, maximum); err != nil {
				t.Fatal(err)
			}
			if err := s.validateReadSize(typ, maximum+1); err == nil {
				t.Fatalf("minor=%d type=%d count above protocol limit accepted", minor, typ)
			}
		}
	}
}

func TestGetCompactScalarString(t *testing.T) {
	const message = "User Operation 400mA top-up"
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
		payload := make([]byte, 32)
		copy(payload, message)
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(String), Count: 1, P1: 1, P2: f.P2, Payload: payload}
	})
	v, err := ch.Get(ch.ctx, WithType(String), WithCount(1))
	if err != nil || v.Text() != message {
		t.Fatalf("compact STRING read: text=%q err=%v", v.Text(), err)
	}
}
