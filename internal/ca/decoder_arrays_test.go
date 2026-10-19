package ca

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"testing"
	"time"
)

func TestAutomaticArrayReceiveConfiguration(t *testing.T) {
	for _, setting := range []string{"", "YES", "yes", "YeS", "NO", "true", "1", "garbage", " YES"} {
		t.Run(setting, func(t *testing.T) {
			t.Setenv("EPICS_CA_AUTO_ARRAY_BYTES", setting)
			o := Options{SearchAddresses: []string{"127.0.0.1"}, MaxArrayBytes: 16384}
			if err := o.defaults(context.Background()); err != nil {
				t.Fatal(err)
			}
			automatic := setting == "" || setting == "YES" || setting == "yes" || setting == "YeS"
			if *o.AutoArrayBytes != automatic {
				t.Fatalf("setting %q automatic=%v", setting, *o.AutoArrayBytes)
			}
			if !automatic && o.receiveLimit() != 16408 {
				t.Fatalf("manual payload limit %d", o.receiveLimit())
			}
		})
	}
	for _, enabled := range []bool{false, true} {
		t.Setenv("EPICS_CA_AUTO_ARRAY_BYTES", "NO")
		requested := enabled
		o := Options{SearchAddresses: []string{"127.0.0.1"}, AutoArrayBytes: &requested, MaxArrayBytes: 512}
		if err := o.defaults(context.Background()); err != nil {
			t.Fatal(err)
		}
		requested = !requested
		if *o.AutoArrayBytes != enabled {
			t.Fatal("resolved array policy aliases caller option")
		}
		if !enabled && o.receiveLimit() != 16384 {
			t.Fatalf("minimum libca receive size=%d", o.receiveLimit())
		}
	}
}

func TestAutomaticReceiveAcceptsLargeArray(t *testing.T) {
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
		payload := bytes.Repeat([]byte{0x3f, 0xf4, 0, 0, 0, 0, 0, 0}, 2500)
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 2500, P1: 1, P2: f.P2, Payload: payload}
	})
	v, err := ch.Get(ch.ctx)
	if err != nil || v.Count() != 2500 || v.Float64() != 1.25 {
		t.Fatalf("large array: count=%d err=%v", v.Count(), err)
	}
}

func TestManualReceiveDrainsOversizedMessage(t *testing.T) {
	a, peer := net.Pipe()
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	automatic := false
	client := &Client{ctx: ctx, opts: Options{ContextTimeout: time.Second, InactivityTimeout: time.Second, MaxArrayBytes: 16384, AutoArrayBytes: &automatic}, circuits: make(map[string]*dialEntry)}
	conn := newCircuit(client, a, "", "")
	conn.peerMinor.Store(13)
	b := &binding{conn: conn, native: Double, count: 3000, lost: make(chan struct{})}
	ch := &Channel{client: client, ctx: ctx, binding: b, changed: make(chan struct{})}
	readerDone := make(chan struct{})
	go func() { conn.readLoop(); close(readerDone) }()
	writerDone := make(chan struct{})
	go func() {
		defer close(writerDone)
		request, err := wire.Read(peer, 1024)
		if err != nil {
			return
		}
		// Even an invalid command is skipped when its payload exceeds the buffer.
		oversized, _ := wire.Encode(wire.Frame{Command: 99, Payload: make([]byte, 16416)})
		normal, _ := wire.Encode(wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 2051, P1: 1, P2: request.P2, Payload: make([]byte, 16408)})
		peer.Write(append(oversized, normal...))
	}()
	defer func() { cancel(); a.Close(); peer.Close(); <-readerDone; <-writerDone }()
	v, err := ch.Get(ctx)
	if err != nil || v.Count() != 2051 {
		t.Fatalf("manual stream did not resume: count=%d err=%v", v.Count(), err)
	}
	select {
	case <-conn.done:
		t.Fatal("oversized frame retired the shared circuit")
	default:
	}
}

func TestArrayReceiveLimitDoesNotLimitWrites(t *testing.T) {
	for _, automatic := range []bool{true, false} {
		for _, notify := range []bool{false, true} {
			a, peer := net.Pipe()
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			client := &Client{ctx: ctx, opts: Options{ContextTimeout: time.Second, InactivityTimeout: time.Second, MaxArrayBytes: 512, AutoArrayBytes: &automatic}, circuits: make(map[string]*dialEntry)}
			conn := newCircuit(client, a, "", "")
			conn.peerMinor.Store(13)
			b := &binding{conn: conn, native: Double, count: 2500, lost: make(chan struct{})}
			ch := &Channel{client: client, ctx: ctx, binding: b, changed: make(chan struct{})}
			readerDone := make(chan struct{})
			go func() { conn.readLoop(); close(readerDone) }()
			received := make(chan wire.Frame, 1)
			serverDone := make(chan struct{})
			go func() {
				defer close(serverDone)
				f, err := wire.Read(peer, 30000)
				if err != nil {
					return
				}
				received <- f
				if notify {
					reply, _ := wire.Encode(wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2})
					peer.Write(reply)
				}
			}()
			v, _ := NewValue(make([]float64, 2500))
			var err error
			if notify {
				err = ch.PutNotify(ctx, v)
			} else {
				err = ch.Put(ctx, v)
			}
			if err != nil {
				t.Errorf("automatic=%v notify=%v: %v", automatic, notify, err)
			} else {
				select {
				case f := <-received:
					if len(f.Payload) != 20000 || f.Count != 2500 {
						t.Errorf("large write shape %+v", f)
					}
				case <-ctx.Done():
					t.Error("write was not received")
				}
			}
			cancel()
			a.Close()
			peer.Close()
			<-readerDone
			<-serverDone
		}
	}
}

func TestDrainIsInterruptedByTruncatedStream(t *testing.T) {
	a, peer := net.Pipe()
	automatic := false
	client := &Client{opts: Options{InactivityTimeout: time.Second, AutoArrayBytes: &automatic, MaxArrayBytes: 512}, circuits: make(map[string]*dialEntry)}
	conn := newCircuit(client, a, "", "")
	done := make(chan struct{})
	go func() { conn.readLoop(); close(done) }()
	frame, _ := wire.Encode(wire.Frame{Command: wire.ReadNotify, Payload: make([]byte, 20000)})
	go func() { peer.Write(frame[:32]); peer.Close() }()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("truncated drain stalled")
	}
	a.Close()
	select {
	case <-conn.done:
	default:
		t.Fatal("truncated payload did not disconnect")
	}
	var limit *wire.PayloadLimitError
	header := bytes.NewReader(frame[:16])
	_, err := wire.Read(header, 512)
	if !errors.As(err, &limit) || limit.Size != 20000 || limit.Limit != 512 {
		t.Fatalf("missing drain metadata: %v", err)
	}
}

func TestScalarStringWriteUsesCompactPayload(t *testing.T) {
	ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
		if f.Type != uint16(String) || f.Count != 1 || len(f.Payload) != 8 || !bytes.Equal(f.Payload[:6], []byte("ready\x00")) {
			t.Errorf("scalar STRING not compact: %+v", f)
		}
		return wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2}
	})
	v, _ := NewValue("ready")
	if err := ch.PutNotify(ch.ctx, v); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(v.Bytes(), append([]byte("ready"), make([]byte, 35)...)) {
		t.Fatal("write changed immutable value")
	}
}

func TestRawTimestampAndEnumCountsAreNotValidationErrors(t *testing.T) {
	timestamp := make([]byte, 24)
	binary.BigEndian.PutUint32(timestamp[4:8], 10)
	binary.BigEndian.PutUint32(timestamp[8:12], 1000000000)
	v, err := DecodeValue(20, 1, timestamp)
	if err != nil || v.Metadata().SecondsPastEpoch != 10 || v.Metadata().Nanoseconds != 1000000000 || !bytes.Equal(v.DBRBytes(), timestamp) {
		t.Fatalf("raw timestamp lost: %+v %v", v.Metadata(), err)
	}
	for _, count := range []uint16{0, 16, 17, 65535} {
		payload := make([]byte, 424)
		binary.BigEndian.PutUint16(payload[4:6], count)
		v, err := DecodeValue(24, 1, payload)
		want := max(0, min(int(int16(count)), 16))
		if err != nil || v.Metadata().EnumCount != int16(count) || len(v.Metadata().EnumLabels) != want || !bytes.Equal(v.DBRBytes(), payload) {
			t.Fatalf("enum count %d: %+v %v", count, v.Metadata(), err)
		}
	}
}

func TestLegacyDefaultReadValidatesBeforeNativeCountSubstitution(t *testing.T) {
	ch, requests := scriptedProtocolChannel(t, 6000, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.ReadNotify, Type: uint16(Double), Count: 1, P1: 1, P2: f.P2, Payload: make([]byte, 8)}
	})
	ch.binding.conn.peerMinor.Store(8)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal("legacy default read was rejected before IOC could decide", err)
	}
	if f := <-requests; f.Count != 6000 {
		t.Fatalf("legacy default count=%d", f.Count)
	}
	if _, err := ch.Get(ctx, WithCount(6000)); err == nil {
		t.Fatal("explicit oversized legacy count was accepted")
	}
	select {
	case f := <-requests:
		t.Fatalf("oversized explicit request reached peer: %+v", f)
	default:
	}
}

func TestDBRBytesPreserveFloatMetadataNaNBits(t *testing.T) {
	payload := make([]byte, 52) // DBR_CTRL_FLOAT
	binary.BigEndian.PutUint32(payload[16:20], 0x7f801234)
	v, err := DecodeValue(30, 1, payload)
	if err != nil || !bytes.Equal(v.DBRBytes(), payload) {
		t.Fatalf("raw FLOAT metadata bits changed: %x %v", v.DBRBytes(), err)
	}
}
