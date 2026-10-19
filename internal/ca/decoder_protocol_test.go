package ca

import (
	"context"
	"encoding/binary"
	"errors"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"reflect"
	"testing"
	"time"
)

func TestOnlyNormalStatusCompletesIO(t *testing.T) {
	for _, command := range []uint16{wire.ReadNotify, wire.WriteNotify} {
		for _, status := range []uint32{0, 1, 3, 339, 347, 801} {
			ch := responseReviewChannel(t, func(f wire.Frame) wire.Frame {
				return wire.Frame{Command: command, Type: uint16(Double), Count: 1, P1: status, P2: f.P2, Payload: make([]byte, 8)}
			})
			var err error
			if command == wire.ReadNotify {
				_, err = ch.Get(ch.ctx)
			} else {
				v, _ := NewValue(1.25)
				err = ch.PutNotify(ch.ctx, v)
			}
			if status == 1 {
				if err != nil {
					t.Fatal(err)
				}
				continue
			}
			var protocol *ProtocolError
			if !errors.As(err, &protocol) || protocol.Status != status {
				t.Fatalf("command=%d status=%d: %v", command, status, err)
			}
		}
	}
}

func decoderMonitor() (*sharedMonitor, *Subscription, *circuit) {
	c := &circuit{}
	b := &binding{lost: make(chan struct{})}
	m := &sharedMonitor{ch: &Channel{client: &Client{}, name: "test"}, restart: make(chan struct{}, 1), binding: b, conn: c, id: 1}
	sub := &Subscription{monitor: m, ctx: context.Background(), events: make(chan Event, 1), errors: make(chan error, 1)}
	m.members = map[*Subscription]struct{}{sub: {}}
	return m, sub, c
}

func TestMonitorIgnoresEmptyProtocolMessages(t *testing.T) {
	m, sub, c := decoderMonitor()
	for _, status := range []uint32{0, 1, 339} {
		for _, count := range []uint32{0, 1, 100} {
			m.deliver(c, wire.Frame{Command: wire.EventAdd, Type: uint16(Native), Count: count, P1: status, P2: 1})
		}
	}
	if len(sub.events) != 0 || len(sub.errors) != 0 || len(m.restart) != 0 || m.rejected {
		t.Fatal("empty messages changed monitor state")
	}
	payload := make([]byte, 8)
	binary.BigEndian.PutUint64(payload, 0x3ff4000000000000)
	m.deliver(c, wire.Frame{Command: wire.EventAdd, Type: uint16(Double), Count: 1, P1: 1, P2: 1, Payload: payload})
	if len(sub.events) != 1 || (<-sub.events).Value.Float64() != 1.25 {
		t.Fatal("monitor stopped after empty messages")
	}
}

func TestMonitorUsesReturnedDBRShape(t *testing.T) {
	m, sub, c := decoderMonitor()
	payload := make([]byte, 8)
	binary.BigEndian.PutUint32(payload, 17)
	binary.BigEndian.PutUint32(payload[4:], 23)
	m.deliver(c, wire.Frame{Command: wire.EventAdd, Type: uint16(Long), Count: 2, P1: 1, P2: 1, Payload: payload})
	if len(sub.events) != 1 {
		t.Fatal("returned type/count rejected")
	}
	event := <-sub.events
	if event.Value.Type() != Long || event.Value.Count() != 2 || !reflect.DeepEqual(event.Value.Slice(), []int32{17, 23}) {
		t.Fatalf("wrong returned value: %+v", event.Value)
	}
}

func TestTCPCommandRecognition(t *testing.T) {
	allowed := map[uint16]bool{0: true, 1: true, 3: true, 6: true, 10: true, 11: true, 12: true, 15: true, 18: true, 19: true, 22: true, 23: true, 26: true, 27: true}
	for command := uint16(0); command <= 28; command++ {
		a, b := net.Pipe()
		c := &Client{opts: Options{ContextTimeout: time.Second, InactivityTimeout: time.Second}, circuits: make(map[string]*dialEntry), errors: make(chan error, 32)}
		s := newCircuit(c, a, "", "")
		ack := make(chan struct{})
		s.rotationEcho = ack
		done := make(chan struct{})
		go func() { s.readLoop(); close(done) }()
		f := wire.Frame{Command: command, Count: 13}
		if command == wire.Version {
			f.Count = 13
		}
		data, _ := wire.Encode(f)
		echo, _ := wire.Encode(wire.Frame{Command: wire.Echo, P2: rotationEchoTag})
		sent := make(chan struct{})
		go func() { defer close(sent); b.Write(append(data, echo...)) }()
		select {
		case <-ack:
			if !allowed[command] {
				t.Errorf("invalid TCP command %d was ignored", command)
			}
		case <-s.done:
			if allowed[command] {
				t.Errorf("valid TCP command %d disconnected circuit", command)
			}
		case <-time.After(time.Second):
			t.Errorf("command %d stalled decoder", command)
		}
		a.Close()
		b.Close()
		<-sent
		<-done
	}
}

func TestMonitorClampsRequestedCountToNativeCapacity(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sub := subscribeTest(t, client.Channel("limited-array"), ctx, WithCount(100))
	event(t, sub)
	if count := server.addCount.Load(); count != 1 {
		t.Fatalf("monitor requested count %d instead of native capacity", count)
	}
	if sub.monitor.key.count != 100 {
		t.Fatal("original count lost from reconnect/sharing semantics")
	}
}

func TestCreateFailDisconnectsAlreadyCreatedChannel(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := client.Channel("disconnect-target")
	healthy := client.Channel("healthy")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := healthy.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	old := ch.binding
	ch.mu.Unlock()
	server.mu.Lock()
	peers := make([]*testPeer, 0, len(server.peers))
	for p := range server.peers {
		peers = append(peers, p)
	}
	server.mu.Unlock()
	for _, p := range peers {
		p.send(wire.Frame{Command: wire.CreateFail, P1: ch.id})
	}
	select {
	case <-old.lost:
	case <-ctx.Done():
		t.Fatal("command 26 did not disconnect active channel")
	}
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal("channel disconnect interrupted shared circuit", err)
	}
	if err := ch.Connect(ctx); err != nil {
		t.Fatal("disconnected channel did not restore", err)
	}
	select {
	case <-old.conn.done:
		t.Fatal("channel disconnect retired the shared circuit")
	default:
	}
}
