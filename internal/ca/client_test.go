package ca

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"github.com/wakonig/cago/internal/wire"
	"math"
	"net"
	"strconv"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// testIOC is an independent minimal protocol peer. It deliberately has no
// dependency on libca or another Go CA implementation.
type testIOC struct {
	tcp                   *net.TCPListener
	udp                   *net.UDPConn
	mu                    sync.Mutex
	peers                 map[*testPeer]bool
	value                 float64
	createCount, subCount atomic.Int32
	reject                atomic.Int32
	silent                atomic.Bool
	minor                 atomic.Uint32
	nativeType            atomic.Uint32
	delayCreate           atomic.Bool
	addCount, cancelCount atomic.Uint32
	unsubCount            atomic.Int32
	suppressEvents        atomic.Bool
	zeroSID               atomic.Bool
	clears                atomic.Int32
	rejectDelay           atomic.Int64
	successDelay          atomic.Int64
	stop                  chan struct{}
	wg                    sync.WaitGroup
	readPause, writePause atomic.Pointer[testReplyPause]
	applyPause            atomic.Pointer[testReplyPause]
	echoPause             atomic.Pointer[testReplyPause]
	writes                atomic.Int32
}

type testReplyPause struct {
	entered chan wire.Frame
	release chan struct{}
}

func (p *testPeer) awaitReply(pause *testReplyPause, f wire.Frame) bool {
	if pause == nil {
		return true
	}
	select {
	case pause.entered <- f:
	default:
	}
	select {
	case <-pause.release:
		return true
	case <-p.srv.stop:
		return false
	}
}

type testPeer struct {
	srv      *testIOC
	net      net.Conn
	mu       sync.Mutex
	channels map[uint32]uint32
	subs     map[uint32]wire.Frame
	closed   bool
}

func newTestIOC(t *testing.T) *testIOC {
	t.Helper()

	var tcp *net.TCPListener
	var udp *net.UDPConn
	var e error
	port := 0
	for attempt := 0; attempt < 20; attempt++ {
		tcp, e = net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if e != nil {
			t.Fatal(e)
		}
		port = tcp.Addr().(*net.TCPAddr).Port
		udp, e = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		if e == nil {
			break
		}
		tcp.Close()
		if !errors.Is(e, syscall.EADDRINUSE) {
			t.Fatal(e)
		}
	}
	if e != nil {
		t.Fatal("could not reserve paired TCP/UDP test port:", e)
	}

	s := &testIOC{tcp: tcp, udp: udp, peers: make(map[*testPeer]bool), value: 1.25, stop: make(chan struct{})}
	s.minor.Store(13)
	s.nativeType.Store(uint32(Double))
	s.wg.Add(2)
	go func() {
		defer s.wg.Done()
		for {
			nc, e := tcp.Accept()
			if e != nil {
				return
			}
			p := &testPeer{srv: s, net: nc, channels: make(map[uint32]uint32), subs: make(map[uint32]wire.Frame)}
			s.mu.Lock()
			s.peers[p] = true
			s.mu.Unlock()
			s.wg.Add(1)
			go func() { defer s.wg.Done(); p.run() }()
		}
	}()
	go func() {
		defer s.wg.Done()
		buf := make([]byte, 65536)
		for {
			n, from, e := udp.ReadFromUDP(buf)
			if e != nil {
				return
			}
			r := bytes.NewReader(buf[:n])
			for r.Len() > 0 {
				f, e := wire.Read(r, 65536)
				if e != nil {
					break
				}
				if f.Command == wire.Search && text(f.Payload) != "missing" {
					response, _ := wire.Encode(wire.Frame{Command: wire.Search, Type: uint16(port), P1: 0xffffffff, P2: f.P2, Payload: []byte{0, 13}})
					udp.WriteToUDP(response, from)
				}
			}
		}
	}()
	t.Cleanup(func() { close(s.stop); tcp.Close(); udp.Close(); s.disconnect(); s.wg.Wait() })
	return s
}
func (s *testIOC) disconnect() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for p := range s.peers {
		p.net.Close()
	}
}
func (p *testPeer) send(f wire.Frame) { p.mu.Lock(); defer p.mu.Unlock(); p.sendLocked(f) }
func (p *testPeer) sendLocked(f wire.Frame) {
	b, _ := wire.Encode(f)
	p.net.SetWriteDeadline(time.Now().Add(time.Second))
	p.net.Write(b)
}
func (p *testPeer) event(f wire.Frame) {
	if p.srv.suppressEvents.Load() {
		return
	}
	p.srv.mu.Lock()
	v := p.srv.value
	p.srv.mu.Unlock()

	typ := DataType(f.Type)
	payload := make([]byte, valueOffset(typ)+typ.Size())
	data := payload[valueOffset(typ):]
	switch typ.Base() {
	case String:
		copy(data, fmt.Sprint(v))
	case Char:
		data[0] = byte(v)
	case Short, Enum:
		binary.BigEndian.PutUint16(data, uint16(v))
	case Long:
		binary.BigEndian.PutUint32(data, uint32(v))
	case Float:
		binary.BigEndian.PutUint32(data, math.Float32bits(float32(v)))
	case Double:
		binary.BigEndian.PutUint64(data, math.Float64bits(v))
	}
	p.send(wire.Frame{Command: wire.EventAdd, Type: f.Type, Count: 1, P1: 1, P2: f.P2, Payload: payload})
}
func (p *testPeer) run() {
	defer func() { p.net.Close(); p.srv.mu.Lock(); delete(p.srv.peers, p); p.srv.mu.Unlock() }()
	for {
		f, e := wire.Read(p.net, 1<<20)
		if e != nil {
			return
		}
		switch f.Command {
		case wire.Version:
			p.send(wire.Frame{Command: wire.Version, Count: p.srv.minor.Load()})
		case wire.Search:
			host, port, _ := net.SplitHostPort(p.srv.tcp.Addr().String())
			ip := net.ParseIP(host).To4()
			n, _ := strconv.Atoi(port)
			p.send(wire.Frame{Command: wire.Search, Type: uint16(n), P1: binary.BigEndian.Uint32(ip), P2: f.P2, Payload: []byte{0, 13}})
		case wire.Create:
			p.srv.createCount.Add(1)
			if text(f.Payload) == "delayed-success" {
				time.Sleep(time.Duration(p.srv.successDelay.Load()))
			}
			if text(f.Payload) == "rejected" {
				time.Sleep(time.Duration(p.srv.rejectDelay.Load()))
				p.send(wire.Frame{Command: wire.CreateFail, P1: f.P1})
				continue
			}
			if p.srv.delayCreate.CompareAndSwap(true, false) {
				time.Sleep(300 * time.Millisecond)
			}
			p.mu.Lock()
			p.channels[f.P1] = f.P1
			p.mu.Unlock()
			p.send(wire.Frame{Command: wire.Access, P1: f.P1, P2: 3})
			sid := f.P1
			if p.srv.zeroSID.Load() {
				sid = 0
			}
			p.send(wire.Frame{Command: wire.Create, Type: uint16(p.srv.nativeType.Load()), Count: 1, P1: f.P1, P2: sid})
		case wire.ReadNotify:
			if !p.awaitReply(p.srv.readPause.Load(), f) {
				return
			}
			if p.srv.silent.Load() {
				continue
			}
			p.srv.mu.Lock()
			v := p.srv.value
			p.srv.mu.Unlock()
			data := make([]byte, 8)
			binary.BigEndian.PutUint64(data, math.Float64bits(v))
			p.send(wire.Frame{Command: wire.ReadNotify, Type: 6, Count: 1, P1: 1, P2: f.P2, Payload: data})
		case wire.Write, wire.WriteNotify:
			if !p.awaitReply(p.srv.applyPause.Load(), f) {
				return
			}
			p.srv.writes.Add(1)
			p.srv.mu.Lock()
			p.srv.value = math.Float64frombits(binary.BigEndian.Uint64(f.Payload))
			p.srv.mu.Unlock()
			if f.Command == wire.WriteNotify {
				if !p.awaitReply(p.srv.writePause.Load(), f) {
					return
				}
				p.send(wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2})
			}
			p.mu.Lock()
			subs := make([]wire.Frame, 0, len(p.subs))
			for _, q := range p.subs {
				subs = append(subs, q)
			}
			p.mu.Unlock()
			for _, q := range subs {
				p.event(q)
			}
		case wire.EventAdd:
			p.srv.subCount.Add(1)
			p.srv.addCount.Store(f.Count)
			if p.srv.reject.CompareAndSwap(1, 0) {
				p.send(wire.Frame{Command: wire.EventAdd, P1: 0, P2: f.P2, Payload: []byte{0}})
				continue
			}
			p.mu.Lock()
			p.subs[f.P2] = f
			p.mu.Unlock()
			p.event(f)
		case wire.EventCancel:
			p.srv.unsubCount.Add(1)
			p.srv.cancelCount.Store(f.Count)
			p.mu.Lock()
			delete(p.subs, f.P2)
			p.mu.Unlock()
			p.send(wire.Frame{Command: wire.EventAdd, P2: f.P2})
		case wire.Echo:
			if !p.awaitReply(p.srv.echoPause.Load(), f) {
				return
			}
			p.send(f)
		case wire.Clear:
			p.srv.clears.Add(1)
			p.mu.Lock()
			delete(p.channels, f.P2)
			p.mu.Unlock()
			p.send(f)
		}
	}
}
func testClient(t *testing.T, s *testIOC, transport SearchTransport) *Client {
	t.Helper()
	c, e := New(context.Background(), Options{SearchAddresses: []string{s.tcp.Addr().String()}, DisableRepeater: true, SearchTransport: transport, ContextTimeout: 200 * time.Millisecond, SearchInterval: 10 * time.Millisecond, MaxSearchPeriod: 100 * time.Millisecond, InactivityTimeout: time.Second, EchoInterval: 100 * time.Millisecond})
	if e != nil {
		t.Fatal(e)
	}
	t.Cleanup(func() { c.Close() })
	return c
}
func event(t *testing.T, s *Subscription) Event {
	t.Helper()
	select {
	case e, ok := <-s.Events():
		if !ok {
			t.Fatal("subscription closed")
		}
		return e
	case e := <-s.Errors():
		t.Fatalf("subscription error: %v", e)
	case <-time.After(3 * time.Second):
		t.Fatal("event timeout")
	}
	return Event{}
}
func TestClientIOAndReconnect(t *testing.T) {
	for _, transport := range []SearchTransport{UDP, TCP} {
		t.Run(fmt.Sprint(transport), func(t *testing.T) {
			s := newTestIOC(t)
			c := testClient(t, s, transport)
			ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
			defer cancel()
			ch := c.Channel("test")
			v, e := ch.Get(ctx)
			if e != nil || v.Float64() != 1.25 {
				t.Fatalf("get %v %v", v, e)
			}
			sub, e := ch.Subscribe(ctx)
			if e != nil {
				t.Fatal(e)
			}
			event(t, sub)
			value, _ := NewValue(42.0)
			if e := ch.PutNotify(ctx, value); e != nil {
				t.Fatal(e)
			}
			if event(t, sub).Value.Float64() != 42 {
				t.Fatal("write monitor")
			}
			s.disconnect()
			if event(t, sub).Value.Float64() != 42 {
				t.Fatal("reconnect snapshot")
			}
			if s.subCount.Load() < 2 {
				t.Fatal("not resubscribed")
			}
			sub.Cancel()
			if _, ok := <-sub.Events(); ok {
				t.Fatal("late event")
			}
			ch.Close()
			if e := ch.Connect(ctx); e == nil {
				t.Fatal("closed channel connected")
			}
			if e := c.Channel("test").Connect(ctx); e != nil {
				t.Fatal(e)
			}
		})
	}
}
func TestMonitorRejectionRetries(t *testing.T) {
	s := newTestIOC(t)
	s.reject.Store(1)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	sub, e := c.Channel("test").Subscribe(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Cancel()
	select {
	case <-sub.Errors():
	case <-ctx.Done():
		t.Fatal("missing monitor failure")
	}
	if event(t, sub).Value.Float64() != 1.25 || s.subCount.Load() < 2 {
		t.Fatal("monitor not retried")
	}
}
func TestCancellationAndMissingPV(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	if e := c.Channel("missing").Connect(ctx); e == nil {
		t.Fatal("missing PV connected")
	}
	ch := c.Channel("test")
	live, cancelLive := context.WithTimeout(context.Background(), time.Second)
	defer cancelLive()
	if e := ch.Connect(live); e != nil {
		t.Fatal(e)
	}
	s.silent.Store(true)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel2()
	if _, e := ch.Get(ctx2); e == nil {
		t.Fatal("read did not cancel")
	}
	start := time.Now()
	c.Close()
	if time.Since(start) > 300*time.Millisecond {
		t.Fatal("slow shutdown")
	}
}
func TestConcurrentChannels(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var wg sync.WaitGroup
	for i := range 32 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch := c.Channel(fmt.Sprint(i))
			if _, e := ch.Get(ctx); e != nil {
				t.Error(e)
			}
		}()
	}
	wg.Wait()
	if c.Stats().Circuits != 1 {
		t.Fatalf("circuits %+v", c.Stats())
	}
}
func TestErrorsDoNotEvictValues(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sub, e := c.Channel("test").Subscribe(ctx)
	if e != nil {
		t.Fatal(e)
	}
	defer sub.Cancel()
	deadline := time.Now().Add(time.Second)
	for len(sub.events) == 0 {
		if time.Now().After(deadline) {
			t.Fatal("no initial value")
		}
		time.Sleep(time.Millisecond)
	}
	sub.report(fmt.Errorf("example failure"))
	if e := <-sub.Events(); e.Value.Float64() != 1.25 {
		t.Fatal("error evicted value")
	}
	if <-sub.Errors() == nil {
		t.Fatal("missing independent error")
	}
}

func TestCreateTimeoutRetiresCircuit(t *testing.T) {
	s := newTestIOC(t)
	s.delayCreate.Store(true)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	v, err := c.Channel("test").Get(ctx)
	if err != nil || v.Float64() != 1.25 {
		t.Fatalf("recovery failed %v %v", v, err)
	}
	if s.createCount.Load() < 2 || c.Stats().Connections < 2 {
		t.Fatal("ambiguous create did not force a fresh connection")
	}
}
func TestLegacyCancelCount(t *testing.T) {
	s := newTestIOC(t)
	s.minor.Store(12)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	sub, err := c.Channel("test").Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	event(t, sub)
	sub.Cancel()
	deadline := time.Now().Add(time.Second)
	for s.cancelCount.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("missing version-adjusted cancel count")
		}
		time.Sleep(time.Millisecond)
	}
	if s.cancelCount.Load() != s.addCount.Load() {
		t.Fatal("cancel/add count mismatch")
	}
}
func TestCancelWhileConsuming(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := c.Channel("test")
	for range 100 {
		sub, err := ch.Subscribe(ctx)
		if err != nil {
			t.Fatal(err)
		}
		consumer := make(chan struct{})
		go func() {
			defer close(consumer)
			for range sub.Events() {
			}
		}()
		done := make(chan struct{})
		go func() { sub.Cancel(); close(done) }()
		select {
		case <-done:
		case <-ctx.Done():
			t.Fatal("cancellation deadlocked")
		}
		<-consumer
	}
}
func TestLegacyExtendedHeaderRejected(t *testing.T) {
	a, b := net.Pipe()
	defer a.Close()
	defer b.Close()
	c := &Client{opts: Options{ContextTimeout: time.Second}}
	s := newCircuit(c, a, "test", "test")
	s.peerMinor.Store(8)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := s.send(ctx, wire.Frame{Command: wire.ReadNotify, Count: 70000}); err == nil {
		t.Fatal("extended request sent to old peer")
	}
}
func TestLegacySearchAddress(t *testing.T) {
	ch := &Channel{results: make(chan string, 1)}
	c := &Client{byID: map[uint32]*Channel{123: ch}}
	c.searchReply(wire.Frame{Command: wire.Search, Type: 5064, P1: 42, P2: 123, Payload: []byte{0, 7}}, "10.0.0.2")
	if got := <-ch.results; got != "10.0.0.2:5064" {
		t.Fatal(got)
	}
}
func TestRoutineBeaconsDoNotResetSearch(t *testing.T) {
	c := &Client{}
	f := wire.Frame{Command: wire.Beacon, Type: 5064, P1: 10}
	if !c.beaconAnomaly(f, "127.0.0.1") {
		t.Fatal("first beacon ignored")
	}
	c.lastNudge = time.Now().Add(-time.Second)
	for i := uint32(11); i < 100; i++ {
		f.P1 = i
		if c.beaconAnomaly(f, "127.0.0.1") {
			t.Fatal("routine beacon treated as anomaly")
		}
	}
	f.P1 = 1
	if !c.beaconAnomaly(f, "127.0.0.1") {
		t.Fatal("IOC restart ignored")
	}
	if c.beaconAnomaly(wire.Frame{Type: 5064}, "127.0.0.2") {
		t.Fatal("beacon storm not rate limited")
	}
}

func TestZeroServerIDCleared(t *testing.T) {
	s := newTestIOC(t)
	s.zeroSID.Store(true)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("zero")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.Close()
	deadline := time.Now().Add(time.Second)
	for s.clears.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("server ID zero was not cleared")
		}
		time.Sleep(time.Millisecond)
	}
}
func TestRejectedCreateDoesNotDisconnectOtherChannels(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	healthy := c.Channel("healthy")
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	bad := c.Channel("rejected")
	short, cancelShort := context.WithTimeout(ctx, 80*time.Millisecond)
	defer cancelShort()
	if err := bad.Connect(short); err == nil {
		t.Fatal("rejected channel connected")
	}
	bad.Close()
	if c.Stats().Connections != 1 || c.Stats().Reconnects != 0 {
		t.Fatalf("unrelated channel disrupted: %+v", c.Stats())
	}
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
}
func TestLegacyPayloadLimit(t *testing.T) {
	s := &circuit{}
	s.peerMinor.Store(8)
	for _, f := range []wire.Frame{{Command: wire.Write, Payload: make([]byte, 20000)}, {Command: wire.ReadNotify, Type: uint16(Double), Count: 3000}, {Command: wire.EventAdd, Type: uint16(Double), Count: 3000}} {
		if err := s.validateRequest(f); err == nil {
			t.Fatal("oversized legacy request accepted")
		}
	}
}

func TestCancelledSubscriptionActivationKeepsCircuit(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	ch := c.Channel("healthy")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	ch.mu.Lock()
	b := ch.binding
	ch.mu.Unlock()
	b.conn.gate <- struct{}{}
	sub, err := ch.Subscribe(ctx)
	if err != nil {
		<-b.conn.gate
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		b.conn.mu.Lock()
		n := len(b.conn.subscriptions)
		b.conn.mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			<-b.conn.gate
			t.Fatal("activation not started")
		}
		time.Sleep(time.Millisecond)
	}
	sub.cancel()
	select {
	case <-sub.Errors():
	case <-ctx.Done():
		<-b.conn.gate
		t.Fatal("cancellation not observed")
	}
	select {
	case <-b.conn.done:
		<-b.conn.gate
		t.Fatal("local cancellation retired shared circuit")
	default:
	}
	<-b.conn.gate
	sub.Cancel()
	if _, err := ch.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Connections != 1 {
		t.Fatal("healthy circuit replaced")
	}
}
func TestCancelledCreateBeforeWriteKeepsCircuit(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	healthy := c.Channel("healthy")
	if err := healthy.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	healthy.mu.Lock()
	b := healthy.binding
	healthy.mu.Unlock()
	b.conn.gate <- struct{}{}
	ch := c.Channel("cancel-before-create")
	deadline := time.Now().Add(time.Second)
	for {
		b.conn.mu.Lock()
		_, registered := b.conn.bindings[ch.id]
		b.conn.mu.Unlock()
		if registered {
			break
		}
		if time.Now().After(deadline) {
			<-b.conn.gate
			t.Fatal("create not registered")
		}
		time.Sleep(time.Millisecond)
	}
	ch.cancel()
	select {
	case <-ch.Done():
	case <-ctx.Done():
		<-b.conn.gate
		t.Fatal("close blocked before write")
	}
	select {
	case <-b.conn.done:
		<-b.conn.gate
		t.Fatal("cancelled unsent create retired circuit")
	default:
	}
	<-b.conn.gate
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
}

func TestCloseRacingCreateRejectionKeepsCircuit(t *testing.T) {
	s := newTestIOC(t)
	s.rejectDelay.Store(int64(60 * time.Millisecond))
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	healthy := c.Channel("healthy")
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	bad := c.Channel("rejected")
	deadline := time.Now().Add(time.Second)
	for s.createCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("create not received")
		}
		time.Sleep(time.Millisecond)
	}
	bad.Close()
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Connections != 1 || c.Stats().Reconnects != 0 {
		t.Fatal("close/rejection race retired healthy circuit")
	}
}

func TestCloseClearsLateSuccessfulCreate(t *testing.T) {
	s := newTestIOC(t)
	s.zeroSID.Store(true)
	s.successDelay.Store(int64(60 * time.Millisecond))
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	healthy := c.Channel("healthy")
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	late := c.Channel("delayed-success")
	deadline := time.Now().Add(time.Second)
	for s.createCount.Load() < 2 {
		if time.Now().After(deadline) {
			t.Fatal("create not received")
		}
		time.Sleep(time.Millisecond)
	}
	late.Close()
	deadline = time.Now().Add(time.Second)
	for s.clears.Load() == 0 {
		if time.Now().After(deadline) {
			t.Fatal("late SID0 creation not cleared")
		}
		time.Sleep(time.Millisecond)
	}
	if _, err := healthy.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Connections != 1 || c.Stats().Reconnects != 0 {
		t.Fatal("late creation cleanup retired healthy circuit")
	}
}
