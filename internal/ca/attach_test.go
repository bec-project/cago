package ca

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

// attachmentIOC reads requests independently of the test-controlled responses,
// so multiple creates on one TCP circuit can actually be outstanding together.
type attachmentIOC struct {
	tcp      *net.TCPListener
	requests chan attachmentRequest
	clears   chan wire.Frame
	mu       sync.Mutex
	peers    map[*attachmentPeer]bool
	wg       sync.WaitGroup
	stop     chan struct{}
}

type attachmentPeer struct {
	net.Conn
	mu sync.Mutex
}

func (p *attachmentPeer) send(f wire.Frame) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	data, err := wire.Encode(f)
	if err != nil {
		return err
	}
	p.SetWriteDeadline(time.Now().Add(time.Second))
	_, err = p.Write(data)
	return err
}

type attachmentRequest struct {
	wire.Frame
	peer *attachmentPeer
}

func (r attachmentRequest) respond(t *testing.T, command uint16) {
	t.Helper()
	if err := r.peer.send(wire.Frame{Command: command, Type: uint16(Double), Count: 1, P1: r.P1, P2: r.P1}); err != nil {
		t.Fatal(err)
	}
}

func newAttachmentIOC(t *testing.T) *attachmentIOC {
	t.Helper()
	tcp, err := net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	s := &attachmentIOC{tcp: tcp, requests: make(chan attachmentRequest, 128), clears: make(chan wire.Frame, 128), peers: make(map[*attachmentPeer]bool), stop: make(chan struct{})}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		for {
			nc, err := tcp.Accept()
			if err != nil {
				return
			}
			p := &attachmentPeer{Conn: nc}
			s.mu.Lock()
			s.peers[p] = true
			s.mu.Unlock()
			s.wg.Add(1)
			go func() {
				defer s.wg.Done()
				defer p.Close()
				defer func() { s.mu.Lock(); delete(s.peers, p); s.mu.Unlock() }()
				for {
					f, err := wire.Read(p, 65536)
					if err != nil {
						return
					}
					switch f.Command {
					case wire.Version:
						p.send(wire.Frame{Command: wire.Version, Count: wire.Minor})
					case wire.Create:
						select {
						case s.requests <- attachmentRequest{f, p}:
						case <-s.stop:
							return
						}
					case wire.Clear:
						select {
						case s.clears <- f:
						case <-s.stop:
							return
						}
					case wire.Echo:
						p.send(f)
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		close(s.stop)
		tcp.Close()
		s.mu.Lock()
		for p := range s.peers {
			p.Close()
		}
		s.mu.Unlock()
		s.wg.Wait()
	})
	return s
}

func attachmentClient(t *testing.T, s *attachmentIOC, limit int, timeout time.Duration) *Client {
	t.Helper()
	c, err := New(context.Background(), Options{SearchAddresses: []string{s.tcp.Addr().String()}, DisableRepeater: true, MaxConcurrentCreates: limit, ContextTimeout: timeout, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: time.Second, InactivityTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	return c
}

func attachmentChannel(c *Client, s *attachmentIOC, name string, priority uint16) *Channel {
	ch := c.Channel(name, WithPriority(priority))
	ch.results <- s.tcp.Addr().String()
	return ch
}

func nextAttachment(t *testing.T, s *attachmentIOC) attachmentRequest {
	t.Helper()
	select {
	case request := <-s.requests:
		return request
	case <-time.After(time.Second):
		t.Fatal("Create did not reach IOC")
		return attachmentRequest{}
	}
}

func noAttachment(t *testing.T, s *attachmentIOC) {
	t.Helper()
	select {
	case request := <-s.requests:
		t.Fatalf("unexpected outstanding Create: %s", text(request.Payload))
	case <-time.After(20 * time.Millisecond):
	}
}

func createUsers(c *Client, address string) int {
	c.createMu.Lock()
	defer c.createMu.Unlock()
	if limit := c.createSlots[address]; limit != nil {
		return limit.users
	}
	return 0
}

func assertNoCreateState(t *testing.T, c *Client) {
	t.Helper()
	eventually(t, func() bool {
		c.createMu.Lock()
		defer c.createMu.Unlock()
		return len(c.createSlots) == 0 && c.metrics.Value(MetricCreatesInFlight) == 0
	})
}

func TestCreateAdmissionBoundsEndpointAcrossPriorities(t *testing.T) {
	for _, priorities := range []bool{false, true} {
		t.Run(fmt.Sprint("different-priorities=", priorities), func(t *testing.T) {
			s := newAttachmentIOC(t)
			c := attachmentClient(t, s, 2, time.Second)
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()
			const count = 8
			var channels []*Channel
			for i := range count {
				var priority uint16
				if priorities {
					priority = uint16(i)
				}
				channels = append(channels, attachmentChannel(c, s, fmt.Sprint("test", i), priority))
			}
			first, second := nextAttachment(t, s), nextAttachment(t, s)
			eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == count })
			noAttachment(t, s)
			if got := c.metrics.Value(MetricCreatesInFlight); got != 2 {
				t.Fatalf("in-flight gauge=%d", got)
			}
			first.respond(t, wire.Create)
			for i := 2; i < count; i++ {
				nextAttachment(t, s).respond(t, wire.Create)
			}
			second.respond(t, wire.Create)
			for _, ch := range channels {
				if err := ch.Connect(ctx); err != nil {
					t.Fatal(err)
				}
			}
			assertNoCreateState(t, c)
			if attempts := c.metrics.Value(MetricCreateAttempts); attempts != count {
				t.Fatalf("CreateAttempts=%d, want %d", attempts, count)
			}
		})
	}
}

func TestCreateAdmissionEndpointsAreIndependent(t *testing.T) {
	blocked, healthy := newAttachmentIOC(t), newAttachmentIOC(t)
	c := attachmentClient(t, blocked, 1, time.Second)
	attachmentChannel(c, blocked, "blocked", 0)
	first := nextAttachment(t, blocked)
	attachmentChannel(c, blocked, "queued", 1)
	eventually(t, func() bool { return createUsers(c, blocked.tcp.Addr().String()) == 2 })
	ch := attachmentChannel(c, healthy, "healthy", 0)
	nextAttachment(t, healthy).respond(t, wire.Create)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	noAttachment(t, blocked)
	first.respond(t, wire.Create)
	nextAttachment(t, blocked).respond(t, wire.Create)
	assertNoCreateState(t, c)
}

func TestCreateAdmissionCancelsQueuedChannel(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Second)
	attachmentChannel(c, s, "first", 0)
	first := nextAttachment(t, s)
	queued := attachmentChannel(c, s, "cancelled", 1)
	eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == 2 })
	closed := make(chan struct{})
	go func() { queued.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("queued Close waited for the current Create")
	}
	if got := createUsers(c, s.tcp.Addr().String()); got != 1 {
		t.Fatalf("cancelled waiter retained: %d users", got)
	}
	first.respond(t, wire.Create)
	assertNoCreateState(t, c)
	noAttachment(t, s)
}

func TestCreateAdmissionShutdownInterruptsQueueAndHandshake(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Hour)
	attachmentChannel(c, s, "first", 0)
	nextAttachment(t, s)
	attachmentChannel(c, s, "queued", 1)
	eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == 2 })
	closed := make(chan struct{})
	go func() { c.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(time.Second):
		t.Fatal("shutdown waited for the attachment timeout")
	}
	assertNoCreateState(t, c)
	if got := c.metrics.Value(MetricCreateAttempts); got != 1 {
		t.Fatalf("queued Create was counted: %d", got)
	}
}

func TestCreateAdmissionKeepsLateReplyReservation(t *testing.T) {
	for _, command := range []uint16{wire.Create, wire.CreateFail} {
		t.Run(fmt.Sprint(command), func(t *testing.T) {
			s := newAttachmentIOC(t)
			c := attachmentClient(t, s, 1, time.Second)
			closing := attachmentChannel(c, s, "closing", 0)
			first := nextAttachment(t, s)
			attachmentChannel(c, s, "queued", 1)
			eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == 2 })
			closed := make(chan struct{})
			go func() { closing.Close(); close(closed) }()
			<-closing.ctx.Done()
			noAttachment(t, s)
			first.respond(t, command)
			select {
			case <-closed:
			case <-time.After(time.Second):
				t.Fatal("late response did not finish Close")
			}
			if command == wire.Create {
				select {
				case f := <-s.clears:
					if f.P1 != first.P1 || f.P2 != first.P1 {
						t.Fatal("late channel cleared with wrong SID/CID")
					}
				case <-time.After(time.Second):
					t.Fatal("late successful Create was not cleared")
				}
			}
			nextAttachment(t, s).respond(t, wire.Create)
			assertNoCreateState(t, c)
			wantFailures := int64(0)
			if command == wire.CreateFail {
				wantFailures = 1
			}
			if got := c.metrics.Value(MetricCreateFailures); got != wantFailures {
				t.Fatalf("CreateFailures=%d, want %d", got, wantFailures)
			}
		})
	}
}

func TestCreateAdmissionReleasesFailedHandshake(t *testing.T) {
	for _, failure := range []string{"reject", "disconnect", "timeout", "invalid-type"} {
		t.Run(failure, func(t *testing.T) {
			s := newAttachmentIOC(t)
			c := attachmentClient(t, s, 1, 75*time.Millisecond)
			firstChannel := attachmentChannel(c, s, "first", 0)
			first := nextAttachment(t, s)
			queued := attachmentChannel(c, s, "queued", 1)
			eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == 2 })
			switch failure {
			case "reject":
				first.respond(t, wire.CreateFail)
			case "disconnect":
				first.peer.Close()
			case "invalid-type":
				if err := first.peer.send(wire.Frame{Command: wire.Create, Type: uint16(PutACKS), Count: 1, P1: first.P1, P2: first.P1}); err != nil {
					t.Fatal(err)
				}
			}
			if failure == "timeout" {
				// Admission may expire alongside the stalled handshake. This IOC
				// fixture has no discovery responder. Wait for the old admission
				// to finish before supplying fresh discovery, otherwise that dying
				// admission can consume and discard our only search reply.
				eventually(t, func() bool { return c.metrics.Value(MetricCreateFailures) == 1 })
				var next attachmentRequest
				eventually(t, func() bool {
					select {
					case next = <-s.requests:
						return true
					default:
					}
					if createUsers(c, s.tcp.Addr().String()) == 0 {
						select {
						case queued.results <- s.tcp.Addr().String():
						default:
						}
					}
					return false
				})
				next.respond(t, wire.Create)
			} else {
				nextAttachment(t, s).respond(t, wire.Create)
			}
			ctx, cancel := context.WithTimeout(context.Background(), time.Second)
			defer cancel()
			if err := queued.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			firstChannel.Close()
			assertNoCreateState(t, c)
			if got := c.metrics.Value(MetricCreateFailures); got != 1 {
				t.Fatalf("CreateFailures=%d", got)
			}
		})
	}
}

func TestCreateAdmissionReleasesFailedDial(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Second)
	address := s.tcp.Addr().String()
	entry := &dialEntry{ready: make(chan struct{}), err: errors.New("dial failed")}
	c.mu.Lock()
	c.circuits[address+"/0"] = entry
	c.mu.Unlock()
	ch := c.Channel("failed-dial")
	ch.results <- address
	eventually(t, func() bool { return createUsers(c, address) == 1 })
	close(entry.ready)
	assertNoCreateState(t, c)
	ch.Close()
	if got := c.metrics.Value(MetricCreateAttempts); got != 0 {
		t.Fatalf("failed dial counted as Create: %d", got)
	}
}

func TestCreateAdmissionDialWaitIsCancelable(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Hour)
	address := s.tcp.Addr().String()
	c.mu.Lock()
	c.circuits[address+"/0"] = &dialEntry{ready: make(chan struct{})}
	c.mu.Unlock()
	ch := attachmentChannel(c, s, "dial-wait", 0)
	eventually(t, func() bool { return createUsers(c, address) == 1 })
	if got := c.metrics.Value(MetricCreatesInFlight); got != 1 {
		t.Fatalf("dialing attachment missing from gauge: %d", got)
	}
	closed := make(chan struct{})
	go func() { ch.Close(); close(closed) }()
	select {
	case <-closed:
	case <-time.After(100 * time.Millisecond):
		t.Fatal("channel Close waited for the shared transport dial")
	}
	assertNoCreateState(t, c)
	noAttachment(t, s)
}

func TestCreateAdmissionReleasesRetiringCircuit(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Second)
	address := s.tcp.Addr().String()
	nc, err := net.Dial("tcp4", address)
	if err != nil {
		t.Fatal(err)
	}
	key := address + "/0"
	conn := newCircuit(c, nc, key, address)
	conn.retiring, conn.rotating = true, true
	ready := make(chan struct{})
	close(ready)
	c.mu.Lock()
	c.circuits[key] = &dialEntry{ready: ready, conn: conn}
	c.mu.Unlock()
	var ch *Channel
	func() {
		conn.mu.Lock()
		defer conn.mu.Unlock()
		ch = attachmentChannel(c, s, "retiring", 0)
		eventually(t, func() bool { return createUsers(c, address) == 1 })
	}()
	assertNoCreateState(t, c)
	ch.Close()
	noAttachment(t, s)
	if got := c.metrics.Value(MetricCreateAttempts); got != 0 {
		t.Fatalf("retiring circuit started a Create: %d", got)
	}
}

func TestCreateAdmissionRotationReleasesReservations(t *testing.T) {
	s := newTestIOC(t)
	c, err := New(context.Background(), Options{SearchAddresses: []string{s.tcp.Addr().String()}, DisableRepeater: true, MaxConcurrentCreates: 1, ContextTimeout: time.Second, SearchInterval: time.Millisecond, MaxSearchPeriod: 10 * time.Millisecond, EchoInterval: time.Second, InactivityTimeout: 2 * time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { c.Close() })
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	first, second := c.Channel("first"), c.Channel("second")
	for _, ch := range []*Channel{first, second} {
		if err := ch.Connect(ctx); err != nil {
			t.Fatal(err)
		}
	}
	old := rotationBinding(t, first)
	seedCircuitIDs(old, uint64(^uint32(0)))
	if _, err := first.Get(ctx); err != nil {
		t.Fatal(err)
	}
	if _, err := second.Get(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoCreateState(t, c)
	if rotationBinding(t, first).conn == old.conn || rotationBinding(t, second).conn == old.conn {
		t.Fatal("rotation did not restore both attachments")
	}
}

func TestCreateAdmissionDefaultAndEndpointChurn(t *testing.T) {
	c := &Client{ctx: context.Background()}
	for i := range 100 {
		address := fmt.Sprint(i)
		var releases []func()
		for range 4 {
			release, err := c.acquireCreate(context.Background(), address)
			if err != nil {
				t.Fatal(err)
			}
			releases = append(releases, release)
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
		if _, err := c.acquireCreate(ctx, address); !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("fifth default attachment admitted: %v", err)
		}
		cancel()
		for _, release := range releases {
			release()
			release() // Cleanup is idempotent, including registry removal.
		}
		assertNoCreateState(t, c)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := c.acquireCreate(ctx, "cancelled"); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	c.ctx = ctx
	if _, err := c.acquireCreate(context.Background(), "closed"); !errors.Is(err, ErrClosed) {
		t.Fatal(err)
	}
	assertNoCreateState(t, c)
}
