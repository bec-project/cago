package cago

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

type beaconState struct {
	sequence uint32
	seen     time.Time
}

type channelKey struct {
	name     string
	priority uint16
}
type dialEntry struct {
	ready chan struct{}
	conn  *circuit
	err   error
}

// Client owns discovery, virtual circuits, and all channel lifetimes.
type Client struct {
	opts            Options
	ctx             context.Context
	cancel          context.CancelFunc
	udp             *net.UDPConn
	addresses       []*net.UDPAddr
	udpSearch       *udpSearch
	tcpSearches     []*tcpSearch
	mu              sync.Mutex
	closed          bool
	channels        map[channelKey]*Channel
	byID            map[uint32]*Channel
	circuits        map[string]*dialEntry
	beacons         map[string]beaconState
	nextBeaconSweep time.Time
	lastNudge       time.Time
	wg              sync.WaitGroup
	closeOnce       sync.Once
	done            chan struct{}
	errors          chan error
	channelIDs      atomic.Uint64
	metrics         ClientMetrics
	createMu        sync.Mutex
	createSlots     map[string]*createLimit
}

func New(ctx context.Context, options Options) (*Client, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	options.SearchAddresses = append([]string(nil), options.SearchAddresses...)
	if err := options.defaults(ctx); err != nil {
		return nil, err
	}
	udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithCancel(ctx)
	c := &Client{opts: options, ctx: ctx, cancel: cancel, udp: udp, channels: make(map[channelKey]*Channel), byID: make(map[uint32]*Channel), circuits: make(map[string]*dialEntry), errors: make(chan error, 32), done: make(chan struct{})}
	for _, s := range options.SearchAddresses {
		a, _ := netip.ParseAddrPort(s) // defaults already resolved and validated it.
		c.addresses = append(c.addresses, net.UDPAddrFromAddrPort(a))
		if options.SearchTransport == TCP {
			worker := &tcpSearch{client: c, address: s, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
			c.tcpSearches = append(c.tcpSearches, worker)
			c.start(worker.run)
		}
	}
	if options.SearchTransport == UDP {
		c.udpSearch = &udpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
		c.start(c.udpSearch.run)
	}
	c.start(c.readUDP)
	if !options.DisableRepeater {
		c.start(c.repeaterLoop)
	}
	// Cancellation closes sockets as well as cancelling goroutines.
	go func() { <-ctx.Done(); c.Close() }()
	return c, nil
}
func (c *Client) start(fn func()) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return false
	}
	c.wg.Add(1)
	go func() { defer c.wg.Done(); fn() }()
	return true
}
func (c *Client) nextChannelID() (uint32, error) {
	n := c.channelIDs.Add(1)
	if n > uint64(^uint32(0)) {
		return 0, ErrIDExhausted
	}
	return uint32(n), nil
}

// Channel returns a shared durable handle for this name and priority.
func (c *Client) Channel(name string, options ...ChannelOption) *Channel {
	cfg := channelConfig{priority: c.opts.Priority}
	for _, o := range options {
		o(&cfg)
	}
	key := channelKey{name, cfg.priority}
	c.mu.Lock()
	defer c.mu.Unlock()
	if ch := c.channels[key]; ch != nil {
		return ch
	}
	ctx, cancel := context.WithCancel(c.ctx)
	ch := &Channel{client: c, name: name, priority: cfg.priority, ctx: ctx, cancel: cancel, changed: make(chan struct{}), updates: make(chan StateChange, 16), results: make(chan string, 1), done: make(chan struct{}), info: ChannelInfo{Name: name, Priority: cfg.priority, State: Searching}}
	id, err := c.nextChannelID()
	ch.id = id
	if c.closed {
		err = ErrClosed
	}
	if name == "" || len(name) > 500 || strings.ContainsRune(name, 0) || cfg.priority > 99 {
		err = fmt.Errorf("invalid CA channel name or priority")
	}
	if err != nil {
		ch.err = err
		ch.info.State = Closed
		cancel()
		close(ch.done)
		close(ch.updates)
		return ch
	}
	c.channels[key] = ch
	c.byID[id] = ch
	c.wg.Add(1)
	go func() { defer c.wg.Done(); ch.run() }()
	return ch
}

// Close promptly interrupts network I/O and waits for all owned work. It is idempotent.
func (c *Client) Close() error {
	c.closeOnce.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.udp.Close()
		var list []*circuit
		for _, e := range c.circuits {
			if e.conn != nil {
				list = append(list, e.conn)
			}
		}
		c.mu.Unlock()
		for _, s := range list {
			s.fail(ErrClosed)
		}
		c.wg.Wait()
		close(c.errors)
		close(c.done)
	})
	<-c.done
	return nil
}
func (c *Client) Done() <-chan struct{} { return c.done }

type Stats struct {
	Channels, Circuits                               int
	Searches, Connections, Reconnects, DroppedValues uint64
}

func (c *Client) Stats() Stats {
	c.mu.Lock()
	defer c.mu.Unlock()
	return Stats{Channels: len(c.channels), Circuits: len(c.circuits), Searches: uint64(c.metrics.Value(MetricSearchRequests)), Connections: uint64(c.metrics.Value(MetricConnections)), Reconnects: uint64(c.metrics.Value(MetricReconnects)), DroppedValues: uint64(c.metrics.Value(MetricDroppedValues))}
}
func (c *Client) dial(ctx context.Context, address string, priority uint16) (*circuit, error) {
	key := address + "/" + strconv.Itoa(int(priority))
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	e := c.circuits[key]
	if e == nil {
		e = &dialEntry{ready: make(chan struct{})}
		c.circuits[key] = e
		c.wg.Add(1)
		go func() {
			defer c.wg.Done()
			c.recordMetric(MetricConnectionAttempts, 1)
			dialCtx, cancel := context.WithTimeout(c.ctx, c.opts.ContextTimeout)
			defer cancel()
			nc, err := (&net.Dialer{}).DialContext(dialCtx, "tcp4", address)
			var s *circuit
			if err == nil {
				s = newCircuit(c, nc, key, address)
				err = s.send(dialCtx, wire.Frame{Command: wire.Version, Type: priority, Count: wire.Minor}, wire.Frame{Command: wire.ClientName, Payload: append([]byte(c.opts.ClientName), 0)}, wire.Frame{Command: wire.HostName, Payload: append([]byte(c.opts.HostName), 0)})
			}
			c.mu.Lock()
			if c.closed && err == nil {
				err = ErrClosed
			}
			e.conn = s
			e.err = err
			if err != nil {
				c.recordMetric(MetricConnectionFailures, 1)
			}
			if err != nil && c.circuits[key] == e {
				delete(c.circuits, key)
			}
			close(e.ready)
			c.mu.Unlock()
			if err != nil {
				if nc != nil {
					nc.Close()
				}
				return
			}
			c.recordMetric(MetricConnections, 1)
			if !c.start(s.readLoop) {
				s.fail(ErrClosed)
				return
			}
			c.start(s.heartbeat)
		}()
	}
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-e.ready:
		return e.conn, e.err
	}
}
func (c *Client) search(ch *Channel) {
	c.recordMetric(MetricSearchRequests, 1)
	if c.opts.SearchTransport == TCP {
		for _, worker := range c.tcpSearches {
			worker.enqueue(ch)
		}
		return
	}
	c.udpSearch.enqueue(ch)
}
func (c *Client) readUDP() {
	buf := make([]byte, 65536)
	for {
		n, from, err := c.udp.ReadFromUDP(buf)
		if err != nil {
			if errors.Is(err, net.ErrClosed) || c.ctx.Err() != nil {
				return
			}
			c.report(fmt.Errorf("CA discovery receive: %w", err))
			if !wait(c.ctx, 100*time.Millisecond) {
				return
			}
			continue
		}
		r := bytes.NewReader(buf[:n])
		for r.Len() > 0 {
			f, err := wire.Read(r, 65536)
			if err != nil {
				break
			}
			switch f.Command {
			case wire.Search:
				c.searchReply(f, from.IP.String())
			case wire.Beacon:
				if c.beaconAnomaly(f, from.IP.String()) {
					c.nudgeSearches()
				}
			}
		}
	}
}
func wait(ctx context.Context, d time.Duration) bool {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return false
	case <-timer.C:
		return true
	}
}
func backoff(d, max time.Duration) time.Duration {
	if d >= max/2 {
		return max
	}
	return d * 2
}

// Errors reports asynchronous server exceptions, including unacknowledged puts
// and monitor errors from Client.Subscribe. Subscription errors include the PV.
// The bounded stream drops oldest errors on overflow; values use separate queues.
func (c *Client) Errors() <-chan error { return c.errors }
func (c *Client) report(err error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return
	}
	select {
	case c.errors <- err:
	default:
		select {
		case <-c.errors:
			c.recordMetric(MetricDroppedErrors, 1)
		default:
		}
		select {
		case c.errors <- err:
		default:
			c.recordMetric(MetricDroppedErrors, 1)
		}
	}
}

func (c *Client) searchReply(f wire.Frame, host string) {
	minor := uint16(4)
	if len(f.Payload) >= 2 {
		minor = binary.BigEndian.Uint16(f.Payload[:2])
	}
	if minor < 4 {
		return
	}
	if minor >= 8 && f.P1 != 0 && f.P1 != ^uint32(0) {
		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, f.P1)
		host = ip.String()
	}
	port := int(f.Type)
	if port == 0 {
		port = c.opts.ServerPort
	}
	c.mu.Lock()
	ch := c.byID[f.P2]
	c.mu.Unlock()
	if ch != nil {
		select {
		case ch.results <- net.JoinHostPort(host, strconv.Itoa(port)):
		default:
		}
	}
}

// Beacons only accelerate normal discovery retries. Bound this advisory cache
// so unsolicited datagrams cannot create unbounded state or per-packet scans.
const maxBeaconStates = 4096

func (c *Client) beaconAnomaly(f wire.Frame, source string) bool {
	if f.P2 != 0 {
		ip := make(net.IP, 4)
		binary.BigEndian.PutUint32(ip, f.P2)
		source = ip.String()
	}
	key := net.JoinHostPort(source, strconv.Itoa(int(f.Type)))
	now := time.Now()
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.beacons == nil {
		c.beacons = make(map[string]beaconState)
	}
	if !now.Before(c.nextBeaconSweep) {
		for key, state := range c.beacons {
			if now.Sub(state.seen) > 2*time.Minute {
				delete(c.beacons, key)
			}
		}
		c.nextBeaconSweep = now.Add(time.Minute)
	}
	old, known := c.beacons[key]
	if known || len(c.beacons) < maxBeaconStates {
		c.beacons[key] = beaconState{sequence: f.P1, seen: now}
	}
	anomaly := !known || f.P1 < old.sequence || now.Sub(old.seen) > 30*time.Second
	if !anomaly || now.Sub(c.lastNudge) < time.Second {
		return false
	}
	c.lastNudge = now
	return true
}
func (c *Client) nudgeSearches() {
	c.mu.Lock()
	defer c.mu.Unlock()
	for _, ch := range c.channels {
		ch.mu.Lock()
		if ch.binding == nil {
			select {
			case ch.results <- "":
			default:
			}
		}
		ch.mu.Unlock()
	}
}
