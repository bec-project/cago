package ca

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

func udpSearchSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	if err := socket.SetReadBuffer(1024 * 1024); err != nil {
		t.Fatal(err)
	}
	return socket
}

func TestUDPSearchBatchesCoalescedNamesWithinMTU(t *testing.T) {
	const count = 1000
	for _, mtu := range []int{576, 1400} {
		t.Run(fmt.Sprint(mtu), func(t *testing.T) {
			sink := udpSearchSocket(t)
			ctx := context.Background()
			c := &Client{ctx: ctx, udp: udpSearchSocket(t), opts: Options{SearchMTU: mtu}, addresses: []*net.UDPAddr{sink.LocalAddr().(*net.UDPAddr)}, errors: make(chan error, 32)}
			search := &udpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
			for id := uint32(1); id <= count; id++ {
				ch := &Channel{id: id, name: fmt.Sprintf("pv:%04d", id), ctx: ctx}
				search.enqueue(ch)
				search.enqueue(ch) // A busy endpoint must not duplicate a queued PV.
			}
			search.flush()
			if err := sink.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
				t.Fatal(err)
			}
			seen := make(map[uint32]bool)
			packets := int64(0)
			buf := make([]byte, 65536)
			for len(seen) < count {
				n, _, err := sink.ReadFromUDP(buf)
				if err != nil {
					t.Fatalf("received %d/%d discovery requests: %v", len(seen), count, err)
				}
				packets++
				if n > mtu {
					t.Fatalf("discovery datagram is %d bytes, MTU is %d", n, mtu)
				}
				reader := bytes.NewReader(buf[:n])
				version, err := wire.Read(reader, 65536)
				if err != nil || version.Command != wire.Version || version.Count != wire.Minor || reader.Len() == 0 {
					t.Fatalf("datagram lacks version plus search: %+v, %v", version, err)
				}
				for reader.Len() > 0 {
					query, err := wire.Read(reader, 65536)
					if err != nil {
						t.Fatal(err)
					}
					if query.Command != wire.Search || query.Type != 5 || query.Count != wire.Minor || query.P1 != query.P2 || query.P2 == 0 || query.P2 > count || text(query.Payload) != fmt.Sprintf("pv:%04d", query.P2) {
						t.Fatalf("invalid discovery request: %+v", query)
					}
					if seen[query.P2] {
						t.Fatalf("duplicate discovery request %d", query.P2)
					}
					seen[query.P2] = true
				}
			}
			if packets >= count/10 || packets != c.metrics.Value(MetricSearchDatagrams) {
				t.Fatalf("1000 names used %d datagrams; metric=%d", packets, c.metrics.Value(MetricSearchDatagrams))
			}
			if len(search.pending) != 0 || len(search.wake) != 0 {
				t.Fatal("flushed names retained discovery state")
			}
			search.flush()
			if c.metrics.Value(MetricSearchDatagrams) != packets {
				t.Fatal("empty batch sent a version-only datagram")
			}
		})
	}
}

func TestUDPSearchLongestNameAndEveryEndpoint(t *testing.T) {
	first, second := udpSearchSocket(t), udpSearchSocket(t)
	c := &Client{ctx: context.Background(), udp: udpSearchSocket(t), opts: Options{SearchMTU: 576}, addresses: []*net.UDPAddr{first.LocalAddr().(*net.UDPAddr), second.LocalAddr().(*net.UDPAddr)}}
	search := &udpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
	name := strings.Repeat("x", 500)
	search.enqueue(&Channel{id: 1, name: name, ctx: context.Background()})
	for _, socket := range []*net.UDPConn{first, second} {
		if err := socket.SetReadDeadline(time.Now().Add(time.Second)); err != nil {
			t.Fatal(err)
		}
	}
	search.flush()
	for _, socket := range []*net.UDPConn{first, second} {
		buf := make([]byte, 576)
		n, _, err := socket.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		reader := bytes.NewReader(buf[:n])
		if _, err := wire.Read(reader, 576); err != nil {
			t.Fatal(err)
		}
		query, err := wire.Read(reader, 576)
		if err != nil || text(query.Payload) != name || reader.Len() != 0 {
			t.Fatalf("longest channel name was not searched: %+v, %v", query, err)
		}
	}
	if c.metrics.Value(MetricSearchDatagrams) != 2 {
		t.Fatal("datagram metrics did not count each endpoint")
	}
}

func TestUDPSearchDropsCancelledNamesAndStops(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	c := &Client{ctx: ctx, udp: udpSearchSocket(t), opts: Options{SearchMTU: 1400}}
	search := &udpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
	chCtx, stop := context.WithCancel(ctx)
	ch := &Channel{id: 1, name: "cancelled", ctx: chCtx}
	search.enqueue(ch)
	stop()
	search.flush()
	search.enqueue(ch)
	if len(search.pending) != 0 || c.metrics.Value(MetricSearchDatagrams) != 0 {
		t.Fatal("cancelled channel retained pending work or sent a search")
	}
	cancel()
	search.pending[ch.id] = ch
	search.run()
	search.enqueue(&Channel{id: 2, name: "late", ctx: context.Background()})
	if search.pending != nil {
		t.Fatal("shutdown retained or accepted queued channels")
	}
}

func TestUDPSearchSendFailureIsObservable(t *testing.T) {
	sink := udpSearchSocket(t)
	sender := udpSearchSocket(t)
	c := &Client{ctx: context.Background(), udp: sender, opts: Options{SearchMTU: 1400}, addresses: []*net.UDPAddr{sink.LocalAddr().(*net.UDPAddr)}, errors: make(chan error, 2)}
	search := &udpSearch{client: c, pending: make(map[uint32]*Channel), wake: make(chan struct{}, 1)}
	search.enqueue(&Channel{id: 1, name: "missing", ctx: context.Background()})
	sender.Close()
	search.flush()
	if c.metrics.Value(MetricSearchSendFailures) != 1 || c.metrics.Value(MetricSearchDatagrams) != 0 {
		t.Fatal("failed discovery send reported success")
	}
	select {
	case err := <-c.Errors():
		if !errors.Is(err, net.ErrClosed) {
			t.Fatalf("discovery socket error was lost: %v", err)
		}
	default:
		t.Fatal("discovery send failure was not reported")
	}
}

func TestBatchedDiscoveryConnectsManyChannelsAndClosesCleanly(t *testing.T) {
	srv := newTestIOC(t)
	c := testClient(t, srv, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	const count = 250
	channels := make([]*Channel, count)
	for i := range channels {
		channels[i] = c.Channel(fmt.Sprintf("batch:%d", i))
	}
	for _, ch := range channels {
		if err := ch.Connect(ctx); err != nil {
			t.Fatalf("connect %s: %v", ch.Name(), err)
		}
	}
	searches := c.metrics.Value(MetricSearchRequests)
	datagrams := c.metrics.Value(MetricSearchDatagrams)
	// Exact batch sizes depend on scheduling. The controlled flush test above
	// proves packet reduction without assuming a timing window on a busy host.
	if searches < count || datagrams == 0 || datagrams > searches {
		t.Fatalf("discovery traffic accounting: requests=%d datagrams=%d", searches, datagrams)
	}
	if c.Stats().Channels != count || c.Stats().Circuits != 1 {
		t.Fatalf("startup did not share one circuit: %+v", c.Stats())
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Stats().Channels != 0 || c.udpSearch.pending != nil {
		t.Fatal("shutdown retained channel discovery state")
	}
}

func TestSearchAndCreationLimitOptions(t *testing.T) {
	clearCAEnvironment(t)
	for _, opts := range []Options{
		{SearchMTU: 575}, {SearchMTU: 65508}, {SearchMTU: -1}, {MaxConcurrentCreates: -1},
	} {
		opts.SearchAddresses = []string{"127.0.0.1"}
		if err := opts.defaults(context.Background()); err == nil {
			t.Fatalf("invalid discovery/creation options accepted: %+v", opts)
		}
	}
	for _, mtu := range []int{0, 576, 1400, 65507} {
		opts := Options{SearchMTU: mtu, SearchAddresses: []string{"127.0.0.1"}}
		if err := opts.defaults(context.Background()); err != nil {
			t.Fatal(err)
		}
		if opts.MaxConcurrentCreates != 4 || mtu == 0 && opts.SearchMTU != 1400 {
			t.Fatalf("default discovery/creation options: %+v", opts)
		}
	}
}
