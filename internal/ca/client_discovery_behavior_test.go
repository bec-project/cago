package ca

import (
	"context"
	"encoding/binary"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/bec-project/cago/internal/wire"
)

func TestSearchReplyRoutingAndLegacyFallback(t *testing.T) {
	ch := &Channel{results: make(chan string, 1)}
	c := &Client{opts: Options{ServerPort: 5064}, byID: map[uint32]*Channel{7: ch}}
	for _, tt := range []struct {
		name  string
		frame wire.Frame
		want  string
	}{
		{"legacy", wire.Frame{P1: 0x7f000002, P2: 7}, "127.0.0.1:5064"},
		{"modern_explicit_host", wire.Frame{Type: 12345, P1: 0x7f000002, P2: 7, Payload: []byte{0, 13}}, "127.0.0.2:12345"},
		{"modern_source_host", wire.Frame{Type: 12345, P1: ^uint32(0), P2: 7, Payload: []byte{0, 13}}, "127.0.0.1:12345"},
		{"unsupported_minor", wire.Frame{P2: 7, Payload: []byte{0, 3}}, ""},
		{"unknown_channel", wire.Frame{P2: 8, Payload: []byte{0, 13}}, ""},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c.searchReply(tt.frame, "127.0.0.1")
			select {
			case got := <-ch.results:
				if tt.want == "" || got != tt.want {
					t.Fatalf("discovery endpoint=%q, want %q", got, tt.want)
				}
			default:
				if tt.want != "" {
					t.Fatal("valid search response was discarded")
				}
			}
		})
	}
	ch.results <- "already-discovered:1234"
	c.searchReply(wire.Frame{P2: 7}, "127.0.0.1")
	if got := <-ch.results; got != "already-discovered:1234" {
		t.Fatalf("duplicate reply replaced an already queued endpoint: %q", got)
	}
}

func TestBeaconNudgesOnlyUndiscoveredChannels(t *testing.T) {
	searching := &Channel{results: make(chan string, 1)}
	queued := &Channel{results: make(chan string, 1)}
	queued.results <- "127.0.0.1:5064"
	connected := &Channel{binding: &binding{}, results: make(chan string, 1)}
	c := &Client{channels: map[channelKey]*Channel{{name: "searching"}: searching, {name: "queued"}: queued, {name: "connected"}: connected}}
	c.nudgeSearches()
	select {
	case result := <-searching.results:
		if result != "" {
			t.Fatalf("beacon supplied a discovery address: %q", result)
		}
	default:
		t.Fatal("beacon did not wake channel discovery")
	}
	if result := <-queued.results; result != "127.0.0.1:5064" {
		t.Fatalf("beacon discarded a valid queued endpoint: %q", result)
	}
	if len(connected.results) != 0 {
		t.Fatal("beacon restarted an attached channel")
	}
}

func TestDiscoveryContinuesAfterMalformedDatagram(t *testing.T) {
	endpoint, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer endpoint.Close()
	c, err := New(context.Background(), Options{SearchAddresses: []string{endpoint.LocalAddr().String()}, DisableRepeater: true})
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()
	address := &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: c.udp.LocalAddr().(*net.UDPAddr).Port}
	for _, packet := range [][]byte{{1}, make([]byte, 17)} {
		if _, err := endpoint.WriteToUDP(packet, address); err != nil {
			t.Fatal(err)
		}
	}
	beacon, err := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 1})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := endpoint.WriteToUDP(beacon, address); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(time.Second)
	for {
		c.mu.Lock()
		state, seen := c.beacons["127.0.0.1:5064"]
		c.mu.Unlock()
		if seen {
			if state.sequence != 1 {
				t.Fatalf("unexpected beacon sequence: %d", state.sequence)
			}
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("valid beacon after malformed datagrams was not received")
		}
		time.Sleep(time.Millisecond)
	}
}

func TestClientErrorQueueRetainsNewestAndStopsAtClosure(t *testing.T) {
	c := &Client{errors: make(chan error, 2)}
	first, second, latest := errors.New("first"), errors.New("second"), errors.New("latest")
	c.report(first)
	c.report(second)
	c.report(latest)
	for _, want := range []error{second, latest} {
		select {
		case got := <-c.Errors():
			if got != want {
				t.Fatalf("retained error=%v, want %v", got, want)
			}
		default:
			t.Fatal("error queue unexpectedly empty")
		}
	}
	c.closed = true
	close(c.errors)
	c.report(errors.New("after closure"))
	if _, ok := <-c.Errors(); ok {
		t.Fatal("closed client accepted a new diagnostic")
	}
}

func TestMalformedAndExtendedServerErrorHeaders(t *testing.T) {
	for _, tt := range []struct {
		name      string
		payload   []byte
		completed bool
	}{
		{"short", []byte("short"), false},
		{"truncated_extended", func() []byte {
			b := make([]byte, 16)
			binary.BigEndian.PutUint16(b, wire.ReadNotify)
			binary.BigEndian.PutUint16(b[2:], 65535)
			binary.BigEndian.PutUint32(b[12:], 9)
			return b
		}(), false},
		{"extended", func() []byte {
			b := make([]byte, 24)
			binary.BigEndian.PutUint16(b, wire.ReadNotify)
			binary.BigEndian.PutUint16(b[2:], 65535)
			binary.BigEndian.PutUint32(b[12:], 9)
			return append(b, []byte("extended request denied\x00")...)
		}(), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c := &Client{errors: make(chan error, 1)}
			result := make(chan response, 1)
			conn := &circuit{client: c, requests: map[uint32]pendingResponse{9: {result: result}}}
			conn.protocolError(wire.Frame{Command: wire.Error, P2: 46 << 3, Payload: tt.payload})
			if got := len(result) != 0; got != tt.completed {
				t.Fatalf("request completed=%v, want %v", got, tt.completed)
			}
			var diagnostic *ProtocolError
			select {
			case err := <-c.Errors():
				if !errors.As(err, &diagnostic) || !errors.Is(err, ErrNoRead) {
					t.Fatalf("invalid server diagnostic: %v", err)
				}
			default:
				t.Fatal("server error was not reported")
			}
			if tt.completed {
				if reply := <-result; reply.err != diagnostic || diagnostic.Command != wire.ReadNotify || diagnostic.Message != "extended request denied" {
					t.Fatalf("extended request lost diagnostic: %+v", reply)
				}
			}
		})
	}
}

func TestServerErrorRejectsPendingChannelCreation(t *testing.T) {
	c := &Client{errors: make(chan error, 1)}
	b := &binding{reply: make(chan wire.Frame, 1)}
	conn := &circuit{client: c, bindings: map[uint32]*binding{4: b}}
	header, err := wire.Encode(wire.Frame{Command: wire.Create, P1: 4})
	if err != nil {
		t.Fatal(err)
	}
	conn.protocolError(wire.Frame{Command: wire.Error, P2: 47 << 3, Payload: append(header, []byte("channel denied\x00")...)})
	select {
	case reply := <-b.reply:
		if reply.Command != wire.CreateFail {
			t.Fatalf("creation error returned command %d", reply.Command)
		}
	default:
		t.Fatal("server error left channel creation waiting for timeout")
	}
}
