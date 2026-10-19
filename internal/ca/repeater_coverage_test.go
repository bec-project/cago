package ca

import (
	"bytes"
	"context"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

func repeaterCoverageSocket(t *testing.T) *net.UDPConn {
	t.Helper()
	socket, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { socket.Close() })
	return socket
}

func repeaterCoverageRead(t *testing.T, socket *net.UDPConn, timeout time.Duration) (wire.Frame, *net.UDPAddr) {
	t.Helper()
	if err := socket.SetReadDeadline(time.Now().Add(timeout)); err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1024)
	n, from, err := socket.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(buf[:n])
	frame, err := wire.Read(reader, 1024)
	if err != nil || reader.Len() != 0 {
		t.Fatalf("expected one complete repeater frame: frame=%+v remaining=%d err=%v", frame, reader.Len(), err)
	}
	return frame, from
}

func repeaterCoverageWrite(t *testing.T, socket *net.UDPConn, to *net.UDPAddr, packet []byte) {
	t.Helper()
	if n, err := socket.WriteToUDP(packet, to); err != nil || n != len(packet) {
		t.Fatalf("write repeater packet: n=%d err=%v", n, err)
	}
}

func repeaterCoverageEncode(t *testing.T, frame wire.Frame) []byte {
	t.Helper()
	packet, err := wire.Encode(frame)
	if err != nil {
		t.Fatal(err)
	}
	return packet
}

func runRepeaterCoverageLoop(t *testing.T, port int) (*Client, context.CancelFunc, <-chan struct{}) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	client := &Client{ctx: ctx, opts: Options{RepeaterPort: port}, udp: repeaterCoverageSocket(t)}
	done := make(chan struct{})
	go func() {
		defer close(done)
		client.repeaterLoop()
	}()
	t.Cleanup(func() {
		cancel()
		select {
		case <-done:
		case <-time.After(2 * time.Second):
			t.Error("repeater registration worker did not stop after cancellation")
		}
	})
	return client, cancel, done
}

func TestRepeaterLoopOwnsAndClosesEmbeddedService(t *testing.T) {
	// Reserve an ephemeral port, then make it available to the embedded service.
	reservation := repeaterCoverageSocket(t)
	port := reservation.LocalAddr().(*net.UDPAddr).Port
	if err := reservation.Close(); err != nil {
		t.Fatal(err)
	}
	client, cancel, done := runRepeaterCoverageLoop(t, port)
	confirmation, from := repeaterCoverageRead(t, client.udp, 2*time.Second)
	if confirmation.Command != wire.RepeaterConfirm || confirmation.P2 != 0x7f000001 || from.Port != port {
		t.Fatalf("embedded repeater did not register its client: frame=%+v from=%v", confirmation, from)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("embedded repeater shutdown blocked")
	}
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero, Port: port})
	if err != nil {
		t.Fatalf("embedded repeater retained its socket after cancellation: %v", err)
	}
	probe.Close()
}

func TestRepeaterLoopRegistersPeriodicallyWithExistingService(t *testing.T) {
	service := repeaterCoverageSocket(t)
	port := service.LocalAddr().(*net.UDPAddr).Port
	client, cancel, done := runRepeaterCoverageLoop(t, port)
	for i := 0; i < 2; i++ {
		registration, from := repeaterCoverageRead(t, service, 8*time.Second)
		if registration.Command != wire.RepeaterRegister || registration.P2 != 0x7f000001 || from.Port != client.udp.LocalAddr().(*net.UDPAddr).Port {
			t.Fatalf("registration %d: frame=%+v from=%v", i, registration, from)
		}
	}
	cancel()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("registration with existing repeater did not stop")
	}
	// The client's worker must leave a separately owned repeater socket usable.
	marker := wire.Frame{Command: wire.Version, P1: 1234}
	repeaterCoverageWrite(t, client.udp, service.LocalAddr().(*net.UDPAddr), repeaterCoverageEncode(t, marker))
	frame, _ := repeaterCoverageRead(t, service, time.Second)
	if frame.Command != marker.Command || frame.P1 != marker.P1 {
		t.Fatalf("existing service was not usable after worker shutdown: %+v", frame)
	}
}

func TestRepeaterContextCancellationClosesSocket(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	repeater, err := StartRepeater(ctx, "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repeater.Close() })
	address := repeater.socket.LocalAddr().(*net.UDPAddr)
	cancel()
	select {
	case <-repeater.done:
	case <-time.After(2 * time.Second):
		t.Fatal("context cancellation did not stop repeater")
	}
	probe, err := net.ListenUDP("udp4", address)
	if err != nil {
		t.Fatalf("cancelled repeater retained its socket: %v", err)
	}
	probe.Close()
	if _, err := repeater.socket.WriteToUDP(nil, address); !errors.Is(err, net.ErrClosed) {
		t.Fatalf("cancelled repeater socket is not closed: %v", err)
	}
}

func TestRepeaterRejectsIncompleteDatagramsAtomically(t *testing.T) {
	registration := repeaterCoverageEncode(t, wire.Frame{Command: wire.RepeaterRegister})
	version := repeaterCoverageEncode(t, wire.Frame{Command: wire.Version})
	beacon := repeaterCoverageEncode(t, wire.Frame{Command: wire.Beacon, P1: 1})
	for _, tt := range []struct {
		name         string
		packet       []byte
		confirmation bool
	}{
		{"truncated_header", []byte{1}, false},
		{"registration_then_truncated_header", append(append([]byte(nil), registration...), 1), true},
		{"version_then_truncated_header", append(append([]byte(nil), version...), 1), false},
		{"beacon_then_truncated_header", append(append([]byte(nil), beacon...), 1), false},
	} {
		t.Run(tt.name, func(t *testing.T) {
			repeater, err := StartRepeater(context.Background(), "127.0.0.1:0")
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { repeater.Close() })
			consumer, sender := repeaterCoverageSocket(t), repeaterCoverageSocket(t)
			address := repeater.socket.LocalAddr().(*net.UDPAddr)
			repeaterCoverageWrite(t, consumer, address, registration)
			confirmation, _ := repeaterCoverageRead(t, consumer, time.Second)
			if confirmation.Command != wire.RepeaterConfirm {
				t.Fatalf("registration confirmation = %+v", confirmation)
			}
			repeaterCoverageWrite(t, sender, address, tt.packet)
			if tt.confirmation {
				confirmation, _ := repeaterCoverageRead(t, sender, time.Second)
				if confirmation.Command != wire.RepeaterConfirm || repeater.Clients() != 2 {
					t.Fatalf("complete registration prefix was lost: frame=%+v clients=%d", confirmation, repeater.Clients())
				}
			}
			// A valid marker follows the malformed datagram on the same socket.
			// Receiving it first proves no partial prefix escaped to the consumer.
			marker := repeaterCoverageEncode(t, wire.Frame{Command: wire.Beacon, P1: 99})
			repeaterCoverageWrite(t, sender, address, marker)
			frame, _ := repeaterCoverageRead(t, consumer, time.Second)
			if frame.Command != wire.Beacon || frame.P1 != 99 || frame.P2 != 0x7f000001 {
				t.Fatalf("malformed datagram was forwarded or blocked later traffic: %+v", frame)
			}
		})
	}
}

func TestLocalIPv4RejectsNonLocalAddresses(t *testing.T) {
	for _, address := range []string{"::1", "2001:db8::1", "192.0.2.1"} {
		if localIPv4(net.ParseIP(address)) {
			t.Errorf("non-local IPv4 registration accepted: %s", address)
		}
	}
	if localIPv4(nil) {
		t.Error("nil registration address accepted")
	}
}
