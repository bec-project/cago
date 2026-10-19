package cago

import (
	"bytes"
	"context"
	"github.com/wakonig/cago/internal/wire"
	"net"
	"testing"
	"time"
)

func TestRepeaterEmptyRegistration(t *testing.T) {
	r, err := StartRepeater(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	u, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	if _, err = u.WriteToUDP(nil, r.socket.LocalAddr().(*net.UDPAddr)); err != nil {
		t.Fatal(err)
	}
	u.SetReadDeadline(time.Now().Add(200 * time.Millisecond))
	buf := make([]byte, 64)
	n, _, err := u.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("empty registration was not confirmed: %v", err)
	}
	f, err := wire.Read(bytes.NewReader(buf[:n]), 64)
	if err != nil || f.Command != wire.RepeaterConfirm {
		t.Fatal("bad registration confirmation")
	}
}

func TestRepeaterPreservesBundledDatagram(t *testing.T) {
	r, err := StartRepeater(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	consumer, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer consumer.Close()
	register, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister})
	consumer.WriteToUDP(register, r.socket.LocalAddr().(*net.UDPAddr))
	consumer.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 128)
	if _, _, err := consumer.ReadFromUDP(buf); err != nil {
		t.Fatal(err)
	}
	sender, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	defer sender.Close()
	first, _ := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 1})
	second, _ := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 2})
	third, _ := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 3, P2: 0x7f000002})
	version, _ := wire.Encode(wire.Frame{Command: wire.Version})
	packet := append(version, first...)
	packet = append(packet, second...)
	packet = append(packet, third...)
	sender.WriteToUDP(packet, r.socket.LocalAddr().(*net.UDPAddr))
	n, _, err := consumer.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	reader := bytes.NewReader(buf[:n])
	if f, err := wire.Read(reader, 128); err != nil || f.Command != wire.Version {
		t.Fatalf("lost version prefix: %+v %v", f, err)
	}
	for _, sequence := range []uint32{1, 2, 3} {
		f, err := wire.Read(reader, 128)
		address := uint32(0x7f000001)
		if sequence == 3 {
			address = 0x7f000002
		}
		if err != nil || f.P1 != sequence || f.P2 != address {
			t.Fatalf("lost bundled beacon %d: frame=%+v err=%v", sequence, f, err)
		}
	}
}

func TestRepeaterRegistrationPrefixAndNoReflection(t *testing.T) {
	r, err := StartRepeater(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	open := func() *net.UDPConn {
		u, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { u.Close() })
		return u
	}
	sink, sender := open(), open()
	registration, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister})
	beacon, _ := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 27})
	buf := make([]byte, 128)
	sink.WriteToUDP(registration, r.socket.LocalAddr().(*net.UDPAddr))
	sink.SetReadDeadline(time.Now().Add(time.Second))
	if _, _, err := sink.ReadFromUDP(buf); err != nil {
		t.Fatal(err)
	}
	sender.WriteToUDP(append(registration, beacon...), r.socket.LocalAddr().(*net.UDPAddr))
	sender.SetReadDeadline(time.Now().Add(time.Second))
	n, _, err := sender.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	f, err := wire.Read(bytes.NewReader(buf[:n]), 128)
	if err != nil || f.Command != wire.RepeaterConfirm {
		t.Fatal("missing registration confirmation")
	}
	n, _, err = sink.ReadFromUDP(buf)
	if err != nil {
		t.Fatal(err)
	}
	f, err = wire.Read(bytes.NewReader(buf[:n]), 128)
	if err != nil || f.Command != wire.Beacon || f.P1 != 27 || f.P2 != 0x7f000001 {
		t.Fatalf("registration prefix lost its beacon: %+v %v", f, err)
	}
	sender.SetReadDeadline(time.Now().Add(30 * time.Millisecond))
	if _, _, err := sender.ReadFromUDP(buf); err == nil {
		t.Fatal("repeater reflected a datagram to its sender")
	} else if e, ok := err.(net.Error); !ok || !e.Timeout() {
		t.Fatal(err)
	}
}

func TestRepeaterLocalInterfaceRegistration(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var local net.IP
	for _, address := range addresses {
		if ip, ok := address.(*net.IPNet); ok && ip.IP.To4() != nil && !ip.IP.IsLoopback() {
			local = ip.IP
			break
		}
	}
	if local == nil {
		t.Skip("no non-loopback IPv4 interface")
	}
	r, err := StartRepeater(context.Background(), net.JoinHostPort(local.String(), "0"))
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	u, err := net.ListenUDP("udp4", &net.UDPAddr{IP: local})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	registration, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister})
	u.WriteToUDP(registration, r.socket.LocalAddr().(*net.UDPAddr))
	u.SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 128)
	n, _, err := u.ReadFromUDP(buf)
	if err != nil {
		t.Fatalf("local interface registration rejected: %v", err)
	}
	f, err := wire.Read(bytes.NewReader(buf[:n]), 128)
	if err != nil || f.Command != wire.RepeaterConfirm {
		t.Fatal("missing confirmation")
	}
}

func TestRepeaterDeduplicatesAlternateLocalAddresses(t *testing.T) {
	addresses, err := net.InterfaceAddrs()
	if err != nil {
		t.Fatal(err)
	}
	var local net.IP
	for _, address := range addresses {
		if ip, ok := address.(*net.IPNet); ok && ip.IP.To4() != nil && !ip.IP.IsLoopback() {
			local = ip.IP
			break
		}
	}
	if local == nil {
		t.Skip("no non-loopback IPv4 interface")
	}
	r, err := StartRepeater(context.Background(), "0.0.0.0:0")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	u, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4zero})
	if err != nil {
		t.Fatal(err)
	}
	defer u.Close()
	registration, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister})
	for _, destination := range []net.IP{net.IPv4(127, 0, 0, 1), local} {
		_, err := u.WriteToUDP(registration, &net.UDPAddr{IP: destination, Port: r.socket.LocalAddr().(*net.UDPAddr).Port})
		if err != nil {
			t.Fatal(err)
		}
		u.SetReadDeadline(time.Now().Add(time.Second))
		buf := make([]byte, 128)
		n, _, err := u.ReadFromUDP(buf)
		if err != nil {
			t.Fatal(err)
		}
		f, err := wire.Read(bytes.NewReader(buf[:n]), 128)
		if err != nil || f.Command != wire.RepeaterConfirm {
			t.Fatal("missing registration confirmation")
		}
	}
	if r.Clients() != 1 {
		t.Fatalf("one socket registered as %d clients", r.Clients())
	}
}

func TestRepeaterPrunesDepartedClients(t *testing.T) {
	r, err := StartRepeater(context.Background(), "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer r.Close()
	register := func() *net.UDPConn {
		u, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if e != nil {
			t.Fatal(e)
		}
		f, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister, P2: 0x7f000001})
		u.WriteToUDP(f, r.socket.LocalAddr().(*net.UDPAddr))
		u.SetReadDeadline(time.Now().Add(time.Second))
		b := make([]byte, 100)
		n, _, e := u.ReadFromUDP(b)
		if e != nil {
			t.Fatal(e)
		}
		reply, e := wire.Read(bytes.NewReader(b[:n]), 100)
		if e != nil || reply.Command != wire.RepeaterConfirm {
			t.Fatal("bad repeater confirmation")
		}
		return u
	}
	first := register()
	first.Close()
	second := register()
	defer second.Close()
	if r.Clients() != 1 {
		t.Fatalf("stale repeater clients %d", r.Clients())
	}
	beacon, _ := wire.Encode(wire.Frame{Command: wire.Beacon, Type: 5064, P1: 1})
	sender, e := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if e != nil {
		t.Fatal(e)
	}
	defer sender.Close()
	sender.WriteToUDP(beacon, r.socket.LocalAddr().(*net.UDPAddr))
	b := make([]byte, 100)
	second.SetReadDeadline(time.Now().Add(time.Second))
	n, _, e := second.ReadFromUDP(b)
	if e != nil {
		t.Fatal(e)
	}
	f, e := wire.Read(bytes.NewReader(b[:n]), 100)
	if e != nil || f.Command != wire.Beacon || f.P2 != 0x7f000001 {
		t.Fatal("beacon forwarding lost origin")
	}
}
