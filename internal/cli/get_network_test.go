package cli

import (
	"bytes"
	"encoding/binary"
	"github.com/wakonig/cago/internal/wire"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
)

// A minimal subscription-free IOC keeps command timeout tests independent of
// EPICS Base. Only HEALTHY responds to discovery; MISSING never does.
func commandIOC(t *testing.T) string {
	t.Helper()
	data := make([]byte, 8)
	binary.BigEndian.PutUint64(data, math.Float64bits(5))
	return commandIOCValue(t, "HEALTHY", 6, data)
}

func commandIOCValue(t *testing.T, pv string, typ uint16, payload []byte) string {
	t.Helper()
	var tcp *net.TCPListener
	var udp *net.UDPConn
	for range 10 {
		var err error
		tcp, err = net.ListenTCP("tcp4", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
		if err != nil {
			t.Fatal(err)
		}
		udp, err = net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: tcp.Addr().(*net.TCPAddr).Port})
		if err == nil {
			break
		}
		tcp.Close()
	}
	if udp == nil {
		t.Fatal("could not allocate IOC TCP/UDP pair")
	}
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		defer wg.Done()
		buf := make([]byte, 1024)
		for {
			n, from, err := udp.ReadFromUDP(buf)
			if err != nil {
				return
			}
			reader := bytes.NewReader(buf[:n])
			for reader.Len() != 0 {
				f, err := wire.Read(reader, 1024)
				if err != nil {
					break
				}
				if f.Command == wire.Search && strings.TrimRight(string(f.Payload), "\x00") == pv {
					out, _ := wire.Encode(wire.Frame{Command: wire.Search, Type: uint16(tcp.Addr().(*net.TCPAddr).Port), P2: f.P2, Payload: []byte{0, 13}})
					udp.WriteToUDP(out, from)
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		nc, err := tcp.AcceptTCP()
		if err != nil {
			return
		}
		defer nc.Close()
		for {
			f, err := wire.Read(nc, 16384)
			if err != nil {
				return
			}
			var out wire.Frame
			switch f.Command {
			case wire.Version:
				out = wire.Frame{Command: wire.Version, Count: 13}
			case wire.Create:
				out = wire.Frame{Command: wire.Create, Type: typ, Count: 1, P1: f.P1, P2: 1}
			case wire.ReadNotify:
				out = wire.Frame{Command: wire.ReadNotify, Type: typ, Count: 1, P1: 1, P2: f.P2, Payload: payload}
			case wire.Echo:
				out = f
			default:
				continue
			}
			data, _ := wire.Encode(out)
			if _, err := nc.Write(data); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() { tcp.Close(); udp.Close(); wg.Wait() })
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(tcp.Addr().(*net.TCPAddr).Port))
}

func TestGetMissingPVDoesNotSuppressHealthyRead(t *testing.T) {
	for _, pvs := range [][]string{{"MISSING", "HEALTHY"}, {"HEALTHY", "MISSING"}} {
		t.Run(strings.Join(pvs, "_"), func(t *testing.T) {
			t.Setenv("EPICS_CA_ADDR_LIST", commandIOC(t))
			t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
			var out, stderr bytes.Buffer
			args := append([]string{"--timeout", "150ms"}, pvs...)
			if code := runGet(args, &out, &stderr); code != 1 || !strings.Contains(out.String(), "HEALTHY                        5") || strings.Contains(stderr.String(), "HEALTHY") {
				t.Fatalf("exit=%d out=%q stderr=%q", code, out.String(), stderr.String())
			}
		})
	}
}

func TestGetCompactScalarString(t *testing.T) {
	const pv = "AGEOP-CPCL-OPMSG:CR-MSG1"
	const message = "User Operation 400mA top-up"
	payload := make([]byte, 32)
	copy(payload, message)
	t.Setenv("EPICS_CA_ADDR_LIST", commandIOCValue(t, pv, 0, payload))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	var out, stderr bytes.Buffer
	cmd := NewRootCommand(&out, &stderr)
	if code := Execute(cmd, []string{"get", "--timeout", "1s", pv}); code != 0 || out.String() != formatPVName(pv, 1)+`"`+message+`"`+"\n" || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}

func TestGetTimestampFlag(t *testing.T) {
	for _, flag := range []string{"--timestamp", "-T"} {
		t.Run(flag, func(t *testing.T) {
			t.Setenv("EPICS_CA_ADDR_LIST", commandIOC(t))
			t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
			var out, stderr bytes.Buffer
			cmd := NewRootCommand(&out, &stderr)
			if code := Execute(cmd, []string{"get", flag, "HEALTHY"}); code != 0 || out.String() != formatPVName("HEALTHY", 1)+"<undefined>                 5\n" || stderr.Len() != 0 {
				t.Fatalf("%s exit=%d out=%q err=%q", flag, code, out.String(), stderr.String())
			}
		})
	}
}
