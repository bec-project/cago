package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"github.com/wakonig/cago/internal/wire"
	"io"
	"math"
	"net"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// monitorIOC serves one value with DBR_TIME_DOUBLE metadata. Its random paired
// TCP/UDP port isolates these command-level tests from running CA installations.
func monitorIOC(t *testing.T) string {
	t.Helper()
	// Occupy an isolated repeater port so the command cannot register with a
	// native repeater or open its own wildcard listener on a shared CA port.
	repeater, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repeater.Close() })
	t.Setenv("EPICS_CA_REPEATER_PORT", strconv.Itoa(repeater.LocalAddr().(*net.UDPAddr).Port))
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
	var mu sync.Mutex
	var conn *net.TCPConn
	closing := false
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
				if f.Command == wire.Search && strings.TrimRight(string(f.Payload), "\x00") == "HEALTHY" {
					data, _ := wire.Encode(wire.Frame{Command: wire.Search, Type: uint16(tcp.Addr().(*net.TCPAddr).Port), P2: f.P2, Payload: []byte{0, 13}})
					udp.WriteToUDP(data, from)
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
		mu.Lock()
		conn = nc
		stop := closing
		mu.Unlock()
		if stop {
			return
		}
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
				out = wire.Frame{Command: wire.Create, Type: 6, Count: 1, P1: f.P1, P2: 1}
			case wire.EventAdd:
				data := make([]byte, 24)
				binary.BigEndian.PutUint32(data[4:8], 1)
				binary.BigEndian.PutUint32(data[8:12], 25000000)
				binary.BigEndian.PutUint64(data[16:24], math.Float64bits(5))
				out = wire.Frame{Command: wire.EventAdd, Type: 20, Count: 1, P1: 1, P2: f.P2, Payload: data}
			case wire.Echo:
				out = f
			default:
				continue
			}
			data, _ := wire.Encode(out)
			if _, err := io.Copy(nc, bytes.NewReader(data)); err != nil {
				return
			}
		}
	}()
	t.Cleanup(func() {
		mu.Lock()
		closing = true
		if conn != nil {
			conn.Close()
		}
		mu.Unlock()
		tcp.Close()
		udp.Close()
		wg.Wait()
	})
	return net.JoinHostPort("127.0.0.1", strconv.Itoa(tcp.Addr().(*net.TCPAddr).Port))
}

type cancelAfterRecord struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cancelAfterRecord) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.cancel()
	return n, err
}

func TestMonitorMonitorSubscriptionEmitsTimestampAndStopsOnCancellation(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", monitorIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := &cancelAfterRecord{cancel: cancel}
	var stderr bytes.Buffer
	cmd := NewMonitorCommand(out, &stderr)
	cmd.SetContext(ctx)
	cmd.SetArgs([]string{"--timestamp", "HEALTHY"})
	if err := cmd.Execute(); err != nil {
		t.Fatal(err)
	}
	if got := out.String(); got != formatPVName("HEALTHY", 1)+time.Unix(631152001, 25000000).Local().Format("2006-01-02 15:04:05.000000")+"  5  \n" || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", got, stderr.String())
	}
}

func TestMonitorMonitorLiveOutputFailureCancelsUnavailableSibling(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", monitorIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	err := monitor(ctx, []string{"MISSING", "HEALTHY"}, failingWriter{}, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "one or more monitors failed") {
		t.Fatalf("live output failure was not reported: %v", err)
	}
	if ctx.Err() != nil {
		t.Fatalf("output failure waited for parent deadline instead of cancelling sibling: %v", ctx.Err())
	}
}

func TestMonitorMonitorInvalidClientConfiguration(t *testing.T) {
	t.Setenv("EPICS_CA_SERVER_PORT", "-1")
	err := monitor(context.Background(), []string{"PV"}, io.Discard, io.Discard)
	if err == nil || !strings.Contains(err.Error(), "invalid CA options") {
		t.Fatalf("invalid client configuration accepted: %v", err)
	}
}
