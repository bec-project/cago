package cli

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/wakonig/cago"
	"github.com/wakonig/cago/internal/wire"
)

type infoTestPV struct {
	typ                                               cago.DataType
	count                                             uint32
	access                                            cago.AccessRights
	value                                             *cago.Value
	description, recordType, className                string
	noDescription, noRecordType, noClass, readFailure bool
	silentClass                                       bool
	metadataField                                     bool
}

type infoTestIOC struct {
	address       string
	searched      chan string
	priority      atomic.Uint32
	reads         atomic.Int32
	classReads    atomic.Int32
	badClassCount atomic.Bool
	maxReadCount  atomic.Uint32
}

// A private loopback IOC and repeater sink isolate these commands from any
// EPICS installation or other IOC running on the test machine.
func newInfoTestIOC(t *testing.T, pvs map[string]infoTestPV) *infoTestIOC {
	t.Helper()
	repeater, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { repeater.Close() })
	t.Setenv("EPICS_CA_REPEATER_PORT", strconv.Itoa(repeater.LocalAddr().(*net.UDPAddr).Port))
	var tcp *net.TCPListener
	var udp *net.UDPConn
	for range 10 {
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
	ioc := &infoTestIOC{address: tcp.Addr().String(), searched: make(chan string, 128)}
	lookup := func(name string) (infoTestPV, bool) {
		if pv, ok := pvs[name]; ok {
			return pv, true
		}
		record, field, _ := strings.Cut(name, ".")
		if field == "DESC" || field == "RTYP" {
			for name, pv := range pvs {
				base, _, _ := strings.Cut(name, ".")
				if base != record {
					continue
				}
				if field == "DESC" && pv.noDescription || field == "RTYP" && pv.noRecordType {
					continue
				}
				text := pv.description
				if field == "RTYP" {
					text = pv.recordType
				}
				v, _ := cago.NewValue(text)
				return infoTestPV{typ: cago.String, count: 1, access: cago.ReadAccess, value: &v, metadataField: true}, true
			}
		}
		return infoTestPV{}, false
	}
	t.Setenv("EPICS_CA_ADDR_LIST", ioc.address)
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	var wg sync.WaitGroup
	var mu sync.Mutex
	closed := false
	connections := make(map[net.Conn]bool)
	wg.Add(2)
	go func() {
		defer wg.Done()
		data := make([]byte, 65535)
		for {
			n, from, err := udp.ReadFromUDP(data)
			if err != nil {
				return
			}
			frames := bytes.NewReader(data[:n])
			for frames.Len() != 0 {
				f, err := wire.Read(frames, len(data))
				if err != nil {
					break
				}
				if f.Command != wire.Search {
					continue
				}
				name := strings.TrimRight(string(f.Payload), "\x00")
				select {
				case ioc.searched <- name:
				default:
				}
				if _, ok := lookup(name); ok {
					reply, _ := wire.Encode(wire.Frame{Command: wire.Search, Type: uint16(tcp.Addr().(*net.TCPAddr).Port), P2: f.P2, Payload: []byte{0, 13}})
					udp.WriteToUDP(reply, from)
				}
			}
		}
	}()
	go func() {
		defer wg.Done()
		for {
			nc, err := tcp.AcceptTCP()
			if err != nil {
				return
			}
			mu.Lock()
			if closed {
				mu.Unlock()
				nc.Close()
				return
			}
			connections[nc] = true
			mu.Unlock()
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer nc.Close()
				bindings := make(map[uint32]infoTestPV)
				for {
					f, err := wire.Read(nc, 16384)
					if err != nil {
						return
					}
					var replies []wire.Frame
					switch f.Command {
					case wire.Version:
						ioc.priority.Store(uint32(f.Type))
						replies = []wire.Frame{{Command: wire.Version, Count: wire.Minor}}
					case wire.Create:
						pv, _ := lookup(strings.TrimRight(string(f.Payload), "\x00"))
						bindings[f.P1+100] = pv
						replies = []wire.Frame{
							{Command: wire.Access, P1: f.P1, P2: uint32(pv.access)},
							{Command: wire.Create, Type: uint16(pv.typ), Count: pv.count, P1: f.P1, P2: f.P1 + 100},
						}
					case wire.ReadNotify:
						pv := bindings[f.P1]
						reply := wire.Frame{Command: wire.ReadNotify, Type: f.Type, P1: 1, P2: f.P2}
						if cago.DataType(f.Type) == cago.ClassName {
							ioc.classReads.Add(1)
							if pv.silentClass {
								continue
							}
							if f.Count != 1 {
								ioc.badClassCount.Store(true)
							}
							reply.Count = 1
							reply.Payload = make([]byte, 40)
							name := pv.className
							if name == "" {
								name = pv.recordType
							}
							copy(reply.Payload, name)
							if pv.noClass {
								reply.P1 = 14 << 3
							}
						} else {
							if !pv.metadataField {
								ioc.reads.Add(1)
								for old := ioc.maxReadCount.Load(); f.Count > old && !ioc.maxReadCount.CompareAndSwap(old, f.Count); old = ioc.maxReadCount.Load() {
								}
							}
							n := f.Count
							if n == 0 || n > pv.count {
								n = pv.count
							}
							base := cago.DataType(f.Type).Base()
							if pv.value != nil && n > pv.value.Count() {
								n = pv.value.Count()
							}
							data := make([]byte, int(n)*base.Size())
							if pv.value != nil {
								copy(data, pv.value.Bytes())
							}
							if cago.DataType(f.Type) >= 14 && cago.DataType(f.Type) <= 20 {
								offset := [...]int{12, 14, 12, 14, 15, 12, 16}
								reply.Payload = make([]byte, offset[base]+len(data))
								binary.BigEndian.PutUint32(reply.Payload[4:8], 100)
								binary.BigEndian.PutUint32(reply.Payload[8:12], 123456789)
								copy(reply.Payload[offset[base]:], data)
							} else {
								reply.Payload = data
							}
							reply.Count = n
							if pv.readFailure {
								reply.P1 = 0
							}
						}
						replies = []wire.Frame{reply}
					case wire.Echo:
						replies = []wire.Frame{f}
					}
					for _, reply := range replies {
						out, _ := wire.Encode(reply)
						if _, err := nc.Write(out); err != nil {
							return
						}
					}
				}
			}()
		}
	}()
	t.Cleanup(func() {
		tcp.Close()
		udp.Close()
		mu.Lock()
		closed = true
		for nc := range connections {
			nc.Close()
		}
		mu.Unlock()
		wg.Wait()
	})
	return ioc
}

func TestInfoReportsNativeDetailsAndReadsOnlyReadablePVs(t *testing.T) {
	pvs := make(map[string]infoTestPV)
	var names []string
	for typ := cago.String; typ <= cago.Double; typ++ {
		name := fmt.Sprintf("PV:%d", typ)
		names = append(names, name)
		pvs[name] = infoTestPV{typ: typ, count: 70000 + uint32(typ), access: cago.AccessRights(typ % 4)}
	}
	ioc := newInfoTestIOC(t, pvs)
	var stdout, stderr bytes.Buffer
	args := append([]string{"--verbose", "--timeout=1s", "--priority=99"}, names...)
	if code := Execute(NewInfoCommand(&stdout, &stderr), args); code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	for _, name := range names {
		pv := pvs[name]
		for _, want := range []string{name + "\n", "Native DBF type: DBF_" + infoTypeName(pv.typ), fmt.Sprintf("Number of elements: %d", pv.count), "CA host name: " + ioc.address, "Priority: 99"} {
			if !strings.Contains(stdout.String(), want) {
				t.Errorf("missing %q in %q", want, stdout.String())
			}
		}
	}
	if ioc.priority.Load() != 99 || ioc.reads.Load() != 3 || ioc.classReads.Load() != 3 || ioc.badClassCount.Load() || ioc.maxReadCount.Load() != 16 || stderr.Len() != 0 {
		t.Fatalf("priority=%d reads=%d classReads=%d badCount=%v preview=%d stderr=%q", ioc.priority.Load(), ioc.reads.Load(), ioc.classReads.Load(), ioc.badClassCount.Load(), ioc.maxReadCount.Load(), stderr.String())
	}
}

func TestInfoMissingPVDoesNotSuppressHealthyInfo(t *testing.T) {
	for _, names := range [][]string{{"MISSING", "WRITE_ONLY"}, {"WRITE_ONLY", "MISSING"}} {
		t.Run(strings.Join(names, "_"), func(t *testing.T) {
			newInfoTestIOC(t, map[string]infoTestPV{"WRITE_ONLY": {typ: cago.Double, count: 1, access: cago.WriteAccess}})
			var stdout, stderr bytes.Buffer
			if code := Execute(NewInfoCommand(&stdout, &stderr), append([]string{"--timeout=150ms"}, names...)); code != 1 {
				t.Fatalf("exit=%d stderr=%q", code, stderr.String())
			}
			if !strings.Contains(stdout.String(), "WRITE_ONLY\n") || !strings.Contains(stderr.String(), "MISSING:") || strings.Contains(stderr.String(), "WRITE_ONLY:") {
				t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
			}
		})
	}
}

func TestInfoStartsAllSearchesBeforeWaiting(t *testing.T) {
	ioc := newInfoTestIOC(t, nil)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- readInfo(ctx, []string{"FIRST", "SECOND"}, time.Second, 0, 16, true, io.Discard) }()
	seen := make(map[string]bool)
	timer := time.NewTimer(500 * time.Millisecond)
	defer timer.Stop()
	for len(seen) < 2 {
		select {
		case name := <-ioc.searched:
			seen[name] = true
		case <-timer.C:
			cancel()
			<-done
			t.Fatal("not all PVs were searched before the first connection deadline")
		}
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
}

func TestInfoUsesIndependentPVTimeouts(t *testing.T) {
	newInfoTestIOC(t, nil)
	start := time.Now()
	err := readInfo(context.Background(), []string{"FIRST", "SECOND"}, 30*time.Millisecond, 0, 16, true, io.Discard)
	if elapsed := time.Since(start); elapsed < 50*time.Millisecond || elapsed > time.Second {
		t.Fatalf("two per-PV deadlines took %s", elapsed)
	}
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "FIRST:") || !strings.Contains(err.Error(), "SECOND:") {
		t.Fatalf("missing connection errors: %v", err)
	}
}

type infoBrokenWriter struct{}

func (infoBrokenWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestInfoOutputFailure(t *testing.T) {
	newInfoTestIOC(t, map[string]infoTestPV{"PV": {typ: cago.Float, count: 1, access: cago.ReadAccess}})
	err := readInfo(context.Background(), []string{"MISSING", "PV"}, 100*time.Millisecond, 0, 16, true, infoBrokenWriter{})
	if !errors.Is(err, io.ErrClosedPipe) || !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("output and previous connection errors lost: %v", err)
	}
}

func TestInfoValidationAndHelp(t *testing.T) {
	for _, args := range [][]string{nil, {""}, {"--timeout=0s", "PV"}, {"--timeout=-1s", "PV"}, {"--priority=100", "PV"}, {"--priority=-1", "PV"}, {"--count=4294967296", "PV"}, {"--count=-1", "PV"}, {"--bogus", "PV"}} {
		var stderr bytes.Buffer
		if code := Execute(NewInfoCommand(io.Discard, &stderr), args); code != 2 || stderr.Len() == 0 {
			t.Fatalf("args=%q exit=%d stderr=%q", args, code, stderr.String())
		}
	}
	for _, arg := range []string{"--help", "-help", "-h"} {
		var stdout bytes.Buffer
		if code := Execute(NewInfoCommand(&stdout, io.Discard), []string{arg}); code != 0 || !strings.Contains(stdout.String(), "info [flags] PV...") || !strings.Contains(stdout.String(), "--priority") {
			t.Fatalf("arg=%s exit=%d stdout=%q", arg, code, stdout.String())
		}
	}
}

func TestVerboseInfoStringRecord(t *testing.T) {
	v, _ := cago.NewValue("User Operation 400mA top-up")
	ioc := newInfoTestIOC(t, map[string]infoTestPV{"MESSAGE": {typ: cago.String, count: 1, access: cago.ReadAccess, value: &v, description: "Operator message", recordType: "stringin"}})
	var out, stderr bytes.Buffer
	if code := Execute(NewInfoCommand(&out, &stderr), []string{"--verbose", "MESSAGE"}); code != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{"MESSAGE\n", "Description: Operator message", "Record type: stringin", "Native DBF type: DBF_STRING", "Number of elements: 1", "Time: " + time.Unix(631152100, 123456789).Local().Format("2006-01-02 15:04:05.000000"), `Value: "User Operation 400mA top-up"`, "Alarm status: NO_ALARM, severity: NO_ALARM", "CA host name: " + ioc.address, "Read access: yes", "Write access: no"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q: %s", want, out.String())
		}
	}
}

func TestInfoOptionalFieldsCanBeMissingOrRejected(t *testing.T) {
	v, _ := cago.NewValue(2.5)
	newInfoTestIOC(t, map[string]infoTestPV{"GATEWAY": {typ: cago.Double, count: 1, access: cago.ReadAccess, value: &v, noDescription: true, noRecordType: true, noClass: true}})
	var out, stderr bytes.Buffer
	start := time.Now()
	if code := Execute(NewInfoCommand(&out, &stderr), []string{"--verbose", "--timeout=2s", "GATEWAY"}); code != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	if elapsed := time.Since(start); elapsed > time.Second {
		t.Fatalf("optional metadata consumed full timeout: %s", elapsed)
	}
	for _, want := range []string{"Description: <unavailable>", "Record type: <unavailable>", "Value: 2.5", "Alarm status: NO_ALARM"} {
		if !strings.Contains(out.String(), want) {
			t.Errorf("missing %q: %s", want, out.String())
		}
	}
}

func TestInfoDescriptionUsesRecordNameForFieldPV(t *testing.T) {
	newInfoTestIOC(t, map[string]infoTestPV{"RECORD.VAL": {typ: cago.Long, count: 1, access: cago.ReadAccess, description: "Record description", recordType: "longin"}})
	var out, stderr bytes.Buffer
	if code := Execute(NewInfoCommand(&out, &stderr), []string{"--verbose", "RECORD.VAL"}); code != 0 || !strings.Contains(out.String(), "Description: Record description") {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}

func TestInfoRecordTypePrefersFieldAndFallsBackToClass(t *testing.T) {
	for _, noField := range []bool{false, true} {
		newInfoTestIOC(t, map[string]infoTestPV{"PV": {typ: cago.Double, count: 1, access: cago.ReadAccess, recordType: "ai", className: "gatewayClass", noRecordType: noField}})
		var out, stderr bytes.Buffer
		want := "Record type: ai"
		if noField {
			want = "Record type: gatewayClass"
		}
		if code := Execute(NewInfoCommand(&out, &stderr), []string{"--verbose", "PV"}); code != 0 || !strings.Contains(out.String(), want) {
			t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
		}
	}
}

func TestInfoCancellationInterruptsOptionalMetadata(t *testing.T) {
	ioc := newInfoTestIOC(t, map[string]infoTestPV{"PV": {typ: cago.Double, count: 1, access: cago.ReadAccess, noDescription: true, noRecordType: true, silentClass: true}})
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- readInfo(ctx, []string{"PV"}, 5*time.Second, 0, 16, true, io.Discard) }()
	deadline := time.NewTimer(time.Second)
	defer deadline.Stop()
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for ioc.classReads.Load() == 0 {
		select {
		case <-deadline.C:
			cancel()
			<-done
			t.Fatal("optional metadata read did not start")
		case <-tick.C:
		}
	}
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("cancellation lost: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("optional metadata kept info running after cancellation")
	}
}

func TestInfoPreviewCountCanBeLimitedOrExplicitlyDynamic(t *testing.T) {
	a := make([]float64, 20)
	for i := range a {
		a[i] = float64(i)
	}
	v, _ := cago.NewValue(a)
	for _, tt := range []struct {
		args    []string
		preview bool
		count   uint32
	}{{nil, true, 16}, {[]string{"--count=1"}, true, 1}, {[]string{"--count=0"}, false, 0}} {
		ioc := newInfoTestIOC(t, map[string]infoTestPV{"WAVEFORM": {typ: cago.Double, count: 1000, access: cago.ReadAccess, value: &v}})
		var out, stderr bytes.Buffer
		if code := Execute(NewInfoCommand(&out, &stderr), append(append([]string{"--verbose"}, tt.args...), "WAVEFORM")); code != 0 || stderr.Len() != 0 {
			t.Fatalf("exit=%d stderr=%q", code, stderr.String())
		}
		if strings.Contains(out.String(), "(preview)") != tt.preview || ioc.maxReadCount.Load() != tt.count || !strings.Contains(out.String(), "Number of elements: 1000") || ioc.badClassCount.Load() {
			t.Fatalf("args=%q count=%d stdout=%q", tt.args, ioc.maxReadCount.Load(), out.String())
		}
	}
}

func TestInfoWriteOnlyAndReadFailureStillPrintConnectionDetails(t *testing.T) {
	for _, access := range []cago.AccessRights{cago.WriteAccess, cago.ReadAccess} {
		ioc := newInfoTestIOC(t, map[string]infoTestPV{"PV": {typ: cago.String, count: 1, access: access, readFailure: true}})
		var out, stderr bytes.Buffer
		want := 0
		if access == cago.ReadAccess {
			want = 1
		}
		if code := Execute(NewInfoCommand(&out, &stderr), []string{"--verbose", "PV"}); code != want {
			t.Fatalf("exit=%d expected=%d stderr=%q", code, want, stderr.String())
		}
		if !strings.Contains(out.String(), "Value: <unavailable>") || !strings.Contains(out.String(), "Time: <unavailable>") || !strings.Contains(out.String(), "Alarm status: <unavailable>") || !strings.Contains(out.String(), "CA host name: "+ioc.address) {
			t.Fatalf("invalid partial report: %s", out.String())
		}
		if access == cago.WriteAccess && ioc.reads.Load() != 0 {
			t.Fatal("attempted value read for write-only channel")
		}
	}
}

func TestInfoLegacyFlagsAndFlagLikePV(t *testing.T) {
	newInfoTestIOC(t, map[string]infoTestPV{"PV": {typ: cago.Char, count: 1}, "--priority=100": {typ: cago.String, count: 1}})
	var stdout, stderr bytes.Buffer
	if code := Execute(NewInfoCommand(&stdout, &stderr), []string{"--verbose", "-timeout", "1s", "-priority", "7", "PV", "--priority=100"}); code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), "--priority=100\n") || strings.Count(stdout.String(), "Priority: 7\n") != 2 {
		t.Fatalf("flags after first PV were parsed: %q", stdout.String())
	}
}

func TestInfoPreservesCobraCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := NewInfoCommand(io.Discard, io.Discard)
	cmd.SetArgs([]string{"PV"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled command: %v", err)
	}
}

func TestInfoTimeoutBoundsDNS(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", "cago-info-timeout.invalid:5064")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	original := net.DefaultResolver
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	defer func() { net.DefaultResolver = original }()
	start := time.Now()
	err := readInfo(context.Background(), []string{"PV"}, 20*time.Millisecond, 0, 16, true, io.Discard)
	if err == nil || time.Since(start) > time.Second {
		t.Fatalf("DNS was not bounded by command timeout: %v, elapsed=%s", err, time.Since(start))
	}
}

func TestInfoNames(t *testing.T) {
	for typ, want := range []string{"STRING", "SHORT", "FLOAT", "ENUM", "CHAR", "LONG", "DOUBLE"} {
		if got := infoTypeName(cago.DataType(typ)); got != want {
			t.Errorf("type %d: %q", typ, got)
		}
	}
	if got := infoTypeName(cago.Native); got != "UNKNOWN(65535)" {
		t.Errorf("unknown type: %q", got)
	}
}

func TestInfoDefaultMatchesEPICSConnectionReport(t *testing.T) {
	ioc := newInfoTestIOC(t, map[string]infoTestPV{
		"PV":         {typ: cago.Double, count: 8, access: cago.ReadAccess | cago.WriteAccess, readFailure: true},
		"WRITE_ONLY": {typ: cago.String, count: 1, access: cago.WriteAccess},
	})
	var out, stderr bytes.Buffer
	if code := Execute(NewInfoCommand(&out, &stderr), []string{"PV", "WRITE_ONLY"}); code != 0 || stderr.Len() != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	want := fmt.Sprintf("PV\n"+
		"    State:            connected\n"+
		"    Host:             %s\n"+
		"    Access:           read, write\n"+
		"    Native data type: DBF_DOUBLE\n"+
		"    Request type:     DBR_DOUBLE\n"+
		"    Element count:    8\n"+
		"WRITE_ONLY\n"+
		"    State:            connected\n"+
		"    Host:             %s\n"+
		"    Access:           no read, write\n"+
		"    Native data type: DBF_STRING\n"+
		"    Request type:     DBR_STRING\n"+
		"    Element count:    1\n", ioc.address, ioc.address)
	if got := out.String(); got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	if ioc.reads.Load() != 0 || ioc.classReads.Load() != 0 {
		t.Fatal("default info read values or record metadata")
	}
}
