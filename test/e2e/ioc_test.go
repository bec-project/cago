//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"crypto/rand"
	_ "embed"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/wakonig/cago/pkg/cago"
)

//go:embed testdata/records.db
var records string

const massChannels = 192

// Each fixture owns its process, database, PV namespace and loopback ports.
// EPICS variables inherited from a user's beamline shell never reach the IOC.
type ioc struct {
	t      *testing.T
	tool   string
	db     string
	prefix string
	port   int
	beacon *net.UDPConn
	cmd    *exec.Cmd
	done   chan struct{}
	log    lockedBuffer
}

type lockedBuffer struct {
	mu sync.Mutex
	b  bytes.Buffer
}

func (b *lockedBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.Write(p)
}

func (b *lockedBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.b.String()
}

func newIOC(t *testing.T) *ioc {
	t.Helper()
	tool, err := exec.LookPath("softIoc")
	if err != nil {
		if os.Getenv("CAGO_REQUIRE_IOC") != "" {
			t.Fatal("CAGO_REQUIRE_IOC is set but softIoc is not on PATH")
		}
		t.Skip("softIoc is not on PATH; build EPICS Base or set CAGO_REQUIRE_IOC=1 to require it")
	}
	var token [8]byte
	if _, err := rand.Read(token[:]); err != nil {
		t.Fatal(err)
	}
	// Retain the beacon destination to avoid sending beacons to another service.
	beacon, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { beacon.Close() })
	// Select the IOC port after binding the beacon sink, so the fixture cannot
	// accidentally reclaim its own just-released IOC port for the beacon socket.
	port := loopbackPort(t)
	i := &ioc{t: t, tool: tool, prefix: fmt.Sprintf("cago:%x:", token), port: port, beacon: beacon}
	t.Cleanup(func() {
		i.stop()
		if t.Failed() {
			t.Logf("softIoc output:\n%s", i.log.String())
		}
	})
	var db strings.Builder
	db.WriteString(records)
	for n := range massChannels {
		fmt.Fprintf(&db, "record(longout, \"$(P)Bulk%03d\") { field(VAL, \"%d\") }\n", n, n)
	}
	i.db = filepath.Join(t.TempDir(), "records.db")
	if err := os.WriteFile(i.db, []byte(db.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	i.start()
	return i
}

// Reserve both transports during selection; the IOC needs UDP and TCP on the
// same port. The short close/start gap is unavoidable for a subprocess server.
func loopbackPort(t *testing.T) int {
	t.Helper()
	for range 20 {
		tcp, err := net.Listen("tcp4", "127.0.0.1:0")
		if err != nil {
			t.Fatal(err)
		}
		port := tcp.Addr().(*net.TCPAddr).Port
		udp, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: port})
		tcp.Close()
		if err == nil {
			udp.Close()
			return port
		}
	}
	t.Fatal("could not select an unused loopback UDP/TCP port")
	return 0
}

func (i *ioc) address() string { return fmt.Sprintf("127.0.0.1:%d", i.port) }

func (i *ioc) start() {
	i.t.Helper()
	if i.cmd != nil {
		i.t.Fatal("IOC is already running")
	}
	cmd := exec.Command(i.tool, "-S", "-m", "P="+i.prefix, "-d", i.db)
	for _, entry := range os.Environ() {
		if !strings.HasPrefix(entry, "EPICS_") {
			cmd.Env = append(cmd.Env, entry)
		}
	}
	cmd.Env = append(cmd.Env,
		"EPICS_CAS_INTF_ADDR_LIST=127.0.0.1",
		"EPICS_CAS_SERVER_PORT="+fmt.Sprint(i.port),
		"EPICS_CAS_AUTO_BEACON_ADDR_LIST=NO",
		"EPICS_CAS_BEACON_ADDR_LIST="+i.beacon.LocalAddr().String(),
		"EPICS_CA_ADDR_LIST="+i.address(),
		"EPICS_CA_AUTO_ADDR_LIST=NO",
		"EPICS_CA_SERVER_PORT="+fmt.Sprint(i.port),
		"EPICS_CA_MAX_ARRAY_BYTES=1048576",
	)
	cmd.Stdout, cmd.Stderr = &i.log, &i.log
	if err := cmd.Start(); err != nil {
		i.t.Fatal(err)
	}
	i.cmd, i.done = cmd, make(chan struct{})
	done := i.done
	go func() {
		_ = cmd.Wait()
		close(done)
	}()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	for {
		conn, err := net.DialTimeout("tcp4", i.address(), 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		select {
		case <-done:
			i.t.Fatalf("softIoc exited during startup:\n%s", i.log.String())
		case <-ctx.Done():
			i.t.Fatalf("softIoc did not listen on %s:\n%s", i.address(), i.log.String())
		case <-time.After(10 * time.Millisecond):
		}
	}
}

func (i *ioc) stop() {
	i.t.Helper()
	if i.cmd == nil {
		return
	}
	_ = i.cmd.Process.Kill()
	select {
	case <-i.done:
	case <-time.After(5 * time.Second):
		i.t.Error("softIoc process did not exit after kill")
	}
	i.cmd = nil
}

func (i *ioc) client(t *testing.T) *cago.Client {
	t.Helper()
	c, err := cago.New(context.Background(), cago.Options{
		SearchAddresses: []string{i.address()}, DisableRepeater: true,
		ContextTimeout: 5 * time.Second, SearchInterval: 10 * time.Millisecond,
		MaxSearchPeriod: 100 * time.Millisecond, MaxArrayBytes: 1048576,
		EchoInterval: 100 * time.Millisecond, InactivityTimeout: 2 * time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := c.Close(); err != nil {
			t.Error(err)
		}
		select {
		case <-c.Done():
		default:
			t.Error("Client.Close returned before Done closed")
		}
	})
	return c
}

func deadline(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func get(t *testing.T, ch *cago.Channel, options ...cago.RequestOption) cago.Value {
	t.Helper()
	v, err := ch.Get(deadline(t), options...)
	if err != nil {
		t.Fatalf("Get(%s): %v", ch.Name(), err)
	}
	return v
}

func put(t *testing.T, ch *cago.Channel, input any) {
	t.Helper()
	v, err := cago.NewValue(input)
	if err != nil {
		t.Fatal(err)
	}
	if err := ch.PutNotify(deadline(t), v); err != nil {
		t.Fatalf("PutNotify(%s): %v", ch.Name(), err)
	}
}

func await(t *testing.T, label string, predicate func() bool) {
	t.Helper()
	ctx := deadline(t)
	ticker := time.NewTicker(10 * time.Millisecond)
	defer ticker.Stop()
	for !predicate() {
		select {
		case <-ctx.Done():
			t.Fatalf("timed out waiting for %s", label)
		case <-ticker.C:
		}
	}
}

func event(t *testing.T, sub *cago.Subscription, value float64) cago.Event {
	t.Helper()
	ctx := deadline(t)
	for {
		select {
		case v, ok := <-sub.Events():
			if !ok {
				t.Fatal("subscription closed before expected event")
			}
			if v.Value.Float64() == value {
				return v
			}
		case <-ctx.Done():
			t.Fatalf("timed out waiting for monitor value %g", value)
		}
	}
}

func TestSoftIOCNativeTypesAndMetadata(t *testing.T) {
	i := newIOC(t)
	c := i.client(t)
	tests := []struct {
		name string
		typ  cago.DataType
		in   any
		want any
	}{
		{"Long", cago.Long, int32(-123456), []int32{-123456}},
		{"Double", cago.Double, 2.5, []float64{2.5}},
		{"Text", cago.String, "operator ready", []string{"operator ready"}},
		{"Enum", cago.Enum, uint16(0), []uint16{0}},
		{"ShortArray", cago.Short, []int16{-32768, 0, 32767}, []int16{-32768, 0, 32767}},
		{"FloatArray", cago.Float, []float32{-1.25, 0, 3.5}, []float32{-1.25, 0, 3.5}},
		{"CharArray", cago.Char, []byte{0, 1, 128, 255}, []byte{0, 1, 128, 255}},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ch := c.Channel(i.prefix + tc.name)
			put(t, ch, tc.in)
			v := get(t, ch)
			if v.Type() != tc.typ || !reflect.DeepEqual(v.Slice(), tc.want) {
				t.Fatalf("native read type=%v value=%#v; want %v %#v", v.Type(), v.Slice(), tc.typ, tc.want)
			}
			info := ch.Info()
			if info.State != cago.Connected || info.NativeType != tc.typ || info.Access != cago.ReadAccess|cago.WriteAccess {
				t.Fatalf("invalid channel info: %+v", info)
			}
		})
	}
	t.Run("control metadata", func(t *testing.T) {
		v := get(t, c.Channel(i.prefix+"Double"), cago.WithMetadata(cago.Control))
		m := v.Metadata()
		if m.Level != cago.Control || m.Units != "mm" || m.Precision != 3 || m.DisplayLow != -10 || m.DisplayHigh != 10 || m.ControlLow != -9 || m.ControlHigh != 9 || m.AlarmLow != -8 || m.AlarmHigh != 8 {
			t.Fatalf("incorrect control metadata: %+v", m)
		}
		labels := get(t, c.Channel(i.prefix+"Enum"), cago.WithMetadata(cago.Control)).Metadata().EnumLabels
		if !reflect.DeepEqual(labels, []string{"OFF", "ON"}) {
			t.Fatalf("incorrect enum labels: %#v", labels)
		}
	})
	t.Run("timestamp", func(t *testing.T) {
		ch := c.Channel(i.prefix + "Double")
		before := time.Now().Add(-time.Second)
		put(t, ch, 3.5)
		m := get(t, ch, cago.WithMetadata(cago.Time)).Metadata()
		if m.Level != cago.Time || m.Timestamp.Before(before) || m.Timestamp.After(time.Now().Add(time.Second)) {
			t.Fatalf("incorrect IOC timestamp: %+v", m)
		}
	})
	t.Run("priority", func(t *testing.T) {
		ch := c.Channel(i.prefix+"Long", cago.WithPriority(40))
		get(t, ch)
		if ch.Info().Priority != 40 || c.Stats().Circuits != 2 {
			t.Fatalf("priority channel did not use a distinct circuit: info=%+v stats=%+v", ch.Info(), c.Stats())
		}
	})
}

func TestSoftIOCExtendedArrayAndCounts(t *testing.T) {
	i := newIOC(t)
	c := i.client(t)
	ch := c.Channel(i.prefix + "DoubleArray")
	large := make([]float64, 70000)
	for n := range large {
		large[n] = float64(n) / 8
	}
	put(t, ch, large)
	if v := get(t, ch); v.Count() != uint32(len(large)) || !reflect.DeepEqual(v.Slice(), large) {
		t.Fatalf("extended-header read did not preserve %d doubles: count=%d", len(large), v.Count())
	}
	if v := get(t, ch, cago.WithCount(3)); !reflect.DeepEqual(v.Slice(), large[:3]) {
		t.Fatalf("incorrect partial array: %#v", v.Slice())
	}
	put(t, ch, []float64{9, 8, 7})
	if v := get(t, ch, cago.WithCount(0)); !reflect.DeepEqual(v.Slice(), []float64{9, 8, 7}) {
		t.Fatalf("dynamic count ignored current IOC array length: %#v", v.Slice())
	}
	if _, err := ch.Get(deadline(t), cago.WithCount(70001)); !errors.Is(err, cago.ErrBadCount) {
		t.Fatalf("oversized count returned %v", err)
	}
	// A locally rejected request must not interrupt the shared TCP circuit.
	if v := get(t, c.Channel(i.prefix+"Long")); v.Int64() != 42 {
		t.Fatalf("healthy scalar changed after rejected request: %v", v.Slice())
	}
}

func TestSoftIOCSharedMonitorAndCancellation(t *testing.T) {
	i := newIOC(t)
	c := i.client(t)
	ch := c.Channel(i.prefix + "Double")
	ctx, cancel := context.WithCancel(deadline(t))
	defer cancel()
	first, err := ch.Subscribe(ctx, cago.WithMetadata(cago.Time))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { first.Cancel() })
	initial := event(t, first, 1.25)
	second, err := ch.Subscribe(deadline(t), cago.WithMetadata(cago.Time), cago.WithBuffer(8))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { second.Cancel() })
	replay := event(t, second, 1.25)
	if replay.ReceivedAt != initial.ReceivedAt || !reflect.DeepEqual(replay.Value.Metadata(), initial.Value.Metadata()) {
		t.Fatal("late subscriber did not receive the original cached snapshot")
	}
	if got := c.Metrics()[cago.MetricMonitorActivations]; got != 1 {
		t.Fatalf("matching subscribers activated %d IOC monitors", got)
	}
	put(t, ch, 2.25)
	event(t, first, 2.25)
	event(t, second, 2.25)
	cancel()
	await(t, "first subscription cancellation", func() bool {
		select {
		case <-first.Done():
			return true
		default:
			return false
		}
	})
	if _, ok := <-first.Events(); ok {
		t.Fatal("cancelled subscription retained queued events")
	}
	put(t, ch, 3.25)
	event(t, second, 3.25)
	if err := second.Cancel(); err != nil {
		t.Fatal(err)
	}
	await(t, "IOC monitor retirement", func() bool { return c.Metrics()[cago.MetricMonitorsActive] == 0 })
	// Client shutdown owns the remaining monitor and synchronously closes it.
	last, err := ch.Subscribe(deadline(t))
	if err != nil {
		t.Fatal(err)
	}
	event(t, last, 3.25)
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-last.Done():
	default:
		t.Fatal("Client.Close returned with an active subscription")
	}
	if c.Metrics()[cago.MetricMonitorsActive] != 0 {
		t.Fatal("active monitor gauge did not return to zero")
	}
}

func TestSoftIOCReconnectRestoresMonitor(t *testing.T) {
	i := newIOC(t)
	c := i.client(t)
	ch := c.Channel(i.prefix + "Double")
	sub, err := ch.Subscribe(deadline(t), cago.WithMetadata(cago.Time))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sub.Cancel() })
	event(t, sub, 1.25)
	put(t, ch, 4.25)
	event(t, sub, 4.25)
	i.stop()
	await(t, "disconnect", func() bool { return ch.Info().State == cago.Searching })
	i.start()
	if err := ch.Connect(deadline(t)); err != nil {
		t.Fatal(err)
	}
	if v := get(t, ch); v.Float64() != 1.25 {
		t.Fatalf("restart did not read fresh IOC value: %v", v.Slice())
	}
	event(t, sub, 1.25)
	put(t, ch, 5.25)
	event(t, sub, 5.25)
	if c.Stats().Reconnects < 1 {
		t.Fatalf("reconnection was not recorded: %+v", c.Stats())
	}
}

func TestSoftIOCMassAttachAndReconnect(t *testing.T) {
	i := newIOC(t)
	c := i.client(t)
	channels := make([]*cago.Channel, massChannels)
	for n := range channels {
		channels[n] = c.Channel(fmt.Sprintf("%sBulk%03d", i.prefix, n))
	}
	connect := func() {
		t.Helper()
		ctx := deadline(t)
		errs := make(chan error, len(channels))
		var wg sync.WaitGroup
		for n, ch := range channels {
			wg.Go(func() {
				v, err := ch.Get(ctx)
				if err == nil && v.Int64() != int64(n) {
					err = fmt.Errorf("%s read %d, want %d", ch.Name(), v.Int64(), n)
				}
				if err != nil {
					errs <- err
				}
			})
		}
		wg.Wait()
		close(errs)
		for err := range errs {
			t.Error(err)
		}
		if t.Failed() {
			t.FailNow()
		}
	}
	connect()
	i.stop()
	await(t, "all bulk channels disconnecting", func() bool {
		for _, ch := range channels {
			if ch.Info().State != cago.Searching {
				return false
			}
		}
		return true
	})
	i.start()
	connect()
	for _, ch := range channels {
		if err := ch.Close(); err != nil {
			t.Error(err)
		}
	}
	if c.Stats().Channels != 0 {
		t.Fatalf("bulk channel handles leaked after close: %+v", c.Stats())
	}
}
