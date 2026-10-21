package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/bec-project/cago/pkg/cago"
	"io"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestPutParse(t *testing.T) {
	t.Setenv("EPICS_CLI_TIMEOUT", "")
	for _, tt := range []struct {
		name    string
		args    []string
		values  []string
		timeout time.Duration
	}{
		{"scalar", []string{"PV", "-1.25"}, []string{"-1.25"}, time.Second},
		{"text", []string{"PV", "operator", "ready"}, []string{"operator ready"}, time.Second},
		{"array", []string{"-a", "PV", "3", "-1", "0", "2"}, []string{"-1", "0", "2"}, time.Second},
		{"native timeout", []string{"-w", "0.25", "PV", "1"}, []string{"1"}, 250 * time.Millisecond},
		{"timeout", []string{"-timeout", "2s", "PV", "1"}, []string{"1"}, 2 * time.Second},
		{"cobra timeout", []string{"--timeout=2s", "PV", "-1.25"}, []string{"-1.25"}, 2 * time.Second},
		{"cobra array", []string{"--array", "--wait", "0.25", "PV", "2", "-1", "2"}, []string{"-1", "2"}, 250 * time.Millisecond},
		{"literal flag", []string{"PV", "--help"}, []string{"--help"}, time.Second},
		{"attached wait", []string{"-w0.25", "PV", "1"}, []string{"1"}, 250 * time.Millisecond},
		{"cluster with legacy timeout", []string{"-cw", "0.25", "-timeout", "2s", "PV", "1"}, []string{"1"}, 2 * time.Second},
		{"attached cluster with legacy timeout", []string{"-cw0.25", "-timeout", "2s", "PV", "1"}, []string{"1"}, 2 * time.Second},
	} {
		t.Run(tt.name, func(t *testing.T) {
			c, err := parse(tt.args, io.Discard)
			if err != nil || c.pv != "PV" || !reflect.DeepEqual(c.values, tt.values) || c.timeout != tt.timeout {
				t.Fatalf("putConfig=%+v err=%v", c, err)
			}
		})
	}
}

func TestPutCobraContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := newPutCommand(io.Discard, io.Discard, func(ctx context.Context, c putConfig) error {
		return Runtime(write(ctx, c, io.Discard))
	})
	cmd.SetArgs([]string{"PV", "1"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled put: %v", err)
	}
}

func TestPutInvalidArguments(t *testing.T) {
	for _, args := range [][]string{
		nil, {"PV"}, {"", "1"}, {"-a", "PV", "0"}, {"-a", "PV", "2", "1"},
		{"-a", "PV", "1", "1", "2"}, {"-a", "PV", "-1", "1"}, {"-a", "-S", "PV", "1", "a"},
		{"-n", "-s", "PV", "1"}, {"-p", "100", "PV", "1"}, {"-timeout", "0s", "PV", "1"}, {"-w", "NaN", "PV", "1"},
		{"-w", "1e100", "PV", "1"}, {"-w", "0.00000000001", "PV", "1"},
		{"PV", strings.Repeat("x", 40)}, {"PV", "a\x00b"}, {"-unknown", "PV", "1"},
	} {
		var stderr bytes.Buffer
		if code := runPut(args, io.Discard, &stderr); code != 2 || stderr.Len() == 0 {
			t.Fatalf("%q: exit=%d stderr=%q", args, code, stderr.String())
		}
	}
	for _, help := range []string{"-h", "--help", "-help"} {
		var out bytes.Buffer
		if code := runPut([]string{help}, &out, io.Discard); code != 0 || !strings.Contains(out.String(), "--completion") {
			t.Fatalf("help=%s exit=%d output=%q", help, code, out.String())
		}
	}
}

func TestPutValues(t *testing.T) {
	for _, tt := range []struct {
		name      string
		putConfig putConfig
		native    cago.DataType
		labels    []string
		want      any
	}{
		{"numeric server conversion", putConfig{values: []string{"-1.25"}}, cago.Double, nil, []string{"-1.25"}},
		{"array server conversion", putConfig{values: []string{"1", "2"}}, cago.Long, nil, []string{"1", "2"}},
		{"enum label", putConfig{values: []string{"ON"}}, cago.Enum, []string{"OFF", "ON"}, []float64{1}},
		{"enum number", putConfig{values: []string{"1"}}, cago.Enum, []string{"OFF", "ON"}, []float64{1}},
		{"numeric label wins", putConfig{values: []string{"1"}}, cago.Enum, []string{"1", "ON"}, []float64{0}},
		{"forced number", putConfig{values: []string{"1"}, numeric: true}, cago.Enum, []string{"1", "ON"}, []float64{1}},
		{"forced label", putConfig{values: []string{"ON"}, stringEnum: true}, cago.Enum, []string{"OFF", "ON"}, []string{"ON"}},
		{"mixed enum array", putConfig{values: []string{"ON", "0"}}, cago.Enum, []string{"OFF", "ON"}, []float64{1, 0}},
		{"long text", putConfig{values: []string{"hi"}, longString: true}, cago.Char, nil, []byte{'h', 'i', 0}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := tt.putConfig.value(tt.native, tt.labels)
			if err != nil || !reflect.DeepEqual(v.Slice(), tt.want) {
				t.Fatalf("value=%v err=%v", v.Slice(), err)
			}
		})
	}
	for _, s := range []string{"missing", "-1", "65536", "1.5", "NaN", "+Inf", "1junk"} {
		if _, err := (putConfig{values: []string{s}}).value(cago.Enum, []string{"OFF", "ON"}); err == nil {
			t.Fatalf("accepted %q", s)
		}
	}
	if _, err := (putConfig{values: []string{"hi"}, longString: true}).value(cago.String, nil); err == nil {
		t.Fatal("-S accepted non-CHAR PV")
	}
	if _, err := (putConfig{values: []string{"1"}, stringEnum: true}).value(cago.Enum, []string{"OFF", "ON"}); err == nil {
		t.Fatal("-s accepted an index that is not a label")
	}
}

type fakeChannel struct {
	info       *cago.ChannelInfo
	written    []cago.Value
	calls      []string
	deadlines  []time.Time
	notify     func(context.Context) error
	put        func(context.Context) error
	readback   any
	readErrors []error
}

func (f *fakeChannel) record(ctx context.Context, call string) {
	f.calls = append(f.calls, call)
	deadline, _ := ctx.Deadline()
	f.deadlines = append(f.deadlines, deadline)
}
func (f *fakeChannel) Connect(ctx context.Context) error { f.record(ctx, "connect"); return nil }
func (f *fakeChannel) Info() cago.ChannelInfo {
	if f.info != nil {
		return *f.info
	}
	return cago.ChannelInfo{NativeType: cago.Double, Count: 1}
}
func (f *fakeChannel) Get(ctx context.Context, _ ...cago.RequestOption) (cago.Value, error) {
	f.record(ctx, "get")
	if len(f.readErrors) > 0 {
		err := f.readErrors[0]
		f.readErrors = f.readErrors[1:]
		if err != nil {
			return cago.Value{}, err
		}
	}
	if f.readback != nil {
		return cago.NewValue(f.readback)
	}
	return cago.NewValue(1.0)
}

func TestPutOldReadbackFailureDoesNotSuppressWrite(t *testing.T) {
	for _, completion := range []bool{false, true} {
		for _, newReadErr := range []error{nil, cago.ErrNoRead} {
			f := &fakeChannel{readErrors: []error{cago.ErrNoRead, newReadErr}}
			c := putConfig{pv: "PV", values: []string{"1"}, timeout: time.Second, completion: completion}
			var out bytes.Buffer
			err := c.execute(context.Background(), f, &out, nil)
			method := "put"
			if completion {
				method = "notify"
			}
			if !errors.Is(err, cago.ErrNoRead) || !reflect.DeepEqual(f.calls, []string{"connect", "get", method, "get"}) {
				t.Fatalf("err=%v calls=%v", err, f.calls)
			}
			if !strings.Contains(out.String(), "Old : "+formatPVName("PV", 1)+"*** no read access") {
				t.Fatal(out.String())
			}
		}
	}
}
func (f *fakeChannel) Put(ctx context.Context, value any) error {
	f.written = append(f.written, value.(cago.Value))
	f.record(ctx, "put")
	if f.put != nil {
		return f.put(ctx)
	}
	return nil
}
func (f *fakeChannel) PutNotify(ctx context.Context, value any) error {
	f.written = append(f.written, value.(cago.Value))
	f.record(ctx, "notify")
	if f.notify != nil {
		return f.notify(ctx)
	}
	return nil
}

func TestPutCompletionModeAndPerOperationTimeout(t *testing.T) {
	for _, completion := range []bool{false, true} {
		f := &fakeChannel{readback: 8.0}
		c := putConfig{pv: "PV", values: []string{"100"}, timeout: time.Second, completion: completion}
		var out bytes.Buffer
		if err := c.execute(context.Background(), f, &out, nil); err != nil {
			t.Fatal(err)
		}
		method := "put"
		if completion {
			method = "notify"
		}
		if !reflect.DeepEqual(f.calls, []string{"connect", "get", method, "get"}) {
			t.Fatal(f.calls)
		}
		if got := out.String(); got != "Old : PV                             8\nNew : PV                             8\n" {
			t.Fatal(got)
		}
		for i := 1; i < len(f.deadlines); i++ {
			if !f.deadlines[i].After(f.deadlines[i-1]) {
				t.Fatal("operations share one command-wide deadline")
			}
		}
	}
}

func TestPutCompletionFailureAndTimeout(t *testing.T) {
	for _, failure := range []error{errors.New("server rejected completion"), context.DeadlineExceeded, cago.ErrDisconnected} {
		f := &fakeChannel{notify: func(context.Context) error { return failure }}
		c := putConfig{pv: "PV", values: []string{"1"}, timeout: time.Second, completion: true, terse: true}
		var out bytes.Buffer
		err := c.execute(context.Background(), f, &out, nil)
		if !errors.Is(err, failure) || out.Len() != 0 || !reflect.DeepEqual(f.calls, []string{"connect", "notify"}) {
			t.Fatalf("err=%v out=%q calls=%v", err, out.String(), f.calls)
		}
		if failure == context.DeadlineExceeded && !strings.Contains(err.Error(), "may still process") {
			t.Fatal("ambiguous timeout not explained")
		}
	}
	f := &fakeChannel{notify: func(ctx context.Context) error { <-ctx.Done(); return ctx.Err() }}
	c := putConfig{pv: "PV", values: []string{"1"}, timeout: time.Millisecond, completion: true, terse: true}
	if err := c.execute(context.Background(), f, io.Discard, nil); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatal(err)
	}
}

func TestPutPlainPutAsynchronousError(t *testing.T) {
	errs := make(chan error, 1)
	f := &fakeChannel{put: func(context.Context) error { errs <- cago.ErrNoWrite; return nil }}
	c := putConfig{pv: "PV", values: []string{"1"}, timeout: time.Second, terse: true}
	if err := c.execute(context.Background(), f, io.Discard, errs); !errors.Is(err, cago.ErrNoWrite) {
		t.Fatal(err)
	}
}

func TestPutCompletionFlagsAndEnvironment(t *testing.T) {
	t.Setenv("EPICS_CLI_TIMEOUT", "2.5")
	for _, tt := range []struct {
		args       []string
		completion bool
		timeout    time.Duration
	}{
		{[]string{"PV", "1"}, false, 2500 * time.Millisecond},
		{[]string{"-w", "3", "PV", "1"}, false, 3 * time.Second},
		{[]string{"-c", "-w", "3", "PV", "1"}, true, 3 * time.Second},
		{[]string{"-c=false", "PV", "1"}, false, 2500 * time.Millisecond},
	} {
		c, err := parse(tt.args, io.Discard)
		if err != nil || c.completion != tt.completion || c.timeout != tt.timeout {
			t.Fatalf("%v putConfig=%+v err=%v", tt.args, c, err)
		}
	}
	t.Setenv("EPICS_CLI_TIMEOUT", "invalid")
	var stderr bytes.Buffer
	c, err := parse([]string{"PV", "1"}, &stderr)
	if err != nil || c.timeout != time.Second || stderr.Len() == 0 {
		t.Fatal(fmt.Sprint(c, err, stderr.String()))
	}
}

func TestPutFormatReadback(t *testing.T) {
	for _, tt := range []struct {
		input       any
		array, long bool
		want        string
	}{
		{8.0, false, false, "8"}, {[]float64{1, 2}, true, false, "2 1 2"},
		{"ON", false, false, `"ON"`}, {[]byte{'h', 'i', 0, 0}, true, true, `"hi"`},
	} {
		v, _ := cago.NewValue(tt.input)
		if got := formatValue(v, tt.array, tt.long); got != tt.want {
			t.Fatalf("%q != %q", got, tt.want)
		}
	}
}

var errHelp = errors.New("help requested")

func parse(args []string, stderr io.Writer) (putConfig, error) {
	var c putConfig
	parsed := false
	cmd := newPutCommand(stderr, stderr, func(_ context.Context, value putConfig) error { c = value; parsed = true; return nil })
	cmd.SetArgs(Args(cmd, args))
	if err := cmd.Execute(); err != nil {
		return c, err
	}
	if !parsed {
		return c, errHelp
	}
	return c, nil
}

func TestPutUnreadableEnumDelegatesAutoConversionToIOC(t *testing.T) {
	for _, completion := range []bool{false, true} {
		for _, input := range []string{"1", "ON"} {
			f := &fakeChannel{info: &cago.ChannelInfo{NativeType: cago.Enum, Count: 1}, readErrors: []error{cago.ErrNoRead, cago.ErrNoRead, cago.ErrNoRead}}
			c := putConfig{pv: "PV", values: []string{input}, timeout: time.Second, completion: completion}
			var out bytes.Buffer
			err := c.execute(context.Background(), f, &out, nil)
			if !errors.Is(err, cago.ErrNoRead) || len(f.written) != 1 {
				t.Fatalf("completion=%v input=%q: err=%v calls=%v", completion, input, err, f.calls)
			}
			if got := f.written[0]; got.Type() != cago.String || got.Text() != input {
				t.Fatalf("IOC must resolve labels before indices: %+v", got)
			}
		}
	}
}

func TestPutEnumMetadataFailureDoesNotBypassStrictLabelMode(t *testing.T) {
	for _, failure := range []error{cago.ErrNoRead, context.DeadlineExceeded} {
		for _, strict := range []bool{false, true} {
			if !strict && errors.Is(failure, cago.ErrNoRead) {
				continue
			}
			f := &fakeChannel{info: &cago.ChannelInfo{NativeType: cago.Enum, Count: 1}, readErrors: []error{failure}}
			c := putConfig{pv: "PV", values: []string{"1"}, timeout: time.Second, stringEnum: strict}
			err := c.execute(context.Background(), f, &bytes.Buffer{}, nil)
			if !errors.Is(err, failure) || len(f.written) != 0 {
				t.Fatalf("strict=%v: err=%v calls=%v", strict, err, f.calls)
			}
		}
	}
}
