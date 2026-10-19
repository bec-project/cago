package cli

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/pkg/cago"
)

func TestEPICSValueFormatting(t *testing.T) {
	for _, tt := range []struct {
		input       any
		array, long bool
		want        string
	}{
		{int16(-32768), false, false, "-32768"},
		{int32(-2147483648), false, false, "-2147483648"},
		{uint16(65535), false, false, "65535"},
		{float32(1.23456789), false, false, "1.23457"},
		{1.23456789, false, false, "1.23457"},
		{999999.5, false, false, "1e+06"},
		{0.0001, false, false, "0.0001"},
		{0.00001, false, false, "1e-05"},
		{math.Copysign(0, -1), false, false, "-0"},
		{math.Inf(1), false, false, "inf"},
		{math.Inf(-1), false, false, "-inf"},
		{math.NaN(), false, false, "nan"},
		{math.Copysign(math.NaN(), -1), false, false, "-nan"},
		{"User Operation 400mA top-up", false, false, "User Operation 400mA top-up"},
		{"\a\b\f\n\r\t\v\\'\"\x01\x7f\xff", false, false, `\a\b\f\n\r\t\v\\\'\"\x01\x7f\xff`},
		{[]byte{0, 127, 128, 255}, true, false, "4 0 127 -128 -1"},
		{[]float64{1.23456789, -2.5}, true, false, "2 1.23457 -2.5"},
		{[]string{"hello world", "", "line\n"}, true, false, `3 hello world  line\n`},
		{[]float64{1}, true, false, "1 1"},
		{[]float64{}, false, false, ""},
		{[]float64{}, true, false, "0 "},
		{[]byte{'a', '\n', 0, 'b'}, true, true, `a\n`},
		{[]byte{'a'}, false, true, "97"},
		{"abc", false, true, "abc"},
	} {
		t.Run(fmt.Sprintf("%T/%q", tt.input, tt.want), func(t *testing.T) {
			v, err := cago.NewValue(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got := formatValue(v, tt.array, tt.long); got != tt.want {
				t.Fatalf("got %q, want %q", got, tt.want)
			}
		})
	}
}

func TestEPICSSynchronousWaveformBuffer(t *testing.T) {
	for _, tt := range []struct {
		input any
		count uint32
		want  string
	}{
		{[]float64{1.25, 2.5}, 4, "4 1.25 2.5 0 0"},
		{[]string{"hello world", "line\n"}, 4, `4 hello world line\n  `},
		{[]int32{}, 2, "2 0 0"},
		{[]int16{1, 2, 3}, 1, "1 1"},
	} {
		v, _ := cago.NewValue(tt.input)
		if got := formatElements(v, tt.count, true, false); got != tt.want {
			t.Fatalf("got %q, want %q", got, tt.want)
		}
	}
}

func TestEPICSPVNamePaddingUsesBytes(t *testing.T) {
	for _, pv := range []string{"PV", "éPV", strings.Repeat("a", 30), strings.Repeat("b", 40)} {
		for _, count := range []uint32{0, 1, 2} {
			want := pv + " "
			if count <= 1 && len(pv) < 30 {
				want = pv + strings.Repeat(" ", 31-len(pv))
			}
			if got := formatPVName(pv, count); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		}
	}
}

func TestEPICSTimestampsAndAlarms(t *testing.T) {
	if got := formatTimestamp(cago.Metadata{Level: cago.Time}); got != "<undefined>" {
		t.Fatal(got)
	}
	for _, tt := range []struct {
		nanos  uint32
		suffix string
	}{
		{0, "000000"}, {499, "000000"}, {500, "000001"},
		{123456499, "123456"}, {123456500, "123457"},
		{999999999, "999999"}, {1000000000, "OVF"}, {math.MaxUint32, "OVF"},
	} {
		m := cago.Metadata{Level: cago.Time, SecondsPastEpoch: 100, Nanoseconds: tt.nanos}
		want := time.Unix(631152100, 0).Local().Format("2006-01-02 15:04:05.") + tt.suffix
		if got := formatTimestamp(m); got != want {
			t.Fatalf("nsec=%d: got %q, want %q", tt.nanos, got, want)
		}
	}
	plain := time.Date(2026, 1, 2, 3, 4, 5, 123456500, time.UTC)
	if got, want := formatTimestamp(cago.Metadata{Timestamp: plain}), plain.Local().Format("2006-01-02 15:04:05.")+"123457"; got != want {
		t.Fatalf("plain timestamp %q != %q", got, want)
	}
	for _, tt := range []struct {
		status, severity uint16
		suffix           string
	}{
		{0, 0, "  \n"}, {3, 2, " HIHI MAJOR\n"}, {0, 1, " NO_ALARM MINOR\n"}, {65535, 65535, " ?? ??\n"},
	} {
		v := infoTimeValue(t, 1.23456789, tt.status, tt.severity)
		want := "PV                             " + formatTimestamp(v.Metadata()) + " 1.23457" + tt.suffix
		if got := formatMonitorValue("PV", v, outputOptions{}); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	}
	v := infoTimeValue(t, []byte{'a', '\t', 0}, 0, 0)
	if got := formatMonitorValue("WAVE", v, outputOptions{longString: true}); !strings.HasPrefix(got, "WAVE ") || !strings.HasSuffix(got, ` a\t`+"  \n") {
		t.Fatal(got)
	}
}

func TestEPICSReadDiagnostics(t *testing.T) {
	for _, tt := range []struct {
		err       error
		connected bool
		want      string
	}{
		{cago.ErrNoRead, true, "*** no read access"},
		{context.DeadlineExceeded, false, "*** not connected"},
		{context.DeadlineExceeded, true, "*** no data available (timeout)"},
		{cago.ErrDisconnected, true, "*** not connected"},
		{cago.ErrClosed, true, "*** not connected"},
		{errors.New("bad value"), true, "*** CA error bad value"},
	} {
		if got := formatReadError(tt.err, tt.connected); got != tt.want {
			t.Fatalf("got %q, want %q", got, tt.want)
		}
	}
}
