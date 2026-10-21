package cli

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/bec-project/cago/internal/ca"
	"github.com/bec-project/cago/pkg/cago"
)

func infoTimeValue(t *testing.T, input any, status, severity uint16) cago.Value {
	t.Helper()
	value, err := cago.NewValue(input)
	if err != nil {
		t.Fatal(err)
	}
	// DBR_TIME layouts have differing alignment requirements for each DBF type.
	offsets := [...]int{12, 14, 12, 14, 15, 12, 16}
	payload := make([]byte, offsets[value.Type()]+len(value.Bytes()))
	binary.BigEndian.PutUint16(payload[0:2], status)
	binary.BigEndian.PutUint16(payload[2:4], severity)
	binary.BigEndian.PutUint32(payload[4:8], 1_161_189_236)
	binary.BigEndian.PutUint32(payload[8:12], 759_734_567)
	copy(payload[offsets[value.Type()]:], value.Bytes())
	result, err := ca.DecodeValue(cago.DataType(cago.Time)*7+value.Type(), value.Count(), payload)
	if err != nil {
		t.Fatal(err)
	}
	return result
}

func TestFormatInfoStringRecord(t *testing.T) {
	value := infoTimeValue(t, "User Operation 400mA top-up", 0, 0)
	got := formatInfo(infoDetails{
		Info: cago.ChannelInfo{
			Name: "AGEOP-CPCL-OPMSG:CR-MSG1", Host: "x07ma-cagw-01.psi.ch:5064",
			State: cago.Connected, NativeType: cago.String, Count: 1, Access: cago.ReadAccess, Priority: 7,
		},
		Description: "", RecordType: "stringin", Value: value,
	})
	want := fmt.Sprintf(`
-------------------------------
AGEOP-CPCL-OPMSG:CR-MSG1
        Description: %s
        Record type: stringin
        Native DBF type: DBF_STRING
        Number of elements: 1
        Time: %s
        Value: "User Operation 400mA top-up"

        Alarm status: NO_ALARM, severity: NO_ALARM

        CA host name: x07ma-cagw-01.psi.ch:5064
        Read access: yes
        Write access: no
        State: connected
        Priority: 7
-------------------------------
`, "", value.Metadata().Timestamp.Local().Format("2006-01-02 15:04:05.000000"))
	if got != want {
		t.Fatalf("channel report differs\ngot:  %q\nwant: %q", got, want)
	}
}

func TestInfoValueTextNativeTypes(t *testing.T) {
	tests := []struct {
		name  string
		input any
		want  string
	}{
		{"string", "line\n\"quoted\"\t\\", `"line\n\"quoted\"\t\\"`},
		{"short", int16(-32768), "-32768"},
		{"float", float32(1.1), "1.1"},
		{"enum", uint16(65535), "65535"},
		{"char", uint8(255), "255"},
		{"long", int32(-2147483648), "-2147483648"},
		{"double", float64(1.23456789012345), "1.23456789012345"},
		{"strings", []string{"hello world", "\"\n", ""}, `["hello world" "\"\n" ""]`},
		{"shorts", []int16{-32768, 32767}, "[-32768 32767]"},
		{"floats", []float32{1.1, -2.2}, "[1.1 -2.2]"},
		{"enums", []uint16{0, 65535}, "[0 65535]"},
		{"chars", []uint8{0, 255}, "[0 255]"},
		{"longs", []int32{-2147483648, 2147483647}, "[-2147483648 2147483647]"},
		{"doubles", []float64{-1.25, 2.5}, "[-1.25 2.5]"},
		{"empty strings", []string{}, "[]"},
		{"empty shorts", []int16{}, "[]"},
		{"empty floats", []float32{}, "[]"},
		{"empty enums", []uint16{}, "[]"},
		{"empty chars", []uint8{}, "[]"},
		{"empty longs", []int32{}, "[]"},
		{"empty doubles", []float64{}, "[]"},
		{"nan", math.NaN(), "NaN"},
		{"positive infinity", math.Inf(1), "+Inf"},
		{"negative infinity", math.Inf(-1), "-Inf"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			value := infoTimeValue(t, test.input, 0, 0)
			if got := infoValueText(value); got != test.want {
				t.Fatalf("value = %q, want %q", got, test.want)
			}
		})
	}
}

func TestFormatInfoAlarmNamesAndPreview(t *testing.T) {
	for _, test := range []struct {
		status, severity uint16
		want             string
	}{
		{3, 2, "HIHI, severity: MAJOR"},
		{11, 1, "HWLIMIT, severity: MINOR"},
		{17, 3, "UDF, severity: INVALID"},
		{20, 0, "READ_ACCESS, severity: NO_ALARM"},
		{21, 0, "WRITE_ACCESS, severity: NO_ALARM"},
		{22, 4, "UNKNOWN(22), severity: UNKNOWN(4)"},
		{65535, 65535, "UNKNOWN(65535), severity: UNKNOWN(65535)"},
	} {
		t.Run(test.want, func(t *testing.T) {
			got := formatInfo(infoDetails{
				Info:        cago.ChannelInfo{NativeType: cago.Short, Count: 1000, Access: cago.WriteAccess},
				Description: "Waveform preview", RecordType: "waveform",
				Value: infoTimeValue(t, []int16{1, 2}, test.status, test.severity), Limited: true,
			})
			for _, want := range []string{
				"Description: Waveform preview\n", "Record type: waveform\n", "Native DBF type: DBF_SHORT\n",
				"Number of elements: 1000\n", "Value: [1 2] (preview)\n", "Alarm status: " + test.want + "\n",
				"Read access: no\n", "Write access: yes\n",
			} {
				if !strings.Contains(got, want) {
					t.Errorf("report lacks %q:\n%s", want, got)
				}
			}
		})
	}
}

func TestFormatInfoUnavailableReadDoesNotInventMetadata(t *testing.T) {
	got := formatInfo(infoDetails{
		Info:        cago.ChannelInfo{NativeType: cago.DataType(99), Access: cago.ReadAccess | cago.WriteAccess},
		Description: "<unavailable>", RecordType: "<unavailable>",
		Value: infoTimeValue(t, int32(42), 0, 0), ValueError: errors.New("CA read access denied"), Limited: true,
	})
	for _, want := range []string{
		"Description: <unavailable>\n", "Record type: <unavailable>\n", "Native DBF type: DBF_UNKNOWN(99)\n",
		"Time: <unavailable>\n", "Value: <unavailable> (CA read access denied)\n", "Alarm status: <unavailable>\n",
		"Read access: yes\n", "Write access: yes\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("report lacks %q:\n%s", want, got)
		}
	}
	for _, unwanted := range []string{"NO_ALARM", "Value: 42", "(preview)", "1970-01-01"} {
		if strings.Contains(got, unwanted) {
			t.Errorf("failed read invented %q:\n%s", unwanted, got)
		}
	}
}

func TestFormatInfoPlainAndZeroTimestamp(t *testing.T) {
	plain, err := cago.NewValue(float64(2.5))
	if err != nil {
		t.Fatal(err)
	}
	got := formatInfo(infoDetails{Value: plain})
	for _, want := range []string{
		"Time: <unavailable>\n", "Value: 2.5\n", "Alarm status: <unavailable>\n", "Read access: no\n", "Write access: no\n",
	} {
		if !strings.Contains(got, want) {
			t.Errorf("plain report lacks %q:\n%s", want, got)
		}
	}
	// A successfully decoded TIME value always has an EPICS-epoch timestamp,
	// while the zero Value carries no metadata and must never print year one.
	got = formatInfo(infoDetails{})
	if !strings.Contains(got, "Time: <unavailable>\n") || strings.Contains(got, time.Time{}.Format("2006-01-02")) {
		t.Fatalf("zero value produced a false timestamp: %s", got)
	}
}

func TestConnectionInfoStatesAndAccess(t *testing.T) {
	for _, tt := range []struct {
		state                 cago.State
		host                  string
		access                cago.AccessRights
		stateText, accessText string
	}{
		{cago.Searching, "", 0, "never connected", "no read, no write"},
		{cago.Searching, "127.0.0.1:5064", cago.ReadAccess, "previously connected", "read, no write"},
		{cago.Closed, "", cago.WriteAccess, "closed", "no read, write"},
		{cago.Connected, "127.0.0.1:5064", cago.ReadAccess | cago.WriteAccess, "connected", "read, write"},
	} {
		got := formatChannelInfo(cago.ChannelInfo{Name: "PV", State: tt.state, Host: tt.host, Access: tt.access, NativeType: cago.Short, Count: 1})
		for _, want := range []string{"    State:            " + tt.stateText + "\n", "    Access:           " + tt.accessText + "\n", "    Native data type: DBF_SHORT\n", "    Request type:     DBR_SHORT\n"} {
			if !strings.Contains(got, want) {
				t.Fatalf("missing %q: %s", want, got)
			}
		}
	}
}
