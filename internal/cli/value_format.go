package cli

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strconv"
	"strings"
	"time"

	"github.com/wakonig/cago/pkg/cago"
)

// Use EPICS numeric and escaping conventions, with quoted text and optional
// timestamps. Keep CLI rendering separate from the public Value.Text method.
type outputOptions struct {
	numeric, terse, longString, callback, timestamp bool
}

func valueOptions(count uint32, numeric bool) []cago.RequestOption {
	typ := cago.String
	if numeric {
		typ = cago.Short
	}
	return []cago.RequestOption{cago.WithMetadata(cago.Time), cago.WithCount(count), cago.WithEnumType(typ)}
}

func formatPVName(pv string, count uint32) string {
	padding := 1
	if count <= 1 && len(pv) < 30 {
		padding += 30 - len(pv)
	}
	return pv + strings.Repeat(" ", padding)
}

func formatValue(v cago.Value, array, longString bool) string {
	return formatElements(v, v.Count(), array, longString)
}

// Synchronous caget/caput allocate a zero-filled buffer for the requested
// count. Reproduce that presentation when a waveform returns fewer elements.
func formatElements(v cago.Value, count uint32, array, longString bool) string {
	if longString && v.Type().Base() == cago.Char && array {
		data := v.Bytes()
		if uint64(len(data)) > uint64(count) {
			data = data[:count]
		}
		return `"` + escapeText(string(data)) + `"`
	}
	var out strings.Builder
	if array {
		out.WriteString(strconv.FormatUint(uint64(count), 10))
		out.WriteByte(' ')
	}
	for i := uint32(0); i < count; i++ {
		if i != 0 {
			out.WriteByte(' ')
		}
		if i >= v.Count() {
			if v.Type().Base() == cago.String {
				out.WriteString(`""`)
			} else {
				out.WriteByte('0')
			}
			continue
		}
		element, _ := v.Index(int(i))
		switch v.Type().Base() {
		case cago.String:
			out.WriteByte('"')
			out.WriteString(escapeText(element.Text()))
			out.WriteByte('"')
		case cago.Float, cago.Double:
			out.WriteString(formatFloat(element.Float64()))
		case cago.Char:
			// tool_lib.c promotes a plain C char, signed on our supported
			// Darwin arm64 and Linux amd64 command-line targets.
			out.WriteString(strconv.FormatInt(int64(int8(element.Int64())), 10))
		default:
			out.WriteString(strconv.FormatInt(element.Int64(), 10))
		}
	}
	return out.String()
}

func formatReadError(err error, connected bool) string {
	switch {
	case errors.Is(err, cago.ErrNoRead):
		return "*** no read access"
	case !connected, errors.Is(err, cago.ErrDisconnected), errors.Is(err, cago.ErrClosed):
		return "*** not connected"
	case errors.Is(err, context.DeadlineExceeded):
		return "*** no data available (timeout)"
	default:
		return "*** CA error " + err.Error()
	}
}

func formatFloat(value float64) string {
	switch {
	case math.IsNaN(value):
		if math.Signbit(value) {
			return "-nan"
		}
		return "nan"
	case math.IsInf(value, 1):
		return "inf"
	case math.IsInf(value, -1):
		return "-inf"
	default:
		return strconv.FormatFloat(value, 'g', 6, 64)
	}
}

func escapeText(text string) string {
	var out strings.Builder
	const hex = "0123456789abcdef"
	for i := 0; i < len(text) && text[i] != 0; i++ {
		switch ch := text[i]; ch {
		case '\a':
			out.WriteString(`\a`)
		case '\b':
			out.WriteString(`\b`)
		case '\f':
			out.WriteString(`\f`)
		case '\n':
			out.WriteString(`\n`)
		case '\r':
			out.WriteString(`\r`)
		case '\t':
			out.WriteString(`\t`)
		case '\v':
			out.WriteString(`\v`)
		case '\\', '\'', '"':
			out.WriteByte('\\')
			out.WriteByte(ch)
		default:
			if ch >= 0x20 && ch <= 0x7e {
				out.WriteByte(ch)
			} else {
				out.WriteString(`\x`)
				out.WriteByte(hex[ch>>4])
				out.WriteByte(hex[ch&15])
			}
		}
	}
	return out.String()
}

func formatTimestamp(metadata cago.Metadata) string {
	if metadata.Level != cago.Time && metadata.Timestamp.IsZero() {
		return "<undefined>"
	}
	stamp := metadata.Timestamp.Local()
	nanos := uint32(stamp.Nanosecond())
	if metadata.Level == cago.Time {
		if metadata.SecondsPastEpoch == 0 && metadata.Nanoseconds == 0 {
			return "<undefined>"
		}
		// Preserve libca's treatment of raw invalid nanoseconds rather than
		// using the normalization performed by time.Time.
		stamp = time.Unix(631152000+int64(metadata.SecondsPastEpoch), 0).Local()
		nanos = metadata.Nanoseconds
	}
	date := stamp.Format("2006-01-02 15:04:05.")
	if nanos >= 1_000_000_000 {
		return date + "OVF"
	}
	micros := (nanos + 500) / 1000
	if micros > 999999 {
		micros = 999999
	}
	return fmt.Sprintf("%s%06d", date, micros)
}

func formatMonitorValue(pv string, v cago.Value, options outputOptions) string {
	metadata := v.Metadata()
	alarm := "  "
	if metadata.Status != 0 || metadata.Severity != 0 {
		status, severity := "??", "??"
		if metadata.Status < 22 {
			status = infoAlarmStatusName(metadata.Status)
		}
		if metadata.Severity < 4 {
			severity = infoAlarmSeverityName(metadata.Severity)
		}
		alarm = " " + status + " " + severity
	}
	return formatPVName(pv, v.Count()) + formatOutputTimestamp(metadata, options) +
		formatValue(v, v.Count() > 1, options.longString) + alarm + "\n"
}

// Keep timestamps optional and separate their fixed-width column from values.
func formatOutputTimestamp(metadata cago.Metadata, options outputOptions) string {
	if !options.timestamp {
		return ""
	}
	return fmt.Sprintf("%-26s  ", formatTimestamp(metadata))
}
