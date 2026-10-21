package cli

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/bec-project/cago/pkg/cago"
)

// infoDetails combines connection information with optional record metadata and
// a bounded value read. Count in Info describes capacity, not the preview size.
type infoDetails struct {
	Info                    cago.ChannelInfo
	Description, RecordType string
	Value                   cago.Value
	ValueError              error
	Limited                 bool
}

func formatChannelInfo(info cago.ChannelInfo) string {
	read, write := "no read", "no write"
	if info.Access&cago.ReadAccess != 0 {
		read = "read"
	}
	if info.Access&cago.WriteAccess != 0 {
		write = "write"
	}
	state := info.State.String()
	if info.State == cago.Searching {
		state = "never connected"
		if info.Host != "" {
			state = "previously connected"
		}
	}
	return fmt.Sprintf("%s\n"+
		"    State:            %s\n"+
		"    Host:             %s\n"+
		"    Access:           %s, %s\n"+
		"    Native data type: DBF_%s\n"+
		"    Request type:     DBR_%s\n"+
		"    Element count:    %d\n",
		info.Name, state, info.Host, read, write, infoTypeName(info.NativeType), infoTypeName(info.NativeType), info.Count)
}

func formatInfo(details infoDetails) string {
	const unavailable = "<unavailable>"
	timestamp, value, alarm := unavailable, unavailable, unavailable
	if details.ValueError != nil {
		value += " (" + details.ValueError.Error() + ")"
	} else {
		value = infoValueText(details.Value)
		if details.Limited {
			value += " (preview)"
		}
		metadata := details.Value.Metadata()
		if metadata.Level == cago.Time && !metadata.Timestamp.IsZero() {
			timestamp = metadata.Timestamp.Local().Format("2006-01-02 15:04:05.000000")
		}
		if metadata.Level >= cago.Status {
			alarm = infoAlarmStatusName(metadata.Status) + ", severity: " + infoAlarmSeverityName(metadata.Severity)
		}
	}
	info := details.Info
	return fmt.Sprintf("\n-------------------------------\n%s\n"+
		"        Description: %s\n"+
		"        Record type: %s\n"+
		"        Native DBF type: DBF_%s\n"+
		"        Number of elements: %d\n"+
		"        Time: %s\n"+
		"        Value: %s\n\n"+
		"        Alarm status: %s\n\n"+
		"        CA host name: %s\n"+
		"        Read access: %s\n"+
		"        Write access: %s\n"+
		"        State: %s\n"+
		"        Priority: %d\n"+
		"-------------------------------\n",
		info.Name, details.Description, details.RecordType, infoTypeName(info.NativeType), info.Count,
		timestamp, value, alarm, info.Host, infoYesNo(info.Access&cago.ReadAccess != 0),
		infoYesNo(info.Access&cago.WriteAccess != 0), info.State, info.Priority)
}

func infoValueText(value cago.Value) string {
	if value.Count() == 1 {
		switch value.Type().Base() {
		case cago.String:
			return strconv.Quote(value.Text())
		case cago.Float:
			return strconv.FormatFloat(value.Float64(), 'g', -1, 32)
		case cago.Double:
			return strconv.FormatFloat(value.Float64(), 'g', -1, 64)
		default:
			return strconv.FormatInt(value.Int64(), 10)
		}
	}
	values := value.Slice()
	if texts, ok := values.([]string); ok {
		var out strings.Builder
		out.WriteByte('[')
		for i, text := range texts {
			if i != 0 {
				out.WriteByte(' ')
			}
			out.WriteString(strconv.Quote(text))
		}
		out.WriteByte(']')
		return out.String()
	}
	return fmt.Sprint(values)
}

func infoAlarmStatusName(status uint16) string {
	names := [...]string{
		"NO_ALARM", "READ", "WRITE", "HIHI", "HIGH", "LOLO", "LOW", "STATE", "COS", "COMM", "TIMEOUT",
		"HWLIMIT", "CALC", "SCAN", "LINK", "SOFT", "BAD_SUB", "UDF", "DISABLE", "SIMM", "READ_ACCESS", "WRITE_ACCESS",
	}
	if int(status) < len(names) {
		return names[status]
	}
	return fmt.Sprintf("UNKNOWN(%d)", status)
}

func infoAlarmSeverityName(severity uint16) string {
	names := [...]string{"NO_ALARM", "MINOR", "MAJOR", "INVALID"}
	if int(severity) < len(names) {
		return names[severity]
	}
	return fmt.Sprintf("UNKNOWN(%d)", severity)
}

func infoYesNo(value bool) string {
	if value {
		return "yes"
	}
	return "no"
}
