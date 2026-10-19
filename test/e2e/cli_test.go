//go:build e2e

package e2e_test

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/cli"
)

func TestSoftIOCUnifiedCLI(t *testing.T) {
	i := newIOC(t)
	t.Setenv("EPICS_CA_ADDR_LIST", i.address())
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	t.Setenv("EPICS_CA_SERVER_PORT", fmt.Sprint(i.port))
	t.Setenv("EPICS_CA_REPEATER_PORT", fmt.Sprint(i.beacon.LocalAddr().(*net.UDPAddr).Port))
	t.Setenv("EPICS_CA_CONN_TMO", "2")
	t.Setenv("EPICS_CLI_TIMEOUT", "2")
	for _, tt := range []struct {
		args []string
		want string
	}{
		{[]string{"get", "--timeout", "2s", i.prefix + "Long"}, fmt.Sprintf("%-30s 42\n", i.prefix+"Long")},
		{[]string{"put", "-c", "-t", i.prefix + "Double", "-1.25"}, "-1.25\n"},
		{[]string{"get", i.prefix + "Double"}, fmt.Sprintf("%-30s -1.25\n", i.prefix+"Double")},
		{[]string{"put", "-c", "-t", i.prefix + "Enum", "OFF"}, "\"OFF\"\n"},
	} {
		var out, stderr bytes.Buffer
		cmd := cli.NewRootCommand(&out, &stderr)
		cmd.SetContext(deadline(t))
		if code := cli.Execute(cmd, tt.args); code != 0 || out.String() != tt.want || stderr.Len() != 0 {
			t.Fatalf("args=%q exit=%d stdout=%q stderr=%q", tt.args, code, out.String(), stderr.String())
		}
	}
	var info, stderr bytes.Buffer
	cmd := cli.NewRootCommand(&info, &stderr)
	cmd.SetContext(deadline(t))
	if code := cli.Execute(cmd, []string{"info", "--verbose", "--priority", "99", i.prefix + "Double"}); code != 0 || stderr.Len() != 0 {
		t.Fatalf("info exit=%d stdout=%q stderr=%q", code, info.String(), stderr.String())
	}
	for _, want := range []string{i.prefix + "Double\n", "State: connected", "CA host name: " + i.address(), "Native DBF type: DBF_DOUBLE", "Number of elements: 1", "Record type: ao", "Value: -1.25", "Read access: yes", "Write access: yes", "Priority: 99"} {
		if !strings.Contains(info.String(), want) {
			t.Fatalf("info omits %q: %s", want, info.String())
		}
	}
	info.Reset()
	cmd = cli.NewRootCommand(&info, &stderr)
	cmd.SetContext(deadline(t))
	if code := cli.Execute(cmd, []string{"info", "--verbose", i.prefix + "Message"}); code != 0 || stderr.Len() != 0 {
		t.Fatalf("message info exit=%d stderr=%q", code, stderr.String())
	}
	for _, want := range []string{"Description: Operator message", "Record type: stringin", "Native DBF type: DBF_STRING", `Value: "User Operation 400mA top-up"`, "Alarm status: NO_ALARM, severity: NO_ALARM"} {
		if !strings.Contains(info.String(), want) {
			t.Errorf("message info omits %q: %s", want, info.String())
		}
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	monitor := &cliMonitorOutput{cancel: cancel}
	stderr.Reset()
	cmd = cli.NewRootCommand(monitor, &stderr)
	cmd.SetContext(ctx)
	if code := cli.Execute(cmd, []string{"monitor", i.prefix + "Long"}); code != 0 || stderr.Len() != 0 || !strings.Contains(monitor.String(), fmt.Sprintf("%-30s ", i.prefix+"Long")) || !strings.Contains(monitor.String(), " 42 UDF NO_ALARM\n") {
		t.Fatalf("monitor exit=%d stdout=%q stderr=%q", code, monitor.String(), stderr.String())
	}
}

type cliMonitorOutput struct {
	bytes.Buffer
	cancel context.CancelFunc
}

func (w *cliMonitorOutput) Write(p []byte) (int, error) {
	n, err := w.Buffer.Write(p)
	w.cancel()
	return n, err
}
