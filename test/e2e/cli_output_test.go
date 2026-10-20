//go:build e2e

package e2e_test

import (
	"bufio"
	"bytes"
	"context"
	"fmt"
	"net"
	"os/exec"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/cli"
)

func cliEnvironment(t *testing.T, i *ioc) {
	t.Helper()
	t.Setenv("EPICS_CA_ADDR_LIST", i.address())
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	t.Setenv("EPICS_CA_SERVER_PORT", fmt.Sprint(i.port))
	t.Setenv("EPICS_CA_REPEATER_PORT", fmt.Sprint(i.beacon.LocalAddr().(*net.UDPAddr).Port))
	t.Setenv("EPICS_CLI_TIMEOUT", "2")
}

func referenceOutput(t *testing.T, tool string, args ...string) string {
	t.Helper()
	cmd := exec.CommandContext(deadline(t), tool, args...)
	var out, stderr bytes.Buffer
	cmd.Stdout, cmd.Stderr = &out, &stderr
	if err := cmd.Run(); err != nil {
		t.Fatalf("%s %q: %v: %s", tool, args, err, &stderr)
	}
	return out.String()
}

func commandOutput(t *testing.T, args ...string) string {
	t.Helper()
	var out, stderr bytes.Buffer
	cmd := cli.NewRootCommand(&out, &stderr)
	cmd.SetContext(deadline(t))
	if code := cli.Execute(cmd, args); code != 0 || stderr.Len() != 0 {
		t.Fatalf("cago %q: exit=%d stderr=%s", args, code, &stderr)
	}
	return out.String()
}

func referenceMonitorLine(t *testing.T, args ...string) string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cmd := exec.CommandContext(ctx, "camonitor", args...)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err = cmd.Start(); err != nil {
		t.Fatal(err)
	}
	reader := bufio.NewReader(out)
	line, readErr := reader.ReadString('\n')
	_ = cmd.Process.Kill()
	_ = cmd.Wait()
	if readErr != nil {
		t.Fatalf("camonitor: %v: %s", readErr, &stderr)
	}
	return line
}

func TestSoftIOCOutputMatchesEPICSTools(t *testing.T) {
	for _, tool := range []string{"caget", "caput", "camonitor", "cainfo"} {
		if _, err := exec.LookPath(tool); err != nil {
			t.Skipf("%s is not on PATH", tool)
		}
	}
	i := newIOC(t)
	cliEnvironment(t, i)
	c := i.client(t)
	put(t, c.Channel(i.prefix+"Double"), 1.23456789)
	put(t, c.Channel(i.prefix+"Text"), "line\n\"quoted\"\t\\")
	put(t, c.Channel(i.prefix+"ShortArray"), []int16{-32768, 32767})
	put(t, c.Channel(i.prefix+"FloatArray"), []float32{1.23456789, -2.5})
	put(t, c.Channel(i.prefix+"CharArray"), []byte{'a', '\n', 255, 0})
	for _, tt := range []struct {
		pv               string
		flags, reference []string
	}{
		{"Long", nil, nil}, {"Double", nil, nil}, {"Text", nil, nil}, {"Message", nil, nil},
		{"Enum", nil, nil}, {"Enum", []string{"-n"}, []string{"-n"}},
		{"Double", []string{"-t"}, []string{"-t"}},
		{"ShortArray", nil, nil}, {"FloatArray", nil, nil}, {"CharArray", nil, nil},
		{"CharArray", []string{"-S"}, []string{"-S"}},
		{"ShortArray", []string{"-c"}, []string{"-c"}},
		{"ShortArray", []string{"--count=1"}, []string{"-#", "1"}},
		{"ShortArray", []string{"--count=100"}, []string{"-#", "100"}},
		{"DoubleArray", []string{"--count=2"}, []string{"-#", "2"}},
		{"DoubleArray", []string{"-c"}, []string{"-c"}},
	} {
		t.Run("get/"+tt.pv+fmt.Sprint(tt.flags), func(t *testing.T) {
			pv := i.prefix + tt.pv
			want := quotedReferenceValues(referenceOutput(t, "caget", append(tt.reference, pv)...))
			got := commandOutput(t, append(append([]string{"get"}, tt.flags...), pv)...)
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
	for _, tt := range []struct {
		pv    string
		flags []string
	}{
		{"Long", nil}, {"Double", nil}, {"Enum", nil}, {"Enum", []string{"-n"}},
		{"Text", nil}, {"ShortArray", nil}, {"CharArray", nil}, {"CharArray", []string{"-S"}},
	} {
		t.Run("monitor/"+tt.pv+fmt.Sprint(tt.flags), func(t *testing.T) {
			pv := i.prefix + tt.pv
			want := quotedReferenceValues(referenceMonitorLine(t, append(tt.flags, pv)...))
			// Cago timestamps are opt-in and use two spaces before the value.
			want = strings.ReplaceAll(want, "<undefined> ", "<undefined>                 ")
			want = regexp.MustCompile(`(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}) `).ReplaceAllString(want, "${1}  ")
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out := &cliMonitorOutput{cancel: cancel}
			var stderr bytes.Buffer
			cmd := cli.NewRootCommand(out, &stderr)
			cmd.SetContext(ctx)
			if code := cli.Execute(cmd, append(append([]string{"monitor", "--plain", "--timestamp"}, tt.flags...), pv)); code != 0 || stderr.Len() != 0 {
				t.Fatalf("exit=%d stderr=%s", code, &stderr)
			}
			if got := out.String(); got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
	t.Run("get/timestamp", func(t *testing.T) {
		pv := i.prefix + "Double"
		want := strings.TrimSuffix(referenceMonitorLine(t, pv), "  \n") + "\n"
		want = regexp.MustCompile(`(\d{4}-\d\d-\d\d \d\d:\d\d:\d\d\.\d{6}) `).ReplaceAllString(want, "${1}  ")
		if got := commandOutput(t, "get", "--timestamp", pv); got != want {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
	for _, tt := range []struct {
		pv            string
		old           any
		flags, values []string
	}{
		{"Double", 1.25, nil, []string{"2.3456789"}},
		{"Double", 1.25, []string{"-t"}, []string{"-1.23456789"}},
		{"Enum", uint16(1), nil, []string{"OFF"}},
		{"Enum", uint16(0), []string{"-n"}, []string{"1"}},
		{"ShortArray", []int16{1, 2}, []string{"-a"}, []string{"2", "-3", "4"}},
		{"CharArray", []byte{'x', 0}, []string{"-S"}, []string{"a\nline"}},
	} {
		t.Run("put/"+tt.pv+fmt.Sprint(tt.flags), func(t *testing.T) {
			pv := i.prefix + tt.pv
			put(t, c.Channel(pv), tt.old)
			args := append(append(append([]string{"-c"}, tt.flags...), pv), tt.values...)
			want := quotedReferenceValues(referenceOutput(t, "caput", args...))
			put(t, c.Channel(pv), tt.old)
			got := commandOutput(t, append([]string{"put"}, args...)...)
			if got != want {
				t.Fatalf("got %q, want %q", got, want)
			}
		})
	}
	t.Run("info", func(t *testing.T) {
		pv := i.prefix + "Double"
		want := referenceOutput(t, "cainfo", pv)
		got := commandOutput(t, "info", pv)
		// libca may finish its asynchronous reverse DNS lookup before printing;
		// cago exposes the numeric peer endpoint. Compare the report layout.
		host := regexp.MustCompile(`(?m)^    Host:             .*`)
		if host.ReplaceAllString(got, "    Host:             <peer>") != host.ReplaceAllString(want, "    Host:             <peer>") {
			t.Fatalf("got %q, want %q", got, want)
		}
	})
}

// These fixture values intentionally differ from EPICS Base's unquoted text.
// Keep the numeric, count, escaping, alarm and timestamp comparisons intact.
func quotedReferenceValues(text string) string {
	for _, value := range []string{`line\n\"quoted\"\t\\`, "User Operation 400mA top-up", "OFF", "ON", `a\n\xff`, `a\nline`, "x"} {
		text = strings.ReplaceAll(text, " "+value+"\n", " "+`"`+value+`"`+"\n")
		text = strings.ReplaceAll(text, " "+value+"  ", " "+`"`+value+`"`+"  ")
		text = strings.ReplaceAll(text, " "+value+" UDF ", " "+`"`+value+`"`+" UDF ")
	}
	return text
}
