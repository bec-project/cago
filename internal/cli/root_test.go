package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"reflect"
	"strings"
	"testing"

	"github.com/spf13/cobra"
)

func TestRootHelpDoesNotConnectOrWarnAboutUnusedPutSettings(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", "invalid:address:!")
	t.Setenv("EPICS_CLI_TIMEOUT", "invalid")
	for _, args := range [][]string{
		nil, {"--help"}, {"-h"}, {"-help"}, {"help"}, {"help", "get"},
		{"get", "--help"}, {"put", "--help"}, {"info", "--help"}, {"monitor", "--help"},
		{"get", "-help"}, {"put", "-help"}, {"info", "-help"}, {"monitor", "-help"},
	} {
		var out, stderr bytes.Buffer
		if code := Execute(NewRootCommand(&out, &stderr), args); code != 0 || stderr.Len() != 0 || !strings.Contains(out.String(), "cago") {
			t.Fatalf("args=%q exit=%d stdout=%q stderr=%q", args, code, out.String(), stderr.String())
		}
		if len(args) == 0 {
			for _, name := range []string{"get", "put", "info", "monitor"} {
				if !strings.Contains(out.String(), name) {
					t.Fatalf("root help omits %s: %s", name, out.String())
				}
			}
		}
	}
}

func TestRootHelpListsFlagsBySubcommand(t *testing.T) {
	for _, args := range [][]string{nil, {"help"}, {"--help"}, {"-h"}, {"-help"}} {
		var out, stderr bytes.Buffer
		if code := Execute(NewRootCommand(&out, &stderr), args); code != 0 || stderr.Len() != 0 {
			t.Fatalf("args=%q exit=%d stderr=%q", args, code, stderr.String())
		}
		text := out.String()
		for _, tt := range []struct {
			usage string
			flags []string
		}{
			{"cago get [flags] PV...", []string{"--count", "--timeout", "default 5s"}},
			{"cago info [flags] PV...", []string{"--priority", "--timeout", "default 5s"}},
			{"cago monitor [flags] PV...", []string{"--help"}},
			{"cago put [flags] PV VALUE...", []string{"-a, --array", "-c, --completion", "-S, --long-string", "--numeric", "--priority", "--string", "--terse", "--timeout", "--wait", "default 1s"}},
		} {
			_, section, found := strings.Cut(text, tt.usage+"\n")
			if !found {
				t.Fatalf("args=%q help omits %q: %s", args, tt.usage, text)
			}
			section, _, _ = strings.Cut(section, "\n\n")
			for _, flag := range tt.flags {
				if !strings.Contains(section, flag) {
					t.Fatalf("args=%q %s omits %q: %s", args, tt.usage, flag, section)
				}
			}
		}
	}
	// A command-specific help page should stay focused on that command's flags.
	var out bytes.Buffer
	if code := Execute(NewRootCommand(&out, io.Discard), []string{"help", "get"}); code != 0 || strings.Contains(out.String(), "Subcommand flags") || strings.Contains(out.String(), "--completion") {
		t.Fatalf("get help includes other commands: exit=%d stdout=%q", code, out.String())
	}
}

func TestRootUsageErrorsAndLegacyFlags(t *testing.T) {
	for _, args := range [][]string{
		{"unknown"}, {"--unknown"}, {"get"}, {"put"}, {"info"}, {"monitor"},
		{"get", "-timeout", "0s", "PV"}, {"get", "-count", "4294967296", "PV"},
		{"put", "-timeout", "0s", "PV", "1"}, {"info", "-timeout", "0s", "PV"},
	} {
		var stderr bytes.Buffer
		if code := Execute(NewRootCommand(io.Discard, &stderr), args); code != 2 || stderr.Len() == 0 {
			t.Fatalf("args=%q exit=%d stderr=%q", args, code, stderr.String())
		}
		if strings.Contains(stderr.String(), "unknown shorthand") {
			t.Fatalf("legacy long flag was not normalized: %q", stderr.String())
		}
	}
}

func TestRootDispatchPreservesFlagValuesAndOperands(t *testing.T) {
	for _, tt := range []struct {
		args, operands []string
	}{
		{[]string{"get", "-timeout", "2s", "--", "-PV"}, []string{"-PV"}},
		{[]string{"get", "PV", "--help"}, []string{"PV", "--help"}},
		{[]string{"put", "-cw", "0.5", "PV", "-timeout", "--help", "-1"}, []string{"PV", "-timeout", "--help", "-1"}},
		{[]string{"put", "-cw0.5", "PV", "-1.25"}, []string{"PV", "-1.25"}},
		{[]string{"put", "-S", "PV", "-timeout"}, []string{"PV", "-timeout"}},
		{[]string{"info", "-timeout=2s", "PV"}, []string{"PV"}},
		{[]string{"monitor", "--", "-PV"}, []string{"-PV"}},
	} {
		root := NewRootCommand(io.Discard, io.Discard)
		ctx := context.WithValue(context.Background(), rootContextKey{}, "inherited")
		root.SetContext(ctx)
		var got []string
		for _, child := range root.Commands() {
			child.RunE = func(cmd *cobra.Command, args []string) error {
				if cmd.Context() != ctx {
					t.Error("subcommand lost root context")
				}
				got = args
				return nil
			}
		}
		original := append([]string(nil), tt.args...)
		if code := Execute(root, tt.args); code != 0 || !reflect.DeepEqual(got, tt.operands) {
			t.Fatalf("args=%q exit=%d operands=%q", tt.args, code, got)
		}
		if !reflect.DeepEqual(tt.args, original) {
			t.Fatal("modified caller's argument slice")
		}
	}
}

type rootContextKey struct{}

func TestRootOperationalErrorsAndCancellation(t *testing.T) {
	for _, args := range [][]string{{"get", "PV"}, {"put", "PV", "1"}, {"info", "PV"}, {"monitor", "PV"}} {
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		cmd := NewRootCommand(io.Discard, io.Discard)
		cmd.SetContext(ctx)
		want := 1
		if args[0] == "monitor" {
			want = 0 // Interrupting a monitor is a normal shutdown.
		}
		if code := Execute(cmd, args); code != want {
			t.Fatalf("args=%q exit=%d, want %d", args, code, want)
		}
	}
	cmd := NewRootCommand(io.Discard, io.Discard)
	for _, child := range cmd.Commands() {
		child.RunE = func(*cobra.Command, []string) error { return Runtime(errors.New("IOC unavailable")) }
	}
	if code := Execute(cmd, []string{"get", "PV"}); code != 1 {
		t.Fatalf("operational failure exit=%d", code)
	}
}

func TestRootGetReadsThroughSharedImplementation(t *testing.T) {
	isolatedRepeater(t)
	t.Setenv("EPICS_CA_ADDR_LIST", commandIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	var out, stderr bytes.Buffer
	if code := Execute(NewRootCommand(&out, &stderr), []string{"get", "-timeout", "1s", "HEALTHY"}); code != 0 || out.String() != "HEALTHY                        5\n" || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}

func TestRootOutputOverridesAreInherited(t *testing.T) {
	var out, stderr bytes.Buffer
	root := NewRootCommand(io.Discard, io.Discard)
	root.SetOut(&out)
	root.SetErr(&stderr)
	if code := Execute(root, []string{"get", "--help"}); code != 0 || !strings.Contains(out.String(), "cago get") || stderr.Len() != 0 {
		t.Fatalf("exit=%d stdout=%q stderr=%q", code, out.String(), stderr.String())
	}
}
