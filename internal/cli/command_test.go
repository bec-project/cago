package cli

import (
	"bytes"
	"errors"
	"io"
	"reflect"
	"testing"

	"github.com/spf13/cobra"
)

func TestArgumentCompatibility(t *testing.T) {
	for _, tt := range []struct {
		args []string
		want []string
	}{
		{[]string{"-timeout", "2s", "PV", "-timeout"}, []string{"--timeout", "2s", "PV", "-timeout"}},
		{[]string{"-timeout=2s", "PV", "-1"}, []string{"--timeout=2s", "PV", "-1"}},
		{[]string{"--timeout", "-1s", "PV"}, []string{"--timeout", "-1s", "PV"}},
		{[]string{"-w", "-timeout", "PV"}, []string{"-w", "-timeout", "PV"}},
		{[]string{"--", "-timeout"}, []string{"--", "-timeout"}},
		{[]string{"-w0.5", "PV", "--help"}, []string{"-w0.5", "PV", "--help"}},
	} {
		cmd := New("test", "", io.Discard, io.Discard)
		cmd.Flags().String("timeout", "", "")
		cmd.Flags().StringP("wait", "w", "", "")
		original := append([]string(nil), tt.args...)
		if got := Args(cmd, tt.args); !reflect.DeepEqual(got, tt.want) {
			t.Fatalf("args=%q: got %q, want %q", tt.args, got, tt.want)
		}
		if !reflect.DeepEqual(original, tt.args) {
			t.Fatal("modified caller arguments")
		}
	}
}

func TestExecutionStatusAndHelp(t *testing.T) {
	for _, tt := range []struct {
		args []string
		fail bool
		code int
	}{
		{[]string{"--help"}, false, 0},
		{[]string{"-h"}, false, 0},
		{[]string{"-help"}, false, 0},
		{nil, false, 2},
		{[]string{"--unknown"}, false, 2},
		{[]string{"PV", "-1"}, false, 0},
		{[]string{"PV"}, true, 1},
	} {
		var out, stderr bytes.Buffer
		cmd := New("test PV...", "Read PVs", &out, &stderr)
		cmd.Args = cobra.MinimumNArgs(1)
		ran := false
		cmd.RunE = func(cmd *cobra.Command, args []string) error {
			ran = true
			if tt.fail {
				return Runtime(errors.New("IOC unavailable"))
			}
			return nil
		}
		if code := Execute(cmd, tt.args); code != tt.code {
			t.Fatalf("args=%q: exit=%d want=%d stderr=%q", tt.args, code, tt.code, stderr.String())
		}
		if out.Len() > 0 && ran {
			t.Fatal("help executed operational code")
		}
	}
}

func TestRuntimePreservesErrorIdentity(t *testing.T) {
	err := io.ErrClosedPipe
	if !errors.Is(Runtime(err), err) {
		t.Fatal("runtime wrapper lost original error")
	}
}
