package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"strings"
	"testing"
)

func TestGetCobraContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := NewGetCommand(io.Discard, io.Discard)
	cmd.SetArgs([]string{"--timeout=5s", "UNAVAILABLE"})
	if err := cmd.ExecuteContext(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled command: %v", err)
	}
}

func TestGetInvalidCountDoesNotReachNetwork(t *testing.T) {
	for _, args := range [][]string{{"-count", "4294967296", "PV"}, {"-count", "-1", "PV"}, {"-timeout", "0s", "PV"}, nil} {
		var stderr bytes.Buffer
		if code := runGet(args, io.Discard, &stderr); code != 2 || stderr.Len() == 0 {
			t.Fatalf("args=%q exit=%d stderr=%q", args, code, stderr.String())
		}
	}
}

func TestGetCobraHelpAndLongFlags(t *testing.T) {
	for _, help := range []string{"-h", "--help", "-help"} {
		var out, stderr bytes.Buffer
		if code := runGet([]string{help}, &out, &stderr); code != 0 || !strings.Contains(out.String(), "--count") {
			t.Fatalf("%s: exit=%d out=%q stderr=%q", help, code, out.String(), stderr.String())
		}
	}
	for _, args := range [][]string{{"--count=4294967296", "PV"}, {"--timeout=0s", "PV"}} {
		if code := runGet(args, io.Discard, io.Discard); code != 2 {
			t.Fatalf("args=%q: exit=%d", args, code)
		}
	}
}
