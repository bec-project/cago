package cli

import (
	"io"
	"reflect"
	"testing"
)

func TestRuntimeSuccessIsNil(t *testing.T) {
	if err := Runtime(nil); err != nil {
		t.Fatalf("successful operation became an error: %v", err)
	}
}

func TestUnknownShorthandDoesNotRewriteFollowingValues(t *testing.T) {
	cmd := New("test", "", io.Discard, io.Discard)
	cmd.Flags().BoolP("completion", "c", false, "")
	cmd.Flags().String("timeout", "", "")
	args := []string{"-cz", "PV", "-timeout"}
	if got := Args(cmd, args); !reflect.DeepEqual(got, args) {
		t.Fatalf("unknown shorthand changed operands: %q", got)
	}
	if code := Execute(cmd, args); code != 2 {
		t.Fatalf("unknown shorthand status=%d, want 2", code)
	}
}
