package cli

import (
	"bytes"
	"context"
	"errors"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

type brokenOutput struct{ cancel context.CancelFunc }

func (w brokenOutput) Write([]byte) (int, error) {
	if w.cancel != nil {
		w.cancel()
	}
	return 0, io.ErrClosedPipe
}

// Holding an isolated local repeater port avoids contacting a native repeater
// that may be running on this machine. Registration has no bearing on reads.
func isolatedRepeater(t *testing.T) {
	t.Helper()
	sock, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { sock.Close() })
	t.Setenv("EPICS_CA_REPEATER_PORT", strconv.Itoa(sock.LocalAddr().(*net.UDPAddr).Port))
}

func TestGetReadSuccessfulCommand(t *testing.T) {
	isolatedRepeater(t)
	t.Setenv("EPICS_CA_ADDR_LIST", commandIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	var stdout, stderr bytes.Buffer
	if code := runGet([]string{"--timeout=1s", "--count=1", "HEALTHY"}, &stdout, &stderr); code != 0 {
		t.Fatalf("exit=%d stderr=%q", code, stderr.String())
	}
	if stdout.String() != "HEALTHY                        1 5\n" || stderr.Len() != 0 {
		t.Fatalf("stdout=%q stderr=%q", stdout.String(), stderr.String())
	}
}

func TestGetReadOutputFailureAndCancellation(t *testing.T) {
	for _, cancelled := range []bool{false, true} {
		t.Run(map[bool]string{false: "write failure", true: "cancelled write"}[cancelled], func(t *testing.T) {
			isolatedRepeater(t)
			t.Setenv("EPICS_CA_ADDR_LIST", commandIOC(t))
			t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			writer := brokenOutput{}
			if cancelled {
				writer.cancel = cancel
			}
			var stderr bytes.Buffer
			err := read(ctx, []string{"HEALTHY"}, time.Second, 0, writer, &stderr)
			if err == nil || !strings.Contains(stderr.String(), io.ErrClosedPipe.Error()) {
				t.Fatalf("write failure lost: err=%v stderr=%q", err, stderr.String())
			}
			if cancelled && !errors.Is(err, context.Canceled) {
				t.Fatalf("caller cancellation lost: %v", err)
			}
		})
	}
}
