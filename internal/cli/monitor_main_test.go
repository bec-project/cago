package cli

import (
	"bytes"
	"context"
	"errors"
	"github.com/wakonig/cago/pkg/cago"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"
)

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, io.ErrClosedPipe }

func TestMonitorMonitorOutputFailure(t *testing.T) {
	for _, diagnostic := range []bool{false, true} {
		events := make(chan cago.Event, 1)
		errs := make(chan error, 1)
		if diagnostic {
			errs <- errors.New("IOC disconnected")
		} else {
			v, _ := cago.NewValue(1.0)
			events <- cago.Event{PV: "test", Value: v}
		}
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		result := make(chan bool, 1)
		go func() { result <- monitorEvents(ctx, "test", events, errs, failingWriter{}, failingWriter{}) }()
		select {
		case failed := <-result:
			if !failed {
				t.Error("output failure was not reported")
			}
		case <-ctx.Done():
			t.Error("output failure did not stop the monitor")
		}
		close(events)
		close(errs)
		cancel()
	}
}

func TestMonitorOutputFailureCancelsSiblingSetup(t *testing.T) {
	port, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	n := port.LocalAddr().(*net.UDPAddr).Port
	port.Close()
	t.Setenv("EPICS_CA_REPEATER_PORT", strconv.Itoa(n))
	t.Setenv("EPICS_CA_ADDR_LIST", "127.0.0.1:1")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- monitor(ctx, []string{"UNAVAILABLE", strings.Repeat("x", 501)}, io.Discard, failingWriter{})
	}()
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("output failure exited successfully")
		}
	case <-time.After(time.Second):
		cancel()
		<-result
		t.Fatal("output failure left sibling setup running")
	}
}

func TestMonitorTerminalMonitorExitStatus(t *testing.T) {
	for _, stopped := range []bool{false, true} {
		ctx, cancel := context.WithCancel(context.Background())
		if stopped {
			cancel()
		}
		events := make(chan cago.Event)
		errs := make(chan error, 1)
		errs <- errors.New("reconnected PV exceeds MaxArrayBytes")
		close(events)
		close(errs)
		if failed := monitorEvents(ctx, "test", events, errs, io.Discard, io.Discard); failed == stopped {
			t.Fatalf("stopped=%v: failure=%v", stopped, failed)
		}
		cancel()
	}
}

func TestMonitorCobraHelpDoesNotConnect(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", "invalid:address:!")
	for _, help := range []string{"--help", "-h"} {
		var out, stderr bytes.Buffer
		cmd := NewMonitorCommand(&out, &stderr)
		cmd.SetArgs([]string{help})
		if err := cmd.Execute(); err != nil || !strings.Contains(out.String(), "Subscribe") || stderr.Len() != 0 {
			t.Fatalf("%s: err=%v out=%q stderr=%q", help, err, out.String(), stderr.String())
		}
	}
}

func TestMonitorCobraContextOverridesConstructorContext(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", "127.0.0.1:1")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	parent, stop := context.WithCancel(context.Background())
	defer stop()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	cmd := NewMonitorCommand(io.Discard, io.Discard)
	cmd.SetContext(parent)
	cmd.SetArgs([]string{"UNAVAILABLE"})
	finished := make(chan struct{})
	go func() { _ = cmd.ExecuteContext(ctx); close(finished) }()
	select {
	case <-finished:
	case <-time.After(time.Second):
		stop()
		<-finished
		t.Fatal("command ignored its Cobra context")
	}
}

func TestMonitorSetupFailureExitStatus(t *testing.T) {
	port, err := net.ListenUDP("udp4", &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatal(err)
	}
	n := port.LocalAddr().(*net.UDPAddr).Port
	port.Close()
	t.Setenv("EPICS_CA_REPEATER_PORT", strconv.Itoa(n))
	t.Setenv("EPICS_CA_ADDR_LIST", "127.0.0.1:1")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if code := runMonitor(ctx, []string{strings.Repeat("x", 501)}); code != 1 {
		t.Fatalf("failed subscription exit=%d, want 1", code)
	}
	// A cancelled monitor may observe either caller cancellation or the shared
	// channel closing. Both are normal shutdown, not a failed subscription.
	stopped, stop := context.WithCancel(context.Background())
	stop()
	for range 20 {
		if code := runMonitor(stopped, []string{"UNAVAILABLE"}); code != 0 {
			t.Fatalf("cancelled monitor exit=%d", code)
		}
	}
}
