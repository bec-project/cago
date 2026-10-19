package cli

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago"
)

type twoRecordWriter struct{ records []string }

func (w *twoRecordWriter) Write(p []byte) (int, error) {
	w.records = append(w.records, string(p))
	if len(w.records) == 2 {
		return 0, io.ErrClosedPipe
	}
	return len(p), nil
}

func TestDisconnectPrecedesReconnectedValue(t *testing.T) {
	for i := 0; i < 100; i++ {
		changes := make(chan cago.StateChange, 3)
		at := time.Now()
		changes <- cago.StateChange{Info: cago.ChannelInfo{State: cago.Searching}, At: at}
		changes <- cago.StateChange{Info: cago.ChannelInfo{State: cago.Searching}, At: at.Add(time.Millisecond)}
		changes <- cago.StateChange{Info: cago.ChannelInfo{State: cago.Connected}, At: at.Add(2 * time.Millisecond)}
		events := make(chan cago.Event, 2)
		v := infoTimeValue(t, 1.25, 0, 0)
		old := infoTimeValue(t, 9.75, 0, 0)
		// Data buffered before the loss must not reappear after disconnect.
		events <- cago.Event{PV: "PV", Value: old, ReceivedAt: at.Add(-time.Millisecond)}
		events <- cago.Event{PV: "PV", Value: v, ReceivedAt: at.Add(3 * time.Millisecond)}
		out := &twoRecordWriter{}
		monitorValueEvents(context.Background(), "PV", events, nil, outputOptions{}, monitorConnection{changes: changes, count: 1}, out, &bytes.Buffer{})
		if len(out.records) != 2 || !strings.HasSuffix(out.records[0], " *** disconnected\n") || !strings.HasSuffix(out.records[1], " 1.25  \n") {
			t.Fatalf("disconnect/value order: %q", out.records)
		}
	}
}

func TestMonitorStateOutputFailureAndClosure(t *testing.T) {
	changes := make(chan cago.StateChange, 1)
	changes <- cago.StateChange{Info: cago.ChannelInfo{State: cago.Searching}, At: time.Now()}
	if !monitorValueEvents(context.Background(), "PV", make(chan cago.Event), nil, outputOptions{}, monitorConnection{changes: changes}, failingWriter{}, io.Discard) {
		t.Fatal("disconnect output failure was lost")
	}
	close(changes)
	closedEvents := make(chan cago.Event)
	close(closedEvents)
	if !monitorValueEvents(context.Background(), "PV", closedEvents, nil, outputOptions{}, monitorConnection{changes: changes}, io.Discard, io.Discard) {
		t.Fatal("unexpected stream closure was lost")
	}
	drainStateChanges(changes)
	drainStateChanges(nil)
}

func TestMonitorMissingPVPrintsInitialWarning(t *testing.T) {
	newInfoTestIOC(t, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	out := &cancelAfterRecord{cancel: cancel}
	if err := monitor(ctx, []string{"MISSING"}, out, io.Discard); err != nil {
		t.Fatal(err)
	}
	if got, want := out.String(), "MISSING                        *** Not connected (PV not found)\n"; got != want {
		t.Fatalf("got %q, want %q", got, want)
	}
	ctx, cancel = context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := monitor(ctx, []string{"MISSING"}, failingWriter{}, io.Discard); err == nil {
		t.Fatal("missing-PV output failure was lost")
	}
}

func TestMonitorDuplicateOperandsRepeatCompleteRecords(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", monitorIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	out := &cancelAfterRecord{cancel: cancel}
	if err := monitor(ctx, []string{"HEALTHY", "HEALTHY"}, out, io.Discard); err != nil {
		t.Fatal(err)
	}
	lines := strings.SplitAfter(out.String(), "\n")
	if len(lines) != 3 || lines[0] == "" || lines[0] != lines[1] {
		t.Fatalf("duplicate operand records: %q", out.String())
	}
}

type shortRecordWriter struct{}

func (shortRecordWriter) Write(p []byte) (int, error) { return len(p) - 1, nil }

func TestRepeatedRecordWriterPreservesFailures(t *testing.T) {
	for _, writer := range []io.Writer{failingWriter{}, shortRecordWriter{}} {
		if _, err := (repeatedWriter{writer: writer, copies: 2}).Write([]byte("record\n")); err == nil {
			t.Fatal("record output failure lost")
		}
	}
}

func TestMonitorCancellationDoesNotPrintDisconnect(t *testing.T) {
	for range 100 {
		ctx, cancel := context.WithCancel(context.Background())
		changes := make(chan cago.StateChange, 1)
		changes <- cago.StateChange{Info: cago.ChannelInfo{State: cago.Searching}, At: time.Now()}
		events := make(chan cago.Event, 1)
		events <- cago.Event{PV: "PV", Value: infoTimeValue(t, 1.25, 0, 0), ReceivedAt: time.Now()}
		close(events)
		cancel()
		var out bytes.Buffer
		if monitorValueEvents(ctx, "PV", events, nil, outputOptions{}, monitorConnection{changes: changes, count: 1}, &out, io.Discard) || out.Len() != 0 {
			t.Fatalf("canceled monitor emitted teardown records: %q", out.String())
		}
	}
}
