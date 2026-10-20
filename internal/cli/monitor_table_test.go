package cli

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"strings"
	"sync"
	"testing"
	"time"

	tea "charm.land/bubbletea/v2"
	"github.com/charmbracelet/x/ansi"
	"github.com/wakonig/cago/pkg/cago"
)

func TestMonitorTableLatestValuesAndConnection(t *testing.T) {
	s := newMonitorTableStore([]string{"PV", "PV", "OTHER"}, outputOptions{})
	s.add(cago.Event{PV: "PV", Value: infoTimeValue(t, 1.25, 3, 2)}, 1)
	s.add(cago.Event{PV: "UNKNOWN", Value: infoTimeValue(t, 99, 0, 0)}, 1)
	rows := s.snapshot()
	if len(rows) != 2 || rows[0][1] != "1.25" || rows[0][3] != "HIHI" || rows[0][4] != "MAJOR" || rows[0][5] != "connected" || rows[1][5] != "searching" {
		t.Fatalf("unexpected rows: %v", rows)
	}
	s.notify("PV", "disconnected")
	if row := s.snapshot()[0]; row[1] != "1.25" || row[5] != "disconnected" {
		t.Fatalf("last value lost: %v", row)
	}
	s.notify("UNKNOWN", "ignored")
	s.add(cago.Event{PV: "PV", Value: infoTimeValue(t, 2.5, 0, 0)}, 1)
	if row := s.snapshot()[0]; row[1] != "2.5" || row[5] != "connected" {
		t.Fatalf("reconnect not reflected: %v", row)
	}
	rows[0][1] = "mutated"
	if s.snapshot()[0][1] == "mutated" {
		t.Fatal("snapshot aliases mutable rows")
	}
	if tableText("PV\x1b\n\r\t") != "PV    " {
		t.Fatal("terminal controls survived")
	}
}

func TestMonitorTableArrayPreviewAndConcurrentUpdates(t *testing.T) {
	s := newMonitorTableStore([]string{"PV"}, outputOptions{})
	v, err := cago.NewValue(make([]float64, 100000))
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	for range 4 {
		wg.Go(func() {
			for range 100 {
				s.add(cago.Event{PV: "PV", Value: v}, 100000)
				s.notify("PV", "disconnected")
				s.snapshot()
			}
		})
	}
	wg.Wait()
	if row := s.snapshot()[0]; len(row[1]) > 100 || !strings.HasSuffix(row[1], " …") {
		t.Fatalf("unbounded preview: %v", row)
	}
}

func TestMonitorTableStickyHeaderResizeAndExit(t *testing.T) {
	pvs := make([]string, 40)
	for i := range pvs {
		pvs[i] = fmt.Sprintf("PV:%02d", i)
	}
	s := newMonitorTableStore(pvs, outputOptions{})
	done := make(chan error, 1)
	m := newMonitorTableModel(s, done)
	if m.Init() == nil {
		t.Fatal("no worker/refresh commands")
	}
	updated, _ := m.Update(tea.WindowSizeMsg{Width: 140, Height: 10})
	m = updated.(monitorTableModel)
	for range 30 {
		updated, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyDown})
		m = updated.(monitorTableModel)
	}
	view := ansi.Strip(m.View().Content)
	if !m.View().AltScreen || !strings.Contains(strings.Split(view, "\n")[0], "PV") || !strings.Contains(view, "Timestamp") || !strings.Contains(view, "PV:30") || strings.Contains(view, "PV:00") {
		t.Fatalf("header or scroll broken: %s", view)
	}
	s.notify("PV:30", "disconnected")
	updated, cmd := m.Update(monitorTableTick(time.Now()))
	if cmd == nil || !strings.Contains(ansi.Strip(updated.(monitorTableModel).View().Content), "disconnected") {
		t.Fatal("refresh lost state")
	}
	for _, width := range []int{80, 20, 1, 140} {
		updated, _ = m.Update(tea.WindowSizeMsg{Width: width, Height: 10})
		m = updated.(monitorTableModel)
		for _, line := range strings.Split(m.View().Content, "\n") {
			if ansi.StringWidth(line) > width {
				t.Fatalf("width %d overflow: %q", width, line)
			}
		}
	}
	for _, key := range []tea.KeyPressMsg{{Code: 'q'}, {Code: 'c', Mod: tea.ModCtrl}} {
		_, cmd := m.Update(key)
		if cmd == nil {
			t.Fatal("quit ignored")
		}
		if _, ok := cmd().(tea.QuitMsg); !ok {
			t.Fatal("no quit message")
		}
	}
	want := errors.New("subscription failed")
	updated, cmd = m.Update(monitorTableDone{want})
	if !errors.Is(updated.(monitorTableModel).err, want) || cmd == nil {
		t.Fatal("worker error lost")
	}
}

func TestMonitorTableRejectsPipes(t *testing.T) {
	cmd := NewMonitorCommand(&bytes.Buffer{}, io.Discard)
	cmd.SetArgs([]string{"--table", "PV"})
	cmd.SetIn(strings.NewReader(""))
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "requires terminal") {
		t.Fatalf("pipe accepted: %v", err)
	}
}

func TestMonitorTableReceivesLiveValues(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", monitorIOC(t))
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	s := newMonitorTableStore([]string{"HEALTHY"}, outputOptions{})
	observe := func(e cago.Event, capacity uint32) { s.add(e, capacity); cancel() }
	if err := monitorValuesNotified(ctx, []string{"HEALTHY"}, outputOptions{}, io.Discard, io.Discard, observe, s.notify); err != nil {
		t.Fatal(err)
	}
	if row := s.snapshot()[0]; row[1] != "5" || row[5] != "connected" {
		t.Fatalf("no live value: %v", row)
	}
}

func TestMonitorTableHistoryOrderAndRetention(t *testing.T) {
	s := newMonitorTableStore([]string{"A", "B"}, outputOptions{})
	s.historyMode = true
	if rows := s.snapshot(); len(rows) != 0 {
		t.Fatal("history starts with synthetic values")
	}
	v := infoTimeValue(t, 1.25, 0, 0)
	for i := 0; i < monitorTableHistoryLimit+3; i++ {
		pv := "A"
		if i%2 == 1 {
			pv = "B"
		}
		s.add(cago.Event{PV: pv, Value: v}, 1)
	}
	rows, first := s.snapshotSequence()
	if len(rows) != monitorTableHistoryLimit || first != 3 || rows[0][0] != "B" || rows[len(rows)-1][0] != "A" {
		t.Fatalf("retention/order: %d %d %v", len(rows), first, rows[0])
	}
	s.notify("A", "disconnected")
	s.notify("A", "disconnected")
	rows, first = s.snapshotSequence()
	if first != 4 || rows[len(rows)-1][5] != "disconnected" || rows[len(rows)-1][1] != "—" {
		t.Fatal("connection event lost or duplicated")
	}
	rows[len(rows)-1][5] = "mutated"
	if s.snapshot()[len(rows)-1][5] == "mutated" {
		t.Fatal("history aliases snapshot")
	}
}

func TestMonitorTableHistoryFollowsAndPauses(t *testing.T) {
	s := newMonitorTableStore([]string{"PV"}, outputOptions{})
	s.historyMode = true
	v := infoTimeValue(t, 1.25, 0, 0)
	for range 40 {
		s.add(cago.Event{PV: "PV", Value: v}, 1)
	}
	m := newMonitorTableModel(s, make(chan error))
	if !m.follow || m.table.Cursor() != 39 {
		t.Fatal("history doesn't follow")
	}
	for _, width := range []int{140, 80, 60, 1, 140} {
		next, _ := m.Update(tea.WindowSizeMsg{Width: width, Height: 10})
		m = next.(monitorTableModel)
		if m.table.Columns()[0].Title != "Timestamp" {
			t.Fatal("timestamp isn't first")
		}
		for _, line := range strings.Split(m.View().Content, "\n") {
			if ansi.StringWidth(line) > width {
				t.Fatal("history width overflow")
			}
		}
	}
	next, _ := m.Update(tea.KeyPressMsg{Code: tea.KeyUp})
	m = next.(monitorTableModel)
	cursor := m.table.Cursor()
	s.add(cago.Event{PV: "PV", Value: v}, 1)
	next, _ = m.Update(monitorTableTick(time.Now()))
	m = next.(monitorTableModel)
	if m.follow || m.table.Cursor() != cursor || !strings.Contains(m.View().Content, "paused") {
		t.Fatal("new events moved paused selection")
	}
	next, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyEnd})
	m = next.(monitorTableModel)
	if !m.follow || m.table.Cursor() != 40 || !strings.Contains(m.View().Content, "following") {
		t.Fatal("End didn't resume follow")
	}
	s.notify("PV", "disconnected")
	next, _ = m.Update(tea.WindowSizeMsg{Width: 80, Height: 10})
	m = next.(monitorTableModel)
	if !strings.Contains(m.View().Content, "disconnected") {
		t.Fatal("compact history hides connection events")
	}
	// Eviction must move the cursor index, rather than the selected event.
	for range monitorTableHistoryLimit {
		s.add(cago.Event{PV: "PV", Value: v}, 1)
	}
	m.refresh()
	next, _ = m.Update(tea.KeyPressMsg{Code: tea.KeyUp})
	m = next.(monitorTableModel)
	cursor = m.table.Cursor()
	s.add(cago.Event{PV: "PV", Value: v}, 1)
	m.refresh()
	if m.table.Cursor() != cursor-1 {
		t.Fatal("eviction changed selected event")
	}
}

func TestMonitorHistoryPVHeadersAndSparseRows(t *testing.T) {
	s := newMonitorTableStore([]string{"BEAM:CURRENT", "TEMP:READBACK", "BEAM:CURRENT"}, outputOptions{})
	s.historyMode = true
	s.add(cago.Event{PV: "BEAM:CURRENT", Value: infoTimeValue(t, 250.1, 0, 0)}, 1)
	s.add(cago.Event{PV: "TEMP:READBACK", Value: infoTimeValue(t, 42.7, 4, 1)}, 1)
	s.notify("BEAM:CURRENT", "disconnected")
	m := newMonitorTableModel(s, make(chan error))
	for _, width := range []int{140, 80, 60} {
		m.resize(width, 12)
		cols := m.table.Columns()
		rows := m.table.Rows()
		if len(cols) != 3 || cols[0].Title != "Timestamp" || cols[1].Title != "BEAM:CURRENT" || cols[2].Title != "TEMP:READBACK" {
			t.Fatalf("wrong headers: %v", cols)
		}
		if rows[0][1] != "250.1" || rows[0][2] != "" || rows[1][1] != "" || rows[1][2] != "42.7 [HIGH MINOR]" || rows[2][1] != "disconnected" || rows[2][2] != "" {
			t.Fatalf("updates in wrong columns: %v", rows)
		}
	}
}

func TestMonitorPlainAndTableMutuallyExclusive(t *testing.T) {
	cmd := NewMonitorCommand(io.Discard, io.Discard)
	cmd.SetArgs([]string{"--plain", "--table", "PV"})
	if err := cmd.Execute(); err == nil || !strings.Contains(err.Error(), "none of the others") {
		t.Fatalf("conflicting output modes accepted: %v", err)
	}
}
