package cli

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"sync"
	"time"
	"unicode"

	"charm.land/bubbles/v2/table"
	tea "charm.land/bubbletea/v2"
	"charm.land/lipgloss/v2"
	"github.com/charmbracelet/x/term"
	"github.com/wakonig/cago/pkg/cago"
)

// Workers retain the latest row and optionally append history. Rendering takes a consistent snapshot,
// so fast subscriptions neither queue UI messages nor block on the terminal.
type monitorTableStore struct {
	mu          sync.Mutex
	pvs         []string
	rows        map[string]table.Row
	output      outputOptions
	history     []table.Row
	next        int
	total       uint64
	historyMode bool
}

func newMonitorTableStore(pvs []string, output outputOptions) *monitorTableStore {
	s := &monitorTableStore{rows: make(map[string]table.Row), output: output}
	for _, pv := range pvs {
		if _, exists := s.rows[pv]; !exists {
			s.pvs = append(s.pvs, pv)
			s.rows[pv] = table.Row{tableText(pv), "—", "—", "—", "—", "searching"}
		}
	}
	return s
}

// PV names and server errors can contain terminal control characters.
func tableText(text string) string {
	return strings.Map(func(r rune) rune {
		if unicode.IsControl(r) {
			return ' '
		}
		return r
	}, text)
}

func (s *monitorTableStore) add(e cago.Event, capacity uint32) {
	metadata := e.Value.Metadata()
	// Bound formatting work and displayed array data for large waveforms.
	count := min(e.Value.Count(), 16)
	value := formatElements(e.Value, count, capacity > 1 || e.Value.Count() > 1, s.output.longString)
	if count < e.Value.Count() {
		value += " …"
	}
	row := table.Row{tableText(e.PV), value, formatTimestamp(metadata),
		infoAlarmStatusName(metadata.Status), infoAlarmSeverityName(metadata.Severity), "connected"}
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, exists := s.rows[e.PV]; exists {
		s.rows[e.PV] = row
		s.appendHistory(row)
	}
}

func (s *monitorTableStore) notify(pv, message string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if row, exists := s.rows[pv]; exists {
		if row[5] == tableText(message) {
			return
		}
		row[5] = tableText(message)
		notice := append(table.Row(nil), row...)
		notice[1], notice[2], notice[3], notice[4] = "—", formatTimestamp(cago.Metadata{Timestamp: time.Now()}), "—", "—"
		s.appendHistory(notice)
	}
}

// Retain a bounded event history, in delivery order, without shifting rows.
const monitorTableHistoryLimit = 10000

func (s *monitorTableStore) appendHistory(row table.Row) {
	if !s.historyMode {
		return
	}
	copy := append(table.Row(nil), row...)
	if len(s.history) < monitorTableHistoryLimit {
		s.history = append(s.history, copy)
	} else {
		s.history[s.next] = copy
	}
	s.next = (s.next + 1) % monitorTableHistoryLimit
	s.total++
}

func (s *monitorTableStore) snapshot() []table.Row {
	rows, _ := s.snapshotSequence()
	return rows
}

func (s *monitorTableStore) snapshotSequence() ([]table.Row, uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.historyMode {
		rows := make([]table.Row, len(s.history))
		start := 0
		if len(s.history) == monitorTableHistoryLimit {
			start = s.next
		}
		for i := range rows {
			rows[i] = append(table.Row(nil), s.history[(start+i)%len(s.history)]...)
		}
		return rows, s.total - uint64(len(rows))
	}
	rows := make([]table.Row, 0, len(s.pvs))
	for _, pv := range s.pvs {
		rows = append(rows, append(table.Row(nil), s.rows[pv]...))
	}
	return rows, 0
}

type monitorTableTick time.Time

const monitorTableFullWidth = 126

type monitorTableDone struct{ err error }

type monitorTableModel struct {
	table         table.Model
	store         *monitorTableStore
	done          <-chan error
	err           error
	width, height int
	follow        bool
	first         uint64
}

func newMonitorTableModel(store *monitorTableStore, done <-chan error) monitorTableModel {
	m := monitorTableModel{store: store, done: done, follow: store.historyMode}
	m.table = table.New(table.WithFocused(true))
	styles := table.DefaultStyles()
	styles.Header = styles.Header.Bold(true).BorderBottom(true).BorderStyle(lipgloss.NormalBorder())
	m.table.SetStyles(styles)
	m.resize(100, 24)
	return m
}

func (m *monitorTableModel) resize(width, height int) {
	m.width, m.height = width, height
	// Narrow terminals retain the essential PV/value/connection columns.
	columns := []table.Column{{Title: "PV", Width: 24}, {Title: "Value", Width: 24},
		{Title: "Timestamp", Width: 26}, {Title: "Status", Width: 12},
		{Title: "Severity", Width: 10}, {Title: "Connection", Width: 18}}
	if width < monitorTableFullWidth {
		columns = []table.Column{{Title: "PV", Width: 24}, {Title: "Value", Width: 24}, {Title: "Connection", Width: 18}}
	}
	if m.store.historyMode {
		timestampWidth := 26
		if width < 80 {
			timestampWidth = 15
		}
		columns = []table.Column{{Title: "Timestamp", Width: timestampWidth}}
		n := len(m.store.pvs)
		remaining := max(n, width-timestampWidth-2*(n+1))
		for i, pv := range m.store.pvs {
			cellWidth := max(1, remaining/max(1, n))
			if i < remaining%max(1, n) {
				cellWidth++
			}
			columns = append(columns, table.Column{Title: tableText(pv), Width: cellWidth})
		}
	} else {
		available := max(3, width-2*len(columns))
		if len(columns) == 3 {
			columns[0].Width = max(1, available/3)
			columns[1].Width = max(1, available/3)
			columns[2].Width = max(1, available-columns[0].Width-columns[1].Width)
		} else {
			columns[1].Width += max(0, available-114)
		}
	}
	cursor := m.table.Cursor()
	first := m.first
	m.table.SetRows(nil)
	m.table.SetColumns(columns)
	m.table.SetWidth(max(1, width))
	m.table.SetHeight(max(3, height-2))
	m.refresh()
	if !m.follow {
		m.table.SetCursor(max(0, cursor-int(m.first-first)))
	}
}

func (m *monitorTableModel) refresh() {
	rows, first := m.store.snapshotSequence()
	cursor := m.table.Cursor()
	if m.store.historyMode {
		// Preserve the selected event when the oldest retained rows expire.
		cursor = max(0, cursor-int(first-m.first))
		m.first = first
		indices := make(map[string]int, len(m.store.pvs))
		for i, pv := range m.store.pvs {
			indices[tableText(pv)] = i + 1
		}
		for i, row := range rows {
			cells := make(table.Row, len(m.store.pvs)+1)
			cells[0] = row[2]
			if m.width < 80 && len(cells[0]) == 26 {
				cells[0] = cells[0][11:]
			}
			value := row[1]
			if row[5] != "connected" {
				value = row[5]
			} else if row[4] != "NO_ALARM" {
				value += " [" + row[3] + " " + row[4] + "]"
			}
			if column, ok := indices[row[0]]; ok {
				cells[column] = value
			}
			rows[i] = cells
		}
	} else if m.width < monitorTableFullWidth {
		for i, row := range rows {
			rows[i] = table.Row{row[0], row[1], row[5]}
		}
	}
	m.table.SetRows(rows)
	if m.follow {
		m.table.GotoBottom()
	} else {
		m.table.SetCursor(cursor)
	}
}

func monitorTableRefresh() tea.Cmd {
	return tea.Tick(100*time.Millisecond, func(at time.Time) tea.Msg { return monitorTableTick(at) })
}

func (m monitorTableModel) Init() tea.Cmd {
	return tea.Batch(monitorTableRefresh(), func() tea.Msg { return monitorTableDone{<-m.done} })
}

func (m monitorTableModel) Update(msg tea.Msg) (tea.Model, tea.Cmd) {
	switch msg := msg.(type) {
	case tea.KeyPressMsg:
		if msg.String() == "q" || msg.String() == "ctrl+c" {
			return m, tea.Quit
		}
		if m.store.historyMode {
			switch msg.String() {
			case "end", "G":
				m.follow = true
			case "up", "k", "pgup", "b", "home", "g", "u", "ctrl+u":
				m.follow = false
			}
		}
	case tea.WindowSizeMsg:
		m.resize(msg.Width, msg.Height)
	case monitorTableTick:
		m.refresh()
		return m, monitorTableRefresh()
	case monitorTableDone:
		m.err = msg.err
		return m, tea.Quit
	}
	var cmd tea.Cmd
	m.table, cmd = m.table.Update(msg)
	if m.follow {
		m.table.GotoBottom()
	}
	return m, cmd
}

func (m monitorTableModel) View() tea.View {
	content := m.table.View() + "\n↑/↓ scroll · PgUp/PgDn page · q quit"
	if m.store.historyMode {
		if m.follow {
			content += " · following"
		} else {
			content += " · paused · End to follow"
		}
	} else if m.width < monitorTableFullWidth {
		content += " · widen for timestamp/alarms"
	}
	view := tea.NewView(lipgloss.NewStyle().MaxWidth(max(1, m.width)).MaxHeight(max(1, m.height)).Render(content))
	view.AltScreen = true
	return view
}

func monitorTable(ctx context.Context, pvs []string, output outputOptions, stdin io.Reader, stdout io.Writer, history bool) error {
	if ctx.Err() != nil {
		return nil
	}
	input, inputOK := stdin.(*os.File)
	terminal, outputOK := stdout.(*os.File)
	if !inputOK || !outputOK || !term.IsTerminal(input.Fd()) || !term.IsTerminal(terminal.Fd()) {
		return fmt.Errorf("monitor requires terminal input and output")
	}
	workerCtx, cancel := context.WithCancel(ctx)
	store := newMonitorTableStore(pvs, output)
	store.historyMode = history
	done := make(chan error, 1)
	stopped := make(chan struct{})
	go func() {
		defer close(stopped)
		done <- monitorValuesNotified(workerCtx, pvs, output, io.Discard, io.Discard, store.add, store.notify)
	}()
	defer func() { cancel(); <-stopped }()
	model, err := tea.NewProgram(newMonitorTableModel(store, done), tea.WithContext(workerCtx), tea.WithInput(stdin), tea.WithOutput(stdout)).Run()
	if ctx.Err() != nil {
		return nil
	}
	if err != nil {
		return err
	}
	return model.(monitorTableModel).err
}
