package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/spf13/cobra"
	"github.com/wakonig/cago/pkg/cago"
)

func NewMonitorCommand(stdout, stderr io.Writer) *cobra.Command {
	cmd := New("monitor [flags] PV...", "Subscribe to EPICS Channel Access updates", stdout, stderr)
	var output outputOptions
	cmd.Flags().BoolVarP(&output.numeric, "numeric", "n", false, "print ENUM values as numbers")
	cmd.Flags().BoolVarP(&output.longString, "long-string", "S", false, "print CHAR arrays as strings")
	cmd.Args = cobra.MinimumNArgs(1)
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return Runtime(monitorValues(cmd.Context(), args, output, cmd.OutOrStdout(), cmd.ErrOrStderr()))
	}
	return cmd
}

func monitor(ctx context.Context, pvs []string, stdout, stderr io.Writer) error {
	return monitorValues(ctx, pvs, outputOptions{}, stdout, stderr)
}

func monitorValues(ctx context.Context, pvs []string, output outputOptions, stdout, stderr io.Writer) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	// Cobra output may be a caller-supplied buffer rather than an os.File.
	// Serialize complete records from the concurrent PV workers.
	outputMu := &sync.Mutex{}
	stdout = &lockedWriter{mu: outputMu, writer: stdout}
	stderr = &lockedWriter{mu: outputMu, writer: stderr}
	client, err := cago.New(ctx, cago.Options{})
	if err != nil {
		if errors.Is(err, context.Canceled) && errors.Is(ctx.Err(), context.Canceled) {
			return nil
		}
		return err
	}
	defer client.Close()
	var wg sync.WaitGroup
	var failed atomic.Bool
	copies := make(map[string]int, len(pvs))
	for _, pv := range pvs {
		copies[pv]++
	}
	for _, pv := range pvs {
		recordCopies := copies[pv]
		if recordCopies == 0 {
			continue
		}
		delete(copies, pv)
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Duplicate operands share a Channel and its single state stream.
			// Consume it once and repeat complete records, including losses.
			stdout := repeatedWriter{writer: stdout, copies: recordCopies}
			stderr := repeatedWriter{writer: stderr, copies: recordCopies}
			ch := client.Channel(pv)
			initial, stop := context.WithTimeout(ctx, time.Second)
			err := ch.Connect(initial)
			stop()
			if errors.Is(err, context.DeadlineExceeded) && ctx.Err() == nil {
				if _, writeErr := fmt.Fprintln(stdout, formatPVName(pv, 0)+"*** Not connected (PV not found)"); writeErr != nil {
					failed.Store(true)
					cancel()
					return
				}
				// Discovery continues after the initial warning, as in camonitor.
				err = ch.Connect(ctx)
			}
			var sub *cago.Subscription
			if err == nil {
				// Discard state history from before this first attachment. Later
				// losses and restorations are consumed with the value stream.
				drainStateChanges(ch.StateChanges())
				sub, err = ch.Subscribe(ctx, valueOptions(0, output.numeric)...)
			}
			if err != nil {
				if !errors.Is(ctx.Err(), context.Canceled) {
					failed.Store(true)
					if _, writeErr := fmt.Fprintln(stderr, pv, err); writeErr != nil {
						cancel()
					}
				}
				return
			}
			defer sub.Cancel()
			connection := monitorConnection{changes: ch.StateChanges(), count: ch.Info().Count}
			if monitorValueEvents(ctx, pv, sub.Events(), sub.Errors(), output, connection, stdout, stderr) {
				failed.Store(true)
				// A terminal stream/output failure must also release siblings
				// waiting for an unavailable PV or its next update.
				cancel()
			}
		}()
	}
	wg.Wait()
	if failed.Load() {
		return errors.New("one or more monitors failed")
	}
	return nil
}

type lockedWriter struct {
	mu     *sync.Mutex
	writer io.Writer
}

type repeatedWriter struct {
	writer io.Writer
	copies int
}

func (w repeatedWriter) Write(p []byte) (int, error) {
	for range w.copies {
		n, err := w.writer.Write(p)
		if err != nil {
			return n, err
		}
		if n != len(p) {
			return n, io.ErrShortWrite
		}
	}
	return len(p), nil
}

func (w *lockedWriter) Write(p []byte) (int, error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.writer.Write(p)
}

// monitorEvents reports an unexpected terminal stream closure. Recoverable
// errors may be printed while the subscription continues reconnecting.
func monitorEvents(ctx context.Context, pv string, events <-chan cago.Event, errs <-chan error, stdout, stderr io.Writer) bool {
	return monitorValueEvents(ctx, pv, events, errs, outputOptions{}, monitorConnection{}, stdout, stderr)
}

type monitorConnection struct {
	changes <-chan cago.StateChange
	count   uint32
}

func drainStateChanges(changes <-chan cago.StateChange) {
	for {
		select {
		case _, ok := <-changes:
			if !ok {
				return
			}
		default:
			return
		}
	}
}

func monitorValueEvents(ctx context.Context, pv string, events <-chan cago.Event, errs <-chan error, output outputOptions, connection monitorConnection, stdout, stderr io.Writer) bool {
	connected := true
	var lastLoss time.Time
	stateChanged := func(change cago.StateChange) bool {
		if ctx.Err() != nil {
			return false
		}
		if change.Info.State == cago.Connected {
			connected = true
		} else if change.Info.State == cago.Searching && connected {
			connected = false
			lastLoss = change.At
			text := formatPVName(pv, connection.count) + formatTimestamp(cago.Metadata{Timestamp: change.At}) + " *** disconnected\n"
			if _, err := io.WriteString(stdout, text); err != nil {
				return true
			}
		}
		return false
	}
	drainChanges := func() bool {
		for connection.changes != nil {
			select {
			case change, ok := <-connection.changes:
				if !ok {
					connection.changes = nil
				} else if stateChanged(change) {
					return true
				}
			default:
				return false
			}
		}
		return false
	}
	for events != nil || errs != nil {
		select {
		case <-ctx.Done():
			return false
		case change, ok := <-connection.changes:
			if !ok {
				connection.changes = nil
				continue
			}
			if stateChanged(change) {
				return true
			}
		case e, ok := <-events:
			if !ok {
				events = nil
				continue
			}
			// A stalled writer can accumulate both a loss/restoration and an
			// update. Print the transition first, then discard older queued data.
			if drainChanges() {
				return true
			}
			if ctx.Err() != nil {
				return false
			}
			if !connected || !e.ReceivedAt.IsZero() && e.ReceivedAt.Before(lastLoss) {
				continue
			}
			connection.count = e.Value.Count()
			if _, err := io.WriteString(stdout, formatMonitorValue(e.PV, e.Value, output)); err != nil {
				return true
			}
		case err, ok := <-errs:
			if !ok {
				errs = nil
				continue
			}
			if _, writeErr := fmt.Fprintln(stderr, pv, err); writeErr != nil {
				return true
			}
		}
	}
	return ctx.Err() == nil
}
