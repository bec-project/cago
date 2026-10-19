package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"strings"
	"sync"
	"time"

	"github.com/spf13/cobra"
	"github.com/wakonig/cago"
)

// NewInfoCommand reports cainfo-compatible connection details by default.
func NewInfoCommand(stdout, stderr io.Writer) *cobra.Command {
	cmd := New("info [flags] PV...", "Show EPICS Channel Access channel information", stdout, stderr)
	timeout := cmd.Flags().Duration("timeout", 5*time.Second, "operation timeout per PV")
	priority := cmd.Flags().UintP("priority", "p", 0, "CA priority (0-99)")
	count := cmd.Flags().Uint64("count", 16, "maximum value preview elements (0 means current length)")
	verbose := cmd.Flags().Bool("verbose", false, "include record metadata, value preview, timestamp and alarms")
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		if *timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		if *priority > 99 {
			return errors.New("priority must be between 0 and 99")
		}
		if *count > math.MaxUint32 {
			return errors.New("count must fit uint32")
		}
		if err := cobra.MinimumNArgs(1)(cmd, args); err != nil {
			return err
		}
		for _, pv := range args {
			if pv == "" {
				return errors.New("PV must not be empty")
			}
		}
		return nil
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return Runtime(readInfo(cmd.Context(), args, *timeout, uint16(*priority), uint32(*count), *verbose, cmd.OutOrStdout()))
	}
	return cmd
}

func readInfo(parent context.Context, pvs []string, timeout time.Duration, priority uint16, count uint32, verbose bool, stdout io.Writer) error {
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	client, err := cago.New(ctx, cago.Options{ContextTimeout: timeout, Priority: priority})
	if err != nil {
		return err
	}
	defer client.Close()
	channels := make([]*cago.Channel, len(pvs))
	for i, pv := range pvs {
		channels[i] = client.Channel(pv)
	}
	var failures []error
	for i, ch := range channels {
		operation, stop := context.WithTimeout(ctx, timeout)
		err := ch.Connect(operation)
		if err != nil {
			stop()
			failures = append(failures, fmt.Errorf("%s: %w", pvs[i], err))
			continue
		}
		text := formatChannelInfo(ch.Info())
		if verbose {
			details := collectInfo(operation, client, ch, count)
			if details.ValueError != nil && !errors.Is(details.ValueError, cago.ErrNoRead) {
				failures = append(failures, fmt.Errorf("%s value: %w", pvs[i], details.ValueError))
			}
			text = formatInfo(details)
		}
		stop()
		_, err = io.WriteString(stdout, text)
		if err != nil {
			return errors.Join(append(failures, fmt.Errorf("write channel information: %w", err))...)
		}
	}
	if err := ctx.Err(); err != nil {
		failures = append(failures, err)
	}
	return errors.Join(failures...)
}

func collectInfo(ctx context.Context, client *cago.Client, ch *cago.Channel, count uint32) infoDetails {
	info := ch.Info()
	details := infoDetails{Info: info, Description: "<unavailable>", RecordType: "<unavailable>", ValueError: cago.ErrNoRead}
	if info.Access&cago.ReadAccess != 0 {
		requested := count
		if requested > info.Count {
			requested = info.Count
		}
		details.Value, details.ValueError = ch.Get(ctx, cago.WithMetadata(cago.Time), cago.WithCount(requested))
		details.Limited = requested > 0 && requested < info.Count && details.Value.Count() == requested
	}
	if ctx.Err() != nil {
		return details
	}
	// Field aliases and gateways may expose neither DESC nor DBR_CLASS_NAME.
	// Fetch them independently after the value; optional metadata must not consume
	// another full per-PV timeout or prevent the main details from being printed.
	metadata, cancel := context.WithTimeout(ctx, 250*time.Millisecond)
	defer cancel()
	var wg sync.WaitGroup
	var className, recordType string
	if info.Access&cago.ReadAccess != 0 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if v, err := ch.Get(metadata, cago.WithType(cago.ClassName), cago.WithCount(1)); err == nil {
				className = v.Text()
			}
		}()
	}
	record, _, _ := strings.Cut(info.Name, ".")
	wg.Add(2)
	go func() {
		defer wg.Done()
		if v, err := client.Channel(record+".RTYP").Get(metadata, cago.WithType(cago.String), cago.WithCount(1)); err == nil {
			recordType = v.Text()
		}
	}()
	go func() {
		defer wg.Done()
		if v, err := client.Channel(record+".DESC").Get(metadata, cago.WithType(cago.String), cago.WithCount(1)); err == nil {
			details.Description = v.Text()
		}
	}()
	wg.Wait()
	// A gateway's channel class can describe the gateway rather than the EPICS
	// record. Prefer the record's RTYP field and use CLASS_NAME as a fallback.
	if recordType != "" {
		details.RecordType = recordType
	} else if className != "" {
		details.RecordType = className
	}
	return details
}

func infoTypeName(typ cago.DataType) string {
	names := [...]string{"STRING", "SHORT", "FLOAT", "ENUM", "CHAR", "LONG", "DOUBLE"}
	if int(typ) < len(names) {
		return names[typ]
	}
	return fmt.Sprintf("UNKNOWN(%d)", typ)
}
