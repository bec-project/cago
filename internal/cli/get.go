package cli

import (
	"context"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/spf13/cobra"
	"github.com/bec-project/cago/pkg/cago"
)

func NewGetCommand(stdout, stderr io.Writer) *cobra.Command {
	cmd := New("get [flags] PV...", "Read EPICS Channel Access values", stdout, stderr)
	timeout := cmd.Flags().Duration("timeout", 5*time.Second, "operation timeout")
	count := cmd.Flags().Uint64("count", 0, "element count (0 means native capacity, or current length with -c)")
	var output outputOptions
	cmd.Flags().BoolVarP(&output.timestamp, "timestamp", "T", false, "print the record timestamp before each value")
	cmd.Flags().BoolVarP(&output.numeric, "numeric", "n", false, "print ENUM values as numbers")
	cmd.Flags().BoolVarP(&output.terse, "terse", "t", false, "print values without PV names")
	cmd.Flags().BoolVarP(&output.longString, "long-string", "S", false, "print CHAR arrays as strings")
	cmd.Flags().BoolVarP(&output.callback, "callback", "c", false, "print the actual returned array length, like caget -c")
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		if *count > math.MaxUint32 || *timeout <= 0 {
			return fmt.Errorf("count must fit uint32 and timeout must be positive")
		}
		return cobra.MinimumNArgs(1)(cmd, args)
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error {
		return Runtime(readValues(cmd.Context(), args, *timeout, uint32(*count), output, cmd.OutOrStdout(), cmd.ErrOrStderr()))
	}
	return cmd
}

func read(parent context.Context, pvs []string, timeout time.Duration, count uint32, stdout, stderr io.Writer) error {
	return readValues(parent, pvs, timeout, count, outputOptions{}, stdout, stderr)
}

func readValues(parent context.Context, pvs []string, timeout time.Duration, count uint32, output outputOptions, stdout, stderr io.Writer) error {
	if err := parent.Err(); err != nil {
		return err
	}
	ctx, cancel := context.WithCancel(parent)
	defer cancel()
	client, err := cago.New(ctx, cago.Options{ContextTimeout: timeout})
	if err != nil {
		return err
	}
	defer client.Close()
	channels := make([]*cago.Channel, len(pvs))
	for i, pv := range pvs {
		channels[i] = client.Channel(pv)
	}
	failed := false
	for i, pv := range pvs {
		operation, stop := context.WithTimeout(ctx, timeout)
		err := channels[i].Connect(operation)
		connected := err == nil
		requested := count
		var v cago.Value
		if err == nil {
			info := channels[i].Info()
			if requested > info.Count || requested == 0 && !output.callback {
				requested = info.Count
			}
			v, err = channels[i].Get(operation, valueOptions(requested, output.numeric)...)
		}
		stop()
		printedCount := requested
		if output.callback {
			printedCount = v.Count()
		}
		name := ""
		if !output.terse {
			name = formatPVName(pv, printedCount)
		}
		if err != nil {
			fmt.Fprintln(stderr, pv, err)
			if _, writeErr := fmt.Fprintln(stdout, name+formatReadError(err, connected)); writeErr != nil {
				fmt.Fprintln(stderr, writeErr)
			}
			failed = true
			continue
		}
		if _, err := fmt.Fprintf(stdout, "%s%s%s\n", name, formatOutputTimestamp(v.Metadata(), output), formatElements(v, printedCount, count != 0 || printedCount > 1, output.longString)); err != nil {
			fmt.Fprintln(stderr, err)
			failed = true
		}
	}
	if failed {
		if err := ctx.Err(); err != nil {
			return err
		}
		return fmt.Errorf("one or more reads failed")
	}
	return nil
}
