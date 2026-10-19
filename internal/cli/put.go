package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/spf13/cobra"
	"github.com/wakonig/cago"
)

type putConfig struct {
	pv                                     string
	values                                 []string
	timeout                                time.Duration
	longString, numeric, stringEnum, terse bool
	completion                             bool
	priority                               uint
}

// NewPutCommand builds the cago put subcommand.
func NewPutCommand(stdout, stderr io.Writer) *cobra.Command {
	var cmd *cobra.Command
	cmd = newPutCommand(stdout, stderr, func(ctx context.Context, c putConfig) error {
		return Runtime(write(ctx, c, cmd.OutOrStdout()))
	})
	return cmd
}

func newPutCommand(stdout, stderr io.Writer, action func(context.Context, putConfig) error) *cobra.Command {
	c := putConfig{timeout: time.Second}
	invalidTimeoutEnv := false
	if s := os.Getenv("EPICS_CLI_TIMEOUT"); s != "" {
		if d, err := seconds(s); err == nil {
			c.timeout = d
		} else {
			invalidTimeoutEnv = true
		}
	}
	cmd := New("put [flags] PV VALUE...", "Write EPICS Channel Access values", stdout, stderr)
	f := cmd.Flags()
	f.DurationVar(&c.timeout, "timeout", c.timeout, "timeout per CA operation")
	f.FuncP("wait", "w", "operation timeout in seconds (EPICS Base spelling)", func(s string) error {
		d, err := seconds(s)
		if err == nil {
			c.timeout = d
		}
		return err
	})
	array := f.BoolP("array", "a", false, "write an array: PV COUNT VALUE...")
	f.BoolVarP(&c.longString, "long-string", "S", false, "write literal text as a NUL-terminated CHAR array")
	f.BoolVarP(&c.numeric, "numeric", "n", false, "interpret ENUM values as numeric indices")
	f.BoolVarP(&c.stringEnum, "string", "s", false, "interpret ENUM values as labels")
	f.BoolVarP(&c.terse, "terse", "t", false, "print only new readback, without PV name")
	f.BoolVarP(&c.completion, "completion", "c", false, "wait for IOC put completion")
	f.UintVarP(&c.priority, "priority", "p", 0, "CA priority (0-99)")
	cmd.Args = func(cmd *cobra.Command, args []string) error {
		if invalidTimeoutEnv {
			fmt.Fprintln(cmd.ErrOrStderr(), "invalid EPICS_CLI_TIMEOUT; using 1s")
		}
		if c.timeout <= 0 {
			return errors.New("timeout must be positive")
		}
		if c.priority > 99 {
			return errors.New("priority must be between 0 and 99")
		}
		if c.numeric && c.stringEnum {
			return errors.New("-n and -s are mutually exclusive")
		}
		if *array && c.longString {
			return errors.New("-a and -S are mutually exclusive")
		}
		if len(args) < 2 {
			return errors.New("PV and value are required")
		}
		c.pv, c.values = args[0], args[1:]
		if c.pv == "" {
			return errors.New("PV must not be empty")
		}
		if *array {
			n, err := strconv.ParseUint(c.values[0], 10, 32)
			if err != nil || n == 0 || n != uint64(len(c.values)-1) {
				return errors.New("array count must be positive and match the number of values")
			}
			c.values = c.values[1:]
		} else {
			c.values = []string{strings.Join(c.values, " ")}
		}
		// Validate STRING bounds before opening a network connection. Long strings
		// use CHAR slots instead; ENUM conversion is resolved after connecting.
		if !c.longString {
			if _, err := cago.NewValue(c.values); err != nil {
				return err
			}
		}
		return nil
	}
	cmd.RunE = func(cmd *cobra.Command, args []string) error { return action(cmd.Context(), c) }
	return cmd
}

func seconds(s string) (time.Duration, error) {
	n, err := strconv.ParseFloat(s, 64)
	if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n <= 0 || n >= float64(math.MaxInt64)/float64(time.Second) {
		return 0, fmt.Errorf("invalid timeout %q", s)
	}
	d := time.Duration(n * float64(time.Second))
	if d <= 0 {
		return 0, fmt.Errorf("timeout %q is below 1ns", s)
	}
	return d, nil
}

func (c putConfig) value(native cago.DataType, labels []string) (cago.Value, error) {
	if c.longString {
		if native != cago.Char {
			return cago.Value{}, errors.New("-S requires a CHAR PV")
		}
		return cago.NewValue(append([]byte(c.values[0]), 0))
	}
	if native != cago.Enum {
		return cago.NewValue(c.values)
	}
	indices := make([]float64, len(c.values))
	for i, s := range c.values {
		found := false
		if !c.numeric {
			for index, label := range labels {
				if s == label {
					indices[i], found = float64(index), true
					break
				}
			}
		}
		if found {
			continue
		}
		if c.stringEnum {
			return cago.Value{}, fmt.Errorf("invalid ENUM label %q", s)
		}
		n, err := strconv.ParseFloat(s, 64)
		if err != nil || math.IsNaN(n) || math.IsInf(n, 0) || n < 0 || n > math.MaxUint16 || math.Trunc(n) != n {
			return cago.Value{}, fmt.Errorf("invalid ENUM label or index %q", s)
		}
		indices[i] = n
	}
	if c.stringEnum {
		return cago.NewValue(c.values)
	}
	return cago.NewValue(indices)
}

type putChannel interface {
	Connect(context.Context) error
	Info() cago.ChannelInfo
	Get(context.Context, ...cago.RequestOption) (cago.Value, error)
	Put(context.Context, cago.Value) error
	PutNotify(context.Context, cago.Value) error
}

func (c putConfig) get(ctx context.Context, ch putChannel, options ...cago.RequestOption) (cago.Value, error) {
	operation, cancel := context.WithTimeout(ctx, c.timeout)
	defer cancel()
	return ch.Get(operation, options...)
}

func (c putConfig) execute(ctx context.Context, ch putChannel, stdout io.Writer, asyncErrors <-chan error) error {
	operation, cancel := context.WithTimeout(ctx, c.timeout)
	err := ch.Connect(operation)
	cancel()
	if err != nil {
		return err
	}
	native := ch.Info().NativeType
	inputType := native
	var labels []string
	if native == cago.Enum && !c.numeric && !c.longString {
		metadata, err := c.get(ctx, ch, cago.WithMetadata(cago.Graphics), cago.WithCount(1))
		if err != nil {
			if c.stringEnum || !errors.Is(err, cago.ErrNoRead) {
				return err
			}
			// A write-only ENUM cannot supply labels. Let the IOC perform
			// STRING conversion, which still resolves labels before indices.
			inputType = cago.String
		} else {
			labels = metadata.Metadata().EnumLabels
		}
	}
	v, err := c.value(inputType, labels)
	if err != nil {
		return err
	}
	readOptions := valueOptions(ch.Info().Count, c.numeric)
	printReadback := func(prefix, text string, count uint32) error {
		var err error
		if c.terse {
			_, err = fmt.Fprintln(stdout, text)
		} else {
			_, err = fmt.Fprintf(stdout, "%s%s%s\n", prefix, formatPVName(c.pv, count), text)
		}
		return err
	}
	var oldReadErr error
	if !c.terse {
		old, readErr := c.get(ctx, ch, readOptions...)
		oldReadErr = readErr
		text := ""
		if readErr != nil {
			text = formatReadError(readErr, true)
		} else {
			text = formatElements(old, ch.Info().Count, ch.Info().Count > 1, c.longString)
		}
		if err := printReadback("Old : ", text, ch.Info().Count); err != nil {
			return err
		}
	}
	operation, cancel = context.WithTimeout(ctx, c.timeout)
	if c.completion {
		err = ch.PutNotify(operation, v)
	} else {
		err = ch.Put(operation, v)
	}
	cancel()
	if err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return fmt.Errorf("write timeout (completion=%t); the IOC may still process the write: %w", c.completion, err)
		}
		return err
	}
	readback, err := c.get(ctx, ch, readOptions...)
	if err != nil {
		if writeErr := printReadback("New : ", formatReadError(err, true), ch.Info().Count); writeErr != nil {
			return errors.Join(err, writeErr)
		}
		return fmt.Errorf("write sent; new readback: %w", err)
	}
	if err := printReadback("New : ", formatElements(readback, ch.Info().Count, ch.Info().Count > 1, c.longString), ch.Info().Count); err != nil {
		return err
	}
	// Plain CA puts report server failures asynchronously. The readback acts
	// as an ordering barrier for errors already emitted on the same circuit.
	select {
	case err, ok := <-asyncErrors:
		if ok {
			return err
		}
	default:
	}
	if oldReadErr != nil {
		return fmt.Errorf("write sent; old readback failed: %w", oldReadErr)
	}
	return nil
}

func write(ctx context.Context, c putConfig, stdout io.Writer) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	client, err := cago.New(ctx, cago.Options{ContextTimeout: c.timeout})
	if err != nil {
		return err
	}
	defer client.Close()
	ch := client.Channel(c.pv, cago.WithPriority(uint16(c.priority)))
	if err = c.execute(ctx, ch, stdout, client.Errors()); err != nil {
		return fmt.Errorf("%s: %w", c.pv, err)
	}
	return nil
}
