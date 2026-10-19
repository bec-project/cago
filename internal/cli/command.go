// Package cli contains the common command-line conventions of the CA tools.
package cli

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/spf13/cobra"
)

func New(use, short string, stdout, stderr io.Writer) *cobra.Command {
	c := &cobra.Command{Use: use, Short: short, SilenceErrors: true, SilenceUsage: true}
	c.SetOut(stdout)
	c.SetErr(stderr)
	c.Flags().SetInterspersed(false) // PV values may start with '-'.
	c.CompletionOptions.DisableDefaultCmd = true
	return c
}

type runtimeError struct{ error }

func (e runtimeError) Unwrap() error { return e.error }

// Runtime marks an operational failure rather than invalid arguments.
func Runtime(err error) error {
	if err == nil {
		return nil
	}
	return runtimeError{err}
}

func Execute(c *cobra.Command, args []string) int {
	c.SetArgs(Args(c, args))
	if err := c.Execute(); err != nil {
		fmt.Fprintln(c.ErrOrStderr(), err)
		var operational runtimeError
		if errors.As(err, &operational) {
			return 1
		}
		return 2
	}
	return 0
}

// Run executes a CLI command with cancellation on interrupt or termination.
func Run(c *cobra.Command, args []string) int {
	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()
	c.SetContext(ctx)
	return Execute(c, args)
}

// Args retains legacy Go flag spellings such as -timeout alongside --timeout.
// Only option tokens before the first operand are rewritten; flag values and
// literal PV values (including negative numbers and flag-like text) stay intact.
func Args(c *cobra.Command, args []string) []string {
	c.InitDefaultHelpFlag()
	out := append([]string(nil), args...)
	for i := 0; i < len(out); i++ {
		token := out[i]
		if token == "--" || token == "-" {
			break
		}
		if !strings.HasPrefix(token, "-") {
			for _, child := range c.Commands() {
				if token == child.Name() {
					copy(out[i+1:], Args(child, out[i+1:]))
					break
				}
			}
			break
		}
		name, _, assigned := strings.Cut(strings.TrimLeft(token, "-"), "=")
		flag := c.Flags().Lookup(name)
		if flag != nil && !strings.HasPrefix(token, "--") && len(name) > 1 {
			out[i] = "-" + token
		}
		if flag == nil && !strings.HasPrefix(token, "--") && len(name) > 0 {
			// A cluster may end with a valued flag: -cw 0.5. A value
			// attached to that flag (-cw0.5) consumes the remaining token.
			for j := 0; j < len(name); j++ {
				short := c.Flags().ShorthandLookup(name[j : j+1])
				if short == nil {
					break
				}
				if short.NoOptDefVal == "" {
					if j == len(name)-1 && !assigned {
						i++
					}
					break
				}
			}
			continue
		}
		if flag != nil && flag.NoOptDefVal == "" && !assigned {
			i++
		}
	}
	return out
}
