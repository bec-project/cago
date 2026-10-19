package cli

import (
	"fmt"
	"io"

	"github.com/spf13/cobra"
)

// NewRootCommand builds the unified cago command-line interface.
func NewRootCommand(stdout, stderr io.Writer) *cobra.Command {
	cmd := New("cago", "EPICS Channel Access tools", stdout, stderr)
	cmd.AddCommand(
		NewGetCommand(nil, nil),
		NewPutCommand(nil, nil),
		NewInfoCommand(nil, nil),
		NewMonitorCommand(nil, nil),
	)
	defaultHelp := cmd.HelpFunc()
	cmd.SetHelpFunc(func(current *cobra.Command, args []string) {
		defaultHelp(current, args)
		if current != cmd {
			return
		}
		out := current.OutOrStdout()
		fmt.Fprintln(out, "\nSubcommand flags (place after the command):")
		for _, child := range current.Commands() {
			if !child.IsAvailableCommand() {
				continue
			}
			child.InitDefaultHelpFlag()
			fmt.Fprintf(out, "\n%s\n%s", child.UseLine(), child.LocalFlags().FlagUsages())
		}
	})
	return cmd
}
