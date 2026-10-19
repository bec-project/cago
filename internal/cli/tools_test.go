package cli

import (
	"context"
	"io"
)

func runGet(args []string, stdout, stderr io.Writer) int {
	return Execute(NewGetCommand(stdout, stderr), args)
}

func runPut(args []string, stdout, stderr io.Writer) int {
	return Execute(NewPutCommand(stdout, stderr), args)
}

func runMonitor(ctx context.Context, args []string) int {
	cmd := NewMonitorCommand(io.Discard, io.Discard)
	cmd.SetContext(ctx)
	return Execute(cmd, args)
}
