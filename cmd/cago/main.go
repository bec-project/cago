package main

import (
	"github.com/bec-project/cago/internal/cli"
	"os"
)

var exit = os.Exit

func main() {
	cmd := cli.NewRootCommand(os.Stdout, os.Stderr)
	exit(cli.Run(cmd, os.Args[1:]))
}
