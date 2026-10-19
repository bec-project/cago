package main

import (
	"github.com/wakonig/cago/internal/cli"
	"os"
)

var exit = os.Exit

func main() {
	cmd := cli.NewRootCommand(os.Stdout, os.Stderr)
	exit(cli.Run(cmd, os.Args[1:]))
}
