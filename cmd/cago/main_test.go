package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestEntrypointHelpAndUsageErrors(t *testing.T) {
	for _, tt := range []struct {
		arg    string
		status int
	}{{"--help", 0}, {"--unknown-option", 2}} {
		t.Run(tt.arg, func(t *testing.T) {
			originalExit, originalArgs, originalOut, originalErr := exit, os.Args, os.Stdout, os.Stderr
			t.Cleanup(func() { exit, os.Args, os.Stdout, os.Stderr = originalExit, originalArgs, originalOut, originalErr })
			output := filepath.Join(t.TempDir(), "output")
			file, err := os.Create(output)
			if err != nil {
				t.Fatal(err)
			}
			defer file.Close()
			os.Stdout, os.Stderr = file, file
			os.Args = []string{"cago", tt.arg}
			status := -1
			exit = func(code int) { status = code }
			main()
			text, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if status != tt.status {
				t.Fatalf("exit=%d, want %d: %s", status, tt.status, text)
			}
			if tt.status == 0 && !strings.Contains(string(text), "cago [") && !strings.Contains(string(text), "cago\n") {
				t.Fatalf("help does not identify the executable: %s", text)
			}
			if tt.status != 0 && !strings.Contains(string(text), "unknown flag") {
				t.Fatalf("missing usage error: %s", text)
			}
		})
	}
}
