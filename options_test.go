package cago

import (
	"context"
	"os/user"
	"runtime"
	"testing"
	"time"
)

func TestDefaultClientIdentity(t *testing.T) {
	t.Setenv("USER", "")
	current, err := user.Current()
	if err != nil {
		t.Skipf("OS identity unavailable: %v", err)
	}
	for _, name := range []string{"", "explicit-client"} {
		o := Options{SearchAddresses: []string{"127.0.0.1"}, ClientName: name}
		if err := o.defaults(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := name
		if want == "" {
			want = clientUserName(current.Username, runtime.GOOS)
		}
		if o.ClientName != want {
			t.Fatalf("name=%q: got %q want %q", name, o.ClientName, want)
		}
	}
	t.Setenv("USER", "environment-client")
	o := Options{SearchAddresses: []string{"127.0.0.1"}}
	if err := o.defaults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.ClientName != "environment-client" {
		t.Fatal("lost explicit environment identity")
	}
}

func TestNativeWindowsClientIdentity(t *testing.T) {
	for _, tt := range []struct{ name, platform, want string }{
		{"DOMAIN\\operator", "windows", "operator"},
		{"operator", "windows", "operator"},
		{"DOMAIN\\operator", "darwin", "DOMAIN\\operator"},
	} {
		if got := clientUserName(tt.name, tt.platform); got != tt.want {
			t.Fatalf("%s %q: got %q want %q", tt.platform, tt.name, got, tt.want)
		}
	}
}

func TestRotationDrainTimeout(t *testing.T) {
	for _, tt := range []struct {
		drain time.Duration
		want  time.Duration
	}{{0, 2 * time.Second}, {time.Minute, time.Minute}, {-time.Second, 0}} {
		o := Options{SearchAddresses: []string{"127.0.0.1"}, ContextTimeout: 2 * time.Second, RotationDrainTimeout: tt.drain}
		err := o.defaults(context.Background())
		if tt.drain < 0 {
			if err == nil {
				t.Fatal("negative drain timeout accepted")
			}
		} else if err != nil || o.RotationDrainTimeout != tt.want {
			t.Fatalf("drain=%v: got=%v err=%v", tt.drain, o.RotationDrainTimeout, err)
		}
	}
}

func TestSearchAddressesDeduplicatedAfterResolution(t *testing.T) {
	o := Options{SearchAddresses: []string{"127.0.0.1", "127.0.0.1:5064", "127.0.0.2:5064", "127.0.0.1"}}
	if err := o.defaults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(o.SearchAddresses) != 2 || o.SearchAddresses[0] != "127.0.0.1:5064" || o.SearchAddresses[1] != "127.0.0.2:5064" {
		t.Fatalf("canonical endpoints: %v", o.SearchAddresses)
	}
}
