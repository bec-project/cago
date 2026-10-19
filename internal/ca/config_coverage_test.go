package ca

import (
	"context"
	"errors"
	"net"
	"net/netip"
	"reflect"
	"strings"
	"testing"
	"time"
)

func clearCAEnvironment(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"EPICS_CA_SERVER_PORT", "EPICS_CA_REPEATER_PORT", "EPICS_CA_CONN_TMO",
		"EPICS_CA_MAX_SEARCH_PERIOD", "EPICS_CA_MAX_ARRAY_BYTES", "EPICS_CA_ADDR_LIST",
		"EPICS_CA_AUTO_ADDR_LIST", "EPICS_CA_AUTO_ARRAY_BYTES",
	} {
		t.Setenv(name, "")
	}
}

func TestOptionsEnvironmentDefaults(t *testing.T) {
	clearCAEnvironment(t)
	t.Setenv("EPICS_CA_SERVER_PORT", "5301")
	t.Setenv("EPICS_CA_REPEATER_PORT", "5302")
	t.Setenv("EPICS_CA_CONN_TMO", "2.25")
	t.Setenv("EPICS_CA_MAX_SEARCH_PERIOD", "12.5")
	t.Setenv("EPICS_CA_MAX_ARRAY_BYTES", "32768")
	t.Setenv("EPICS_CA_ADDR_LIST", " 127.0.0.1\t127.0.0.1:5301\n127.0.0.2:5303 ")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	o := Options{}
	if err := o.defaults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.ServerPort != 5301 || o.RepeaterPort != 5302 || o.MaxArrayBytes != 32768 {
		t.Fatalf("integer environment defaults: %+v", o)
	}
	if o.ContextTimeout != 2250*time.Millisecond || o.RotationDrainTimeout != o.ContextTimeout || o.MaxSearchPeriod != 12500*time.Millisecond {
		t.Fatalf("fractional-second environment defaults: %+v", o)
	}
	want := []string{"127.0.0.1:5301", "127.0.0.2:5303"}
	if !reflect.DeepEqual(o.SearchAddresses, want) {
		t.Fatalf("environment endpoints = %v, want %v", o.SearchAddresses, want)
	}

	// Explicit values must continue to win when environment defaults are set.
	explicit := Options{
		ServerPort: 5401, RepeaterPort: 5402, MaxArrayBytes: 65536,
		ContextTimeout: time.Second, RotationDrainTimeout: 3 * time.Second,
		MaxSearchPeriod: time.Minute, SearchAddresses: []string{"127.0.0.3"},
	}
	if err := explicit.defaults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if explicit.ServerPort != 5401 || explicit.RepeaterPort != 5402 || explicit.MaxArrayBytes != 65536 ||
		explicit.ContextTimeout != time.Second || explicit.RotationDrainTimeout != 3*time.Second || explicit.MaxSearchPeriod != time.Minute ||
		!reflect.DeepEqual(explicit.SearchAddresses, []string{"127.0.0.3:5401"}) {
		t.Fatalf("environment overwrote explicit options: %+v", explicit)
	}
}

func TestOptionsMalformedEnvironmentFallsBack(t *testing.T) {
	clearCAEnvironment(t)
	for _, name := range []string{"EPICS_CA_SERVER_PORT", "EPICS_CA_REPEATER_PORT", "EPICS_CA_CONN_TMO", "EPICS_CA_MAX_SEARCH_PERIOD", "EPICS_CA_MAX_ARRAY_BYTES"} {
		t.Setenv(name, "invalid")
	}
	o := Options{SearchAddresses: []string{"127.0.0.1"}}
	if err := o.defaults(context.Background()); err != nil {
		t.Fatal(err)
	}
	if o.ServerPort != 5064 || o.RepeaterPort != 5065 || o.MaxArrayBytes != 16384 ||
		o.ContextTimeout != 5*time.Second || o.MaxSearchPeriod != 5*time.Minute {
		t.Fatalf("malformed environment did not use defaults: %+v", o)
	}
}

func TestOptionsRejectsInvalidConfiguration(t *testing.T) {
	clearCAEnvironment(t)
	for _, tt := range []struct {
		name   string
		change func(*Options)
	}{
		{"server_negative", func(o *Options) { o.ServerPort = -1 }},
		{"server_overflow", func(o *Options) { o.ServerPort = 65536 }},
		{"repeater_negative", func(o *Options) { o.RepeaterPort = -1 }},
		{"repeater_overflow", func(o *Options) { o.RepeaterPort = 65536 }},
		{"priority", func(o *Options) { o.Priority = 100 }},
		{"context_timeout", func(o *Options) { o.ContextTimeout = -time.Second }},
		{"search_interval", func(o *Options) { o.SearchInterval = -time.Second }},
		{"max_search_period", func(o *Options) { o.MaxSearchPeriod = time.Millisecond }},
		{"echo_interval", func(o *Options) { o.EchoInterval = -time.Second }},
		{"inactivity_timeout", func(o *Options) { o.InactivityTimeout = 10 * time.Second }},
		{"array_limit", func(o *Options) { o.MaxArrayBytes = -1 }},
		{"transport", func(o *Options) { o.SearchTransport = TCP + 1 }},
		{"idle_timeout", func(o *Options) { o.CircuitIdleTimeout = -time.Second }},
		{"rotation_timeout", func(o *Options) { o.RotationDrainTimeout = -time.Second }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			o := Options{SearchAddresses: []string{"127.0.0.1"}}
			tt.change(&o)
			if err := o.defaults(context.Background()); err == nil {
				t.Fatalf("invalid %s accepted: %+v", tt.name, o)
			}
		})
	}
}

func TestOptionsSearchAddressValidation(t *testing.T) {
	clearCAEnvironment(t)
	for _, tt := range []struct{ address, message string }{
		{"[", "search address"},
		{"127.0.0.1:invalid-port", "search address"},
		{"127.0.0.1:0", "invalid IPv4 CA search address"},
		{"127.0.0.1:65536", "search address"},
	} {
		t.Run(tt.address, func(t *testing.T) {
			o := Options{SearchAddresses: []string{tt.address}}
			if err := o.defaults(context.Background()); err == nil || !strings.Contains(err.Error(), tt.message) {
				t.Fatalf("address %q: got %v, want %q", tt.address, err, tt.message)
			}
		})
	}
}

func TestOptionsAutomaticAddressSelection(t *testing.T) {
	clearCAEnvironment(t)
	for _, setting := range []string{"NO", "false", "0"} {
		t.Run(setting, func(t *testing.T) {
			t.Setenv("EPICS_CA_AUTO_ADDR_LIST", setting)
			o := Options{}
			if err := o.defaults(context.Background()); err == nil || !strings.Contains(err.Error(), "no CA search addresses") {
				t.Fatalf("disabled automatic discovery without addresses: %v", err)
			}
		})
	}

	t.Setenv("EPICS_CA_ADDR_LIST", "127.0.0.1:5304")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "YES")
	for _, transport := range []SearchTransport{UDP, TCP} {
		o := Options{SearchTransport: transport}
		if err := o.defaults(context.Background()); err != nil {
			t.Fatal(err)
		}
		want := []string{"127.0.0.1:5304"}
		if transport == UDP {
			seen := map[string]bool{want[0]: true}
			for _, address := range broadcasts() {
				endpoint := net.JoinHostPort(address, "5064")
				if !seen[endpoint] {
					want = append(want, endpoint)
					seen[endpoint] = true
				}
			}
		}
		if !reflect.DeepEqual(o.SearchAddresses, want) {
			t.Fatalf("transport %v addresses = %v, want %v", transport, o.SearchAddresses, want)
		}
	}
}

func TestBroadcastAddressesBelongToActiveIPv4Interfaces(t *testing.T) {
	interfaces, err := net.Interfaces()
	if err != nil {
		t.Fatal(err)
	}
	var prefixes []netip.Prefix
	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addresses, err := iface.Addrs()
		if err != nil {
			t.Fatal(err)
		}
		for _, address := range addresses {
			prefix, err := netip.ParsePrefix(address.String())
			if err == nil && prefix.Addr().Is4() {
				prefixes = append(prefixes, prefix)
			}
		}
	}
	addresses := broadcasts()
	if len(addresses) != len(prefixes) {
		t.Fatalf("got %d broadcasts for %d active IPv4 addresses", len(addresses), len(prefixes))
	}
	for _, address := range addresses {
		ip, err := netip.ParseAddr(address)
		if err != nil || !ip.Is4() {
			t.Fatalf("broadcast is not IPv4: %q", address)
		}
		found := false
		for _, prefix := range prefixes {
			// A broadcast is the final address in its interface's prefix.
			if prefix.Contains(ip) && !prefix.Contains(ip.Next()) {
				found = true
				break
			}
		}
		if !found {
			t.Errorf("%s is not the final address of an active IPv4 interface prefix", address)
		}
	}
}

func TestRequestRejectsMetadataOnFullWireTypes(t *testing.T) {
	for _, typ := range []DataType{7, PutACKT, PutACKS, STSACKString, ClassName} {
		cfg, err := requestConfig([]RequestOption{WithType(typ), WithMetadata(Status)})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := cfg.wireType(Double); !errors.Is(err, ErrBadType) {
			t.Errorf("metadata added to full wire type %d: got %v, want ErrBadType", typ, err)
		}
	}
}
