package cago

import (
	"context"
	"fmt"
	"net"
	"os"
	"os/user"
	"runtime"
	"strconv"
	"strings"
	"time"
)

type SearchTransport uint8

const (
	UDP SearchTransport = iota
	TCP
)

type Options struct {
	SearchTransport    SearchTransport
	CircuitIdleTimeout time.Duration
	// SearchMTU bounds a UDP discovery datagram, including the CA version frame.
	// Zero uses 1400 bytes; supported values are 576 through 65507.
	SearchMTU int
	// MaxConcurrentCreates limits simultaneous channel attachments per IOC
	// endpoint, across all priorities. Zero uses four.
	MaxConcurrentCreates int
	// Metrics optionally receives concurrent counter and gauge updates.
	Metrics Metrics
	// RotationDrainTimeout bounds the wait for admitted operations at ID rollover.
	// Zero uses ContextTimeout. Expiry fails unfinished calls without replaying them.
	RotationDrainTimeout                                                             time.Duration
	SearchAddresses                                                                  []string
	ServerPort, RepeaterPort                                                         int
	ContextTimeout, SearchInterval, MaxSearchPeriod, InactivityTimeout, EchoInterval time.Duration
	// AutoArrayBytes controls receive-buffer growth. Nil uses
	// EPICS_CA_AUTO_ARRAY_BYTES (YES by default); an explicit pointer overrides it.
	AutoArrayBytes *bool
	// MaxArrayBytes sets the manual receive limit when AutoArrayBytes is false.
	// Libca allows at least 16384 bytes and adds 24 bytes of protocol headroom.
	// It does not limit outgoing writes or admission of read/monitor requests.
	MaxArrayBytes        int
	Priority             uint16
	ClientName, HostName string
	// DisableRepeater disables the embedded repeater and registration.
	DisableRepeater bool
}

func (o *Options) defaults(ctx context.Context) error {
	if o.SearchMTU == 0 {
		o.SearchMTU = 1400
	}
	if o.MaxConcurrentCreates == 0 {
		o.MaxConcurrentCreates = 4
	}
	if o.SearchMTU < 576 || o.SearchMTU > 65507 || o.MaxConcurrentCreates < 1 {
		return fmt.Errorf("invalid search MTU or concurrent create limit")
	}
	if o.ServerPort == 0 {
		o.ServerPort = envInt("EPICS_CA_SERVER_PORT", 5064)
	}
	if o.RepeaterPort == 0 {
		o.RepeaterPort = envInt("EPICS_CA_REPEATER_PORT", 5065)
	}
	if o.ContextTimeout == 0 {
		o.ContextTimeout = envDuration("EPICS_CA_CONN_TMO", 5*time.Second)
	}
	if o.RotationDrainTimeout == 0 {
		o.RotationDrainTimeout = o.ContextTimeout
	}
	if o.SearchInterval == 0 {
		o.SearchInterval = 100 * time.Millisecond
	}
	if o.MaxSearchPeriod == 0 {
		o.MaxSearchPeriod = envDuration("EPICS_CA_MAX_SEARCH_PERIOD", 5*time.Minute)
	}
	if o.InactivityTimeout == 0 {
		o.InactivityTimeout = 30 * time.Second
	}
	if o.EchoInterval == 0 {
		o.EchoInterval = 10 * time.Second
	}
	if o.MaxArrayBytes == 0 {
		o.MaxArrayBytes = envInt("EPICS_CA_MAX_ARRAY_BYTES", 16384)
		if o.MaxArrayBytes < 0 {
			o.MaxArrayBytes = 16384
		}
	}
	auto := true
	if setting := os.Getenv("EPICS_CA_AUTO_ARRAY_BYTES"); setting != "" {
		// Libca's environment parser enables booleans only for YES.
		auto = strings.EqualFold(setting, "YES")
	}
	if o.AutoArrayBytes != nil {
		auto = *o.AutoArrayBytes
	}
	// Own the resolved value so later caller mutations cannot race network I/O.
	o.AutoArrayBytes = &auto
	if o.CircuitIdleTimeout == 0 {
		o.CircuitIdleTimeout = 2 * time.Minute
	}
	if o.SearchTransport > TCP || o.CircuitIdleTimeout < 0 || o.RotationDrainTimeout <= 0 {
		return fmt.Errorf("invalid search transport, idle timeout or rotation drain timeout")
	}
	if o.HostName == "" {
		o.HostName, _ = os.Hostname()
	}
	if o.ClientName == "" {
		o.ClientName = os.Getenv("USER")
		if o.ClientName == "" {
			if current, err := user.Current(); err == nil {
				o.ClientName = clientUserName(current.Username, runtime.GOOS)
			}
		}
	}
	if o.ServerPort < 1 || o.ServerPort > 65535 || o.RepeaterPort < 1 || o.RepeaterPort > 65535 || o.Priority > 99 || o.ContextTimeout <= 0 || o.SearchInterval <= 0 || o.MaxSearchPeriod < o.SearchInterval || o.EchoInterval <= 0 || o.InactivityTimeout <= o.EchoInterval || o.MaxArrayBytes < 0 {
		return fmt.Errorf("invalid CA options")
	}
	if len(o.SearchAddresses) == 0 {
		o.SearchAddresses = strings.Fields(os.Getenv("EPICS_CA_ADDR_LIST"))
		auto := strings.ToLower(os.Getenv("EPICS_CA_AUTO_ADDR_LIST"))
		if o.SearchTransport == UDP && auto != "no" && auto != "false" && auto != "0" {
			o.SearchAddresses = append(o.SearchAddresses, broadcasts()...)
		}
	}
	if len(o.SearchAddresses) == 0 {
		return fmt.Errorf("no CA search addresses configured")
	}
	ctx, cancel := context.WithTimeout(ctx, o.ContextTimeout)
	defer cancel()
	resolved := make([]string, 0, len(o.SearchAddresses))
	seen := make(map[string]bool)
	for _, s := range o.SearchAddresses {
		if _, _, err := net.SplitHostPort(s); err != nil {
			s = net.JoinHostPort(s, strconv.Itoa(o.ServerPort))
		}
		host, port, err := net.SplitHostPort(s)
		if err != nil {
			return fmt.Errorf("search address %q: %w", s, err)
		}
		ips, err := net.DefaultResolver.LookupIP(ctx, "ip4", host)
		if err != nil {
			return fmt.Errorf("search address %q: %w", s, err)
		}
		p, err := net.DefaultResolver.LookupPort(ctx, "udp", port)
		if err != nil {
			return fmt.Errorf("search address %q: %w", s, err)
		}
		if len(ips) == 0 || ips[0].To4() == nil || p <= 0 || p > 65535 {
			return fmt.Errorf("invalid IPv4 CA search address %q", s)
		}
		address := (&net.UDPAddr{IP: ips[0], Port: p}).String()
		if !seen[address] {
			seen[address] = true
			resolved = append(resolved, address)
		}
	}
	o.SearchAddresses = resolved
	return nil
}

// receiveLimit matches libca's automatic growth and manual buffer allowance.
func (o Options) receiveLimit() int {
	limit := uint64(^uint32(0))
	if o.AutoArrayBytes != nil && !*o.AutoArrayBytes {
		limit = min(limit, max(uint64(16384), uint64(o.MaxArrayBytes)+24))
	}
	return int(min(limit, uint64(^uint(0)>>1)))
}

func clientUserName(name, platform string) string {
	// Go returns DOMAIN\user on Windows; libca's GetUserName returns user.
	if platform == "windows" {
		if i := strings.LastIndexByte(name, '\\'); i >= 0 {
			return name[i+1:]
		}
	}
	return name
}

func envInt(name string, fallback int) int {
	n, err := strconv.Atoi(os.Getenv(name))
	if err != nil {
		return fallback
	}
	return n
}
func envDuration(name string, fallback time.Duration) time.Duration {
	n, err := strconv.ParseFloat(os.Getenv(name), 64)
	if err != nil {
		return fallback
	}
	return time.Duration(n * float64(time.Second))
}
func broadcasts() []string {
	result := []string{}
	ifs, _ := net.Interfaces()
	for _, iface := range ifs {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}
		addrs, _ := iface.Addrs()
		for _, addr := range addrs {
			ipn, ok := addr.(*net.IPNet)
			if !ok || ipn.IP.To4() == nil || len(ipn.Mask) != 4 {
				continue
			}
			ip := ipn.IP.To4()
			b := make(net.IP, 4)
			for i := range b {
				b[i] = ip[i] | ^ipn.Mask[i]
			}
			result = append(result, b.String())
		}
	}
	return result
}

type requestOptions struct {
	typ      DataType
	enumType DataType
	count    uint32
	level    MetadataLevel
	mask     EventMask
	buffer   int
}
type RequestOption func(*requestOptions)

func WithType(t DataType) RequestOption { return func(o *requestOptions) { o.typ = t } }

// WithEnumType converts native ENUM channels to the specified base type while
// leaving other native types unchanged. It is resolved again after reconnects.
// An explicit WithType takes precedence; Native disables the conversion.
func WithEnumType(t DataType) RequestOption { return func(o *requestOptions) { o.enumType = t } }
func WithCount(n uint32) RequestOption      { return func(o *requestOptions) { o.count = n } }
func WithMetadata(level MetadataLevel) RequestOption {
	return func(o *requestOptions) { o.level = level }
}
func WithMask(mask EventMask) RequestOption { return func(o *requestOptions) { o.mask = mask } }
func WithBuffer(n int) RequestOption        { return func(o *requestOptions) { o.buffer = n } }
func requestConfig(options []RequestOption) (requestOptions, error) {
	o := requestOptions{typ: Native, enumType: Native, mask: ValueEvent | AlarmEvent, buffer: 1}
	for _, f := range options {
		f(&o)
	}
	if o.typ != Native && !o.typ.Valid() || o.enumType != Native && o.enumType > Double || o.level > Control || o.buffer <= 0 || o.mask == 0 || o.mask & ^EventMask(15) != 0 {
		return o, fmt.Errorf("invalid CA request options")
	}
	// Full DBR types and base-type/metadata options describe the same request.
	// Canonicalize them so metadata event masks and monitor sharing agree.
	if o.level == Plain && o.typ >= 7 && o.typ <= 34 {
		o.level = MetadataLevel(o.typ / 7)
		o.typ = o.typ.Base()
	}
	if o.typ != Native {
		o.enumType = Native
	}
	return o, nil
}
func (o requestOptions) wireType(native DataType) (DataType, error) {
	t := o.typ
	if t == Native {
		t = native
		if native == Enum && o.enumType != Native {
			t = o.enumType
		}
	}
	if o.level != Plain {
		if t > 6 {
			return 0, ErrBadType
		}
		t += DataType(o.level) * 7
	}
	return t, nil
}
