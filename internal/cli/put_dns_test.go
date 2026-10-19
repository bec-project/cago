package cli

import (
	"context"
	"io"
	"net"
	"sync"
	"testing"
	"time"
)

func TestPutConfiguredTimeoutBoundsDNS(t *testing.T) {
	t.Setenv("EPICS_CA_ADDR_LIST", "cago-cli-timeout-review.invalid:5064")
	t.Setenv("EPICS_CA_AUTO_ADDR_LIST", "NO")
	original := net.DefaultResolver
	started := make(chan struct{})
	var once sync.Once
	net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
		once.Do(func() { close(started) })
		<-ctx.Done()
		return nil, ctx.Err()
	}}
	defer func() { net.DefaultResolver = original }()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	result := make(chan error, 1)
	go func() {
		result <- write(ctx, putConfig{pv: "PV", values: []string{"1"}, timeout: 10 * time.Millisecond}, io.Discard)
	}()
	select {
	case <-started:
	case <-time.After(time.Second):
		cancel()
		<-result
		t.Fatal("DNS did not start")
	}
	select {
	case err := <-result:
		if err == nil {
			t.Fatal("DNS unexpectedly succeeded")
		}
	case <-time.After(80 * time.Millisecond):
		cancel()
		<-result
		t.Fatal("10ms CLI timeout did not bound initial DNS lookup")
	}
}
