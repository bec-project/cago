package ca

import (
	"context"
	"errors"
	"net"
	"sync"
	"testing"
	"time"
)

func TestNewDNSRespectsCancellation(t *testing.T) {
	for _, mode := range []string{"during_dns", "before_dns", "configuration_timeout"} {
		t.Run(mode, func(t *testing.T) {
			preCancelled := mode == "before_dns"
			timeout := time.Second
			want := error(context.Canceled)
			if mode == "configuration_timeout" {
				timeout = 10 * time.Millisecond
				want = context.DeadlineExceeded
			}
			original := net.DefaultResolver
			started := make(chan struct{})
			release := make(chan struct{})
			var once sync.Once
			net.DefaultResolver = &net.Resolver{PreferGo: true, Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
				once.Do(func() { close(started) })
				select {
				case <-release:
					return nil, errors.New("test resolver released")
				case <-ctx.Done():
					return nil, ctx.Err()
				}
			}}
			defer func() { net.DefaultResolver = original; close(release) }()
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			if preCancelled {
				cancel()
			}
			result := make(chan error, 1)
			go func() {
				_, err := New(ctx, Options{SearchAddresses: []string{"cago-context-review.invalid:5064"}, DisableRepeater: true, ContextTimeout: timeout})
				result <- err
			}()
			if !preCancelled {
				select {
				case <-started:
				case <-time.After(time.Second):
					t.Fatal("resolver did not start")
				}
				if mode != "configuration_timeout" {
					cancel()
				}
			}
			select {
			case err := <-result:
				if !errors.Is(err, want) {
					t.Fatalf("cancelled constructor returned %v", err)
				}
			case <-time.After(200 * time.Millisecond):
				t.Fatal("cancelled New blocked in DNS lookup")
			}
		})
	}
}

func TestResolutionDeadlineDoesNotLimitClientLifetime(t *testing.T) {
	server := newTestIOC(t)
	client, err := New(context.Background(), Options{
		SearchAddresses: []string{server.tcp.Addr().String()},
		DisableRepeater: true, ContextTimeout: 30 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	time.Sleep(60 * time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if _, err := client.Channel("test").Get(ctx); err != nil {
		t.Fatalf("configuration deadline shortened client lifetime: %v", err)
	}
}
