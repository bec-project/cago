package ca

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/bec-project/cago/internal/wire"
)

func TestSubscribeChannelCancellation(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := client.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case e := <-events:
		if e.PV != "test" || e.Value.Float64() != 1.25 {
			t.Fatalf("unexpected event %+v", e)
		}
	case <-ctx.Done():
		t.Fatal("missing initial snapshot")
	}
	cancel()
	select {
	case _, ok := <-events:
		if ok {
			t.Fatal("events remain after cancellation")
		}
	case <-time.After(time.Second):
		t.Fatal("subscription did not stop")
	}
	// Context cancellation disposes only this subscription, preserving the
	// shared channel for other callers.
	live, stop := context.WithTimeout(context.Background(), time.Second)
	defer stop()
	if _, err := client.Channel("test").Get(live); err != nil {
		t.Fatal(err)
	}
}

func TestSubscribeChannelErrorsAndRecovery(t *testing.T) {
	server := newTestIOC(t)
	server.reject.Store(1)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := client.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-client.Errors():
		var pe *ProtocolError
		if !errors.As(err, &pe) || !strings.Contains(err.Error(), "test") {
			t.Fatalf("missing PV/protocol context: %v", err)
		}
	case <-ctx.Done():
		t.Fatal("missing monitor error")
	}
	select {
	case e := <-events:
		if e.Value.Float64() != 1.25 {
			t.Fatal("bad recovery snapshot")
		}
	case <-ctx.Done():
		t.Fatal("monitor did not recover")
	}
}

func TestSubscribeChannelSetupError(t *testing.T) {
	client := testClient(t, newTestIOC(t), UDP)
	events, err := client.Subscribe(context.Background(), "test", WithBuffer(0))
	if err == nil || events != nil {
		t.Fatal("invalid request should return error and nil stream")
	}
	if client.Stats().Channels != 0 {
		t.Fatal("invalid request created a durable channel")
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if events, err := client.Subscribe(ctx, "missing"); !errors.Is(err, context.Canceled) || events != nil {
		t.Fatal("cancelled request should return context error and nil stream")
	}
	if client.Stats().Channels != 0 {
		t.Fatal("cancelled request created a durable channel")
	}
}

func TestSubscribeProtocolErrorReportedOnce(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	events, err := client.Subscribe(ctx, "test")
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-events:
	case <-ctx.Done():
		t.Fatal("missing snapshot")
	}
	server.mu.Lock()
	var peer *testPeer
	for p := range server.peers {
		peer = p
		break
	}
	server.mu.Unlock()
	if peer == nil {
		t.Fatal("no connection")
	}
	peer.mu.Lock()
	var request wire.Frame
	for _, f := range peer.subs {
		request = f
		break
	}
	peer.mu.Unlock()
	raw, err := wire.Encode(request)
	if err != nil {
		t.Fatal(err)
	}
	payload := append(raw[:16:16], []byte("monitor rejected\x00")...)
	peer.send(wire.Frame{Command: wire.Error, P2: 0, Payload: payload})
	select {
	case err := <-client.Errors():
		if !strings.Contains(err.Error(), "test") {
			t.Fatal(err)
		}
	case <-ctx.Done():
		t.Fatal("missing protocol error")
	}
	// A subsequent reply on the same circuit is a barrier: the error handler
	// has completed, including any duplicate forwarding, before Get returns.
	if _, err := client.Channel("test").Get(ctx); err != nil {
		t.Fatal(err)
	}
	if n := len(client.errors); n != 0 {
		t.Fatalf("duplicate errors: %d", n)
	}
}
