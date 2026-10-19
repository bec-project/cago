package ca

import (
	"context"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

func TestCreateAdmissionRefreshesQueuedEndpoint(t *testing.T) {
	old, healthy := newAttachmentIOC(t), newAttachmentIOC(t)
	c := attachmentClient(t, old, 1, 5*time.Second)
	// Hold the old endpoint independently of the channel. A healthy replacement
	// must be usable before the old endpoint releases its reservation.
	release, err := c.acquireCreate(context.Background(), old.tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ch := attachmentChannel(c, old, "moved", 0)
	eventually(t, func() bool { return createUsers(c, old.tcp.Addr().String()) == 2 })
	ch.results <- healthy.tcp.Addr().String()
	nextAttachment(t, healthy).respond(t, wire.Create)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if got := ch.Info().Host; got != healthy.tcp.Addr().String() {
		t.Fatalf("connected to %s, want replacement endpoint", got)
	}
	if got := createUsers(c, old.tcp.Addr().String()); got != 1 {
		t.Fatalf("old endpoint retained %d reservations", got)
	}
	release()
	assertNoCreateState(t, c)
	noAttachment(t, old)
}

func TestCreateAdmissionDeadlineRediscover(t *testing.T) {
	old, healthy := newAttachmentIOC(t), newAttachmentIOC(t)
	c := attachmentClient(t, old, 1, 50*time.Millisecond)
	release, err := c.acquireCreate(context.Background(), old.tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ch := attachmentChannel(c, old, "timeout", 0)
	eventually(t, func() bool { return createUsers(c, old.tcp.Addr().String()) == 2 })
	eventually(t, func() bool {
		return createUsers(c, old.tcp.Addr().String()) == 1 && c.metrics.Value(MetricSearchRequests) >= 2
	})
	if got := ch.Info().State; got == Closed {
		t.Fatal("admission timeout permanently closed the channel")
	}
	ch.results <- healthy.tcp.Addr().String()
	nextAttachment(t, healthy).respond(t, wire.Create)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	assertNoCreateState(t, c)
}

func TestCreateAdmissionBeaconRediscover(t *testing.T) {
	old, healthy := newAttachmentIOC(t), newAttachmentIOC(t)
	c := attachmentClient(t, old, 1, time.Hour)
	release, err := c.acquireCreate(context.Background(), old.tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ch := attachmentChannel(c, old, "beacon", 0)
	eventually(t, func() bool { return createUsers(c, old.tcp.Addr().String()) == 2 })
	ch.results <- "" // The beacon discovery path wakes searching channels this way.
	eventually(t, func() bool {
		return createUsers(c, old.tcp.Addr().String()) == 1 && c.metrics.Value(MetricSearchRequests) >= 2
	})
	ch.results <- healthy.tcp.Addr().String()
	nextAttachment(t, healthy).respond(t, wire.Create)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	release()
	assertNoCreateState(t, c)
}

func TestCreateAdmissionDuplicateSearchPreservesEndpointBound(t *testing.T) {
	s := newAttachmentIOC(t)
	c := attachmentClient(t, s, 1, time.Second)
	release, err := c.acquireCreate(context.Background(), s.tcp.Addr().String())
	if err != nil {
		t.Fatal(err)
	}
	defer release()
	ch := attachmentChannel(c, s, "duplicate", 0)
	eventually(t, func() bool { return createUsers(c, s.tcp.Addr().String()) == 2 })
	ch.results <- s.tcp.Addr().String()
	noAttachment(t, s)
	if got := createUsers(c, s.tcp.Addr().String()); got != 2 {
		t.Fatalf("duplicate search changed queued reservation count: %d", got)
	}
	release()
	nextAttachment(t, s).respond(t, wire.Create)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	assertNoCreateState(t, c)
}

func TestCreateAdmissionQueuedUpdateWinsReadySlot(t *testing.T) {
	c := &Client{ctx: context.Background()}
	for _, updated := range []string{"replacement", ""} {
		for range 100 {
			updates := make(chan string, 1)
			updates <- updated
			release, address, err := c.acquireCreateOrUpdate(context.Background(), "old", updates)
			if release != nil {
				release()
				t.Fatal("admitted a stale candidate with an already queued update")
			}
			if err != nil || address != updated {
				t.Fatalf("replacement=%q, error=%v", address, err)
			}
			assertNoCreateState(t, c)
		}
	}
	if got := c.metrics.Value(MetricCreatesInFlight); got != 0 {
		t.Fatalf("discarded candidates incremented in-flight gauge: %d", got)
	}
}
