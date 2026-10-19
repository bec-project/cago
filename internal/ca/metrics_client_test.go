package ca

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestMetricsClientConnectionsAndGlobalErrorOverflow(t *testing.T) {
	server := newTestIOC(t)
	c := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	for _, priority := range []uint16{0, 99} {
		if err := c.Channel("metrics", WithPriority(priority)).Connect(ctx); err != nil {
			t.Fatal(err)
		}
	}
	metrics := c.Metrics()
	if metrics[MetricConnectionAttempts] != 2 || metrics[MetricConnections] != 2 || metrics[MetricConnectionFailures] != 0 || metrics[MetricCreateAttempts] != 2 || metrics[MetricCreateFailures] != 0 || metrics[MetricCreatesInFlight] != 0 {
		t.Fatalf("connection/creation counters: %v", metrics)
	}
	for n := 0; n < cap(c.errors)+3; n++ {
		c.report(errors.New("diagnostic"))
	}
	if got := c.Metrics()[MetricDroppedErrors]; got != 3 {
		t.Fatalf("global error queue lost %d entries, want 3", got)
	}
	if err := c.Close(); err != nil {
		t.Fatal(err)
	}
	if c.Metrics()[MetricCreatesInFlight] != 0 || c.Metrics()[MetricMonitorsActive] != 0 {
		t.Fatal("client shutdown left active work in metrics")
	}
}

func TestMetricsFailedDialIsCountedAndRemoved(t *testing.T) {
	c := testClient(t, newTestIOC(t), UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	// No listener can bind destination port zero: Listen assigns an ephemeral
	// port instead. This cannot race with the IOC or another ephemeral listener.
	if _, err := c.dial(ctx, "127.0.0.1:0", 0); err == nil {
		t.Fatal("dial to destination port zero succeeded")
	}
	metrics := c.Metrics()
	if metrics[MetricConnectionAttempts] != 1 || metrics[MetricConnectionFailures] != 1 || metrics[MetricConnections] != 0 || c.Stats().Circuits != 0 {
		t.Fatalf("failed shared dial was lost or retained: metrics=%v stats=%+v", metrics, c.Stats())
	}
}
