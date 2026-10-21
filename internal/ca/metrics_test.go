package ca

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/bec-project/cago/internal/wire"
)

type metricFunc func(Metric, int64)

func (f metricFunc) Add(metric Metric, delta int64) { f(metric, delta) }

func TestMetricsConcurrentCollectorAndIndependentSnapshots(t *testing.T) {
	var sink ClientMetrics
	client := &Client{opts: Options{Metrics: &sink}}
	const workers, iterations = 24, 200
	var wg sync.WaitGroup
	for range workers {
		wg.Go(func() {
			for range iterations {
				client.recordMetric(MetricSearchRequests, 1)
				client.recordMetric(MetricMonitorsActive, 1)
				client.recordMetric(MetricMonitorsActive, -1)
				_ = client.Metrics()
			}
		})
	}
	wg.Wait()
	for _, collector := range []*ClientMetrics{&client.metrics, &sink} {
		if got := collector.Value(MetricSearchRequests); got != workers*iterations {
			t.Fatalf("lost counter updates: %d", got)
		}
		if got := collector.Value(MetricMonitorsActive); got != 0 {
			t.Fatalf("unbalanced gauge: %d", got)
		}
		collector.Add(Metric(255), 1)
		if got := collector.Value(Metric(255)); got != 0 {
			t.Fatalf("unknown metric has value %d", got)
		}
	}
	snapshot := client.Metrics()
	if len(snapshot) != int(metricCount) {
		t.Fatalf("missing metric observations: %d", len(snapshot))
	}
	snapshot[MetricSearchRequests] = -1
	delete(snapshot, MetricMonitorsActive)
	if got := client.Metrics()[MetricSearchRequests]; got != workers*iterations {
		t.Fatalf("snapshot aliases client state: %d", got)
	}
	if stats := client.Stats(); stats.Searches != workers*iterations {
		t.Fatalf("legacy Stats differs from metrics: %+v", stats)
	}
	seen := make(map[string]bool)
	for metric := Metric(0); metric < metricCount; metric++ {
		name := metric.String()
		if name == "" || seen[name] {
			t.Fatalf("empty or duplicated metric name: %q", name)
		}
		seen[name] = true
	}
	if got := Metric(255).String(); got != "Metric(255)" {
		t.Fatal(got)
	}
}

func TestMetricsSinkPanicsCannotInterruptClient(t *testing.T) {
	server := newTestIOC(t)
	client, err := New(context.Background(), Options{SearchAddresses: []string{server.tcp.Addr().String()}, DisableRepeater: true, Metrics: metricFunc(func(Metric, int64) { panic("sink unavailable") })})
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	sub := subscribeTest(t, client.Channel("test"), ctx)
	event(t, sub)
	sub.Cancel()
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	metrics := client.Metrics()
	if metrics[MetricMonitorActivations] != 1 || metrics[MetricMonitorsActive] != 0 {
		t.Fatalf("sink panic interrupted monitor lifetime: %v", metrics)
	}
	if metrics[MetricSinkPanics] < 3 {
		t.Fatalf("sink panics were not counted: %v", metrics)
	}
	var absent *Client
	absent.recordMetric(MetricSearchRequests, 1)
	before := metrics[MetricSinkPanics]
	client.recordMetric(MetricSearchRequests, 0)
	if client.Metrics()[MetricSinkPanics] != before {
		t.Fatal("zero delta invoked the sink")
	}
}

func TestMetricsSharedMonitorRetryReconnectAndCleanup(t *testing.T) {
	server := newTestIOC(t)
	server.reject.Store(1)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	a := subscribeTest(t, ch, ctx)
	select {
	case <-a.Errors():
	case <-ctx.Done():
		t.Fatal("missing rejection")
	}
	event(t, a)
	b := subscribeTest(t, ch, ctx)
	event(t, b)
	if got := client.Metrics(); got[MetricMonitorActivations] != 2 || got[MetricMonitorFailures] != 1 || got[MetricMonitorsActive] != 1 {
		t.Fatalf("shared/retried monitor metrics: %v", got)
	}
	a.Cancel()
	if got := client.Metrics()[MetricMonitorsActive]; got != 1 {
		t.Fatalf("local cancellation retired shared generation: %d", got)
	}
	server.disconnect()
	event(t, b)
	if got := client.Metrics(); got[MetricMonitorActivations] != 3 || got[MetricMonitorFailures] != 1 || got[MetricMonitorsActive] != 1 {
		t.Fatalf("reconnect metrics: %v", got)
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	if got := client.Metrics()[MetricMonitorsActive]; got != 0 {
		t.Fatalf("client close leaked active monitor: %d", got)
	}
}

func TestMetricsActivationSendFailureAndIntentionalCancellation(t *testing.T) {
	for _, cancelActivation := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelActivation), func(t *testing.T) {
			server := newTestIOC(t)
			client := testClient(t, server, UDP)
			ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
			defer cancel()
			ch := client.Channel("test")
			if err := ch.Connect(ctx); err != nil {
				t.Fatal(err)
			}
			conn := rotationBinding(t, ch).conn
			conn.gate <- struct{}{}
			defer func() { <-conn.gate }()
			sub := subscribeTest(t, ch, ctx)
			eventually(t, func() bool { return client.Metrics()[MetricMonitorsActive] == 1 })
			if !cancelActivation {
				select {
				case err := <-sub.Errors():
					if !errors.Is(err, context.DeadlineExceeded) {
						t.Fatal(err)
					}
				case <-ctx.Done():
					t.Fatal("queued activation did not time out")
				}
			}
			sub.Cancel()
			got := client.Metrics()
			attempts, failures := got[MetricMonitorActivations], got[MetricMonitorFailures]
			if attempts < 1 || got[MetricMonitorsActive] != 0 || cancelActivation && failures != 0 || !cancelActivation && (failures < 1 || failures > attempts || attempts-failures > 1) {
				t.Fatalf("activation cleanup metrics: %v", got)
			}
		})
	}
}

func TestMetricsMonitorFailureCountsOncePerGeneration(t *testing.T) {
	client := &Client{}
	conn := &circuit{}
	m := &sharedMonitor{ch: &Channel{client: client}, conn: conn, id: 1, restart: make(chan struct{}, 1), metricActive: true, members: make(map[*Subscription]struct{})}
	client.recordMetric(MetricMonitorsActive, 1)
	for range 2 {
		m.reject(conn, 1, ErrDisconnected)
	}
	if got := client.Metrics(); got[MetricMonitorFailures] != 1 || got[MetricMonitorsActive] != 0 {
		t.Fatalf("duplicate rejection changed metrics: %v", got)
	}
	// A send error and a subsequent remote rejection belong to one generation.
	m.rejected, m.metricFailed = false, true
	client.recordMetric(MetricMonitorFailures, 1)
	m.reject(conn, 1, ErrDisconnected)
	if got := client.Metrics()[MetricMonitorFailures]; got != 2 {
		t.Fatalf("send failure plus rejection counted twice: %d", got)
	}
}

func TestMetricsSubscriptionOverflowCountsLostEntries(t *testing.T) {
	client := &Client{}
	m := &sharedMonitor{ch: &Channel{client: client}}
	ctx, cancel := context.WithCancel(context.Background())
	sub := &Subscription{monitor: m, ctx: ctx, cancel: cancel, done: make(chan struct{}), events: make(chan Event, 1), errors: make(chan error, 2)}
	sub.enqueue(Event{PV: "first"})
	sub.enqueue(Event{PV: "latest"})
	for i := range 5 {
		sub.report(fmt.Errorf("error %d", i))
	}
	got := client.Metrics()
	if got[MetricDroppedValues] != 1 || got[MetricDroppedErrors] != 3 {
		t.Fatalf("overflow count differs from lost entries: %v", got)
	}
	if e := <-sub.events; e.PV != "latest" {
		t.Fatal("latest event was discarded")
	}
	if err := <-sub.errors; err.Error() != "error 3" {
		t.Fatalf("newest errors were discarded: %v", err)
	}
	sub.closeLocal()
	sub.enqueue(Event{})
	sub.report(ErrClosed)
	if client.Metrics()[MetricDroppedErrors] != 3 || client.Metrics()[MetricDroppedValues] != 1 {
		t.Fatal("closure counted as overflow")
	}
}

func TestMetricsProtocolErrorCountsWireFramesOnce(t *testing.T) {
	client := &Client{errors: make(chan error, 8)}
	conn := &circuit{client: client, requests: make(map[uint32]pendingResponse)}
	result := make(chan response, 1)
	conn.requests[1] = pendingResponse{result: result}
	raw, _ := wire.Encode(wire.Frame{Command: wire.ReadNotify, P2: 1})
	conn.protocolError(wire.Frame{Payload: append(raw, []byte("read failed\x00")...)})
	if r := <-result; r.err == nil {
		t.Fatal("request did not receive protocol error")
	}
	conn.protocolError(wire.Frame{Payload: []byte("short")})
	if got := client.Metrics()[MetricProtocolErrors]; got != 2 {
		t.Fatalf("protocol errors should count frames, not fan-out: %d", got)
	}
}

func TestMetricsCircuitRotationCountsOneStart(t *testing.T) {
	server := newTestIOC(t)
	client := rotationClient(t, server, time.Second)
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	ch := client.Channel("test")
	if err := ch.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	seedCircuitIDs(rotationBinding(t, ch), uint64(^uint32(0)))
	var wg sync.WaitGroup
	for range 12 {
		wg.Go(func() {
			if _, err := ch.Get(ctx); err != nil {
				t.Error(err)
			}
		})
	}
	wg.Wait()
	if got := client.Metrics()[MetricCircuitRotations]; got != 1 {
		t.Fatalf("one rollover counted %d starts", got)
	}
}
