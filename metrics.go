package cago

import (
	"fmt"
	"sync/atomic"
)

// Metric identifies a bounded client counter or gauge. MetricCreatesInFlight
// and MetricMonitorsActive are gauges; all other metrics are cumulative counters.
type Metric uint8

const (
	MetricSearchRequests Metric = iota
	MetricSearchDatagrams
	MetricSearchSendFailures
	MetricConnectionAttempts
	MetricConnectionFailures
	MetricConnections
	MetricReconnects
	MetricCreateAttempts
	MetricCreateFailures
	MetricCreatesInFlight
	MetricMonitorActivations
	MetricMonitorFailures
	MetricMonitorsActive
	MetricDroppedValues
	MetricDroppedErrors
	MetricProtocolErrors
	MetricCircuitRotations
	MetricSinkPanics
	metricCount
)

var metricNames = [...]string{
	"search_requests", "search_datagrams", "search_send_failures",
	"connection_attempts", "connection_failures", "connections", "reconnects",
	"create_attempts", "create_failures", "creates_in_flight",
	"monitor_activations", "monitor_failures", "monitors_active",
	"dropped_values", "dropped_errors", "protocol_errors", "circuit_rotations", "sink_panics",
}

// String returns a stable metric name suitable for an external collector.
func (metric Metric) String() string {
	if metric < metricCount {
		return metricNames[metric]
	}
	return fmt.Sprintf("Metric(%d)", metric)
}

// Metrics receives counter and gauge deltas. Add may run concurrently and must
// return promptly without calling back into the client. Negative deltas occur
// only for gauges. A sink panic is recovered and counted in MetricSinkPanics.
// Callbacks run inline on client goroutines, including serialized monitor
// delivery; a slow sink delays that work. Use Client.Metrics for polling instead.
//
// MetricMonitorActivations counts EventAdd send attempts. MetricMonitorsActive
// counts installed monitor generations, including pending activation, until
// rejection, send failure, cancellation, or binding loss. Matching local
// subscribers share one generation. MetricMonitorFailures counts each rejected
// or failed activation once, excluding intentional subscription cancellation.
// MetricProtocolErrors counts incoming CA Error messages, once per wire message.
// MetricDroppedValues and MetricDroppedErrors count discarded queue entries;
// discarding queued values during subscription cancellation is not overflow.
type Metrics interface {
	Add(Metric, int64)
}

// ClientMetrics is a concurrency-safe collector. Its zero value is usable.
type ClientMetrics struct {
	values [metricCount]atomic.Int64
}

// Add records a counter or gauge delta. Unknown metric IDs are ignored.
func (m *ClientMetrics) Add(metric Metric, delta int64) {
	if metric < metricCount {
		m.values[metric].Add(delta)
	}
}

// Value returns a metric's current value, or zero for an unknown metric ID.
func (m *ClientMetrics) Value(metric Metric) int64 {
	if metric >= metricCount {
		return 0
	}
	return m.values[metric].Load()
}

// Snapshot returns independent observations of all counters and gauges.
func (m *ClientMetrics) Snapshot() map[Metric]int64 {
	values := make(map[Metric]int64, metricCount)
	for metric := Metric(0); metric < metricCount; metric++ {
		values[metric] = m.Value(metric)
	}
	return values
}

// Metrics returns this client's counters and gauges. The map is a copy; the
// individual observations are atomic, but the snapshot is not a transaction.
func (c *Client) Metrics() map[Metric]int64 { return c.metrics.Snapshot() }

func (c *Client) recordMetric(metric Metric, delta int64) {
	if c == nil || delta == 0 {
		return
	}
	c.metrics.Add(metric, delta)
	if c.opts.Metrics != nil {
		defer func() {
			if recover() != nil {
				c.metrics.Add(MetricSinkPanics, 1)
			}
		}()
		c.opts.Metrics.Add(metric, delta)
	}
}
