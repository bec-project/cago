package cago

import "github.com/bec-project/cago/internal/ca"

// Metric identifies a bounded client counter or gauge.
type Metric = ca.Metric

const (
	MetricSearchRequests     = ca.MetricSearchRequests
	MetricSearchDatagrams    = ca.MetricSearchDatagrams
	MetricSearchSendFailures = ca.MetricSearchSendFailures
	MetricConnectionAttempts = ca.MetricConnectionAttempts
	MetricConnectionFailures = ca.MetricConnectionFailures
	MetricConnections        = ca.MetricConnections
	MetricReconnects         = ca.MetricReconnects
	MetricCreateAttempts     = ca.MetricCreateAttempts
	MetricCreateFailures     = ca.MetricCreateFailures
	MetricCreatesInFlight    = ca.MetricCreatesInFlight
	MetricMonitorActivations = ca.MetricMonitorActivations
	MetricMonitorFailures    = ca.MetricMonitorFailures
	MetricMonitorsActive     = ca.MetricMonitorsActive
	MetricDroppedValues      = ca.MetricDroppedValues
	MetricDroppedErrors      = ca.MetricDroppedErrors
	MetricProtocolErrors     = ca.MetricProtocolErrors
	MetricCircuitRotations   = ca.MetricCircuitRotations
	MetricSinkPanics         = ca.MetricSinkPanics
)

// Metrics receives counter and gauge deltas. Add may run concurrently and must
// return promptly without calling back into the client.
type Metrics = ca.Metrics

// ClientMetrics is a concurrency-safe collector whose zero value is usable.
type ClientMetrics = ca.ClientMetrics
