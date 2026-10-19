// Package cago provides a pure-Go EPICS Channel Access client.
//
// Applications use New to create a Client, Client.Channel to obtain durable
// channel handles, and NewValue to construct values for writes. Channels support
// reads, writes, metadata, and subscriptions controlled by Go contexts.
//
// This package defines the supported application API. The connection runtime,
// protocol decoding, discovery, queues, and repeater are implemented internally.
// Handle and value types are explicitly selected aliases, preserving shared
// handle identity and avoiding extra event queues or conversion goroutines.
package cago
