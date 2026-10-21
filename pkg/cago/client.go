package cago

import (
	"context"

	"github.com/bec-project/cago/internal/ca"
)

// Client owns application channels and subscriptions. Close releases its resources.
type Client = ca.Client

// Channel is a shared durable handle to one PV and priority.
type Channel = ca.Channel

// Subscription is one consumer of a shared monitor, with its own lifetime and queues.
type Subscription = ca.Subscription

// Stats is a snapshot of client activity and resource counts.
type Stats = ca.Stats

// Group batches independent operations with bounded concurrency. Its zero value
// is usable; it is not an atomic server transaction.
type Group = ca.Group

// ReadResult holds a grouped read's value and error after Group.Run returns.
type ReadResult = ca.ReadResult

// New creates a client. Canceling ctx or calling Close releases its resources.
func New(ctx context.Context, options Options) (*Client, error) {
	return ca.New(ctx, options)
}
