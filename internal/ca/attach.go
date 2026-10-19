package ca

import (
	"context"
	"sync"
)

// createLimit is shared by all priorities for one IOC endpoint. users counts
// both waiting and admitted callers so idle entries can be removed safely.
type createLimit struct {
	slots chan struct{}
	users int
}

// acquireCreate reserves one attachment to an IOC, independently of priority.
// Keep the reservation until the dial and Create response have completed,
// including cleanup of a successful response received after cancellation.
func (c *Client) acquireCreate(ctx context.Context, address string) (func(), error) {
	release, _, err := c.acquireCreateOrUpdate(ctx, address, nil)
	return release, err
}

// acquireCreateOrUpdate abandons a queued endpoint when discovery supplies a
// different address (or a beacon wakeup). A nil release with no error means the
// caller should use the returned address instead of the original candidate.
func (c *Client) acquireCreateOrUpdate(ctx context.Context, address string, updates <-chan string) (func(), string, error) {
	if err := ctx.Err(); err != nil {
		return nil, address, err
	}
	if c.ctx.Err() != nil {
		return nil, address, ErrClosed
	}
	c.createMu.Lock()
	if c.createSlots == nil {
		c.createSlots = make(map[string]*createLimit)
	}
	limit := c.createSlots[address]
	if limit == nil {
		n := c.opts.MaxConcurrentCreates
		if n <= 0 { // Minimal internal clients do not pass through Options.defaults.
			n = 4
		}
		limit = &createLimit{slots: make(chan struct{}, n)}
		c.createSlots[address] = limit
	}
	limit.users++
	c.createMu.Unlock()
	dropUser := func() {
		c.createMu.Lock()
		limit.users--
		if limit.users == 0 {
			delete(c.createSlots, address)
		}
		c.createMu.Unlock()
	}
	waiting := true
	for waiting {
		select {
		case <-ctx.Done():
			dropUser()
			return nil, address, ctx.Err()
		case <-c.ctx.Done():
			dropUser()
			return nil, address, ErrClosed
		case updated := <-updates:
			if updated != address {
				dropUser()
				return nil, updated, nil
			}
		case limit.slots <- struct{}{}:
			waiting = false
		}
	}
	// A ready slot and cancellation can win the select together.
	if err := ctx.Err(); err != nil {
		<-limit.slots
		dropUser()
		return nil, address, err
	}
	if c.ctx.Err() != nil {
		<-limit.slots
		dropUser()
		return nil, address, ErrClosed
	}
	// A queued replacement and a ready slot may win the select together. Avoid
	// starting a stale Create even when the old endpoint has spare capacity.
	select {
	case updated := <-updates:
		if updated != address {
			<-limit.slots
			dropUser()
			return nil, updated, nil
		}
	default:
	}
	c.recordMetric(MetricCreatesInFlight, 1)
	var once sync.Once
	return func() {
		once.Do(func() {
			c.recordMetric(MetricCreatesInFlight, -1)
			<-limit.slots
			dropUser()
		})
	}, address, nil
}

// admitCreate bounds time spent queued independently of the later dial/Create
// deadline. Channel.run restarts discovery on admission timeout or beacon wakeup.
func (ch *Channel) admitCreate(address string) (string, func(), error) {
	ctx, cancel := context.WithTimeout(ch.ctx, ch.client.opts.ContextTimeout)
	defer cancel()
	for {
		release, updated, err := ch.client.acquireCreateOrUpdate(ctx, address, ch.results)
		if err != nil || release != nil || updated == "" {
			return updated, release, err
		}
		address = updated
	}
}
