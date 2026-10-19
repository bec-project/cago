package ca

import (
	"context"
	"sync"

	"github.com/wakonig/cago/internal/wire"
)

// One owned worker per TCP nameserver keeps a slow endpoint from delaying
// other endpoints or a channel consuming its discovery reply. Repeated searches
// for the same channel coalesce while an endpoint is stalled.
type tcpSearch struct {
	client  *Client
	address string
	mu      sync.Mutex
	pending map[uint32]*Channel
	wake    chan struct{}
}

func (s *tcpSearch) enqueue(ch *Channel) {
	s.mu.Lock()
	if s.client.ctx.Err() != nil || ch.ctx.Err() != nil {
		s.mu.Unlock()
		return
	}
	s.pending[ch.id] = ch
	s.mu.Unlock()
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (s *tcpSearch) run() {
	defer func() {
		s.mu.Lock()
		s.pending = nil
		s.mu.Unlock()
	}()
	for {
		select {
		case <-s.client.ctx.Done():
			return
		case <-s.wake:
		}
		s.mu.Lock()
		batch := s.pending
		s.pending = make(map[uint32]*Channel)
		s.mu.Unlock()
		ctx, cancel := context.WithTimeout(s.client.ctx, s.client.opts.ContextTimeout)
		conn, err := s.client.dial(ctx, s.address, 0)
		if err == nil {
			for _, ch := range batch {
				if ch.ctx.Err() != nil {
					continue
				}
				err = conn.send(ctx, wire.Frame{Command: wire.Search, Type: 5, Count: wire.Minor, P1: ch.id, P2: ch.id, Payload: append([]byte(ch.name), 0)})
				if err != nil {
					if s.client.ctx.Err() == nil {
						s.client.recordMetric(MetricSearchSendFailures, 1)
					}
					break
				}
			}
		}
		cancel()
		if err != nil && !wait(s.client.ctx, s.client.opts.SearchInterval) {
			return
		}
	}
}
