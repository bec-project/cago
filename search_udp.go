package cago

import (
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

// A small bounded delay combines concurrently starting channels without
// coupling their search backoff or connection lifetimes.
const searchBatchDelay = 2 * time.Millisecond

type udpSearch struct {
	client  *Client
	mu      sync.Mutex
	pending map[uint32]*Channel
	wake    chan struct{}
}

func (s *udpSearch) enqueue(ch *Channel) {
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

func (s *udpSearch) run() {
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
		if !wait(s.client.ctx, searchBatchDelay) {
			return
		}
		s.flush()
	}
}

func (s *udpSearch) flush() {
	s.mu.Lock()
	batch := s.pending
	s.pending = make(map[uint32]*Channel)
	// Clear wake while holding the same lock as queue insertion. An enqueue
	// racing the following send will either be in this batch or wake the next.
	select {
	case <-s.wake:
	default:
	}
	s.mu.Unlock()
	version, _ := wire.Encode(wire.Frame{Command: wire.Version, Count: wire.Minor})
	packet := append([]byte(nil), version...)
	for _, ch := range batch {
		if s.client.ctx.Err() != nil {
			return
		}
		if ch.ctx.Err() != nil {
			continue
		}
		query, _ := wire.Encode(wire.Frame{Command: wire.Search, Type: 5, Count: wire.Minor, P1: ch.id, P2: ch.id, Payload: append([]byte(ch.name), 0)})
		// SearchMTU's minimum fits the maximum accepted channel name. Keep
		// this guard for internal callers as well as future protocol changes.
		if len(version)+len(query) > s.client.opts.SearchMTU {
			s.client.recordMetric(MetricSearchSendFailures, 1)
			s.client.report(fmt.Errorf("CA search for %q exceeds SearchMTU", ch.name))
			continue
		}
		if len(packet)+len(query) > s.client.opts.SearchMTU {
			s.send(packet)
			packet = append(packet[:0], version...)
		}
		packet = append(packet, query...)
	}
	if len(packet) > len(version) {
		s.send(packet)
	}
}

func (s *udpSearch) send(packet []byte) {
	for _, address := range s.client.addresses {
		if s.client.ctx.Err() != nil {
			return
		}
		n, err := s.client.udp.WriteToUDP(packet, address)
		if err == nil && n != len(packet) {
			err = io.ErrShortWrite
		}
		if err != nil {
			if s.client.ctx.Err() == nil {
				s.client.recordMetric(MetricSearchSendFailures, 1)
				s.client.report(fmt.Errorf("CA discovery send to %s: %w", address, err))
			}
			continue
		}
		s.client.recordMetric(MetricSearchDatagrams, 1)
	}
}
