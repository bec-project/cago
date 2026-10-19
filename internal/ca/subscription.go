package ca

import (
	"context"
	"sync"
)

// Subscription is one local consumer of a shared IOC monitor. Each consumer
// has its own buffer, error stream, and cancellation lifetime.
type Subscription struct {
	monitor       *sharedMonitor
	ctx           context.Context
	cancel        context.CancelFunc
	done          chan struct{}
	events        chan Event
	errors        chan error
	mu            sync.Mutex
	once          sync.Once
	closed        bool
	forwardErrors bool
}

func (ch *Channel) Subscribe(ctx context.Context, options ...RequestOption) (*Subscription, error) {
	cfg, err := requestConfig(options)
	if err != nil {
		return nil, err
	}
	return ch.subscribe(ctx, false, cfg)
}

// Subscribe returns PV updates. Matching requests share one IOC monitor.
// Cancelling ctx closes this consumer's stream; the last consumer also stops
// the IOC monitor. Asynchronous errors are reported through Client.Errors.
func (c *Client) Subscribe(ctx context.Context, pv string, options ...RequestOption) (<-chan Event, error) {
	cfg, err := requestConfig(options)
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	sub, err := c.Channel(pv).subscribe(ctx, true, cfg)
	if err != nil {
		return nil, err
	}
	return sub.Events(), nil
}

func (ch *Channel) subscribe(ctx context.Context, forwardErrors bool, cfg requestOptions) (*Subscription, error) {
	key := monitorKey{typ: cfg.typ, enumType: cfg.enumType, level: cfg.level, count: cfg.count, mask: cfg.mask}
	if cfg.level >= Graphics {
		key.mask |= PropertyEvent
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		b, err := ch.waitBinding(ctx)
		if err != nil {
			return nil, err
		}
		typ, err := cfg.wireType(b.native)
		if err != nil {
			return nil, err
		}
		count := cfg.monitorCount(b)
		if err = b.conn.validateReadSize(typ, count); err != nil {
			return nil, err
		}

		ch.monitorMu.Lock()
		if ch.monitors == nil {
			ch.monitors = make(map[monitorKey]*sharedMonitor)
		}
		m := ch.monitors[key]
		fresh := m == nil
		if fresh {
			ch.mu.Lock()
			if ch.ctx.Err() != nil {
				ch.mu.Unlock()
				ch.monitorMu.Unlock()
				return nil, ErrClosed
			}
			ch.subWG.Add(1)
			ch.mu.Unlock()
			remoteCtx, cancel := context.WithCancel(ch.ctx)
			m = &sharedMonitor{ch: ch, key: key, cfg: cfg, ctx: remoteCtx, cancel: cancel, done: make(chan struct{}), restart: make(chan struct{}, 1), members: make(map[*Subscription]struct{})}
			ch.monitors[key] = m
		}
		m.mu.Lock()
		if m.closing || m.ctx.Err() != nil {
			if fresh {
				m.closing = true
				m.mu.Unlock()
				ch.monitorMu.Unlock()
				ch.subWG.Done()
				m.finish()
				close(m.done)
				return nil, ErrClosed
			}
			m.mu.Unlock()
			ch.monitorMu.Unlock()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-m.done:
				continue
			}
		}
		subCtx, cancel := context.WithCancel(ctx)
		s := &Subscription{monitor: m, ctx: subCtx, cancel: cancel, done: make(chan struct{}), events: make(chan Event, cfg.buffer), errors: make(chan error, 16), forwardErrors: forwardErrors}
		m.members[s] = struct{}{}
		// Joining and replay are serialized with live fan-out, so the cached
		// snapshot cannot be enqueued after a newer live update.
		if m.latest != nil && bindingAlive(m.binding) && !m.rejected {
			s.enqueue(*m.latest)
		}
		m.mu.Unlock()
		started := true
		if fresh {
			started = ch.client.start(func() { defer ch.subWG.Done(); m.run() })
		}
		ch.monitorMu.Unlock()
		context.AfterFunc(subCtx, s.detach)
		if !started {
			ch.subWG.Done()
			m.finish()
			close(m.done)
			return nil, ErrClosed
		}
		return s, nil
	}
}

func (s *Subscription) Events() <-chan Event  { return s.events }
func (s *Subscription) Errors() <-chan error  { return s.errors }
func (s *Subscription) Done() <-chan struct{} { return s.done }
func (s *Subscription) Cancel() error {
	s.cancel()
	<-s.done
	if s.monitor.ctx.Err() != nil {
		<-s.monitor.done
	}
	return nil
}
func (s *Subscription) detach() {
	m := s.monitor
	m.mu.Lock()
	delete(m.members, s)
	if len(m.members) == 0 && !m.closing {
		m.closing = true
		m.latest = nil
		m.cancel()
	}
	m.mu.Unlock()
	s.closeLocal()
}
func (s *Subscription) closeLocal() {
	s.once.Do(func() {
		s.cancel()
		s.mu.Lock()
		s.closed = true
	drain:
		for {
			select {
			case <-s.events:
			default:
				break drain
			}
		}
		close(s.events)
		close(s.errors)
		s.mu.Unlock()
		close(s.done)
	})
}
func (s *Subscription) enqueue(event Event) {
	s.mu.Lock()
	var dropped int64
	defer func() {
		s.mu.Unlock()
		s.monitor.ch.client.recordMetric(MetricDroppedValues, dropped)
	}()
	if s.closed || s.ctx.Err() != nil {
		return
	}
	select {
	case s.events <- event:
		return
	default:
	}
	select {
	case <-s.events:
		dropped++
	default:
	}
	select {
	case s.events <- event:
	default:
		dropped++
	}
}

// report queues a local error or tells the shared monitor to report it once
// through Client.Errors, regardless of how many direct consumers are attached.
func (s *Subscription) report(err error) bool {
	s.mu.Lock()
	var dropped int64
	defer func() {
		s.mu.Unlock()
		s.monitor.ch.client.recordMetric(MetricDroppedErrors, dropped)
	}()
	if s.closed || s.ctx.Err() != nil {
		return false
	}
	if s.forwardErrors {
		return true
	}
	select {
	case s.errors <- err:
	default:
		select {
		case <-s.errors:
			dropped++
		default:
		}
		select {
		case s.errors <- err:
		default:
			dropped++
		}
	}
	return false
}
