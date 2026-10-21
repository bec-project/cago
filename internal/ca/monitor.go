package ca

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/bec-project/cago/internal/wire"
)

// Preserve requested type semantics: Native and explicit Double remain
// distinct because Native may resolve differently after an IOC restart.
// Buffer size is local and deliberately absent from the sharing key.
type monitorKey struct {
	typ      DataType
	enumType DataType
	level    MetadataLevel
	count    uint32
	mask     EventMask
}
type sharedMonitor struct {
	ch                *Channel
	key               monitorKey
	cfg               requestOptions
	ctx               context.Context
	cancel            context.CancelFunc
	done              chan struct{}
	restart           chan struct{}
	mu                sync.Mutex
	members           map[*Subscription]struct{}
	closing, rejected bool
	conn              *circuit
	binding           *binding
	id                uint32
	latest            *Event
	metricActive      bool
	metricFailed      bool
}

func bindingAlive(b *binding) bool {
	if b == nil {
		return false
	}
	select {
	case <-b.lost:
		return false
	default:
		return true
	}
}
func (m *sharedMonitor) report(err error) bool {
	m.mu.Lock()
	forward := false
	for s := range m.members {
		if s.report(err) {
			forward = true
		}
	}
	m.mu.Unlock()
	if forward {
		m.ch.client.report(fmt.Errorf("PV %s: %w", m.ch.name, err))
	}
	return forward
}
func (m *sharedMonitor) reject(conn *circuit, id uint32, err error) bool {
	m.mu.Lock()
	active := !m.closing && !m.rejected && m.conn == conn && m.id == id
	counted := false
	failed := false
	if active {
		m.rejected = true
		m.latest = nil
		failed = !m.metricFailed
		m.metricFailed = true
		counted = m.metricActive
		m.metricActive = false
		select {
		case m.restart <- struct{}{}:
		default:
		}
	}
	m.mu.Unlock()
	if active {
		if counted {
			m.ch.client.recordMetric(MetricMonitorsActive, -1)
		}
		if failed {
			m.ch.client.recordMetric(MetricMonitorFailures, 1)
		}
		return m.report(err)
	}
	return false
}
func (m *sharedMonitor) deliver(conn *circuit, f wire.Frame) {
	// Empty EventAdd messages are cancellation acknowledgements/noops in libca,
	// regardless of their status, type, count, or whether an ID is still routed.
	if len(f.Payload) == 0 {
		return
	}
	if err := statusError(f.P1, wire.EventAdd); err != nil {
		m.reject(conn, f.P2, err)
		return
	}
	m.mu.Lock()
	active := !m.closing && !m.rejected && m.conn == conn && m.id == f.P2
	m.mu.Unlock()
	if !active {
		return
	}
	value, err := decodeOwnedValue(DataType(f.Type), f.Count, f.Payload)
	if err != nil {
		m.reject(conn, f.P2, err)
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.closing || m.rejected || m.conn != conn || m.id != f.P2 || !bindingAlive(m.binding) {
		return
	}
	e := Event{PV: m.ch.name, Value: value, ReceivedAt: time.Now().UTC()}
	m.latest = &e
	for s := range m.members {
		s.enqueue(e)
	}
}
func (m *sharedMonitor) deactivate(b *binding, id uint32, typ DataType, sent bool) {
	m.mu.Lock()
	counted := m.metricActive
	m.metricActive = false
	m.conn = nil
	m.binding = nil
	m.id = 0
	m.latest = nil
	m.mu.Unlock()
	if counted {
		m.ch.client.recordMetric(MetricMonitorsActive, -1)
	}
	b.conn.mu.Lock()
	delete(b.conn.subscriptions, id)
	b.conn.mu.Unlock()
	if !sent || !bindingAlive(b) {
		return
	}
	ctx, cancel := context.WithTimeout(m.ch.client.ctx, m.ch.client.opts.ContextTimeout)
	defer cancel()
	if err := b.conn.sendBound(ctx, b, wire.Frame{Command: wire.EventCancel, Type: uint16(typ), Count: m.cfg.monitorCount(b), P1: b.sid, P2: id}); err != nil {
		// Channel teardown owns the Clear that releases this monitor. A queued
		// cancellation must neither follow it using the old SID nor retire
		// other channels' shared circuit when that binding is cancelled.
		if !bindingAlive(b) || b.ctx != nil && b.ctx.Err() != nil {
			return
		}
		// If cancellation cannot be sent, retiring the circuit releases the old
		// server monitor instead of leaving an orphan alongside its replacement.
		b.conn.fail(err)
	}
}
func (m *sharedMonitor) finish() {
	m.cancel()
	m.mu.Lock()
	counted := m.metricActive
	m.metricActive = false
	m.closing = true
	m.latest = nil
	m.conn = nil
	m.binding = nil
	members := m.members
	m.members = make(map[*Subscription]struct{})
	m.mu.Unlock()
	if counted {
		m.ch.client.recordMetric(MetricMonitorsActive, -1)
	}
	m.ch.monitorMu.Lock()
	if m.ch.monitors[m.key] == m {
		delete(m.ch.monitors, m.key)
	}
	m.ch.monitorMu.Unlock()
	for s := range members {
		s.closeLocal()
	}
}
func (m *sharedMonitor) run() {
	defer close(m.done)
	defer m.finish()
	delay := 100 * time.Millisecond
	var previous *binding
	for m.ctx.Err() == nil {
		b, err := m.ch.waitBinding(m.ctx)
		if err != nil {
			return
		}
		if b != previous {
			delay = 100 * time.Millisecond
			previous = b
		}
		typ, err := m.cfg.wireType(b.native)
		if err != nil {
			m.report(err)
			return
		}
		count := m.cfg.monitorCount(b)
		if err = b.conn.validateReadSize(typ, count); err != nil {
			m.report(err)
			return
		}
		if err = b.conn.validateRequest(wire.Frame{Command: wire.EventAdd, Type: uint16(typ), Count: count}); err != nil {
			m.report(err)
			return
		}
		id, err := b.conn.beginIO(m.ctx, true)
		if err != nil {
			if errors.Is(err, errCircuitRotating) || errors.Is(err, ErrDisconnected) {
				continue
			}
			m.report(err)
			return
		}
		m.mu.Lock()
		// A rejection from the previous binding may have raced its loss.
		// Restart requests are local to the monitor generation being installed.
		select {
		case <-m.restart:
		default:
		}
		m.conn = b.conn
		m.binding = b
		m.id = id
		m.latest = nil
		m.rejected = false
		m.metricActive = true
		m.metricFailed = false
		m.mu.Unlock()
		m.ch.client.recordMetric(MetricMonitorActivations, 1)
		m.ch.client.recordMetric(MetricMonitorsActive, 1)
		b.conn.mu.Lock()
		b.conn.subscriptions[id] = m
		b.conn.mu.Unlock()
		payload := make([]byte, 16)
		binary.BigEndian.PutUint16(payload[12:14], uint16(m.key.mask))
		ctx, cancel := context.WithTimeout(m.ctx, m.ch.client.opts.ContextTimeout)
		err = b.conn.sendBound(ctx, b, wire.Frame{Command: wire.EventAdd, Type: uint16(typ), Count: count, P1: b.sid, P2: id, Payload: payload})
		cancel()
		b.conn.endIO()
		rejected := false
		if err == nil {
			select {
			case <-m.ctx.Done():
			case <-b.lost:
				delay = 100 * time.Millisecond
			case <-m.restart:
				rejected = true
			}
		} else {
			m.mu.Lock()
			failed := !m.metricFailed && m.ctx.Err() == nil
			m.metricFailed = true
			m.mu.Unlock()
			if failed {
				m.ch.client.recordMetric(MetricMonitorFailures, 1)
			}
			m.report(err)
		}
		m.deactivate(b, id, typ, err == nil)
		if m.ctx.Err() != nil {
			return
		}
		if rejected || err != nil && bindingAlive(b) {
			timer := time.NewTimer(delay)
			select {
			case <-m.ctx.Done():
				timer.Stop()
				return
			case <-b.lost:
				delay = 100 * time.Millisecond
			case <-timer.C:
				if bindingAlive(b) {
					delay = backoff(delay, 30*time.Second)
				} else {
					delay = 100 * time.Millisecond
				}
			}
			timer.Stop()
		}
	}
}
func (cfg requestOptions) monitorCount(b *binding) uint32 {
	if cfg.count > b.count || cfg.count == 0 && b.conn.peerMinor.Load() < 13 {
		return b.count
	}
	return cfg.count
}
