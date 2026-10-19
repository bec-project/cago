package ca

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

type channelConfig struct{ priority uint16 }
type ChannelOption func(*channelConfig)

func WithPriority(priority uint16) ChannelOption {
	return func(c *channelConfig) { c.priority = priority }
}

type binding struct {
	ctx      context.Context
	cancel   context.CancelFunc
	conn     *circuit
	cid, sid uint32
	native   DataType
	count    uint32
	reply    chan wire.Frame
	lost     chan struct{}
	once     sync.Once
	access   AccessRights
	created  bool // protected by conn.mu
}

func (b *binding) disconnect() {
	b.once.Do(func() {
		if b.cancel != nil {
			b.cancel()
		}
		close(b.lost)
	})
}

type Channel struct {
	client    *Client
	name      string
	id        uint32
	priority  uint16
	ctx       context.Context
	cancel    context.CancelFunc
	mu        sync.Mutex
	monitorMu sync.Mutex
	monitors  map[monitorKey]*sharedMonitor
	subWG     sync.WaitGroup
	binding   *binding
	info      ChannelInfo
	err       error
	changed   chan struct{}
	updates   chan StateChange
	results   chan string
	done      chan struct{}
}

func (ch *Channel) Name() string                     { return ch.name }
func (ch *Channel) Info() ChannelInfo                { ch.mu.Lock(); defer ch.mu.Unlock(); return ch.info }
func (ch *Channel) StateChanges() <-chan StateChange { return ch.updates }
func (ch *Channel) Done() <-chan struct{}            { return ch.done }
func (ch *Channel) publishLocked(state State, err error) {
	ch.info.State = state
	ch.err = err
	close(ch.changed)
	ch.changed = make(chan struct{})
	change := StateChange{Info: ch.info, Err: err, At: time.Now()}
	select {
	case ch.updates <- change:
	default:
		select {
		case <-ch.updates:
		default:
		}
		select {
		case ch.updates <- change:
		default:
		}
	}
}
func (ch *Channel) waitBinding(ctx context.Context) (*binding, error) {
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		ch.mu.Lock()
		b, state, err, changed := ch.binding, ch.info.State, ch.err, ch.changed
		ch.mu.Unlock()
		if state == Closed {
			if err == nil {
				err = ErrClosed
			}
			return nil, err
		}
		if b != nil {
			select {
			case <-b.lost:
			default:
				if b.conn != nil {
					b.conn.mu.Lock()
					retiring := b.conn.retiring
					b.conn.mu.Unlock()
					if retiring {
						select {
						case <-ctx.Done():
							return nil, ctx.Err()
						case <-ch.ctx.Done():
							return nil, ErrClosed
						case <-b.lost:
						}
						continue
					}
				}
				return b, nil
			}
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-ch.ctx.Done():
			return nil, ErrClosed
		case <-changed:
		}
	}
}
func (ch *Channel) Connect(ctx context.Context) error { _, err := ch.waitBinding(ctx); return err }
func (ch *Channel) run() {
	defer close(ch.done)
	defer func() {
		ch.mu.Lock()
		ch.binding = nil
		ch.publishLocked(Closed, ErrClosed)
		close(ch.updates)
		ch.mu.Unlock()
		ch.subWG.Wait()
		ch.client.mu.Lock()
		delete(ch.client.byID, ch.id)
		key := channelKey{ch.name, ch.priority}
		if ch.client.channels[key] == ch {
			delete(ch.client.channels, key)
		}
		ch.client.mu.Unlock()
	}()
	delay := ch.client.opts.SearchInterval
	connectedBefore := false
	for ch.ctx.Err() == nil {
		ch.client.search(ch)
		var address string
		timer := time.NewTimer(delay)
		select {
		case <-ch.ctx.Done():
			timer.Stop()
			return
		case address = <-ch.results:
			timer.Stop()
		case <-timer.C:
			delay = backoff(delay, ch.client.opts.MaxSearchPeriod)
			continue
		}
		if address == "" {
			delay = ch.client.opts.SearchInterval
			continue
		}
		address, releaseCreate, err := ch.admitCreate(address)
		if err != nil {
			if ch.ctx.Err() != nil {
				return
			}
			delay = ch.client.opts.SearchInterval
			continue
		}
		if releaseCreate == nil { // A beacon invalidated the queued candidate.
			delay = ch.client.opts.SearchInterval
			continue
		}
		conn, err := ch.client.dial(ch.ctx, address, ch.priority)
		if err != nil {
			releaseCreate()
			if !wait(ch.ctx, delay) {
				return
			}
			delay = backoff(delay, ch.client.opts.MaxSearchPeriod)
			continue
		}
		bindingCtx, bindingCancel := context.WithCancel(ch.ctx)
		b := &binding{ctx: bindingCtx, cancel: bindingCancel, conn: conn, cid: ch.id, reply: make(chan wire.Frame, 1), lost: make(chan struct{})}
		conn.mu.Lock()
		if conn.retiring {
			conn.mu.Unlock()
			b.disconnect()
			releaseCreate()
			if !wait(ch.ctx, delay) {
				return
			}
			continue
		}
		conn.channels[ch.id] = ch
		conn.bindings[ch.id] = b
		conn.active++ // Include the Create handshake in an identifier-rotation drain.
		conn.mu.Unlock()
		createSent := false
		ch.client.recordMetric(MetricCreateAttempts, 1)
		err = func() error {
			ctx, cancel := context.WithTimeout(ch.ctx, ch.client.opts.ContextTimeout)
			defer cancel()
			if err := conn.send(ctx, wire.Frame{Command: wire.Create, P1: ch.id, P2: wire.Minor, Payload: append([]byte(ch.name), 0)}); err != nil {
				return err
			}
			createSent = true

			var f wire.Frame
			select {
			case <-ctx.Done():
				if ch.ctx.Err() == nil {
					return ctx.Err()
				}
				// A sent Create still owns a response even if this handle is closed.
				// Resolve it within its original deadline so a simultaneous rejection
				// stays local and a successful late creation can be cleared by SID.
				deadline, _ := ctx.Deadline()
				cleanup, stop := context.WithDeadline(ch.client.ctx, deadline)
				defer stop()
				select {
				case f = <-b.reply:
				case <-cleanup.Done():
					return cleanup.Err()
				case <-b.lost:
					return ErrDisconnected
				}
			case <-b.lost:
				return ErrDisconnected
			case f = <-b.reply:
			}
			if f.Command == wire.CreateFail {
				return fmt.Errorf("%w: %s", ErrCreateRejected, ch.name)
			}
			b.sid = f.P2
			b.native = DataType(f.Type)
			b.count = f.Count
			if b.native > Double {
				return ErrBadType
			}
			return nil

		}()
		// Create, channel loss, and cancellation/deadline signals can all be
		// ready together. A server-retired channel owns its cleanup regardless
		// of which signal the handshake selected; preserve healthy peer PVs.
		if !bindingAlive(b) {
			err = ErrDisconnected
		}
		conn.endIO()
		if err != nil {
			ch.client.recordMetric(MetricCreateFailures, 1)
		}
		if err != nil && createSent && !errors.Is(err, ErrCreateRejected) && !errors.Is(err, ErrDisconnected) {
			conn.fail(err)
		}
		if err != nil || ch.ctx.Err() == nil {
			releaseCreate()
			releaseCreate = nil
		}
		if err == nil && ch.ctx.Err() == nil {
			ch.mu.Lock()
			ch.binding = b
			ch.info.Host = address
			ch.info.NativeType = b.native
			ch.info.Count = b.count
			ch.info.Access = b.access
			if connectedBefore {
				ch.client.recordMetric(MetricReconnects, 1)
			}
			connectedBefore = true
			ch.publishLocked(Connected, nil)
			ch.mu.Unlock()
			delay = ch.client.opts.SearchInterval
			select {
			case <-ch.ctx.Done():
			case <-b.lost:
			}
		}
		if ch.ctx.Err() != nil && err == nil && bindingAlive(b) {
			ctx, cancel := context.WithTimeout(ch.client.ctx, ch.client.opts.ContextTimeout)
			if clearErr := conn.sendBound(ctx, b, wire.Frame{Command: wire.Clear, P1: b.sid, P2: b.cid}); clearErr != nil && !errors.Is(clearErr, ErrDisconnected) {
				conn.fail(clearErr)
			}
			cancel()
		}
		if releaseCreate != nil {
			releaseCreate()
		}
		b.disconnect()
		conn.mu.Lock()
		if conn.bindings[ch.id] == b {
			delete(conn.channels, ch.id)
			delete(conn.bindings, ch.id)
		}
		conn.mu.Unlock()
		ch.mu.Lock()
		ch.binding = nil
		ch.publishLocked(Searching, err)
		ch.mu.Unlock()
		// A fresh search is mandatory after channel-level disconnects, even when
		// the shared TCP circuit remains healthy.
		if !wait(ch.ctx, delay) {
			return
		}
		if err != nil {
			delay = backoff(delay, ch.client.opts.MaxSearchPeriod)
		}
	}
}

// Close permanently disposes this handle. A later Client.Channel creates a new one.
func (ch *Channel) Close() error { ch.cancel(); <-ch.done; return nil }

func (ch *Channel) Get(ctx context.Context, options ...RequestOption) (Value, error) {
	cfg, err := requestConfig(options)
	if err != nil {
		return Value{}, err
	}
	for {
		b, err := ch.waitBinding(ctx)
		if err != nil {
			return Value{}, err
		}
		typ, err := cfg.wireType(b.native)
		if err != nil {
			return Value{}, err
		}
		count := cfg.count
		if count > b.count {
			return Value{}, ErrBadCount
		}
		response, err := b.conn.request(ctx, wire.Frame{Command: wire.ReadNotify, Type: uint16(typ), Count: count, P1: b.sid}, b)
		if errors.Is(err, errCircuitRotating) {
			continue
		}
		if err != nil {
			return Value{}, err
		}
		if err = statusError(response.P1, wire.ReadNotify); err != nil {
			return Value{}, err
		}
		return decodeOwnedValue(DataType(response.Type), response.Count, response.Payload)
	}
}

// Put sends a value; PutNotify also waits for server processing acknowledgement.
func (ch *Channel) Put(ctx context.Context, value Value) error       { return ch.put(ctx, value, false) }
func (ch *Channel) PutNotify(ctx context.Context, value Value) error { return ch.put(ctx, value, true) }
func (ch *Channel) put(ctx context.Context, value Value, notify bool) error {
	for {
		b, err := ch.waitBinding(ctx)
		if err != nil {
			return err
		}
		if !value.typ.Valid() || value.typ > 6 && value.typ != PutACKT && value.typ != PutACKS {
			return ErrBadType
		}
		if value.count == 0 || value.count > b.count {
			return ErrBadCount
		}
		payload := value.data
		if value.typ == String {
			// Decoding preserves complete 40-byte slots, including malformed
			// unterminated strings. Libca validates every slot before writing.
			for start := 0; start < len(payload); start += String.Size() {
				if bytes.IndexByte(payload[start:start+String.Size()], 0) < 0 {
					return fmt.Errorf("CA STRING must contain a NUL within its 40-byte slot")
				}
			}
			if value.count == 1 {
				payload = payload[:bytes.IndexByte(payload, 0)+1]
			}
		}
		f := wire.Frame{Command: wire.Write, Type: uint16(value.typ), Count: value.count, P1: b.sid, P2: b.cid, Payload: payload}
		if !notify {
			if _, err := b.conn.beginIO(ctx, false); err != nil {
				if errors.Is(err, errCircuitRotating) {
					continue
				}
				return err
			}
			err := b.conn.sendBound(ctx, b, f)
			b.conn.endIO()
			return err
		}
		f.Command = wire.WriteNotify
		response, err := b.conn.request(ctx, f, b)
		if errors.Is(err, errCircuitRotating) {
			continue
		}
		if err != nil {
			return err
		}
		return statusError(response.P1, wire.WriteNotify)
	}
}

// Acknowledge writes the alarm acknowledgement DBR types used by alarm handlers.
func (ch *Channel) Acknowledge(ctx context.Context, transient bool, severity uint16) error {
	a, _ := NewValue(uint16(0))
	if transient {
		a, _ = NewValue(uint16(1))
	}
	a.typ = PutACKT
	if err := ch.PutNotify(ctx, a); err != nil {
		return err
	}
	a, _ = NewValue(severity)
	a.typ = PutACKS
	return ch.PutNotify(ctx, a)
}
