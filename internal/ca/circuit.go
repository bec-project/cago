package ca

import (
	"bufio"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"sync/atomic"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

type response struct {
	frame wire.Frame
	err   error
}
type pendingResponse struct {
	command uint16
	result  chan response
}

// inactivityReader renews the deadline whenever the socket makes progress,
// allowing large frames to take longer than a single inactivity interval.
type inactivityReader struct {
	socket  net.Conn
	timeout time.Duration
}

func (r inactivityReader) Read(p []byte) (int, error) {
	if err := r.socket.SetReadDeadline(time.Now().Add(r.timeout)); err != nil {
		return 0, err
	}
	return r.socket.Read(p)
}

type circuit struct {
	client        *Client
	socket        net.Conn
	key, address  string
	gate          chan struct{}
	mu            sync.Mutex
	channels      map[uint32]*Channel
	bindings      map[uint32]*binding
	requests      map[uint32]pendingResponse
	subscriptions map[uint32]*sharedMonitor
	done          chan struct{}
	once          sync.Once
	peerMinor     atomic.Uint32
	idleSince     time.Time
	retiring      bool
	rotating      bool
	ids           atomic.Uint64
	active        int
	activity      chan struct{}
	rotationEcho  chan struct{}
}

func newCircuit(c *Client, nc net.Conn, key, address string) *circuit {
	return &circuit{client: c, socket: nc, key: key, address: address, gate: make(chan struct{}, 1), channels: make(map[uint32]*Channel), bindings: make(map[uint32]*binding), requests: make(map[uint32]pendingResponse), subscriptions: make(map[uint32]*sharedMonitor), done: make(chan struct{}), activity: make(chan struct{}, 1)}
}
func (s *circuit) send(ctx context.Context, frames ...wire.Frame) error {
	return s.sendBound(ctx, nil, frames...)
}

// The binding validity check and Clear share the socket gate. A queued
// operation can never pass a Clear and transmit using a retired server ID.
func (s *circuit) sendBound(ctx context.Context, b *binding, frames ...wire.Frame) error {
	caller := ctx
	var bindingCtx context.Context
	if b != nil {
		bindingCtx = b.ctx
		// Clear is cleanup after the handle has been canceled. It must still
		// check server-side loss under the gate, without inheriting cancellation
		// from the handle whose SID it is releasing.
		if len(frames) == 1 && frames[0].Command == wire.Clear {
			bindingCtx = nil
		}
	}
	operationError := func(fallback error) error {
		if err := caller.Err(); err != nil {
			return err
		}
		if b != nil && (!bindingAlive(b) || bindingCtx != nil && bindingCtx.Err() != nil) {
			return ErrDisconnected
		}
		return fallback
	}
	var lost <-chan struct{}
	if b != nil {
		lost = b.lost
		if bindingCtx != nil {
			operation, cancel := context.WithCancel(ctx)
			stop := context.AfterFunc(bindingCtx, cancel)
			defer func() { stop(); cancel() }()
			ctx = operation
		}
	}
	select {
	case <-lost:
		return ErrDisconnected
	case <-ctx.Done():
		return operationError(ctx.Err())
	case <-s.done:
		return ErrDisconnected
	case s.gate <- struct{}{}:
	}
	defer func() { <-s.gate }()
	if b != nil {
		if !bindingAlive(b) || bindingCtx != nil && bindingCtx.Err() != nil {
			return ErrDisconnected
		}
	}
	if err := ctx.Err(); err != nil {
		return operationError(err)
	}
	deadline := time.Now().Add(s.client.opts.ContextTimeout)
	if d, ok := ctx.Deadline(); ok && d.Before(deadline) {
		deadline = d
	}
	if err := s.socket.SetWriteDeadline(deadline); err != nil {
		s.fail(err)
		return err
	}
	cancelled := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { s.socket.SetWriteDeadline(time.Now()); close(cancelled) })
	defer func() {
		if !stop() {
			<-cancelled
		}
		s.socket.SetWriteDeadline(time.Time{})
	}()
	for _, f := range frames {
		minor := s.peerMinor.Load()
		// ReadNotify admission validates the requested count before the
		// legacy zero-count substitution in request, as libca does.
		if f.Command != wire.ReadNotify {
			if err := s.validateRequest(f); err != nil {
				return err
			}
		}
		if minor > 0 && minor < 9 && (f.Count >= 65535 || len(f.Payload) >= 65529) {
			return fmt.Errorf("CA minor %d does not support extended headers", minor)
		}
		bytes, err := wire.Encode(f)
		if err != nil {
			return err
		}
		for len(bytes) > 0 {
			n, err := s.socket.Write(bytes)
			if err != nil {
				result := operationError(err)
				s.fail(err)
				return result
			}
			if n == 0 {
				s.fail(io.ErrShortWrite)
				return io.ErrShortWrite
			}
			bytes = bytes[n:]
		}
	}
	return nil
}
func (s *circuit) request(ctx context.Context, f wire.Frame, b *binding) (wire.Frame, error) {
	if f.Command == wire.ReadNotify {
		if err := s.validateReadSize(DataType(f.Type), f.Count); err != nil {
			return wire.Frame{}, err
		}
		if f.Count == 0 && s.peerMinor.Load() < 13 {
			f.Count = b.count
		}
	}
	id, err := s.beginIO(ctx, true)
	if err != nil {
		return wire.Frame{}, err
	}
	defer s.endIO()
	f.P2 = id
	result := make(chan response, 1)
	s.mu.Lock()
	s.requests[id] = pendingResponse{command: f.Command, result: result}
	s.mu.Unlock()
	defer func() { s.mu.Lock(); delete(s.requests, id); s.mu.Unlock() }()
	if err = s.sendBound(ctx, b, f); err != nil {
		return wire.Frame{}, err
	}
	select {
	case <-ctx.Done():
		return wire.Frame{}, ctx.Err()
	case <-b.lost:
		return wire.Frame{}, ErrDisconnected
	case <-s.done:
		return wire.Frame{}, ErrDisconnected
	case r := <-result:
		return r.frame, r.err
	}
}

func (s *circuit) validateReadSize(typ DataType, count uint32) error {
	if !typ.Valid() {
		return ErrBadType
	}
	maxBytes := uint64(0xfffffff0)
	if minor := s.peerMinor.Load(); minor > 0 && minor < 9 {
		maxBytes = 16384
	}
	width := uint64(typ.Size())
	if uint64(count) > (maxBytes-uint64(valueOffset(typ))-width)/width {
		return fmt.Errorf("CA requested count exceeds protocol message limit")
	}
	return nil
}
func (s *circuit) complete(id uint32, r response) {
	s.mu.Lock()
	p, ok := s.requests[id]
	s.mu.Unlock()
	if ok {
		if r.err == nil && r.frame.Command != p.command {
			r.err = fmt.Errorf("CA reply command %d does not match %d", r.frame.Command, p.command)
		}
		ch := p.result
		select {
		case ch <- r:
		default:
		}
	}
}
func (s *circuit) fail(err error) {
	s.once.Do(func() {
		s.mu.Lock()
		s.retiring = true
		s.mu.Unlock()
		s.socket.Close()
		close(s.done)
		s.mu.Lock()
		list := make([]*binding, 0, len(s.bindings))
		for _, b := range s.bindings {
			list = append(list, b)
		}
		s.mu.Unlock()
		for _, b := range list {
			b.disconnect()
		}
		s.client.mu.Lock()
		if e := s.client.circuits[s.key]; e != nil && e.conn == s {
			delete(s.client.circuits, s.key)
		}
		s.client.mu.Unlock()
	})
}
func (s *circuit) readLoop() {
	reader := bufio.NewReader(inactivityReader{s.socket, s.client.opts.InactivityTimeout})
	for {
		f, err := wire.Read(reader, s.client.opts.receiveLimit())
		if err != nil {
			var oversized *wire.PayloadLimitError
			if errors.As(err, &oversized) {
				// Libca skips an oversized body without dispatching it or
				// disturbing other channels on the same TCP connection.
				if _, drainErr := io.CopyN(io.Discard, reader, int64(oversized.Size)); drainErr == nil {
					continue
				} else {
					err = drainErr
				}
			}
			s.fail(err)
			return
		}
		switch f.Command {
		case wire.Version:
			if f.Count < 4 {
				s.fail(fmt.Errorf("CA minor %d unsupported; need at least %d", f.Count, 4))
				return
			}
			s.peerMinor.Store(f.Count)
		case wire.Search:
			host, _, _ := net.SplitHostPort(s.address)
			s.client.searchReply(f, host)
		case wire.Echo:
			if f.P2 == rotationEchoTag && f.P1 == 0 && f.Type == 0 && f.Count == 0 && len(f.Payload) == 0 {
				s.mu.Lock()
				ack := s.rotationEcho
				s.rotationEcho = nil
				s.mu.Unlock()
				if ack != nil {
					close(ack)
				}
			}
		case wire.Create, wire.CreateFail:
			s.mu.Lock()
			b := s.bindings[f.P1]
			created := b != nil && b.created
			if b != nil && f.Command == wire.Create {
				b.created = true
			}
			s.mu.Unlock()
			if b != nil {
				if f.Command == wire.CreateFail && created {
					// Libca treats command 26 as a channel disconnect once the
					// Create response has arrived, including during publication.
					b.disconnect()
				} else {
					select {
					case b.reply <- f:
					default:
					}
				}
			}
		case wire.Access:
			s.mu.Lock()
			ch, b := s.channels[f.P1], s.bindings[f.P1]
			s.mu.Unlock()
			if ch != nil && b != nil {
				ch.mu.Lock()
				b.access = AccessRights(f.P2)
				if ch.binding == b {
					ch.info.Access = b.access
					ch.publishLocked(Connected, nil)
				}
				ch.mu.Unlock()
			}
		case wire.Disconnect:
			s.mu.Lock()
			b := s.bindings[f.P1]
			s.mu.Unlock()
			if b != nil {
				b.disconnect()
			}
		case 3, wire.ReadNotify, wire.WriteNotify:
			s.complete(f.P2, response{frame: f})
		case wire.EventAdd:
			s.mu.Lock()
			sub := s.subscriptions[f.P2]
			s.mu.Unlock()
			if sub != nil {
				sub.deliver(s, f)
			}
		case wire.Error:
			s.protocolError(f)
		case 10, wire.Clear:
			// Legacy READ_SYNC echo and channel-clear acknowledgements.
		default:
			s.fail(fmt.Errorf("undecipherable CA TCP command %d", f.Command))
			return
		}
	}
}
func (s *circuit) heartbeat() {
	timer := time.NewTicker(s.client.opts.EchoInterval)
	defer timer.Stop()
	for {
		select {
		case <-s.done:
			return
		case <-s.client.ctx.Done():
			return
		case <-timer.C:
			s.mu.Lock()
			idle := len(s.bindings) == 0 && len(s.requests) == 0 && len(s.subscriptions) == 0
			if !idle {
				s.idleSince = time.Time{}
			} else if s.idleSince.IsZero() {
				s.idleSince = time.Now()
			}
			expired := idle && !s.idleSince.IsZero() && time.Since(s.idleSince) >= s.client.opts.CircuitIdleTimeout
			if expired {
				// Prevent an attachment from racing this idle decision while
				// fail closes the socket outside the state lock.
				s.retiring = true
			}
			s.mu.Unlock()
			if expired {
				s.fail(ErrClosed)
				return
			}
			ctx, cancel := context.WithTimeout(s.client.ctx, s.client.opts.ContextTimeout)
			// A busy socket gate can exhaust this deadline without sending.
			// Actual socket failures retire the circuit inside send; the read
			// deadline independently detects an unresponsive server.
			_ = s.send(ctx, wire.Frame{Command: wire.Echo})
			cancel()
		}
	}
}
func (s *circuit) protocolError(f wire.Frame) {
	s.client.recordMetric(MetricProtocolErrors, 1)
	err := &ProtocolError{Status: f.P2, Message: text(f.Payload)}
	if len(f.Payload) < 16 {
		s.client.report(err)
		return
	}
	cmd := binary.BigEndian.Uint16(f.Payload[:2])
	p1 := binary.BigEndian.Uint32(f.Payload[8:12])
	p2 := binary.BigEndian.Uint32(f.Payload[12:16])
	header := 16
	if binary.BigEndian.Uint16(f.Payload[2:4]) == 65535 {
		header = 24
	}
	if len(f.Payload) < header {
		s.client.report(err)
		return
	}
	err.Command = cmd
	err.Message = text(f.Payload[header:])
	switch cmd {
	case wire.ReadNotify, wire.WriteNotify:
		s.complete(p2, response{err: err})
	case wire.EventAdd:
		s.mu.Lock()
		sub := s.subscriptions[p2]
		s.mu.Unlock()
		if sub != nil {
			if sub.reject(s, p2, err) {
				return // The direct subscription already reported this error.
			}
		}
	case wire.Create:
		s.mu.Lock()
		b := s.bindings[p1]
		s.mu.Unlock()
		if b != nil {
			select {
			case b.reply <- wire.Frame{Command: wire.CreateFail}:
			default:
			}
		}
	}
	s.client.report(err)
}

func (s *circuit) validateRequest(f wire.Frame) error {
	minor := s.peerMinor.Load()
	if f.Command == wire.ReadNotify || f.Command == wire.EventAdd {
		return s.validateReadSize(DataType(f.Type), f.Count)
	}
	if f.Command == wire.Write || f.Command == wire.WriteNotify {
		typ := DataType(f.Type)
		if !typ.Valid() {
			return ErrBadType
		}
		if f.Count != 1 {
			maxBytes := uint64(^uint32(0))
			if minor > 0 && minor < 9 {
				maxBytes = 16384 - 16
			}
			width := uint64(typ.Size())
			if uint64(f.Count) >= (maxBytes-8-uint64(valueOffset(typ))-width)/width {
				return fmt.Errorf("CA write count exceeds protocol message limit")
			}
		}
	}
	if minor > 0 && minor < 9 && len(f.Payload) > 16368 {
		return fmt.Errorf("CA minor %d write exceeds legacy message limit", minor)
	}
	return nil
}
