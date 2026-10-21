package ca

import (
	"context"
	"errors"

	"github.com/bec-project/cago/internal/wire"
)

// Heartbeat Echos use zero. Each connection has only one rotation, so a tagged
// Echo cannot be mistaken for an earlier heartbeat response.
const rotationEchoTag uint32 = 1

// Only admission can return this error: no request bytes have been sent, so a
// caller may safely wait for the replacement binding and try admission again.
var errCircuitRotating = errors.New("CA circuit rotating identifiers")

// beginIO reserves an operation until endIO. Requests and monitor activations
// also consume an ID, unique for the entire lifetime of this TCP connection.
func (s *circuit) beginIO(ctx context.Context, withID bool) (uint32, error) {
	s.mu.Lock()
	if err := ctx.Err(); err != nil {
		s.mu.Unlock()
		return 0, err
	}
	if s.retiring {
		rotating := s.rotating
		s.mu.Unlock()
		if rotating {
			return 0, errCircuitRotating
		}
		return 0, ErrDisconnected
	}
	if withID && s.ids.Load() >= uint64(^uint32(0)) {
		s.retiring, s.rotating = true, true
		s.mu.Unlock()
		s.client.recordMetric(MetricCircuitRotations, 1)
		if !s.client.start(s.rotate) {
			s.fail(ErrClosed)
		}
		return 0, errCircuitRotating
	}
	var id uint32
	if withID {
		id = uint32(s.ids.Add(1))
	}
	s.active++
	s.mu.Unlock()
	return id, nil
}

func (s *circuit) endIO() {
	s.mu.Lock()
	s.active--
	s.mu.Unlock()
	select {
	case s.activity <- struct{}{}:
	default:
	}
}

func (s *circuit) rotate() {
	ctx, cancel := context.WithTimeout(s.client.ctx, s.client.opts.RotationDrainTimeout)
	defer cancel()
	for {
		s.mu.Lock()
		idle := s.active == 0
		s.mu.Unlock()
		if idle {
			break
		}
		select {
		case <-s.activity:
		case <-ctx.Done():
			s.fail(errCircuitRotating)
			return
		case <-s.done:
			return
		}
	}
	// Socket-write completion alone does not ensure the IOC has consumed a
	// plain Put. Its Echo response fences earlier requests before a new circuit
	// may issue reads. This confirms protocol handling, not record completion.
	ack := make(chan struct{})
	s.mu.Lock()
	s.rotationEcho = ack
	s.mu.Unlock()
	if err := s.send(ctx, wire.Frame{Command: wire.Echo, P2: rotationEchoTag}); err == nil {
		select {
		case <-ack:
		case <-ctx.Done():
		case <-s.done:
		}
	}
	// Closing the old socket confines all delayed replies to its old routing
	// tables. Channel and monitor loops restore themselves on a fresh circuit.
	s.fail(errCircuitRotating)
}
