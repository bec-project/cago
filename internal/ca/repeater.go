package ca

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"net"
	"strconv"
	"sync/atomic"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

// Repeater is an optional standalone CA UDP beacon forwarding service.
type Repeater struct {
	socket  *net.UDPConn
	done    chan struct{}
	clients atomic.Int64
}

func StartRepeater(ctx context.Context, address string) (*Repeater, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	packet, err := (&net.ListenConfig{}).ListenPacket(ctx, "udp4", address)
	if err != nil {
		return nil, err
	}
	sock := packet.(*net.UDPConn)
	r := &Repeater{socket: sock, done: make(chan struct{})}
	go func() {
		select {
		case <-ctx.Done():
			sock.Close()
		case <-r.done:
		}
	}()
	go func() {
		defer close(r.done)
		clients := map[string]*net.UDPAddr{}
		buf := make([]byte, 65536)
		for {
			n, from, err := sock.ReadFromUDP(buf)
			if err != nil {
				if errors.Is(err, net.ErrClosed) || !wait(ctx, 100*time.Millisecond) {
					return
				}
				continue
			}
			reader := bytes.NewReader(buf[:n])
			f := wire.Frame{Command: wire.RepeaterRegister}
			if n != 0 {
				f, err = wire.Read(reader, 65536)
			}
			if err != nil {
				continue
			}
			if f.Command == wire.RepeaterRegister {
				// Registration is local only, as in the native repeater service.
				if !localIPv4(from.IP) {
					continue
				}
				// UDP writes cannot detect normally departed clients. A successfully
				// bound local port is no longer owned by a CA application.
				for key, a := range clients {
					if a.Port == from.Port {
						delete(clients, key)
						continue
					}
					probe, e := net.ListenUDP("udp4", a)
					if e == nil {
						probe.Close()
						delete(clients, key)
					}
				}
				clients[from.String()] = from
				r.clients.Store(int64(len(clients)))
				confirmation, _ := wire.Encode(wire.Frame{Command: wire.RepeaterConfirm, P2: binary.BigEndian.Uint32(from.IP.To4())})
				_, _ = sock.WriteToUDP(confirmation, from)
				// Registration may prefix ordinary CA messages in the same packet.
				consumed := n - reader.Len()
				copy(buf, buf[consumed:n])
				n -= consumed
				if n == 0 {
					continue
				}
				f, err = wire.Read(bytes.NewReader(buf[:n]), 65536)
				if err != nil {
					continue
				}
			}
			// A zero beacon address means the sender's address. Normalize every
			// frame in a bundle before forwarding it from the repeater's address.
			reader = bytes.NewReader(buf[:n])
			valid := true
			for reader.Len() > 0 {
				offset := n - reader.Len()
				f, err = wire.Read(reader, 65536)
				if err != nil {
					valid = false
					break
				}
				if f.Command == wire.Beacon && f.P2 == 0 {
					binary.BigEndian.PutUint32(buf[offset+12:offset+16], binary.BigEndian.Uint32(from.IP.To4()))
				}
			}
			if !valid {
				continue
			}
			for key, a := range clients {
				if key == from.String() {
					continue
				}
				if _, err := sock.WriteToUDP(buf[:n], a); err != nil {
					delete(clients, key)
				}
			}
			r.clients.Store(int64(len(clients)))
		}
	}()
	return r, nil
}

// A bind to port zero succeeds only for an address owned by this host. Native
// clients may register using either loopback or a local interface address.
func localIPv4(ip net.IP) bool {
	if ip.To4() == nil {
		return false
	}
	if ip.IsLoopback() {
		return true
	}
	probe, err := net.ListenUDP("udp4", &net.UDPAddr{IP: ip})
	if err != nil {
		return false
	}
	probe.Close()
	return true
}

func (r *Repeater) Close() error { err := r.socket.Close(); <-r.done; return err }
func (c *Client) repeaterLoop() {
	var owned *Repeater
	defer func() {
		if owned != nil {
			owned.Close()
		}
	}()
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	register := func() {
		if owned == nil {
			owned, _ = StartRepeater(c.ctx, net.JoinHostPort("0.0.0.0", strconv.Itoa(c.opts.RepeaterPort)))
		}
		frame, _ := wire.Encode(wire.Frame{Command: wire.RepeaterRegister, P2: 0x7f000001})
		_, _ = c.udp.WriteToUDP(frame, &net.UDPAddr{IP: net.IPv4(127, 0, 0, 1), Port: c.opts.RepeaterPort})
	}
	register()
	for {
		select {
		case <-c.ctx.Done():
			return
		case <-ticker.C:
			register()
		}
	}
}

// Clients reports currently registered local endpoints.
func (r *Repeater) Clients() int { return int(r.clients.Load()) }
