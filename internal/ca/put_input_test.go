package ca

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"strings"
	"testing"
	"time"

	"github.com/wakonig/cago/internal/wire"
)

func TestPutGoInputsOnWire(t *testing.T) {
	prepared, err := NewValue(int16(42))
	if err != nil {
		t.Fatal(err)
	}
	for _, notify := range []bool{false, true} {
		for _, tt := range []struct {
			name    string
			input   any
			typ     DataType
			count   uint32
			payload []byte
		}{
			{"scalar", 1.25, Double, 1, binary.BigEndian.AppendUint64(nil, math.Float64bits(1.25))},
			{"string", "ready", String, 1, []byte("ready\x00")},
			{"slice", []int32{1, -2}, Long, 2, []byte{0, 0, 0, 1, 255, 255, 255, 254}},
			{"array", [2]byte{3, 4}, Char, 2, []byte{3, 4}},
			{"value", prepared, Short, 1, []byte{0, 42}},
		} {
			name := "Put/" + tt.name
			if notify {
				name = "PutNotify/" + tt.name
			}
			t.Run(name, func(t *testing.T) {
				ch, requests := scriptedProtocolChannel(t, 2, func(f wire.Frame) wire.Frame {
					if f.Command == wire.WriteNotify {
						return wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2}
					}
					return wire.Frame{Command: wire.Echo}
				})
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				put, command := ch.Put, wire.Write
				if notify {
					put, command = ch.PutNotify, wire.WriteNotify
				}
				if err := put(ctx, tt.input); err != nil {
					t.Fatal(err)
				}
				select {
				case f := <-requests:
					if f.Command != command || f.Type != uint16(tt.typ) || f.Count != tt.count ||
						!bytes.HasPrefix(f.Payload, tt.payload) || !bytes.Equal(f.Payload[len(tt.payload):], make([]byte, len(f.Payload)-len(tt.payload))) {
						t.Fatalf("unexpected write: %+v; want type=%v count=%d payload=%x", f, tt.typ, tt.count, tt.payload)
					}
				case <-ctx.Done():
					t.Fatal("write did not reach peer")
				}
			})
		}
	}
}

func TestPutRejectsInvalidGoInputsBeforeAttachment(t *testing.T) {
	// No client or binding: invalid inputs must fail before waiting for an IOC.
	var ch Channel
	for _, input := range []any{nil, true, int64(math.MaxInt32) + 1, strings.Repeat("x", 40), "a\x00b"} {
		for _, put := range []func(context.Context, any) error{ch.Put, ch.PutNotify} {
			if err := put(context.Background(), input); err == nil {
				t.Fatalf("accepted invalid input: %v", input)
			}
		}
	}
}

func TestGroupSnapshotsGoWriteInputs(t *testing.T) {
	ch, requests := scriptedProtocolChannel(t, 2, func(f wire.Frame) wire.Frame {
		return wire.Frame{Command: wire.WriteNotify, P1: 1, P2: f.P2}
	})
	var g Group
	if err := g.Put(ch, true); err == nil {
		t.Fatal("invalid grouped input accepted")
	}
	input := []int32{1, 2}
	if err := g.Put(ch, input); err != nil {
		t.Fatal(err)
	}
	input[0] = 99
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := g.Run(ctx, 1); err != nil {
		t.Fatal(err)
	}
	f := <-requests
	if f.Command != wire.WriteNotify || f.Type != uint16(Long) || f.Count != 2 ||
		!bytes.Equal(f.Payload, []byte{0, 0, 0, 1, 0, 0, 0, 2}) {
		t.Fatalf("queued write did not preserve original input: %+v", f)
	}
	select {
	case f := <-requests:
		t.Fatalf("invalid grouped input queued a request: %+v", f)
	default:
	}
}
