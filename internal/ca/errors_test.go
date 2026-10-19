package ca

import (
	"errors"
	"testing"
)

func TestStatusErrors(t *testing.T) {
	for _, tt := range []struct {
		code uint32
		want error
	}{{14, ErrBadType}, {22, ErrBadCount}, {24, ErrDisconnected}, {46, ErrNoRead}, {47, ErrNoWrite}} {
		err := statusError(tt.code<<3, 15)
		if !errors.Is(err, tt.want) {
			t.Fatalf("status %d: %v", tt.code, err)
		}
	}
	if statusError(1, 15) != nil || statusError(3, 15) == nil {
		t.Fatal("successful status rejected")
	}
}

func TestChannelIdentifierExhaustionDoesNotReuseIDs(t *testing.T) {
	c := &Client{}
	last := ^uint32(0)
	c.channelIDs.Store(uint64(last) - 1)
	if id, err := c.nextChannelID(); err != nil || id != last {
		t.Fatalf("last ID=%d err=%v", id, err)
	}
	for range 2 {
		if id, err := c.nextChannelID(); id != 0 || !errors.Is(err, ErrIDExhausted) {
			t.Fatalf("exhausted ID=%d err=%v", id, err)
		}
	}
}
