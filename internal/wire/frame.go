// Package wire implements bounded EPICS Channel Access framing.
package wire

import (
	"encoding/binary"
	"fmt"
	"io"
)

const (
	Version          uint16 = 0
	EventAdd         uint16 = 1
	EventCancel      uint16 = 2
	Write            uint16 = 4
	Search           uint16 = 6
	Error            uint16 = 11
	Clear            uint16 = 12
	Beacon           uint16 = 13
	ReadNotify       uint16 = 15
	RepeaterConfirm  uint16 = 17
	Create           uint16 = 18
	WriteNotify      uint16 = 19
	ClientName       uint16 = 20
	HostName         uint16 = 21
	Access           uint16 = 22
	Echo             uint16 = 23
	RepeaterRegister uint16 = 24
	CreateFail       uint16 = 26
	Disconnect       uint16 = 27
	Minor            uint32 = 13
)

type Frame struct {
	Command uint16
	Type    uint16
	Count   uint32
	P1, P2  uint32
	Payload []byte
}

// PayloadLimitError leaves the declared payload unread. A stream receiver may
// drain Size bytes and continue with the next frame without allocating the body.
type PayloadLimitError struct {
	Size  uint32
	Limit int
}

func (e *PayloadLimitError) Error() string {
	return fmt.Sprintf("CA payload %d exceeds limit %d", e.Size, e.Limit)
}

func Read(r io.Reader, limit int) (Frame, error) {
	var h [16]byte
	if _, err := io.ReadFull(r, h[:]); err != nil {
		return Frame{}, err
	}
	f := Frame{Command: binary.BigEndian.Uint16(h[0:2]), Type: binary.BigEndian.Uint16(h[4:6]), Count: uint32(binary.BigEndian.Uint16(h[6:8])), P1: binary.BigEndian.Uint32(h[8:12]), P2: binary.BigEndian.Uint32(h[12:16])}
	n := uint32(binary.BigEndian.Uint16(h[2:4]))
	if n == 65535 {
		var ext [8]byte
		if _, err := io.ReadFull(r, ext[:]); err != nil {
			return f, err
		}
		n = binary.BigEndian.Uint32(ext[:4])
		f.Count = binary.BigEndian.Uint32(ext[4:])
	}
	if n%8 != 0 {
		return f, fmt.Errorf("CA payload is not aligned: %d", n)
	}
	if uint64(n) > uint64(limit) || limit < 0 {
		return f, &PayloadLimitError{Size: n, Limit: limit}
	}
	if n <= 16384 {
		f.Payload = make([]byte, int(n))
		_, err := io.ReadFull(r, f.Payload)
		return f, err
	}
	// Automatic array mode permits large frames. Grow with received bytes so
	// a header alone cannot force allocation of its entire advertised body.
	payload, err := io.ReadAll(io.LimitReader(r, int64(n)))
	f.Payload = payload
	if len(payload) == int(n) {
		// Match ReadFull: a complete frame succeeds even when the final
		// requested bytes arrive with an error.
		err = nil
	} else if err == nil {
		err = io.ErrUnexpectedEOF
		if len(payload) == 0 {
			err = io.EOF
		}
	}
	return f, err
}
func Encode(f Frame) ([]byte, error) {
	n := (uint64(len(f.Payload)) + 7) &^ 7
	if n > uint64(^uint32(0)) {
		return nil, fmt.Errorf("CA frame too large")
	}
	header := 16
	if n >= 65535 || f.Count >= 65535 {
		header = 24
	}
	b := make([]byte, header+int(n))
	binary.BigEndian.PutUint16(b[:2], f.Command)
	binary.BigEndian.PutUint16(b[4:6], f.Type)
	binary.BigEndian.PutUint32(b[8:12], f.P1)
	binary.BigEndian.PutUint32(b[12:16], f.P2)
	if header == 24 {
		binary.BigEndian.PutUint16(b[2:4], 65535)
		binary.BigEndian.PutUint32(b[16:20], uint32(n))
		binary.BigEndian.PutUint32(b[20:24], f.Count)
	} else {
		binary.BigEndian.PutUint16(b[2:4], uint16(n))
		binary.BigEndian.PutUint16(b[6:8], uint16(f.Count))
	}
	copy(b[header:], f.Payload)
	return b, nil
}
