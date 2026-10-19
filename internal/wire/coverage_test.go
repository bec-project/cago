package wire

import (
	"bytes"
	"encoding/binary"
	"errors"
	"io"
	"strconv"
	"strings"
	"testing"
)

func TestReadRejectsTruncatedHeadersAndPayloads(t *testing.T) {
	encoded, err := Encode(Frame{Command: ReadNotify, Type: 6, Count: 70000, P1: 17, P2: 29, Payload: make([]byte, 8)})
	if err != nil {
		t.Fatal(err)
	}
	for _, size := range []int{0, 1, 15, 16, 17, 23, 24, 25, 31} {
		t.Run(strconv.Itoa(size), func(t *testing.T) {
			f, err := Read(bytes.NewReader(encoded[:size]), 1024)
			want := error(io.ErrUnexpectedEOF)
			if size == 0 || size == 16 || size == 24 {
				want = io.EOF
			}
			if !errors.Is(err, want) {
				t.Fatalf("%d bytes: got %v, want %v", size, err, want)
			}
			if size >= 16 && (f.Command != ReadNotify || f.Type != 6 || f.P1 != 17 || f.P2 != 29) {
				t.Fatalf("header fields lost on truncated frame: %+v", f)
			}
			if size >= 24 && f.Count != 70000 {
				t.Fatalf("extended element count lost on truncated payload: %d", f.Count)
			}
		})
	}
}

func TestReadRejectsInvalidLengthsBeforeReadingPayload(t *testing.T) {
	for _, tc := range []struct {
		name     string
		extended bool
		size     uint32
		limit    int
		message  string
	}{
		{"short unaligned", false, 7, 1024, "not aligned"},
		{"extended unaligned", true, 65535, 1024, "not aligned"},
		{"negative limit", false, 8, -1, "exceeds limit"},
		{"zero limit", false, 8, 0, "exceeds limit"},
		{"extended over limit", true, 65536, 1024, "exceeds limit"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			headerSize := 16
			if tc.extended {
				headerSize = 24
			}
			data := make([]byte, headerSize+8)
			if tc.extended {
				binary.BigEndian.PutUint16(data[2:4], 65535)
				binary.BigEndian.PutUint32(data[16:20], tc.size)
			} else {
				binary.BigEndian.PutUint16(data[2:4], uint16(tc.size))
			}
			r := bytes.NewReader(data)
			f, err := Read(r, tc.limit)
			if err == nil || !strings.Contains(err.Error(), tc.message) {
				t.Fatalf("invalid length accepted: frame=%+v err=%v", f, err)
			}
			if f.Payload != nil || r.Len() != 8 {
				t.Fatalf("invalid length read/allocated payload: frame=%+v unread=%d", f, r.Len())
			}
		})
	}
}

func TestLargeHeaderDoesNotAllocateMissingPayload(t *testing.T) {
	header := make([]byte, 24)
	binary.BigEndian.PutUint16(header[2:4], 65535)
	binary.BigEndian.PutUint32(header[16:20], 64<<20)
	f, err := Read(bytes.NewReader(header), 64<<20)
	if !errors.Is(err, io.EOF) {
		t.Fatalf("header-only error=%v", err)
	}
	if cap(f.Payload) > 16384 {
		t.Fatalf("24-byte header preallocated %d body bytes", cap(f.Payload))
	}
}

func TestLargePayloadTruncationAndFrameBoundary(t *testing.T) {
	frame, _ := Encode(Frame{Command: ReadNotify, Count: 70000, Payload: bytes.Repeat([]byte{42}, 65536)})
	for _, received := range []int{0, 1, 32768, 65535} {
		f, err := Read(bytes.NewReader(frame[:24+received]), 65536)
		want := error(io.ErrUnexpectedEOF)
		if received == 0 {
			want = io.EOF
		}
		if !errors.Is(err, want) || f.Command != ReadNotify || f.Count != 70000 {
			t.Fatalf("received=%d frame=%+v err=%v", received, f, err)
		}
	}
	next, _ := Encode(Frame{Command: Echo, P2: 7})
	reader := bytes.NewReader(append(frame, next...))
	if f, err := Read(reader, 65536); err != nil || len(f.Payload) != 65536 || !bytes.Equal(f.Payload, bytes.Repeat([]byte{42}, 65536)) {
		t.Fatal("large frame read failed", err)
	}
	if f, err := Read(reader, 65536); err != nil || f.Command != Echo || f.P2 != 7 {
		t.Fatal("large frame consumed next header", err)
	}
}

// A Reader may return data and an error together. The final error must not
// discard a complete frame, but must survive when the body is incomplete.
type finalReadError struct{ data []byte }

func (r *finalReadError) Read(p []byte) (int, error) {
	n := copy(p, r.data)
	r.data = r.data[n:]
	if len(r.data) == 0 {
		return n, io.ErrClosedPipe
	}
	return n, nil
}

func TestCompletePayloadWithFinalReadError(t *testing.T) {
	for _, size := range []int{16384, 16392, 65536} {
		encoded, _ := Encode(Frame{Command: ReadNotify, Payload: bytes.Repeat([]byte{42}, size)})
		f, err := Read(&finalReadError{data: encoded}, size)
		if err != nil || len(f.Payload) != size || !bytes.Equal(f.Payload, bytes.Repeat([]byte{42}, size)) {
			t.Fatalf("size=%d: complete frame failed: err=%v payload=%d", size, err, len(f.Payload))
		}
		_, err = Read(&finalReadError{data: encoded[:len(encoded)-1]}, size)
		if !errors.Is(err, io.ErrClosedPipe) {
			t.Fatalf("size=%d: incomplete frame lost error: %v", size, err)
		}
	}
}
