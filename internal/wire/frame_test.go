package wire

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestFrames(t *testing.T) {
	for _, n := range []int{0, 1, 8, 65528, 65536} {
		for _, count := range []uint32{0, 1, 65535, 70000} {
			f := Frame{Command: 15, Type: 6, Count: count, P1: 7, P2: 8, Payload: bytes.Repeat([]byte{42}, n)}
			b, e := Encode(f)
			if e != nil {
				t.Fatal(e)
			}
			got, e := Read(bytes.NewReader(b), 100000)
			if e != nil {
				t.Fatal(e)
			}
			if got.Count != count || got.Type != f.Type || got.P1 != 7 || got.P2 != 8 || !bytes.Equal(got.Payload[:n], f.Payload) {
				t.Fatal("frame mismatch")
			}
		}
	}
}
func TestRejectOversizeBeforeAllocation(t *testing.T) {
	b := make([]byte, 24)
	binary.BigEndian.PutUint16(b[2:4], 65535)
	binary.BigEndian.PutUint32(b[16:20], 0xfffffff8)
	if _, e := Read(bytes.NewReader(b), 1024); e == nil {
		t.Fatal("oversized frame accepted")
	}
}
func FuzzFrame(f *testing.F) {
	b, _ := Encode(Frame{Command: 15, Count: 1, Payload: make([]byte, 8)})
	f.Add(b)
	f.Fuzz(func(t *testing.T, b []byte) { _, _ = Read(bytes.NewReader(b), 65536) })
}
