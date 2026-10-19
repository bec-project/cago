package cago

import (
	"bytes"
	"encoding/binary"
	"math"
	"testing"
)

// Exercise every vector/block boundary, all byte alignments and exact-length
// slices. Floating-point comparisons use bits so NaNs and signed zero count.
func TestNumericConversionsBitExact(t *testing.T) {
	counts := []int{0, 1, 2, 3, 4, 7, 8, 9, 15, 16, 17, 31, 32, 33, 63, 64, 65, 127, 128, 129, 257, 1024, 70001}
	bits64 := []uint64{0, 1, 0x8000000000000000, 0x7ff0000000000000, 0xfff0000000000000, 0x7ff8000000001234, 0x7ff0000000001234, 0xfff0000000004321, 0x3ff4000000000000}
	bits32 := []uint32{0, 1, 0x80000000, 0x7f800000, 0xff800000, 0x7fc01234, 0x7f801234, 0xff801234, 0x3fa00000}
	for _, n := range counts {
		for offset := 0; offset < 16; offset++ {
			for _, size := range []int{4, 8} {
				raw := bytes.Repeat([]byte{0xa5}, offset+n*size+17)
				payload := raw[offset : offset+n*size : offset+n*size]
				encoded := bytes.Repeat([]byte{0xa5}, len(raw))
				destination := encoded[offset : offset+n*size : offset+n*size]
				if size == 8 {
					for i := 0; i < n; i++ {
						binary.BigEndian.PutUint64(payload[i*8:], bits64[i%len(bits64)])
					}
					out := make([]float64, n+2)
					out[0], out[n+1] = -123, -456
					decodeFloat64s(out[1:n+1], payload)
					for i, x := range out[1 : n+1] {
						if math.Float64bits(x) != bits64[i%len(bits64)] {
							t.Fatalf("DOUBLE count=%d offset=%d index=%d", n, offset, i)
						}
					}
					if out[0] != -123 || out[n+1] != -456 {
						t.Fatal("decode overwrote guard")
					}
					encodeFloat64s(destination, out[1:n+1])
				} else {
					for i := 0; i < n; i++ {
						binary.BigEndian.PutUint32(payload[i*4:], bits32[i%len(bits32)])
					}
					out := make([]float32, n+2)
					out[0], out[n+1] = -123, -456
					decodeFloat32s(out[1:n+1], payload)
					for i, x := range out[1 : n+1] {
						if math.Float32bits(x) != bits32[i%len(bits32)] {
							t.Fatalf("FLOAT count=%d offset=%d index=%d", n, offset, i)
						}
					}
					if out[0] != -123 || out[n+1] != -456 {
						t.Fatal("decode overwrote guard")
					}
					encodeFloat32s(destination, out[1:n+1])
				}
				if !bytes.Equal(raw, encoded) {
					t.Fatalf("encode count=%d offset=%d size=%d modified data or guards", n, offset, size)
				}
			}
		}
	}
}

func FuzzNumericRoundTrip(f *testing.F) {
	f.Add([]byte{0x7f, 0xf0, 0, 0, 0, 0, 0, 1, 0x80, 0, 0, 0, 0, 0, 0, 0})
	f.Add(make([]byte, 129))
	f.Fuzz(func(t *testing.T, data []byte) {
		if len(data) > 65536 {
			return
		}
		for _, typ := range []DataType{Float, Double} {
			size := typ.Size()
			n := len(data) / size
			raw := data[:n*size]
			v, err := DecodeValue(typ, uint32(n), raw)
			if err != nil {
				t.Fatal(err)
			}
			out, err := NewValue(v.Slice())
			if err != nil {
				t.Fatal(err)
			}
			if !bytes.Equal(raw, out.Bytes()) {
				t.Fatalf("DBR %d did not preserve bits", typ)
			}
		}
	})
}
