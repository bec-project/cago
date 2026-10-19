package cago

import (
	"encoding/binary"
	"math"
	"strconv"
	"testing"
)

func BenchmarkNumeric(b *testing.B) {
	for _, n := range []int{1, 16, 1024, 70000} {
		b.Run(strconv.Itoa(n), func(b *testing.B) {
			f64 := make([]float64, n)
			f32 := make([]float32, n)
			raw64, raw32 := make([]byte, n*8), make([]byte, n*4)
			for i := 0; i < n; i++ {
				f64[i] = float64(i%97) + .25
				f32[i] = float32(i%97) + .25
				binary.BigEndian.PutUint64(raw64[i*8:], math.Float64bits(f64[i]))
				binary.BigEndian.PutUint32(raw32[i*4:], math.Float32bits(f32[i]))
			}
			b.Run("F64Decode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n * 8))
				for b.Loop() {
					decodeFloat64s(f64, raw64)
				}
			})
			b.Run("F64Encode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n * 8))
				for b.Loop() {
					encodeFloat64s(raw64, f64)
				}
			})
			b.Run("F32Decode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n * 4))
				for b.Loop() {
					decodeFloat32s(f32, raw32)
				}
			})
			b.Run("F32Encode", func(b *testing.B) {
				b.ReportAllocs()
				b.SetBytes(int64(n * 4))
				for b.Loop() {
					encodeFloat32s(raw32, f32)
				}
			})
		})
	}
}
