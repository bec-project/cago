package cago

import (
	"encoding/binary"
	"math"
)

func decodeFloat64sScalar(dst []float64, src []byte) {
	for i := range dst {
		dst[i] = math.Float64frombits(binary.BigEndian.Uint64(src[i*8:]))
	}
}
func decodeFloat32sScalar(dst []float32, src []byte) {
	for i := range dst {
		dst[i] = math.Float32frombits(binary.BigEndian.Uint32(src[i*4:]))
	}
}
func encodeFloat64sScalar(dst []byte, src []float64) {
	for i, x := range src {
		binary.BigEndian.PutUint64(dst[i*8:], math.Float64bits(x))
	}
}
func encodeFloat32sScalar(dst []byte, src []float32) {
	for i, x := range src {
		binary.BigEndian.PutUint32(dst[i*4:], math.Float32bits(x))
	}
}
