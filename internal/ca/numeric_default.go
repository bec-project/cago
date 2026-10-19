//go:build !goexperiment.simd || !arm64 || cago_scalar

package ca

func decodeFloat64s(dst []float64, src []byte) { decodeFloat64sScalar(dst, src) }
func decodeFloat32s(dst []float32, src []byte) { decodeFloat32sScalar(dst, src) }
func encodeFloat64s(dst []byte, src []float64) { encodeFloat64sScalar(dst, src) }
func encodeFloat32s(dst []byte, src []float32) { encodeFloat32sScalar(dst, src) }
