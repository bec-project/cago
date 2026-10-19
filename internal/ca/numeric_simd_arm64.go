//go:build goexperiment.simd && arm64 && !cago_scalar

package ca

import "simd/archsimd"

// CA floats use network byte order. Shuffle bytes within each element, then
// reinterpret the bits; arithmetic conversions would alter NaNs and signed zero.
// Work in 64-byte blocks with unaligned-safe loads and scalar tails. Small
// values keep the scalar path to avoid vector setup overhead.
func decodeFloat64s(dst []float64, src []byte) {
	if len(dst) < 8 {
		decodeFloat64sScalar(dst, src)
		return
	}
	mask := archsimd.LoadUint8x16([]byte{7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8})
	i := 0
	for ; i <= len(dst)-8; i += 8 {
		block := (*[64]byte)(src[i*8:])
		out := (*[8]float64)(dst[i:])
		archsimd.LoadUint8x16(block[0:16]).LookupOrZero(mask).ReshapeToUint64s().BitsToFloat64().Store(out[0:2])
		archsimd.LoadUint8x16(block[16:32]).LookupOrZero(mask).ReshapeToUint64s().BitsToFloat64().Store(out[2:4])
		archsimd.LoadUint8x16(block[32:48]).LookupOrZero(mask).ReshapeToUint64s().BitsToFloat64().Store(out[4:6])
		archsimd.LoadUint8x16(block[48:64]).LookupOrZero(mask).ReshapeToUint64s().BitsToFloat64().Store(out[6:8])
	}
	for ; i <= len(dst)-2; i += 2 {
		archsimd.LoadUint8x16(src[i*8:]).LookupOrZero(mask).ReshapeToUint64s().BitsToFloat64().Store(dst[i:])
	}
	decodeFloat64sScalar(dst[i:], src[i*8:])
}

func decodeFloat32s(dst []float32, src []byte) {
	if len(dst) < 16 {
		decodeFloat32sScalar(dst, src)
		return
	}
	mask := archsimd.LoadUint8x16([]byte{3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12})
	i := 0
	for ; i <= len(dst)-16; i += 16 {
		block := (*[64]byte)(src[i*4:])
		out := (*[16]float32)(dst[i:])
		archsimd.LoadUint8x16(block[0:16]).LookupOrZero(mask).ReshapeToUint32s().BitsToFloat32().Store(out[0:4])
		archsimd.LoadUint8x16(block[16:32]).LookupOrZero(mask).ReshapeToUint32s().BitsToFloat32().Store(out[4:8])
		archsimd.LoadUint8x16(block[32:48]).LookupOrZero(mask).ReshapeToUint32s().BitsToFloat32().Store(out[8:12])
		archsimd.LoadUint8x16(block[48:64]).LookupOrZero(mask).ReshapeToUint32s().BitsToFloat32().Store(out[12:16])
	}
	for ; i <= len(dst)-4; i += 4 {
		archsimd.LoadUint8x16(src[i*4:]).LookupOrZero(mask).ReshapeToUint32s().BitsToFloat32().Store(dst[i:])
	}
	decodeFloat32sScalar(dst[i:], src[i*4:])
}

func encodeFloat64s(dst []byte, src []float64) {
	if len(src) < 8 {
		encodeFloat64sScalar(dst, src)
		return
	}
	mask := archsimd.LoadUint8x16([]byte{7, 6, 5, 4, 3, 2, 1, 0, 15, 14, 13, 12, 11, 10, 9, 8})
	i := 0
	for ; i <= len(src)-8; i += 8 {
		in := (*[8]float64)(src[i:])
		block := (*[64]byte)(dst[i*8:])
		archsimd.LoadFloat64x2(in[0:2]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[0:16])
		archsimd.LoadFloat64x2(in[2:4]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[16:32])
		archsimd.LoadFloat64x2(in[4:6]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[32:48])
		archsimd.LoadFloat64x2(in[6:8]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[48:64])
	}
	for ; i <= len(src)-2; i += 2 {
		archsimd.LoadFloat64x2(src[i:]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(dst[i*8:])
	}
	encodeFloat64sScalar(dst[i*8:], src[i:])
}

func encodeFloat32s(dst []byte, src []float32) {
	if len(src) < 16 {
		encodeFloat32sScalar(dst, src)
		return
	}
	mask := archsimd.LoadUint8x16([]byte{3, 2, 1, 0, 7, 6, 5, 4, 11, 10, 9, 8, 15, 14, 13, 12})
	i := 0
	for ; i <= len(src)-16; i += 16 {
		in := (*[16]float32)(src[i:])
		block := (*[64]byte)(dst[i*4:])
		archsimd.LoadFloat32x4(in[0:4]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[0:16])
		archsimd.LoadFloat32x4(in[4:8]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[16:32])
		archsimd.LoadFloat32x4(in[8:12]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[32:48])
		archsimd.LoadFloat32x4(in[12:16]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(block[48:64])
	}
	for ; i <= len(src)-4; i += 4 {
		archsimd.LoadFloat32x4(src[i:]).ToBits().ReshapeToUint8s().LookupOrZero(mask).Store(dst[i*4:])
	}
	encodeFloat32sScalar(dst[i*4:], src[i:])
}
