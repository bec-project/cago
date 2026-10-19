package cago

import (
	"bytes"
	"context"
	"encoding/binary"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

func TestTypedSliceEncodingMatchesGeneralPath(t *testing.T) {
	for _, input := range []any{
		[]float64{0, math.Copysign(0, -1), math.Inf(1), math.Float64frombits(0x7ff8000000001234), math.Float64frombits(0x7ff0000000001234), -1.25},
		[]float32{0, float32(math.Copysign(0, -1)), float32(math.Inf(-1)), math.Float32frombits(0x7fc01234), math.Float32frombits(0x7f801234), -1.25},
		[]int32{math.MinInt32, 0, math.MaxInt32}, []int16{math.MinInt16, 0, math.MaxInt16},
		[]uint16{0, 1, math.MaxUint16}, []byte{0, 1, 255},
		[]float64(nil), []float32{}, []int32{}, []int16(nil), []uint16(nil), []byte{},
	} {
		t.Run(reflect.TypeOf(input).String()+strconv.Itoa(reflect.ValueOf(input).Len()), func(t *testing.T) {
			slice := reflect.ValueOf(input)
			array := reflect.New(reflect.ArrayOf(slice.Len(), slice.Type().Elem())).Elem()
			reflect.Copy(array, slice)
			fast, err := NewValue(input)
			if err != nil {
				t.Fatal(err)
			}
			general, err := NewValue(array.Interface())
			if err != nil {
				t.Fatal(err)
			}
			if fast.Type() != general.Type() || fast.Count() != general.Count() || !bytes.Equal(fast.Bytes(), general.Bytes()) {
				t.Fatal("typed and general encoders disagree")
			}
			for i := 0; i < slice.Len(); i++ {
				slice.Index(i).SetZero()
			}
			if !bytes.Equal(fast.Bytes(), general.Bytes()) {
				t.Fatal("encoded value aliases input")
			}
			out := reflect.ValueOf(fast.Slice())
			for i := 0; i < out.Len(); i++ {
				out.Index(i).SetZero()
			}
			if !bytes.Equal(fast.Bytes(), general.Bytes()) {
				t.Fatal("typed accessor aliases value")
			}
		})
	}
}

func TestFloat32ScalarAndDefinedTypesPreserveBits(t *testing.T) {
	type sample float32
	x := math.Float32frombits(0x7f801234)
	for _, input := range []any{x, sample(x), []sample{sample(x)}, [1]sample{sample(x)}} {
		v, err := NewValue(input)
		if err != nil || binary.BigEndian.Uint32(v.Bytes()) != 0x7f801234 {
			t.Fatalf("%T: bits=%x err=%v", input, v.Bytes(), err)
		}
	}
}

func TestDecodeValueKeepsCallerOwnership(t *testing.T) {
	for typ := DataType(0); typ <= ClassName; typ++ {
		payload := make([]byte, valueOffset(typ)+2*typ.Size())
		v, err := DecodeValue(typ, 2, payload)
		if err != nil {
			t.Fatal(typ, err)
		}
		original := v.Bytes()
		for i := range payload {
			payload[i] = 255
		}
		if !bytes.Equal(original, v.Bytes()) {
			t.Fatalf("DBR %d aliases caller payload", typ)
		}
	}
}

func TestValuesSurviveLaterNetworkFrames(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("value-lifetime")
	first, err := ch.Get(ctx)
	if err != nil {
		t.Fatal(err)
	}
	sub, err := ch.Subscribe(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.Cancel()
	initial := event(t, sub).Value
	for _, n := range []float64{2.5, 3.5, 4.5} {
		next, _ := NewValue(n)
		if err := ch.PutNotify(ctx, next); err != nil {
			t.Fatal(err)
		}
		if ev := event(t, sub); ev.Value.Float64() != n {
			t.Fatal(ev.Value.Float64())
		}
		if v, err := ch.Get(ctx); err != nil || v.Float64() != n {
			t.Fatal(v.Float64(), err)
		}
	}
	if first.Float64() != 1.25 || initial.Float64() != 1.25 {
		t.Fatal("later frames mutated retained values")
	}
}

func TestSignedPrecision(t *testing.T) {
	for _, typ := range []DataType{23, 27, 30, 34} {
		payload := make([]byte, valueOffset(typ)+typ.Size())
		binary.BigEndian.PutUint16(payload[4:6], 0xffff)
		v, err := DecodeValue(typ, 1, payload)
		if err != nil || v.Metadata().Precision != -1 {
			t.Fatalf("DBR %d precision=%d err=%v", typ, v.Metadata().Precision, err)
		}
	}
}

func TestValueRoundTrip(t *testing.T) {
	for _, input := range []any{"hello", int16(-32768), uint16(65535), byte(255), int32(-2147483648), float32(1.25), math.Pi, []string{"a", "b"}, []byte{0, 255}, []int32{1, -2}, []float64{math.NaN(), math.Inf(1)}, []float64{}} {
		v, e := NewValue(input)
		if e != nil {
			t.Fatal(e)
		}
		got, e := DecodeValue(v.Type(), v.Count(), v.Bytes())
		if e != nil {
			t.Fatal(e)
		}
		if !reflect.DeepEqual(v.Bytes(), got.Bytes()) {
			t.Fatal("value mismatch")
		}
		if v.Count() > 0 {
			x, e := v.Index(0)
			if e != nil || x.Count() != 1 {
				t.Fatal(e)
			}
		}
		bytes := got.Bytes()
		if len(bytes) > 0 {
			bytes[0] ^= 255
			if reflect.DeepEqual(bytes, got.Bytes()) {
				t.Fatal("mutable value")
			}
		}
	}
}
func TestBadValues(t *testing.T) {
	for _, v := range []any{nil, true, int64(1) << 40, "too long too long too long too long too long", "a\x00b"} {
		if _, e := NewValue(v); e == nil {
			t.Fatalf("accepted %v", v)
		}
	}
	if _, e := DecodeValue(Double, ^uint32(0), []byte{1}); e == nil {
		t.Fatal("invalid count")
	}
	if _, e := DecodeValue(Native, 1, nil); e == nil {
		t.Fatal("invalid type")
	}
}

// Offset constants are the wire layouts specified by EPICS db_access.h, not Go ABI sizes.
func TestDBRLayouts(t *testing.T) {
	offsets := []int{0, 0, 0, 0, 0, 0, 0, 4, 4, 4, 4, 5, 4, 8, 12, 14, 12, 14, 15, 12, 16, 4, 24, 40, 422, 19, 36, 64, 4, 28, 48, 422, 21, 44, 80, 0, 0, 8, 0}
	for i, offset := range offsets {
		typ := DataType(i)
		p := make([]byte, offset+typ.Size()*2)
		if typ.Base() == Enum && typ >= 21 && typ <= 34 {
			binary.BigEndian.PutUint16(p[4:6], 2)
			copy(p[6:32], "OFF")
			copy(p[32:58], "ON")
		}
		v, e := DecodeValue(typ, 2, p)
		if e != nil || len(v.Bytes()) != 2*typ.Size() {
			t.Fatalf("DBR %d offset %d: %v", i, offset, e)
		}
		if _, e := DecodeValue(typ, 2, p[:len(p)-1]); e == nil {
			t.Fatalf("DBR %d accepted truncation", i)
		}
	}
}
func TestMetadata(t *testing.T) {
	p := make([]byte, 24)
	binary.BigEndian.PutUint16(p[:2], 3)
	binary.BigEndian.PutUint16(p[2:4], 2)
	binary.BigEndian.PutUint32(p[4:8], 100)
	binary.BigEndian.PutUint32(p[8:12], 123456789)
	binary.BigEndian.PutUint64(p[16:24], math.Float64bits(12.5))
	v, e := DecodeValue(20, 1, p)
	if e != nil {
		t.Fatal(e)
	}
	m := v.Metadata()
	if m.Status != 3 || m.Severity != 2 || !m.Timestamp.Equal(time.Unix(631152100, 123456789)) || v.Float64() != 12.5 {
		t.Fatal(m)
	}
	binary.BigEndian.PutUint32(p[8:12], 1000000000)
	if v, e := DecodeValue(20, 1, p); e != nil || v.Metadata().Nanoseconds != 1000000000 {
		t.Fatal("raw nanoseconds were rejected or lost", e)
	}
}
func FuzzDecodeValue(f *testing.F) {
	f.Add(uint16(6), uint32(1), make([]byte, 8))
	f.Add(uint16(24), uint32(0), make([]byte, 422))
	f.Fuzz(func(t *testing.T, typ uint16, n uint32, p []byte) {
		if len(p) > 65536 {
			return
		}
		_, _ = DecodeValue(DataType(typ), n, p)
	})
}

func TestDecodeCompactScalarString(t *testing.T) {
	for _, length := range []int{0, 6, 7, 8, 14, 15, 16, 22, 23, 24, 30, 31} {
		t.Run(strconv.Itoa(length), func(t *testing.T) {
			want := strings.Repeat("x", length)
			payload := make([]byte, (length+1+7)&^7)
			copy(payload, want)
			v, err := DecodeValue(String, 1, payload)
			if err != nil {
				t.Fatal(err)
			}
			if v.Type() != String || v.Count() != 1 || v.Text() != want || !reflect.DeepEqual(v.Slice(), []string{want}) || len(v.Bytes()) != 40 {
				t.Fatalf("invalid compact string: type=%v count=%d value=%#v bytes=%d", v.Type(), v.Count(), v.Slice(), len(v.Bytes()))
			}
			item, err := v.Index(0)
			if err != nil || item.Text() != want {
				t.Fatalf("Index(0): %q %v", item.Text(), err)
			}
			for i := range payload {
				payload[i] = 'z'
			}
			if v.Text() != want {
				t.Fatal("compact string aliases caller payload")
			}
		})
	}
}

func TestCompactStringDoesNotHideTruncation(t *testing.T) {
	for _, payload := range [][]byte{nil, bytes.Repeat([]byte{'x'}, 8), bytes.Repeat([]byte{'x'}, 32)} {
		if _, err := DecodeValue(String, 1, payload); err == nil {
			t.Fatalf("accepted compact STRING without NUL: %x", payload)
		}
	}
	// Only a plain scalar STRING may be compact. Arrays and metadata retain
	// fixed slots, even if their truncated value contains a NUL.
	for _, typ := range []DataType{String, 7, 14, 21, 28, STSACKString, ClassName} {
		count := uint32(1)
		if typ == String {
			count = 2
		}
		payload := make([]byte, valueOffset(typ)+int(count)*typ.Size()-1)
		if _, err := DecodeValue(typ, count, payload); err == nil {
			t.Fatalf("accepted truncated DBR %d count %d", typ, count)
		}
	}
}
