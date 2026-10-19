package cago

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"
)

// Value owns an immutable wire payload. Accessors return copies of arrays.
type Value struct {
	typ   DataType
	count uint32
	data  []byte
	meta  Metadata
	dbr   []byte
}

func (v Value) Type() DataType { return v.typ }
func (v Value) Count() uint32  { return v.count }
func (v Value) Metadata() Metadata {
	m := v.meta
	m.EnumLabels = append([]string(nil), m.EnumLabels...)
	return m
}
func (v Value) Bytes() []byte { return append([]byte(nil), v.data...) }

// DBRBytes returns the complete DBR record in network byte order, including
// metadata, structure padding, and every fixed enum/unit string slot. It omits
// frame alignment bytes and preserves bits lost by convenience conversions.
// Compact scalar STRING records are expanded to a zero-filled 40-byte slot.
func (v Value) DBRBytes() []byte {
	if v.dbr != nil {
		return append([]byte(nil), v.dbr...)
	}
	return v.Bytes()
}

// Index returns one plain base-type element while retaining convenience metadata.
// Its DBRBytes contains only that element, consistent with the returned Type.
func (v Value) Index(i int) (Value, error) {
	if i < 0 || uint64(i) >= uint64(v.count) {
		return Value{}, ErrBadCount
	}
	n := v.typ.Size()
	return Value{typ: v.typ.Base(), count: 1, data: append([]byte(nil), v.data[i*n:(i+1)*n]...), meta: v.Metadata()}, nil
}
func (v Value) Float64() float64 {
	if v.count == 0 {
		return 0
	}
	return number(v.typ.Base(), v.data)
}
func (v Value) Int64() int64 { return int64(v.Float64()) }
func (v Value) Text() string {
	if v.count == 0 {
		return ""
	}
	if v.typ.Base() == String {
		return text(v.data[:40])
	}
	return strconv.FormatFloat(v.Float64(), 'g', -1, 64)
}
func (v Value) Slice() any {
	n := int(v.count)
	switch v.typ.Base() {
	case String:
		a := make([]string, n)
		for i := range a {
			a[i] = text(v.data[i*40 : (i+1)*40])
		}
		return a
	case Short:
		a := make([]int16, n)
		for i := range a {
			a[i] = int16(binary.BigEndian.Uint16(v.data[i*2:]))
		}
		return a
	case Enum:
		a := make([]uint16, n)
		for i := range a {
			a[i] = binary.BigEndian.Uint16(v.data[i*2:])
		}
		return a
	case Char:
		return v.Bytes()
	case Long:
		a := make([]int32, n)
		for i := range a {
			a[i] = int32(binary.BigEndian.Uint32(v.data[i*4:]))
		}
		return a
	case Float:
		a := make([]float32, n)
		decodeFloat32s(a, v.data)
		return a
	case Double:
		a := make([]float64, n)
		decodeFloat64s(a, v.data)
		return a
	}
	return nil
}
func text(b []byte) string {
	if i := strings.IndexByte(string(b), 0); i >= 0 {
		return string(b[:i])
	}
	return string(b)
}
func number(t DataType, b []byte) float64 {
	switch t {
	case Char:
		return float64(b[0])
	case Short:
		return float64(int16(binary.BigEndian.Uint16(b)))
	case Enum:
		return float64(binary.BigEndian.Uint16(b))
	case Long:
		return float64(int32(binary.BigEndian.Uint32(b)))
	case Float:
		return float64(math.Float32frombits(binary.BigEndian.Uint32(b)))
	case Double:
		return math.Float64frombits(binary.BigEndian.Uint64(b))
	case String:
		f, _ := strconv.ParseFloat(text(b[:40]), 64)
		return f
	}
	return 0
}

// NewValue accepts strings, int16, uint16 (enum), byte, int32, float32,
// float64, and slices of those types. Go int/int64 values must fit CA LONG.
func NewValue(input any) (Value, error) {
	// Common typed slices avoid reflection and type dispatch for each element.
	// Arrays and user-defined element types retain the general path below.
	switch a := input.(type) {
	case []float64:
		v, err := newArrayValue(Double, len(a))
		if err != nil {
			return Value{}, err
		}
		encodeFloat64s(v.data, a)
		return v, nil
	case []float32:
		v, err := newArrayValue(Float, len(a))
		if err != nil {
			return Value{}, err
		}
		encodeFloat32s(v.data, a)
		return v, nil
	case []int32:
		v, err := newArrayValue(Long, len(a))
		if err != nil {
			return Value{}, err
		}
		for i, x := range a {
			binary.BigEndian.PutUint32(v.data[i*4:], uint32(x))
		}
		return v, nil
	case []int16:
		v, err := newArrayValue(Short, len(a))
		if err != nil {
			return Value{}, err
		}
		for i, x := range a {
			binary.BigEndian.PutUint16(v.data[i*2:], uint16(x))
		}
		return v, nil
	case []uint16:
		v, err := newArrayValue(Enum, len(a))
		if err != nil {
			return Value{}, err
		}
		for i, x := range a {
			binary.BigEndian.PutUint16(v.data[i*2:], x)
		}
		return v, nil
	case []byte:
		v, err := newArrayValue(Char, len(a))
		if err != nil {
			return Value{}, err
		}
		copy(v.data, a)
		return v, nil
	}
	r := reflect.ValueOf(input)
	if !r.IsValid() {
		return Value{}, ErrBadType
	}
	array := r.Kind() == reflect.Slice || r.Kind() == reflect.Array
	n := 1
	t := r.Type()
	if array {
		n = r.Len()
		t = t.Elem()
	}
	var typ DataType
	switch t.Kind() {
	case reflect.String:
		typ = String
	case reflect.Int16:
		typ = Short
	case reflect.Uint16:
		typ = Enum
	case reflect.Uint8:
		typ = Char
	case reflect.Int, reflect.Int32, reflect.Int64:
		typ = Long
	case reflect.Float32:
		typ = Float
	case reflect.Float64:
		typ = Double
	default:
		return Value{}, fmt.Errorf("%w: %s", ErrBadType, t)
	}
	v, err := newArrayValue(typ, n)
	if err != nil {
		return Value{}, err
	}
	for i := 0; i < n; i++ {
		x := r
		if array {
			x = r.Index(i)
		}
		b := v.data[i*typ.Size():]
		switch typ {
		case String:
			s := x.String()
			if len(s) > 39 || strings.ContainsRune(s, 0) {
				return Value{}, fmt.Errorf("CA STRING must contain at most 39 bytes and no NUL")
			}
			copy(b[:40], s)
		case Short:
			binary.BigEndian.PutUint16(b, uint16(x.Int()))
		case Enum:
			binary.BigEndian.PutUint16(b, uint16(x.Uint()))
		case Char:
			b[0] = byte(x.Uint())
		case Long:
			j := x.Int()
			if j < math.MinInt32 || j > math.MaxInt32 {
				return Value{}, fmt.Errorf("CA LONG overflow: %d", j)
			}
			binary.BigEndian.PutUint32(b, uint32(j))
		case Float:
			// Reflect.Float widens through float64 and can quiet signaling NaNs.
			// Convert only the Go type, preserving the original float32 bits.
			f := x.Convert(reflect.TypeFor[float32]()).Interface().(float32)
			binary.BigEndian.PutUint32(b, math.Float32bits(f))
		case Double:
			binary.BigEndian.PutUint64(b, math.Float64bits(x.Float()))
		}
	}
	return v, nil
}

func newArrayValue(typ DataType, count int) (Value, error) {
	if uint64(count) > uint64(^uint32(0)) || uint64(count)*uint64(typ.Size()) > uint64(^uint(0)>>1) {
		return Value{}, ErrBadCount
	}
	return Value{typ: typ, count: uint32(count), data: make([]byte, count*typ.Size())}, nil
}

// DecodeValue validates wire bounds before allocating. Metadata offsets follow
// EPICS db_access.h; all multi-byte fields use network byte order.
func DecodeValue(typ DataType, count uint32, payload []byte) (Value, error) {
	v, err := decodeOwnedValue(typ, count, payload)
	if err != nil {
		return Value{}, err
	}
	owned := append([]byte(nil), v.dbr...)
	offset := valueOffset(v.typ)
	v.dbr = owned
	v.data = owned[offset:len(owned):len(owned)]
	return v, nil
}

// decodeOwnedValue takes exclusive ownership of payload. The network callers
// pass fresh wire.Read allocations and must never mutate them afterward.
// Public DecodeValue still copies caller-owned bytes to preserve immutability.
func decodeOwnedValue(typ DataType, count uint32, payload []byte) (Value, error) {
	if !typ.Valid() {
		return Value{}, ErrBadType
	}
	// Scalar DBR_STRING messages may omit unused bytes after the NUL,
	// leaving only the string and wire padding. Keep the fixed 40-byte
	// representation used by Value accessors without accepting a truncated
	// unterminated string, an array, or a metadata-bearing DBR type.
	if typ == String && count == 1 && len(payload) < String.Size() && bytes.IndexByte(payload, 0) >= 0 {
		slot := make([]byte, String.Size())
		copy(slot, payload)
		payload = slot
	}
	offset := valueOffset(typ)
	size := typ.Size()
	if offset < 0 || size == 0 || uint64(offset)+uint64(count)*uint64(size) > uint64(len(payload)) {
		return Value{}, fmt.Errorf("truncated DBR %d count %d payload %d", typ, count, len(payload))
	}
	m := Metadata{}
	if typ >= 7 && typ <= 34 || typ == STSACKString {
		m.Status = binary.BigEndian.Uint16(payload[:2])
		m.Severity = binary.BigEndian.Uint16(payload[2:4])
	}
	if typ >= 7 && typ <= 34 {
		m.Level = MetadataLevel(typ / 7)
	}
	if m.Level == Time {
		sec := binary.BigEndian.Uint32(payload[4:8])
		ns := binary.BigEndian.Uint32(payload[8:12])
		m.SecondsPastEpoch, m.Nanoseconds = sec, ns
		// time.Time normalizes out-of-range nanoseconds; retain both raw words.
		m.Timestamp = time.Unix(int64(sec)+631152000, int64(ns)).UTC()
	}
	if m.Level == Graphics || m.Level == Control {
		base := typ.Base()
		if base == Enum {
			m.EnumCount = int16(binary.BigEndian.Uint16(payload[4:6]))
			// The complete fixed array remains available through DBRBytes even
			// when a server supplies an out-of-range signed label count.
			for i := 0; i < min(int(m.EnumCount), 16); i++ {
				m.EnumLabels = append(m.EnumLabels, text(payload[6+i*26:6+(i+1)*26]))
			}
		}
		if base != Enum && base != String {
			start := 4
			if base == Float || base == Double {
				m.Precision = int16(binary.BigEndian.Uint16(payload[4:6]))
				start = 8
			}
			m.Units = text(payload[start : start+8])
			start += 8
			fields := []*float64{&m.DisplayHigh, &m.DisplayLow, &m.AlarmHigh, &m.WarningHigh, &m.WarningLow, &m.AlarmLow}
			if m.Level == Control {
				fields = append(fields, &m.ControlHigh, &m.ControlLow)
			}
			for i, p := range fields {
				*p = number(base, payload[start+i*size:])
			}
		}
	}
	if typ == STSACKString {
		m.ACKTransient = binary.BigEndian.Uint16(payload[4:6])
		m.ACKSeverity = binary.BigEndian.Uint16(payload[6:8])
	}
	end := offset + int(count)*size
	return Value{typ: typ, count: count, data: payload[offset:end:end], dbr: payload[:end:end], meta: m}, nil
}
func valueOffset(t DataType) int {
	if t < 7 || t == PutACKT || t == PutACKS || t == ClassName {
		return 0
	}
	switch t {
	case STSACKString:
		return 8
	case 7, 8, 9, 10, 12, 21, 28:
		return 4
	case 11:
		return 5
	case 13:
		return 8
	case 14, 16, 19:
		return 12
	case 15, 17:
		return 14
	case 18:
		return 15
	case 20:
		return 16
	case 22:
		return 24
	case 23:
		return 40
	case 24, 31:
		return 422
	case 25:
		return 19
	case 26:
		return 36
	case 27:
		return 64
	case 29:
		return 28
	case 30:
		return 48
	case 32:
		return 21
	case 33:
		return 44
	case 34:
		return 80
	}
	return -1
}
