package ca

import (
	"bytes"
	"encoding/binary"
	"errors"
	"math"
	"reflect"
	"strings"
	"testing"
)

func TestValueScalarAccessors(t *testing.T) {
	for _, tt := range []struct {
		name  string
		input any
		typ   DataType
		text  string
		float float64
		int   int64
		slice any
	}{
		{"string-number", "-12.75", String, "-12.75", -12.75, -12, []string{"-12.75"}},
		{"string-text", "running", String, "running", 0, 0, []string{"running"}},
		{"short", int16(math.MinInt16), Short, "-32768", math.MinInt16, math.MinInt16, []int16{math.MinInt16}},
		{"enum", uint16(math.MaxUint16), Enum, "65535", math.MaxUint16, math.MaxUint16, []uint16{math.MaxUint16}},
		{"char", byte(255), Char, "255", 255, 255, []byte{255}},
		{"long", int32(math.MinInt32), Long, "-2.147483648e+09", math.MinInt32, math.MinInt32, []int32{math.MinInt32}},
		{"float", float32(-1.5), Float, "-1.5", -1.5, -1, []float32{-1.5}},
		{"double", 3.75, Double, "3.75", 3.75, 3, []float64{3.75}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			v, err := NewValue(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if v.Type() != tt.typ || v.Count() != 1 {
				t.Fatalf("type=%d count=%d; want type=%d count=1", v.Type(), v.Count(), tt.typ)
			}
			if v.Text() != tt.text || v.Float64() != tt.float || v.Int64() != tt.int {
				t.Fatalf("Text=%q Float64=%g Int64=%d; want %q %g %d", v.Text(), v.Float64(), v.Int64(), tt.text, tt.float, tt.int)
			}
			if got := v.Slice(); !reflect.DeepEqual(got, tt.slice) {
				t.Fatalf("Slice=%#v; want %#v", got, tt.slice)
			}
		})
	}
}

func TestValueEmptyAccessorsAndIndexBounds(t *testing.T) {
	for _, input := range []any{
		[]string{}, []int16(nil), []uint16{}, []byte(nil), []int32{}, []float32(nil), []float64{},
	} {
		v, err := NewValue(input)
		if err != nil {
			t.Fatal(err)
		}
		if v.Count() != 0 || v.Float64() != 0 || v.Int64() != 0 || v.Text() != "" || len(v.Bytes()) != 0 {
			t.Fatalf("empty %T has nonempty accessors: %#v", input, v)
		}
		if got := reflect.ValueOf(v.Slice()); got.Type() != reflect.TypeOf(input) || got.Len() != 0 {
			t.Fatalf("empty %T returned %v", input, v.Slice())
		}
		for _, index := range []int{-1, 0, 1, math.MaxInt} {
			if _, err := v.Index(index); !errors.Is(err, ErrBadCount) {
				t.Fatalf("empty %T Index(%d): %v", input, index, err)
			}
		}
	}
	var zero Value
	if zero.Count() != 0 || zero.Text() != "" || zero.Float64() != 0 || zero.Int64() != 0 {
		t.Fatalf("zero value is not empty: %#v", zero)
	}
	v, err := NewValue([]int16{10, -20})
	if err != nil {
		t.Fatal(err)
	}
	for _, index := range []int{-1, 2, math.MaxInt} {
		if _, err := v.Index(index); !errors.Is(err, ErrBadCount) {
			t.Fatalf("Index(%d): %v", index, err)
		}
	}
}

func TestValueDefinedTypesAndArrays(t *testing.T) {
	type label string
	type short int16
	type enum uint16
	type octet byte
	type integer int
	type long int32
	type wide int64
	type real float32
	type precise float64
	for _, tt := range []struct {
		input any
		want  any
	}{
		{label("OFF"), []string{"OFF"}},
		{[]label{"OFF", "ON"}, []string{"OFF", "ON"}},
		{[2]short{-7, 12}, []int16{-7, 12}},
		{[]enum{0, math.MaxUint16}, []uint16{0, math.MaxUint16}},
		{[2]octet{0, 255}, []byte{0, 255}},
		{[]integer{math.MinInt32, math.MaxInt32}, []int32{math.MinInt32, math.MaxInt32}},
		{[2]long{-12, 13}, []int32{-12, 13}},
		{[]wide{math.MinInt32, math.MaxInt32}, []int32{math.MinInt32, math.MaxInt32}},
		{[2]real{-1.5, 2.25}, []float32{-1.5, 2.25}},
		{[]precise{-1.5, 2.25}, []float64{-1.5, 2.25}},
	} {
		t.Run(reflect.TypeOf(tt.input).String(), func(t *testing.T) {
			v, err := NewValue(tt.input)
			if err != nil {
				t.Fatal(err)
			}
			if got := v.Slice(); !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("Slice=%#v; want %#v", got, tt.want)
			}
			for i := 0; i < int(v.Count()); i++ {
				item, err := v.Index(i)
				if err != nil || item.Type() != v.Type() || item.Count() != 1 {
					t.Fatalf("Index(%d): %#v, %v", i, item, err)
				}
				if got, want := reflect.ValueOf(item.Slice()).Index(0).Interface(), reflect.ValueOf(tt.want).Index(i).Interface(); !reflect.DeepEqual(got, want) {
					t.Fatalf("Index(%d)=%v; want %v", i, got, want)
				}
			}
		})
	}
}

func TestValueIndexKeepsMetadataAndOwnership(t *testing.T) {
	// DBR_CTRL_ENUM has a 422-byte metadata prefix and two uint16 values.
	payload := make([]byte, 426)
	binary.BigEndian.PutUint16(payload[0:2], 3)
	binary.BigEndian.PutUint16(payload[2:4], 2)
	binary.BigEndian.PutUint16(payload[4:6], 2)
	copy(payload[6:32], "OFF")
	copy(payload[32:58], "ON")
	binary.BigEndian.PutUint16(payload[422:424], 0)
	binary.BigEndian.PutUint16(payload[424:426], 1)
	v, err := DecodeValue(31, 2, payload)
	if err != nil {
		t.Fatal(err)
	}
	item, err := v.Index(1)
	if err != nil {
		t.Fatal(err)
	}
	if item.Type() != Enum || item.Count() != 1 || item.Int64() != 1 {
		t.Fatalf("indexed value: %#v", item)
	}
	wantMeta := Metadata{Level: Control, Status: 3, Severity: 2, EnumCount: 2, EnumLabels: []string{"OFF", "ON"}}
	if got := item.Metadata(); !reflect.DeepEqual(got, wantMeta) {
		t.Fatalf("indexed metadata=%#v; want %#v", got, wantMeta)
	}
	metadata := item.Metadata()
	metadata.EnumLabels[0] = "changed"
	returned := item.Slice().([]uint16)
	returned[0] = 99
	returnedBytes := item.Bytes()
	returnedBytes[1] = 99
	// The indexed value also owns its storage independently of the parent.
	v.data[3] = 77
	v.meta.EnumLabels[0] = "parent changed"
	if item.Int64() != 1 || !reflect.DeepEqual(item.Metadata(), wantMeta) {
		t.Fatal("indexed value aliases parent or returned storage")
	}
}

func TestValueStringWireBoundaries(t *testing.T) {
	for _, text := range []string{strings.Repeat("x", 39), strings.Repeat("é", 19) + "!"} {
		v, err := NewValue(text)
		if err != nil || v.Text() != text || len(v.Bytes()) != 40 || v.Bytes()[39] != 0 {
			t.Fatalf("39-byte string: Text=%q bytes=%x err=%v", v.Text(), v.Bytes(), err)
		}
	}
	for _, text := range []string{strings.Repeat("x", 40), strings.Repeat("é", 20), "a\x00b"} {
		if _, err := NewValue([]string{"valid", text}); err == nil {
			t.Fatalf("accepted invalid array element %q", text)
		}
	}
	// Servers may fill all 40 bytes without a trailing NUL. Preserve the text.
	payload := bytes.Repeat([]byte{'z'}, 40)
	v, err := DecodeValue(String, 1, payload)
	if err != nil || v.Text() != string(payload) || !reflect.DeepEqual(v.Slice(), []string{string(payload)}) {
		t.Fatalf("unterminated wire string: Text=%q Slice=%#v err=%v", v.Text(), v.Slice(), err)
	}
}

func TestNewValueRejectsUnsupportedTypesAndLongOverflow(t *testing.T) {
	for _, input := range []any{uint(1), uint32(1), uint64(1), int8(1), complex64(1), new(int32), []bool{true}, [][]int16{{1}}, struct{}{}, map[string]int{"x": 1}} {
		if _, err := NewValue(input); !errors.Is(err, ErrBadType) {
			t.Fatalf("%T: want ErrBadType, got %v", input, err)
		}
	}
	for _, input := range []any{int64(math.MinInt32) - 1, int64(math.MaxInt32) + 1, []int64{1, int64(math.MinInt32) - 1}, [2]int64{1, int64(math.MaxInt32) + 1}} {
		if _, err := NewValue(input); err == nil || !strings.Contains(err.Error(), "CA LONG overflow") {
			t.Fatalf("%v: want overflow error, got %v", input, err)
		}
	}
	for _, count := range []int{-1, math.MaxInt} {
		if _, err := newArrayValue(Double, count); !errors.Is(err, ErrBadCount) {
			t.Fatalf("count %d: want ErrBadCount, got %v", count, err)
		}
	}
}

func TestValueEnumLabelCountWireLimit(t *testing.T) {
	for _, typ := range []DataType{24, 31} { // DBR_GR_ENUM and DBR_CTRL_ENUM.
		payload := make([]byte, 424)
		binary.BigEndian.PutUint16(payload[4:6], 16)
		for i := range 16 {
			copy(payload[6+i*26:6+(i+1)*26], strings.Repeat(string(rune('A'+i)), 26))
		}
		v, err := DecodeValue(typ, 1, payload)
		if err != nil {
			t.Fatal(err)
		}
		labels := v.Metadata().EnumLabels
		if len(labels) != 16 || labels[15] != strings.Repeat("P", 26) {
			t.Fatalf("DBR %d lost labels at wire limit: %#v", typ, labels)
		}
		binary.BigEndian.PutUint16(payload[4:6], 17)
		if v, err := DecodeValue(typ, 1, payload); err != nil || v.Metadata().EnumCount != 17 || len(v.Metadata().EnumLabels) != 16 {
			t.Fatalf("DBR %d lost raw enum label count: %v", typ, err)
		}
	}
}
