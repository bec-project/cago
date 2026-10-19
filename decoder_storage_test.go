package cago

import (
	"bytes"
	"encoding/binary"
	"testing"
)

func TestDBRBytesPreserveAllMetadataAndOwnStorage(t *testing.T) {
	for typ := DataType(0); typ <= ClassName; typ++ {
		for count := uint32(0); count <= 2; count++ {
			size := valueOffset(typ) + int(count)*typ.Size()
			payload := bytes.Repeat([]byte{0xa5}, size+8)
			if typ >= 14 && typ <= 20 {
				binary.BigEndian.PutUint32(payload[8:12], 1000000000)
			}
			if typ == 24 || typ == 31 {
				binary.BigEndian.PutUint16(payload[4:6], 17)
			}
			want := append([]byte(nil), payload[:size]...)
			v, err := DecodeValue(typ, count, payload)
			if err != nil {
				t.Fatalf("type=%d count=%d: %v", typ, count, err)
			}
			if !bytes.Equal(v.DBRBytes(), want) {
				t.Fatalf("DBR %d count %d raw metadata/value changed", typ, count)
			}
			for i := range payload {
				payload[i] = 0
			}
			raw := v.DBRBytes()
			for i := range raw {
				raw[i] = 0
			}
			if !bytes.Equal(v.DBRBytes(), want) {
				t.Fatalf("DBR %d raw record aliases caller", typ)
			}
			if !bytes.Equal(v.Bytes(), want[valueOffset(typ):]) {
				t.Fatalf("DBR %d value view lost after full-record copy", typ)
			}
			if count > 0 {
				item, err := v.Index(0)
				if err != nil || item.Type() != typ.Base() || !bytes.Equal(item.DBRBytes(), v.Bytes()[:typ.Size()]) {
					t.Fatalf("indexed DBR %d is not one plain value: %v", typ, err)
				}
			}
		}
	}
}

func TestDBRBytesForConstructedAndCompactValues(t *testing.T) {
	v, err := NewValue([]int16{-1, 2})
	if err != nil {
		t.Fatal(err)
	}
	raw := v.DBRBytes()
	raw[0] = 0
	if !bytes.Equal(v.DBRBytes(), v.Bytes()) {
		t.Fatal("constructed value was modified through raw record")
	}
	compact := []byte{'r', 'e', 'a', 'd', 'y', 0, 1, 2}
	v, err = DecodeValue(String, 1, compact)
	if err != nil || len(v.DBRBytes()) != 40 || !bytes.Equal(v.DBRBytes()[:8], compact) || !bytes.Equal(v.DBRBytes()[8:], make([]byte, 32)) {
		t.Fatal("compact string expansion lost received bytes", err)
	}
}
