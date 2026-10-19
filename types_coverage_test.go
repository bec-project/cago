package cago

import (
	"errors"
	"testing"
)

func TestDataTypeWireFamilies(t *testing.T) {
	// EPICS DBR families repeat the seven native types for plain/status/time/
	// graphics/control, followed by the four special-purpose wire types.
	sizes := []int{40, 2, 4, 2, 1, 4, 8}
	for family := range 5 {
		for base, size := range sizes {
			typ := DataType(family*7 + base)
			if !typ.Valid() || typ.Base() != DataType(base) || typ.Size() != size {
				t.Fatalf("DBR %d: valid=%t base=%d size=%d", typ, typ.Valid(), typ.Base(), typ.Size())
			}
		}
	}
	for _, tt := range []struct {
		typ  DataType
		base DataType
		size int
	}{
		{PutACKT, Enum, 2}, {PutACKS, Enum, 2},
		{STSACKString, String, 40}, {ClassName, String, 40},
	} {
		if !tt.typ.Valid() || tt.typ.Base() != tt.base || tt.typ.Size() != tt.size {
			t.Fatalf("DBR %d: valid=%t base=%d size=%d", tt.typ, tt.typ.Valid(), tt.typ.Base(), tt.typ.Size())
		}
	}
	for _, typ := range []DataType{39, 1000, Native} {
		if typ.Valid() || typ.Base() != Native || typ.Size() != 0 {
			t.Fatalf("invalid DBR %d: valid=%t base=%d size=%d", typ, typ.Valid(), typ.Base(), typ.Size())
		}
		if _, err := DecodeValue(typ, 0, nil); !errors.Is(err, ErrBadType) {
			t.Fatalf("invalid DBR %d decoded: %v", typ, err)
		}
	}
}

func TestChannelStateStrings(t *testing.T) {
	for _, tt := range []struct {
		state State
		want  string
	}{{Searching, "searching"}, {Connected, "connected"}, {Closed, "closed"}, {State(255), "unknown"}} {
		if got := tt.state.String(); got != tt.want {
			t.Fatalf("State(%d).String()=%q; want %q", tt.state, got, tt.want)
		}
	}
}

func TestProtocolErrorPreservesUnknownStatus(t *testing.T) {
	const status = uint32(100 << 3)
	err := statusError(status, 15)
	var protocol *ProtocolError
	if !errors.As(err, &protocol) || protocol.Status != status || protocol.Command != 15 || protocol.Message != "server rejected request" {
		t.Fatalf("lost protocol diagnostics: %#v", err)
	}
	if got := protocol.Error(); got != "CA status 800 on command 15: server rejected request" {
		t.Fatalf("Error()=%q", got)
	}
	if protocol.Unwrap() != nil {
		t.Fatalf("unknown status maps to unrelated sentinel: %v", protocol.Unwrap())
	}
	if statusError(status|1, 15) == nil {
		t.Fatal("non-normal status accepted as success")
	}
}
