package ca

import (
	"context"
	"testing"
	"time"
)

func TestEnumConversionTracksNativeReconnect(t *testing.T) {
	server := newTestIOC(t)
	client := testClient(t, server, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	ch := client.Channel("test")
	text := subscribeTest(t, ch, ctx, WithMetadata(Time), WithEnumType(String))
	number := subscribeTest(t, ch, ctx, WithMetadata(Time), WithEnumType(Short))
	if text.monitor == number.monitor {
		t.Fatal("different future ENUM conversions shared one request")
	}
	want := func(sub *Subscription, typ DataType) {
		t.Helper()
		if e := event(t, sub); e.Value.Type() != typ {
			t.Fatalf("got type %d, want %d", e.Value.Type(), typ)
		}
	}
	want(text, 20)
	want(number, 20)
	server.nativeType.Store(uint32(Enum))
	server.disconnect()
	want(text, 14)
	want(number, 15)
	server.nativeType.Store(uint32(Double))
	server.disconnect()
	want(text, 20)
	want(number, 20)
}

func TestEnumConversionOptions(t *testing.T) {
	for _, typ := range []DataType{String, Short, Float, Enum, Char, Long, Double, Native} {
		cfg, err := requestConfig([]RequestOption{WithEnumType(typ)})
		if err != nil {
			t.Fatal(err)
		}
		want := typ
		if typ == Native {
			want = Enum
		}
		if got, _ := cfg.wireType(Enum); got != want {
			t.Fatalf("conversion %d: got %d, want %d", typ, got, want)
		}
		if got, _ := cfg.wireType(Double); got != Double {
			t.Fatal("non-ENUM changed", got)
		}
	}
	for _, typ := range []DataType{7, 99, 65534} {
		if _, err := requestConfig([]RequestOption{WithEnumType(typ)}); err == nil {
			t.Fatal("invalid conversion accepted", typ)
		}
	}
	a, _ := requestConfig([]RequestOption{WithType(Double), WithMetadata(Time)})
	b, _ := requestConfig([]RequestOption{WithType(20), WithEnumType(String)})
	if a != b {
		t.Fatalf("irrelevant conversion prevents canonical sharing: %+v != %+v", a, b)
	}
	if got, _ := b.wireType(Enum); got != 20 {
		t.Fatal("explicit type lost precedence", got)
	}
}
