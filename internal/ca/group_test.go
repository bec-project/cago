package ca

import (
	"context"
	"testing"
	"time"
)

func TestGroupReadWriteReset(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("test")
	g := &Group{}
	v, _ := NewValue(7.0)
	if e := g.Put(ch, v); e != nil {
		t.Fatal(e)
	}
	if e := g.Run(ctx, 1); e != nil {
		t.Fatal(e)
	}
	g.Reset()
	a, e := g.Get(ch)
	if e != nil {
		t.Fatal(e)
	}
	b, e := g.Get(ch)
	if e != nil {
		t.Fatal(e)
	}
	if e := g.Run(ctx, 2); e != nil {
		t.Fatal(e)
	}
	if a.Value.Float64() != 7 || b.Value.Float64() != 7 {
		t.Fatal("group read mismatch")
	}
	g.Reset()
	g.Get(c.Channel("missing"))
	short, cancelShort := context.WithTimeout(ctx, 20*time.Millisecond)
	defer cancelShort()
	if e := g.Run(short, 1); e == nil {
		t.Fatal("group timeout ignored")
	}
	if e := g.Reset(); e != nil {
		t.Fatal(e)
	}
}

func TestGroupCapturesReadOptions(t *testing.T) {
	s := newTestIOC(t)
	c := testClient(t, s, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	g := &Group{}
	options := []RequestOption{WithType(Double)}
	first, err := g.Get(c.Channel("test"), options...)
	if err != nil {
		t.Fatal(err)
	}
	options[0] = WithType(DataType(1000))
	second, err := g.Get(c.Channel("test"), options...)
	if err != nil {
		t.Fatal(err)
	}
	if err = g.Run(ctx, 1); err == nil {
		t.Fatal("invalid second request succeeded")
	}
	if first.Err != nil {
		t.Fatalf("later options mutation changed first request: %v", first.Err)
	}
	if second.Err == nil {
		t.Fatal("second request lost its invalid options")
	}
}
