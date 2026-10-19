package ca

import (
	"context"
	"testing"
	"time"
)

func TestFullDBRPropertySemantics(t *testing.T) {
	ioc := newTestIOC(t)
	c := testClient(t, ioc, UDP)
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	ch := c.Channel("test")
	a := subscribeTest(t, ch, ctx, WithType(Double), WithMetadata(Control))
	event(t, a)
	b := subscribeTest(t, ch, ctx, WithType(DataType(34)))
	event(t, b)
	if a.monitor != b.monitor {
		t.Fatal("equivalent DBR requests did not share their monitor")
	}
	if a.monitor.key.mask&PropertyEvent == 0 {
		t.Fatal("control metadata omitted property events")
	}
	if a.monitor.key.mask != b.monitor.key.mask {
		t.Fatalf("equivalent DBR_CTRL_DOUBLE requests have masks %d and %d; full DBR misses property-only events", a.monitor.key.mask, b.monitor.key.mask)
	}
}
