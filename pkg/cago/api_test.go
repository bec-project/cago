package cago_test

import (
	"context"
	"errors"
	"math"
	"testing"
	"time"

	"github.com/wakonig/cago/pkg/cago"
)

func TestPublicClientAndChannelContract(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	collector := &cago.ClientMetrics{}
	client, err := cago.New(ctx, cago.Options{
		SearchAddresses: []string{"127.0.0.1:9"}, SearchTransport: cago.TCP,
		DisableRepeater: true, Metrics: collector, ContextTimeout: time.Second,
	})
	if err != nil {
		cancel()
		t.Fatal(err)
	}
	defer func() { cancel(); client.Close() }()
	first := client.Channel("API:PV", cago.WithPriority(7))
	second := client.Channel("API:PV", cago.WithPriority(7))
	if first != second || first.Name() != "API:PV" || first.Info().Priority != 7 {
		t.Fatal("public handles lost channel identity or options")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	if client.Channel("API:PV", cago.WithPriority(7)) == first {
		t.Fatal("closed public handle was reused")
	}
	if err := client.Close(); err != nil {
		t.Fatal(err)
	}
	select {
	case <-client.Done():
	default:
		t.Fatal("Close did not close Done")
	}
	if client.Stats().Channels != 0 {
		t.Fatal("Close retained public channels")
	}
	if client.Metrics() == nil {
		t.Fatal("public metrics unavailable")
	}
}

func TestPublicRequestOptionsAndErrors(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	client, err := cago.New(ctx, cago.Options{})
	if client != nil || !errors.Is(err, context.Canceled) {
		t.Fatalf("New: client=%v err=%v", client, err)
	}
	// Validation runs before attachment. A canceled context avoids any network
	// operations while exercising the public option types at the API boundary.
	var channel cago.Channel
	_, err = channel.Get(ctx, cago.WithType(cago.Native), cago.WithEnumType(cago.String),
		cago.WithCount(3), cago.WithMetadata(cago.Time), cago.WithMask(cago.ValueEvent|cago.AlarmEvent), cago.WithBuffer(2))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("valid public options rejected: %v", err)
	}
	_, err = channel.Get(ctx, cago.WithType(cago.DataType(99)))
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("invalid public type accepted: %v", err)
	}
	protocol := &cago.ProtocolError{Status: 24<<3 | 2, Command: 15}
	if !errors.Is(protocol, cago.ErrDisconnected) {
		t.Fatal("protocol error lost sentinel identity")
	}
	var decoded *cago.ProtocolError
	if !errors.As(protocol, &decoded) || decoded != protocol {
		t.Fatal("protocol error lost type identity")
	}
}

func TestPublicValuesGroupsAndMetrics(t *testing.T) {
	value, err := cago.NewValue([]int32{1, -2})
	if err != nil {
		t.Fatal(err)
	}
	if value.Type() != cago.Long || value.Count() != 2 {
		t.Fatal("public value shape changed")
	}
	element, err := value.Index(1)
	if err != nil || element.Int64() != -2 {
		t.Fatalf("public Index: value=%v err=%v", element, err)
	}
	data := value.Bytes()
	data[0] = 255
	if value.Bytes()[0] != 0 {
		t.Fatal("public value exposed mutable storage")
	}
	if _, err := cago.NewValue(uint64(math.MaxUint64)); err == nil {
		t.Fatal("overflow accepted")
	}
	var group cago.Group
	if err := group.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := group.Run(context.Background(), 1); err != nil {
		t.Fatal(err)
	}
	var metrics cago.ClientMetrics
	var sink cago.Metrics = &metrics
	sink.Add(cago.MetricConnections, 2)
	if metrics.Value(cago.MetricConnections) != 2 || metrics.Snapshot()[cago.MetricConnections] != 2 {
		t.Fatal("metrics type identity changed")
	}
}
