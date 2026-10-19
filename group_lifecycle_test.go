package cago

import (
	"context"
	"errors"
	"sync"
	"testing"
)

func TestGroupRejectsChangesDuringRun(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	g := &Group{jobs: []func(context.Context) error{func(context.Context) error {
		close(started)
		<-release
		return nil
	}}}
	done := make(chan error, 1)
	go func() { done <- g.Run(context.Background(), 1) }()
	<-started
	if result, err := g.Get(nil); err == nil || result != nil {
		t.Errorf("Get during Run: result=%v err=%v", result, err)
	}
	if err := g.Put(nil, Value{}); err == nil {
		t.Error("Put during Run succeeded")
	}
	if err := g.Reset(); err == nil {
		t.Error("Reset during Run succeeded")
	}
	if err := g.Run(context.Background(), 1); err == nil {
		t.Error("concurrent Run succeeded")
	}
	close(release)
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if err := g.Reset(); err != nil {
		t.Fatalf("Reset after Run: %v", err)
	}
	if err := g.Run(context.Background(), 1); err != nil {
		t.Fatalf("empty reused group: %v", err)
	}
}

func TestGroupBoundsConcurrencyAndJoinsErrors(t *testing.T) {
	const jobCount = 8
	firstErr, lastErr := errors.New("first request failed"), errors.New("last request failed")
	started := make(chan struct{}, jobCount)
	release := make(chan struct{})
	var mu sync.Mutex
	active, maximum := 0, 0
	completed := make([]bool, jobCount)
	g := &Group{}
	for i := range jobCount {
		g.jobs = append(g.jobs, func(context.Context) error {
			mu.Lock()
			active++
			maximum = max(maximum, active)
			mu.Unlock()
			started <- struct{}{}
			<-release
			mu.Lock()
			active--
			completed[i] = true
			mu.Unlock()
			switch i {
			case 0:
				return firstErr
			case jobCount - 1:
				return lastErr
			default:
				return nil
			}
		})
	}
	done := make(chan error, 1)
	go func() { done <- g.Run(context.Background(), 2) }()
	<-started
	<-started
	mu.Lock()
	if active != 2 {
		t.Errorf("active=%d; want two blocked workers", active)
	}
	mu.Unlock()
	select {
	case err := <-done:
		t.Errorf("Run returned before blocked jobs completed: %v", err)
	default:
	}
	close(release)
	err := <-done
	if !errors.Is(err, firstErr) || !errors.Is(err, lastErr) {
		t.Fatalf("Run lost request errors: %v", err)
	}
	if maximum != 2 || active != 0 {
		t.Fatalf("maximum=%d active=%d; want maximum=2 active=0", maximum, active)
	}
	for i, finished := range completed {
		if !finished {
			t.Errorf("Run skipped or failed to await job %d", i)
		}
	}
}

func TestGroupCancellationWaitsForWorkersAndQueuedJobs(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	started := make(chan struct{})
	cancelObserved := make(chan struct{})
	release := make(chan struct{})
	secondRan := false
	g := &Group{jobs: []func(context.Context) error{
		func(ctx context.Context) error {
			close(started)
			<-ctx.Done()
			close(cancelObserved)
			<-release
			return ctx.Err()
		},
		func(ctx context.Context) error {
			secondRan = true
			return ctx.Err()
		},
	}}
	done := make(chan error, 1)
	go func() { done <- g.Run(ctx, 1) }()
	<-started
	cancel()
	<-cancelObserved
	select {
	case err := <-done:
		t.Errorf("Run returned while a canceled worker was unfinished: %v", err)
	default:
	}
	close(release)
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("cancellation lost: %v", err)
	}
	if !secondRan {
		t.Fatal("queued job was skipped instead of observing cancellation")
	}
	if err := g.Reset(); err != nil {
		t.Fatalf("canceled Run left group running: %v", err)
	}
}

func TestGroupInvalidConcurrencyAndReset(t *testing.T) {
	runs := 0
	g := &Group{jobs: []func(context.Context) error{func(context.Context) error {
		runs++
		return nil
	}}}
	for _, concurrency := range []int{-1, 0} {
		if err := g.Run(context.Background(), concurrency); err == nil {
			t.Fatalf("accepted concurrency %d", concurrency)
		}
	}
	if runs != 0 {
		t.Fatal("invalid Run executed queued work")
	}
	if err := g.Run(context.Background(), 10); err != nil || runs != 1 {
		t.Fatalf("Run with concurrency above job count: runs=%d err=%v", runs, err)
	}
	if err := g.Reset(); err != nil {
		t.Fatal(err)
	}
	if err := g.Run(context.Background(), 10); err != nil || runs != 1 {
		t.Fatalf("Reset retained old job: runs=%d err=%v", runs, err)
	}
}
