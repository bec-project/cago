package ca

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

// Group collects independent operations and executes them with bounded
// concurrency. It is not an atomic server transaction. Run waits for all work;
// cancellation cancels unfinished requests. A group can be Reset and reused.
type Group struct {
	mu      sync.Mutex
	running bool
	jobs    []func(context.Context) error
}

// ReadResult is populated by Group.Run; access it after Run returns.
type ReadResult struct {
	Value Value
	Err   error
}

func (g *Group) Get(ch *Channel, options ...RequestOption) (*ReadResult, error) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running {
		return nil, errors.New("group running")
	}
	options = append([]RequestOption(nil), options...)
	result := &ReadResult{}
	g.jobs = append(g.jobs, func(ctx context.Context) error { result.Value, result.Err = ch.Get(ctx, options...); return result.Err })
	return result, nil
}
func (g *Group) Put(ch *Channel, v Value) error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running {
		return errors.New("group running")
	}
	g.jobs = append(g.jobs, func(ctx context.Context) error { return ch.PutNotify(ctx, v) })
	return nil
}
func (g *Group) Reset() error {
	g.mu.Lock()
	defer g.mu.Unlock()
	if g.running {
		return errors.New("group running")
	}
	g.jobs = nil
	return nil
}
func (g *Group) Run(ctx context.Context, concurrency int) error {
	if concurrency < 1 {
		return fmt.Errorf("group concurrency must be positive")
	}
	g.mu.Lock()
	if g.running {
		g.mu.Unlock()
		return errors.New("group running")
	}
	g.running = true
	jobs := append([]func(context.Context) error(nil), g.jobs...)
	g.mu.Unlock()
	defer func() { g.mu.Lock(); g.running = false; g.mu.Unlock() }()
	if concurrency > len(jobs) {
		concurrency = len(jobs)
	}
	indices := make(chan int)
	errs := make([]error, len(jobs))
	var wg sync.WaitGroup
	for range concurrency {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := range indices {
				errs[i] = jobs[i](ctx)
			}
		}()
	}
	for i := range jobs {
		indices <- i
	}
	close(indices)
	wg.Wait()
	return errors.Join(errs...)
}
