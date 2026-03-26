// Package pool provides a semaphore-based concurrency limiter that caps
// the number of requests handled simultaneously.
package pool

import (
	"context"
	"errors"
)

// ErrPoolFull is returned when the pool is at capacity and no slot is available.
var ErrPoolFull = errors.New("connection pool: no available slot")

// ConnPool limits the number of concurrent operations using a buffered channel
// as a semaphore.
type ConnPool struct {
	sem chan struct{}
}

// New creates a ConnPool with the given maximum concurrency.
func New(maxConns int) *ConnPool {
	return &ConnPool{sem: make(chan struct{}, maxConns)}
}

// Acquire claims one slot, blocking until one is available or ctx is cancelled.
func (p *ConnPool) Acquire(ctx context.Context) error {
	select {
	case p.sem <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// TryAcquire attempts to claim one slot without blocking.
// Returns ErrPoolFull if no slot is available.
func (p *ConnPool) TryAcquire() error {
	select {
	case p.sem <- struct{}{}:
		return nil
	default:
		return ErrPoolFull
	}
}

// Release returns a slot to the pool. Must be called exactly once per
// successful Acquire or TryAcquire.
func (p *ConnPool) Release() {
	<-p.sem
}

// Available returns the number of immediately available slots.
func (p *ConnPool) Available() int {
	return cap(p.sem) - len(p.sem)
}

// Capacity returns the total pool size.
func (p *ConnPool) Capacity() int {
	return cap(p.sem)
}

// InUse returns the number of currently acquired slots.
func (p *ConnPool) InUse() int {
	return len(p.sem)
}
