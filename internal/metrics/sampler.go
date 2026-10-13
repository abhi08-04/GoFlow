package metrics

import (
	"context"
	"time"
)

// StoreGauges exposes the two numbers the sampler needs to read each
// tick. Any store.Store satisfies this interface — kept local so this
// package does not import store.
type StoreGauges interface {
	QueueDepth() int
	DLQSize() int
}

// Sampler ticks on an interval, snapshots the registry, and pushes the
// snapshot into the ring buffer. QueueDepth and DLQSize are read from
// the store so the gauges reflect authoritative state instead of
// drifting from +/- increments.
type Sampler struct {
	registry *Registry
	ring     *RingBuffer
	store    StoreGauges
	interval time.Duration
}

func NewSampler(r *Registry, ring *RingBuffer, store StoreGauges, interval time.Duration) *Sampler {
	return &Sampler{registry: r, ring: ring, store: store, interval: interval}
}

func (s *Sampler) Run(ctx context.Context) {
	s.tick()

	t := time.NewTicker(s.interval)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.tick()
		}
	}
}

func (s *Sampler) tick() {
	s.registry.SetQueueDepth(int64(s.store.QueueDepth()))
	s.registry.SetDLQSize(int64(s.store.DLQSize()))
	s.ring.Add(s.registry.Snapshot())
}
