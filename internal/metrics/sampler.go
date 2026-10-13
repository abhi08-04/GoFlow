package metrics

import (
	"context"
	"time"

	"goflow/internal/model"
)

// DLQSizer lets the sampler read DLQ depth without importing store
// (and dragging in a cycle). store.Store satisfies this interface.
type DLQSizer interface {
	DLQSize() int
}

// Sampler ticks on an interval, snapshots the registry, and pushes the
// snapshot into the ring buffer. It reads len(jobs) and dlq.Size() each
// tick so the queue-depth and dlq-size gauges stay in sync with reality
// (as opposed to drifting if an event path forgets to +/-).
//
// len() on a channel is safe from any goroutine and does not block —
// the runtime reads the buffered count atomically from the hchan header.
type Sampler struct {
	registry *Registry
	ring     *RingBuffer
	jobs     chan model.Job
	dlq      DLQSizer
	interval time.Duration
}

func NewSampler(r *Registry, ring *RingBuffer, jobs chan model.Job, dlq DLQSizer, interval time.Duration) *Sampler {
	return &Sampler{registry: r, ring: ring, jobs: jobs, dlq: dlq, interval: interval}
}

// Run blocks until ctx is cancelled. Intended to be launched as its own
// goroutine from main. The initial snapshot is taken immediately so a
// freshly started dashboard has one point to render.
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
	s.registry.SetQueueDepth(int64(len(s.jobs)))
	s.registry.SetDLQSize(int64(s.dlq.DLQSize()))
	s.ring.Add(s.registry.Snapshot())
}
