// Package broker owns the background goroutines that keep the queue
// correct: today, just the lease-expiry watcher. The gRPC server
// package handles everything on the request path.
package broker

import (
	"context"
	"time"

	"goflow/internal/metrics"
	"goflow/internal/model"
	"goflow/internal/retry"
	"goflow/internal/store"
)

// LeaseWatcher periodically sweeps expired leases out of the store and
// pushes each one back through the retry scheduler. From the retry
// scheduler's point of view a lease expiry is identical to a nack —
// same backoff curve, same DLQ fallback.
type LeaseWatcher struct {
	store       store.Store
	scheduler   *retry.Scheduler
	registry    *metrics.Registry
	interval    time.Duration
	baseBackoff time.Duration
}

func NewLeaseWatcher(st store.Store, sched *retry.Scheduler, reg *metrics.Registry, interval, baseBackoff time.Duration) *LeaseWatcher {
	return &LeaseWatcher{
		store:       st,
		scheduler:   sched,
		registry:    reg,
		interval:    interval,
		baseBackoff: baseBackoff,
	}
}

func (w *LeaseWatcher) Run(ctx context.Context) {
	t := time.NewTicker(w.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, j := range w.store.ExpiredLeases(now) {
				w.handleExpired(j)
			}
		}
	}
}

func (w *LeaseWatcher) handleExpired(job model.Job) {
	w.registry.RecordLeaseExpired()

	if job.Attempts > job.MaxRetries {
		w.store.DLQAdd(job)
		w.registry.RecordDLQd()
		return
	}

	// Schedule the retry with the same exponential-backoff curve the
	// nack path uses.
	attempts := 1
	if job.Attempts > 0 {
		attempts = job.Attempts
	}
	delay := w.baseBackoff * time.Duration(1<<(attempts-1))
	if ok := w.scheduler.Schedule(job, delay, func() {
		w.store.DLQAdd(job)
		w.registry.RecordDLQd()
	}); !ok {
		w.store.DLQAdd(job)
		w.registry.RecordDLQd()
	}
}
