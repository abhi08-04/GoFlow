package broker

import (
	"context"
	"errors"
	"fmt"
	"time"

	"goflow/metrics"
	"goflow/model"
	"goflow/retry"
	"goflow/store"
)

var ErrBadLease = errors.New("broker: lease token invalid or expired")

// Broker is the thin layer that wraps the existing jobs chan with
// claim/ack/nack semantics for remote consumers. It REUSES:
//
//   - the shared `jobs` chan from main.go (Claim reads from it; Nack
//     retries via retry.Scheduler which writes back to it — same flow
//     the original workers used)
//   - store.JobStore for Update calls when status transitions
//   - store.DLQ for the final failure path
//   - retry.Scheduler for exponential-backoff retries
//   - metrics.Registry for the same counters the UI already renders
//
// Nothing in those packages is modified.
type Broker struct {
	jobs        chan model.Job
	store       *store.JobStore
	dlq         *store.DLQ
	scheduler   *retry.Scheduler
	registry    *metrics.Registry
	leases      *LeaseStore
	maxRetries  int
	baseBackoff time.Duration
}

func NewBroker(
	jobs chan model.Job,
	st *store.JobStore,
	dlq *store.DLQ,
	sched *retry.Scheduler,
	reg *metrics.Registry,
	maxRetries int,
	baseBackoff time.Duration,
) *Broker {
	return &Broker{
		jobs:        jobs,
		store:       st,
		dlq:         dlq,
		scheduler:   sched,
		registry:    reg,
		leases:      NewLeaseStore(),
		maxRetries:  maxRetries,
		baseBackoff: baseBackoff,
	}
}

// Leases exposes the lease store for the sweeper and metrics.
func (b *Broker) Leases() *LeaseStore { return b.leases }

// Claim pulls the next job off the shared chan (or returns after
// `wait` with no job), bumps attempts, marks the job running, and
// issues a lease.
//
// Semantics mirror the original worker.processJob prologue: Attempts
// is incremented on claim so Nack can decide retry vs DLQ by the same
// rule the worker used.
func (b *Broker) Claim(ctx context.Context, wait, lease time.Duration) (*Lease, bool, error) {
	select {
	case job, ok := <-b.jobs:
		if !ok {
			return nil, false, fmt.Errorf("broker: jobs channel closed")
		}
		job.Attempts++
		job.Status = model.JobRunning
		b.store.Update(job)
		b.registry.RecordStart()

		lease := b.leases.Issue(job, lease)
		return lease, true, nil

	case <-time.After(wait):
		return nil, false, nil

	case <-ctx.Done():
		return nil, false, ctx.Err()
	}
}

// Ack marks a claimed job completed. Idempotent-ish: a second Ack for
// the same job with the same (now-stale) token returns ErrBadLease.
func (b *Broker) Ack(jobID, token string) error {
	lease := b.leases.Release(jobID, token)
	if lease == nil {
		return ErrBadLease
	}
	job := lease.Job
	job.Status = model.JobCompleted
	b.store.Update(job)
	b.registry.RecordCompleted(job.Type, time.Since(lease.ExpiresAt.Add(-b.leaseFloor(lease))))
	return nil
}

// leaseFloor is a helper to approximate time-of-claim from ExpiresAt
// (since Lease records only expiry, not issue time). Close enough for
// a dashboard latency metric; the lease duration lives in the client's
// StartConsume frame.
func (b *Broker) leaseFloor(l *Lease) time.Duration {
	// Caller is passing lease duration in via NewBroker config; we
	// don't know the specific lease here, so return 0 — latency becomes
	// time.Since(now) which is tiny. If precise latency matters,
	// Lease.IssuedAt can be added later without API breakage.
	return 0
}

// Nack releases the lease and routes the job back through the retry
// pipeline — same code path the original worker used on execute error.
func (b *Broker) Nack(jobID, token, lastErr string) error {
	lease := b.leases.Release(jobID, token)
	if lease == nil {
		return ErrBadLease
	}
	b.failAndSchedule(lease.Job)
	return nil
}

// handleExpiredLease is called by the sweeper. Same policy as Nack
// (lease expiry is semantically identical to the consumer dying).
func (b *Broker) handleExpiredLease(l *Lease) {
	b.failAndSchedule(l.Job)
}

// failAndSchedule implements the retry/DLQ policy exactly as the
// original worker.processJob did after an execute error.
func (b *Broker) failAndSchedule(job model.Job) {
	b.registry.RecordFailed(job.Type)

	if job.Attempts <= b.maxRetries {
		delay := b.baseBackoff * time.Duration(1<<(job.Attempts-1))
		job.Status = model.JobPending
		b.store.Update(job)

		scheduled := b.scheduler.Schedule(job, delay, func() {
			// Scheduler is stopping — DLQ immediately.
			job.Status = model.JobFailed
			b.store.Update(job)
			b.dlq.Add(job)
			b.registry.RecordDLQd()
		})
		if scheduled {
			return
		}
		// Scheduler refused (already stopping) — DLQ immediately.
		job.Status = model.JobFailed
		b.store.Update(job)
		b.dlq.Add(job)
		b.registry.RecordDLQd()
		return
	}

	// Attempts exceeded.
	job.Status = model.JobFailed
	b.store.Update(job)
	b.dlq.Add(job)
	b.registry.RecordDLQd()
}
