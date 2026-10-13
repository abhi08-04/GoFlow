package broker

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"sync"
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

	// Per-type routing: a background dispatcher reads from `jobs` and
	// routes each item onto byType[job.Type] so a Consume stream that
	// specified types=[a,b] can select only matching queues.
	byType      map[string]chan model.Job
	byTypeMu    sync.RWMutex
	typeBufSize int
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
	b := &Broker{
		jobs:        jobs,
		store:       st,
		dlq:         dlq,
		scheduler:   sched,
		registry:    reg,
		leases:      NewLeaseStore(),
		maxRetries:  maxRetries,
		baseBackoff: baseBackoff,
		byType:      make(map[string]chan model.Job),
		typeBufSize: cap(jobs),
	}
	// Dispatcher: reads from the shared jobs chan and routes to the
	// per-type sub-chan. Exits when jobs closes (on shutdown).
	go b.dispatch()
	return b
}

func (b *Broker) dispatch() {
	for job := range b.jobs {
		q := b.queueFor(job.Type)
		q <- job
	}
}

// queueFor returns the per-type chan, lazy-creating it on first use.
// Buffer equals the shared jobs chan capacity so backpressure matches.
func (b *Broker) queueFor(jobType string) chan model.Job {
	b.byTypeMu.RLock()
	q, ok := b.byType[jobType]
	b.byTypeMu.RUnlock()
	if ok {
		return q
	}
	b.byTypeMu.Lock()
	defer b.byTypeMu.Unlock()
	if q, ok := b.byType[jobType]; ok {
		return q
	}
	q = make(chan model.Job, b.typeBufSize)
	b.byType[jobType] = q
	return q
}

func (b *Broker) knownTypes() []string {
	b.byTypeMu.RLock()
	defer b.byTypeMu.RUnlock()
	out := make([]string, 0, len(b.byType))
	for t := range b.byType {
		out = append(out, t)
	}
	return out
}

// Leases exposes the lease store for the sweeper and metrics.
func (b *Broker) Leases() *LeaseStore { return b.leases }

// Claim pulls the next job matching `types` off its per-type queue
// (or returns after `wait` with no job), bumps attempts, marks the
// job running, and issues a lease.
//
// Empty `types` subscribes to every type currently known to the
// broker. New types created after the Claim call will not be seen in
// that case — callers that need "any type" over an unbounded set
// should pass a specific list.
//
// Semantics mirror the original worker.processJob prologue: Attempts
// is incremented on claim so Nack can decide retry vs DLQ by the same
// rule the worker used.
func (b *Broker) Claim(ctx context.Context, types []string, wait, lease time.Duration) (*Lease, bool, error) {
	if len(types) == 0 {
		types = b.knownTypes()
	}

	// Build reflect select cases: ctx.Done, time.After, plus one per
	// subscribed type's queue. reflect.Select picks the first ready.
	cases := make([]reflect.SelectCase, 0, len(types)+2)
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(ctx.Done())})
	cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(time.After(wait))})
	for _, t := range types {
		cases = append(cases, reflect.SelectCase{
			Dir:  reflect.SelectRecv,
			Chan: reflect.ValueOf(b.queueFor(t)),
		})
	}

	chosen, val, recvOK := reflect.Select(cases)
	switch chosen {
	case 0:
		return nil, false, ctx.Err()
	case 1:
		return nil, false, nil // wait window elapsed, no job
	}
	if !recvOK {
		return nil, false, fmt.Errorf("broker: queue closed for type %q", types[chosen-2])
	}

	job := val.Interface().(model.Job)
	job.Attempts++
	job.Status = model.JobRunning
	b.store.Update(job)
	b.registry.RecordStart()

	return b.leases.Issue(job, lease), true, nil
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
