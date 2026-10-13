// Package store defines the broker's persistence surface.
//
// The in-memory implementation (MemoryStore) is the only adapter today;
// a Postgres or Redis adapter can be dropped in behind Store without
// touching callers.
package store

import (
	"context"
	"errors"
	"time"

	"goflow/internal/model"
)

// ErrQueueFull is returned when the pending queue is at capacity.
var ErrQueueFull = errors.New("job queue is full")

// ErrBadLease is returned when an Ack/Nack arrives with a token that
// does not match the current lease on that job (stale consumer).
var ErrBadLease = errors.New("lease token invalid or expired")

// LeaseToken is a server-issued opaque handle that the client must
// echo back on Ack / Nack. It expires with the lease.
type LeaseToken string

// Store is the broker's persistence surface — producers, consumers,
// retry scheduler and dashboard all go through it.
type Store interface {
	// --- Producer side ---
	Create(job model.Job, idemKey string) (model.Job, bool, error)
	Get(id string) (model.Job, bool)
	List() []model.Job
	Size() int
	QueueDepth() int

	// --- Consumer side ---
	// ClaimOne blocks until a pending job matches `types` (empty slice
	// matches any type), or ctx expires. Issues a lease; the caller
	// must Ack/Nack within `lease` or ExpiredLeases will reclaim it.
	ClaimOne(ctx context.Context, types []string, lease time.Duration) (model.Job, LeaseToken, error)

	// Ack marks the job completed. Latency is recorded by the caller.
	Ack(id string, token LeaseToken) error

	// Nack releases the lease and reports the error. The returned flag
	// tells the caller whether the attempt budget is spent (true = may
	// retry, false = must DLQ). The job's attempt count has already
	// been incremented at Claim time, so the caller just checks
	// shouldRetry.
	Nack(id string, token LeaseToken, lastErr string) (shouldRetry bool, attempts int, err error)

	// EnqueueRetry re-queues a job for another attempt. Called by the
	// retry scheduler when a backoff timer fires, and by the lease
	// watcher when a lease expires.
	EnqueueRetry(job model.Job) error

	// ExpiredLeases returns every lease older than now, and reclaims
	// them from the leases map. Caller decides what to do with each
	// (retry via scheduler, or DLQ).
	ExpiredLeases(now time.Time) []model.Job

	// --- Dead-letter queue ---
	DLQAdd(job model.Job)
	DLQGetAll() []model.Job
	DLQClaim(id string) (model.Job, bool)
	DLQClaimAll() []model.Job
	DLQSize() int
	DLQRemove(id string)
}
