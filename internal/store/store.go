// Package store defines the broker's persistence surface. The Store
// interface will later gain Claim/Ack/Nack and lease state as the gRPC
// server comes online — for now it mirrors today's JobStore + DLQ
// shape so no caller behavior changes.
package store

import (
	"errors"

	"goflow/internal/model"
)

// ErrQueueFull is returned when the backing jobs channel cannot accept
// another entry without blocking.
var ErrQueueFull = errors.New("job queue is full")

// Store abstracts job + DLQ persistence. The current implementation is
// in-memory (NewMemoryStore); later rollout steps may add Postgres,
// Redis, or other adapters behind the same interface.
type Store interface {
	// Job lifecycle.
	Create(job model.Job, idemKey string, jobs chan<- model.Job) (model.Job, bool, error)
	Update(job model.Job)
	Get(id string) (model.Job, bool)
	List() []model.Job
	Size() int
	EnqueueRetry(job model.Job, jobs chan<- model.Job) bool

	// Dead-letter queue.
	DLQAdd(job model.Job)
	DLQGetAll() []model.Job
	DLQClaim(id string) (model.Job, bool)
	DLQClaimAll() []model.Job
	DLQSize() int
	DLQRemove(id string)
}
