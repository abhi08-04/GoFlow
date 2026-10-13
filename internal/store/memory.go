package store

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/google/uuid"

	"goflow/internal/model"
)

// MemoryStore is the default in-process Store.
//
// Concurrency design:
//   - jobMu guards everything on the producer/claim path (jobs, idemKeys,
//     pending-by-type, leases). ClaimOne waits on jobCond when there is
//     nothing pending for its type set; Enqueue broadcasts after append.
//   - dlqMu is independent so a slow DLQ scan does not block producers.
//   - QueueCap (soft limit) bounds total pending across types; Create
//     returns ErrQueueFull on overflow.
type MemoryStore struct {
	QueueCap int // 0 = unlimited; set from Config.JobQueueSize

	jobMu   sync.Mutex
	jobCond *sync.Cond

	jobs            map[string]model.Job
	idempotencyKeys map[string]string
	pendingByType   map[string][]model.Job // FIFO per type
	pendingTotal    int
	leases          map[string]*leaseEntry // keyed by job ID

	dlqMu sync.RWMutex
	dlq   map[string]model.Job
}

type leaseEntry struct {
	token     LeaseToken
	expiresAt time.Time
	job       model.Job
}

func NewMemoryStore() *MemoryStore {
	s := &MemoryStore{
		jobs:            make(map[string]model.Job),
		idempotencyKeys: make(map[string]string),
		pendingByType:   make(map[string][]model.Job),
		leases:          make(map[string]*leaseEntry),
		dlq:             make(map[string]model.Job),
	}
	s.jobCond = sync.NewCond(&s.jobMu)
	return s
}

// Create enqueues a brand-new job. Returns (job, true, nil) if newly
// created, (existing, false, nil) if idempotency key hit a prior job
// with matching content, or (_, false, err) on queue-full / conflict.
func (s *MemoryStore) Create(job model.Job, idemKey string) (model.Job, bool, error) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	if existingID, exists := s.idempotencyKeys[idemKey]; exists {
		existingJob, found := s.jobs[existingID]
		if !found {
			return model.Job{}, false, fmt.Errorf("idempotency key exists but job was not found")
		}
		if existingJob.Type != job.Type || existingJob.Payload != job.Payload {
			return model.Job{}, false, fmt.Errorf("idempotency key reused with different request content")
		}
		return existingJob, false, nil
	}

	if s.QueueCap > 0 && s.pendingTotal >= s.QueueCap {
		return model.Job{}, false, ErrQueueFull
	}

	now := time.Now()
	if job.CreatedAt.IsZero() {
		job.CreatedAt = now
	}
	job.UpdatedAt = now
	job.Status = model.JobPending

	s.jobs[job.ID] = job
	s.idempotencyKeys[idemKey] = job.ID
	s.pendingByType[job.Type] = append(s.pendingByType[job.Type], job)
	s.pendingTotal++
	s.jobCond.Broadcast()

	return job, true, nil
}

func (s *MemoryStore) Get(id string) (model.Job, bool) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()
	job, exists := s.jobs[id]
	return job, exists
}

func (s *MemoryStore) List() []model.Job {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()
	out := make([]model.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		out = append(out, j)
	}
	return out
}

func (s *MemoryStore) Size() int {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()
	return len(s.jobs)
}

func (s *MemoryStore) QueueDepth() int {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()
	return s.pendingTotal
}

// ClaimOne pops the oldest pending job matching `types` (empty = any)
// and issues a lease.
//
// Cancellation: ctx.Done fires -> we spin up a watcher goroutine that
// broadcasts the cond, which wakes the sleeping ClaimOne so it can
// return ctx.Err. Without this, sync.Cond.Wait would park forever.
func (s *MemoryStore) ClaimOne(ctx context.Context, types []string, lease time.Duration) (model.Job, LeaseToken, error) {
	stop := make(chan struct{})
	defer close(stop)
	go func() {
		select {
		case <-ctx.Done():
			s.jobMu.Lock()
			s.jobCond.Broadcast()
			s.jobMu.Unlock()
		case <-stop:
		}
	}()

	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return model.Job{}, "", err
		}

		if job, ok := s.popNextMatching(types); ok {
			job.Status = model.JobRunning
			job.Attempts++
			job.UpdatedAt = time.Now()
			s.jobs[job.ID] = job

			token := LeaseToken(uuid.New().String())
			s.leases[job.ID] = &leaseEntry{
				token:     token,
				expiresAt: time.Now().Add(lease),
				job:       job,
			}
			return job, token, nil
		}

		s.jobCond.Wait()
	}
}

// popNextMatching returns the oldest pending job whose type is in the
// `types` whitelist (empty whitelist matches any type). Caller holds
// jobMu.
func (s *MemoryStore) popNextMatching(types []string) (model.Job, bool) {
	if len(types) == 0 {
		// Any type — pick the queue with the oldest head.
		var picked string
		var pickedTS time.Time
		for t, q := range s.pendingByType {
			if len(q) == 0 {
				continue
			}
			if picked == "" || q[0].UpdatedAt.Before(pickedTS) {
				picked = t
				pickedTS = q[0].UpdatedAt
			}
		}
		if picked == "" {
			return model.Job{}, false
		}
		return s.popHead(picked), true
	}
	// Specific types — pick the whitelist entry with the oldest head.
	var picked string
	var pickedTS time.Time
	for _, t := range types {
		q := s.pendingByType[t]
		if len(q) == 0 {
			continue
		}
		if picked == "" || q[0].UpdatedAt.Before(pickedTS) {
			picked = t
			pickedTS = q[0].UpdatedAt
		}
	}
	if picked == "" {
		return model.Job{}, false
	}
	return s.popHead(picked), true
}

func (s *MemoryStore) popHead(jobType string) model.Job {
	q := s.pendingByType[jobType]
	job := q[0]
	s.pendingByType[jobType] = q[1:]
	s.pendingTotal--
	return job
}

// Ack removes the lease and marks the job completed. The job stays in
// the jobs map for inspection via Get/List.
func (s *MemoryStore) Ack(id string, token LeaseToken) error {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	lease, ok := s.leases[id]
	if !ok || lease.token != token {
		return ErrBadLease
	}
	delete(s.leases, id)

	if job, found := s.jobs[id]; found {
		job.Status = model.JobCompleted
		job.UpdatedAt = time.Now()
		s.jobs[id] = job
	}
	return nil
}

// Nack releases the lease. shouldRetry = Attempts <= MaxRetries after
// the Claim-time increment. Caller is responsible for scheduling the
// retry (via retry.Scheduler) or sending the job to the DLQ.
func (s *MemoryStore) Nack(id string, token LeaseToken, lastErr string) (bool, int, error) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	lease, ok := s.leases[id]
	if !ok || lease.token != token {
		return false, 0, ErrBadLease
	}
	delete(s.leases, id)

	job, found := s.jobs[id]
	if !found {
		return false, 0, fmt.Errorf("job not found: %s", id)
	}
	job.LastError = lastErr
	job.UpdatedAt = time.Now()

	max := job.MaxRetries
	shouldRetry := job.Attempts <= max

	if !shouldRetry {
		job.Status = model.JobFailed
	}
	s.jobs[id] = job
	return shouldRetry, job.Attempts, nil
}

// EnqueueRetry re-pushes a job to the pending queue (from scheduler
// timer fire or lease expiry).
func (s *MemoryStore) EnqueueRetry(job model.Job) error {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	if s.QueueCap > 0 && s.pendingTotal >= s.QueueCap {
		return ErrQueueFull
	}
	job.Status = model.JobPending
	job.UpdatedAt = time.Now()
	s.jobs[job.ID] = job
	s.pendingByType[job.Type] = append(s.pendingByType[job.Type], job)
	s.pendingTotal++
	s.jobCond.Broadcast()
	return nil
}

// ExpiredLeases returns and removes every lease whose deadline is now
// in the past.
func (s *MemoryStore) ExpiredLeases(now time.Time) []model.Job {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	var out []model.Job
	for id, lease := range s.leases {
		if now.After(lease.expiresAt) {
			out = append(out, lease.job)
			delete(s.leases, id)
		}
	}
	return out
}

// --- DLQ (unchanged shape) ---

func (s *MemoryStore) DLQAdd(job model.Job) {
	s.dlqMu.Lock()
	defer s.dlqMu.Unlock()
	s.dlq[job.ID] = job
}

func (s *MemoryStore) DLQGetAll() []model.Job {
	s.dlqMu.RLock()
	defer s.dlqMu.RUnlock()
	out := make([]model.Job, 0, len(s.dlq))
	for _, j := range s.dlq {
		out = append(out, j)
	}
	return out
}

func (s *MemoryStore) DLQClaim(id string) (model.Job, bool) {
	s.dlqMu.Lock()
	defer s.dlqMu.Unlock()
	job, exists := s.dlq[id]
	if !exists {
		return model.Job{}, false
	}
	delete(s.dlq, id)
	return job, true
}

func (s *MemoryStore) DLQClaimAll() []model.Job {
	s.dlqMu.Lock()
	defer s.dlqMu.Unlock()
	out := make([]model.Job, 0, len(s.dlq))
	for _, j := range s.dlq {
		out = append(out, j)
	}
	s.dlq = make(map[string]model.Job)
	return out
}

func (s *MemoryStore) DLQSize() int {
	s.dlqMu.RLock()
	defer s.dlqMu.RUnlock()
	return len(s.dlq)
}

func (s *MemoryStore) DLQRemove(id string) {
	s.dlqMu.Lock()
	defer s.dlqMu.Unlock()
	delete(s.dlq, id)
}
