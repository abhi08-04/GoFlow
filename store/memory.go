package store

import (
	"fmt"
	"sync"

	"goflow/model"
)

// MemoryStore is the default in-process Store. It keeps two logically
// separate critical sections — the job map and the DLQ map — each
// guarded by its own RWMutex, so a slow DLQ scan does not block
// producer enqueues on the hot path.
type MemoryStore struct {
	jobMu           sync.RWMutex
	jobs            map[string]model.Job
	idempotencyKeys map[string]string

	dlqMu sync.RWMutex
	dlq   map[string]model.Job
}

func NewMemoryStore() *MemoryStore {
	return &MemoryStore{
		jobs:            make(map[string]model.Job),
		idempotencyKeys: make(map[string]string),
		dlq:             make(map[string]model.Job),
	}
}

// --- jobs ---

func (s *MemoryStore) Create(job model.Job, idemKey string, jobs chan<- model.Job) (model.Job, bool, error) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	if existingID, exists := s.idempotencyKeys[idemKey]; exists {
		existingJob, found := s.jobs[existingID]
		if !found {
			return model.Job{}, false, fmt.Errorf("Idempotency key exists but job was not found")
		}
		if existingJob.Type != job.Type || existingJob.Payload != job.Payload {
			return model.Job{}, false, fmt.Errorf("Idempotency key reused with different request content")
		}
		return existingJob, false, nil
	}

	s.jobs[job.ID] = job
	s.idempotencyKeys[idemKey] = job.ID

	select {
	case jobs <- job:
		return job, true, nil
	default:
		delete(s.jobs, job.ID)
		delete(s.idempotencyKeys, idemKey)
		return model.Job{}, false, ErrQueueFull
	}
}

func (s *MemoryStore) Update(job model.Job) {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	s.jobs[job.ID] = job
}

func (s *MemoryStore) EnqueueRetry(job model.Job, jobs chan<- model.Job) bool {
	s.jobMu.Lock()
	defer s.jobMu.Unlock()

	select {
	case jobs <- job:
		s.jobs[job.ID] = job
		return true
	default:
		return false
	}
}

func (s *MemoryStore) Get(id string) (model.Job, bool) {
	s.jobMu.RLock()
	defer s.jobMu.RUnlock()

	job, exists := s.jobs[id]
	return job, exists
}

func (s *MemoryStore) List() []model.Job {
	s.jobMu.RLock()
	defer s.jobMu.RUnlock()

	jobs := make([]model.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *MemoryStore) Size() int {
	s.jobMu.RLock()
	defer s.jobMu.RUnlock()

	return len(s.jobs)
}

// --- DLQ ---

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
