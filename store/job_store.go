package store

import (
	"errors"
	"fmt"
	"goflow/model"
	"sync"
)

var ErrQueueFull = errors.New("job queue is full")

type JobStore struct {
	mu              sync.RWMutex
	jobs            map[string]model.Job
	idempotencyKeys map[string]string
}

func NewJobStore() *JobStore {
	return &JobStore{
		jobs:            make(map[string]model.Job),
		idempotencyKeys: make(map[string]string),
	}
}

func (s *JobStore) Create(job model.Job, idemKey string, jobs chan<- model.Job) (model.Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID, exists := s.idempotencyKeys[idemKey]

	if exists {
		existingJob, found := s.jobs[jobID]
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

func (s *JobStore) Update(job model.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
}

func (s *JobStore) EnqueueRetry(job model.Job, jobs chan<- model.Job) bool {
	s.mu.Lock()
	defer s.mu.Unlock()

	select {
	case jobs <- job:
		s.jobs[job.ID] = job
		return true
	default:
		return false
	}
}

func (s *JobStore) Get(id string) (model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	return job, exists
}

// List returns a snapshot of all jobs. The caller receives a copy, so
// mutating the returned slice does not affect the store.
func (s *JobStore) List() []model.Job {
	s.mu.RLock()
	defer s.mu.RUnlock()

	jobs := make([]model.Job, 0, len(s.jobs))
	for _, j := range s.jobs {
		jobs = append(jobs, j)
	}
	return jobs
}

func (s *JobStore) Size() int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	return len(s.jobs)
}
