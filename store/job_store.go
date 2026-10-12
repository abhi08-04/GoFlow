package store

import (
	"fmt"
	"sync"
	"goflow/model"
)

type JobStore struct {
	mu sync.RWMutex
	jobs map[string]model.Job
	idempotencyKeys map[string]string
}

func NewJobStore() *JobStore {
	return &JobStore {
		jobs : make(map[string]model.Job),
		idempotencyKeys: make(map[string]string),
	}
}

func (s *JobStore) Create(job model.Job, idemKey string) (model.Job, bool, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	jobID, exists := s.idempotencyKeys[idemKey]

	if exists {
		existingJob, found := s.jobs[jobID]
		if !found {
			return model.Job{}, false, fmt.Errorf("Idempotency key exists but job was not found")
		}

		if existingJob.Type != job.Type || existingJob.Payload != job.Payload{
			return model.Job{}, false, fmt.Errorf("Idempotency key reused with different request content")
		}

		return existingJob, false, nil
	} else {
		s.jobs[job.ID] = job
		s.idempotencyKeys[idemKey] = job.ID
		return job, true, nil
	}
}


func (s *JobStore) Update(job model.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
}

func (s *JobStore) Get(id string) (model.Job, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	job, exists := s.jobs[id]
	return job, exists
}