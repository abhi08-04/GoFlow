package store

import (
	"sync"
	"goflow/model"
)

type JobStore struct {
	mu sync.RWMutex
	jobs map[string]model.Job
}

func NewJobStore() *JobStore {
	return &JobStore {
		jobs : make(map[string]model.Job),
	}
}

func (s *JobStore) Create(job model.Job) {
	s.mu.Lock()
	defer s.mu.Unlock()

	s.jobs[job.ID] = job
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