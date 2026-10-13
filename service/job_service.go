package service

import (
	"github.com/google/uuid"
	"goflow/model"
	"goflow/store"
)

type JobService struct {
	jobs  chan<- model.Job
	store *store.JobStore
}

func NewJobService(jobs chan<- model.Job, jobStore *store.JobStore) *JobService {
	return &JobService{
		jobs:  jobs,
		store: jobStore,
	}
}

func (s *JobService) CreateJob(req model.CreateJobRequest, idemKey string) (model.Job, bool, error) {
	job := model.Job{
		ID:       uuid.New().String(),
		Type:     req.Type,
		Payload:  req.Payload,
		Status:   model.JobPending,
		Attempts: 0,
	}
	job, created, err := s.store.Create(job, idemKey, s.jobs)

	if err != nil {
		return model.Job{}, created, err
	}

	return job, created, nil
}

func (s *JobService) GetJob(id string) (model.Job, bool) {
	return s.store.Get(id)
}

func (s *JobService) RetryJob(job model.Job) bool {
	job.Attempts = 0
	job.Status = model.JobPending

	return s.store.EnqueueRetry(job, s.jobs)
}
