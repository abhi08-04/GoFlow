package service

import (
	"github.com/google/uuid"
	"goflow/metrics"
	"goflow/model"
	"goflow/store"
)

type JobService struct {
	jobs     chan<- model.Job
	store    *store.JobStore
	registry *metrics.Registry
}

func NewJobService(jobs chan<- model.Job, jobStore *store.JobStore, registry *metrics.Registry) *JobService {
	return &JobService{
		jobs:     jobs,
		store:    jobStore,
		registry: registry,
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

	if created {
		s.registry.RecordCreated(job.Type)
	}

	return job, created, nil
}

func (s *JobService) GetJob(id string) (model.Job, bool) {
	return s.store.Get(id)
}

func (s *JobService) ListJobs() []model.Job {
	return s.store.List()
}

func (s *JobService) RetryJob(job model.Job) bool {
	job.Attempts = 0
	job.Status = model.JobPending

	ok := s.store.EnqueueRetry(job, s.jobs)
	if ok {
		s.registry.RecordRequeued()
	}
	return ok
}
