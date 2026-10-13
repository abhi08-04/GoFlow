package service

import (
	"github.com/google/uuid"

	"goflow/internal/metrics"
	"goflow/internal/model"
	"goflow/internal/store"
)

// JobService is the broker's producer-side façade. Consumers claim via
// the gRPC server, not through here.
type JobService struct {
	store      store.Store
	registry   *metrics.Registry
	maxRetries int
}

func NewJobService(st store.Store, registry *metrics.Registry, maxRetries int) *JobService {
	return &JobService{store: st, registry: registry, maxRetries: maxRetries}
}

func (s *JobService) CreateJob(req model.CreateJobRequest, idemKey string) (model.Job, bool, error) {
	maxRetries := req.MaxRetries
	if maxRetries == 0 {
		maxRetries = s.maxRetries
	}
	job := model.Job{
		ID:         uuid.New().String(),
		Type:       req.Type,
		Payload:    req.Payload,
		Status:     model.JobPending,
		Attempts:   0,
		MaxRetries: maxRetries,
	}
	job, created, err := s.store.Create(job, idemKey)
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

// RetryJob is called by the DLQ-requeue path. It re-enqueues the job
// as a fresh attempt (resets Attempts and LastError).
func (s *JobService) RetryJob(job model.Job) bool {
	job.Attempts = 0
	job.LastError = ""
	if job.MaxRetries == 0 {
		job.MaxRetries = s.maxRetries
	}
	if err := s.store.EnqueueRetry(job); err != nil {
		return false
	}
	s.registry.RecordRequeued()
	return true
}
