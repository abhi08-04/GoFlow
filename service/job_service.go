package service

import (
	"goflow/store"
	"goflow/model"
	"github.com/google/uuid"
)


type JobService struct {
	jobs chan <- model.Job
	store *store.JobStore

}

func NewJobService(jobs chan <- model.Job, jobStore *store.JobStore) *JobService {
	return &JobService{
		jobs : jobs,
		store : jobStore,
	}
}

func (s *JobService) CreateJob (req model.CreateJobRequest) model.Job {
	job := model.Job {
		ID : uuid.New().String(),
		Type : req.Type,
		Payload : req.Payload,
		Status : model.JobPending,
		Attempts : 0,
	}
	s.store.Create(job)
	s.jobs <- job
	return job
}

func (s *JobService) GetJob(id string) (model.Job, bool) {
	return s.store.Get(id)
}