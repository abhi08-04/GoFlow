package service

import (
	"goflow/model"
	"github.com/google/uuid"
)


type JobService struct {
	jobs chan <- model.Job

}

func NewJobService(jobs chan <- model.Job) *JobService {
	return &JobService{
		jobs : jobs,
	}
}

func (s *JobService) CreateJob (req model.CreateJobRequest) model.Job {
	job := model.Job {
		ID : uuid.New().String(),
		Type : req.Type,
		Payload : req.Payload,
		Status : "pending",
	}
	s.jobs <- job
	return job
}