package service

import (
	"goflow/model"
	"github.com/google/uuid"
)


type JobService struct {

}

func NewJobService() *JobService {
	return &JobService{}
}

func (s *JobService) CreateJob (req model.CreateJobRequest) model.Job {
	return model.Job {
		ID : uuid.New().String(),
		Type : req.Type,
		Payload : req.Payload,
		Status : "pending",
	}
}