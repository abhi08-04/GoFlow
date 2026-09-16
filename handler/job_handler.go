package handler

import (
	"net/http"
	"goflow/model"
	"goflow/service"
	"github.com/gin-gonic/gin"
)


type JobHandler struct {
	jobService *service.JobService
}

func NewJobHandler(jobService *service.JobService) *JobHandler {
	return &JobHandler{
		jobService: jobService,
	}
}

func (h *JobHandler) CreateJob(c *gin.Context) {
	var req model.CreateJobRequest

	if err := c.ShouldBindJSON(&req); err != nil {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": err.Error(),
		})
		return
	}

	job := h.jobService.CreateJob(req)

	c.JSON(http.StatusCreated, job)
}