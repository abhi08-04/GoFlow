package handler

import (
	"errors"
	"net/http"
	"goflow/model"
	"goflow/service"
	"goflow/store"
	"github.com/gin-gonic/gin"
)


type JobHandler struct {
	dlq        *store.DLQ
	jobService *service.JobService
}

func NewJobHandler(jobService *service.JobService, dlq *store.DLQ) *JobHandler {
	return &JobHandler{
		dlq : dlq,
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
	
	idemKey := c.GetHeader("Idempotency-Key")
	if idemKey == "" {
		c.JSON(http.StatusBadRequest, gin.H{
			"error": "Idempotency-Key header is required",
		})
		return
	}

	job, created, err := h.jobService.CreateJob(req, idemKey)
	if err != nil {
		if errors.Is(err, store.ErrQueueFull) {
			c.JSON(http.StatusServiceUnavailable, gin.H{
				"error": "job queue is full, try again later",
			})
			return
		}

		c.JSON(http.StatusConflict, gin.H{
			"error": err.Error(),
		})
		return
	}
	if !created {
		c.JSON(http.StatusOK, job)
	} else {
		c.JSON(http.StatusCreated, job)
	}

}

func (h *JobHandler) GetJob(c *gin.Context) {
	id := c.Param("id")

	job, exists := h.jobService.GetJob(id)

	if !exists {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "job not found",
		})
		return
	}

	c.JSON(http.StatusOK, job)
}

func (h *JobHandler) GetDLQ(c *gin.Context) {
	jobs := h.dlq.GetAll()

	c.JSON(http.StatusOK, jobs)
}


func (h *JobHandler) RetryDLQJob (c *gin.Context) {
	id := c.Param("id")
	var job model.Job
	found := false

	for _, dlqJob := range h.dlq.GetAll() {
		if dlqJob.ID == id {
			job = dlqJob
			found = true
			break
		}
	}

	if !found {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "job not found in DLQ",
		})
		return
	}

	if !h.jobService.RetryJob(job) {
		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "job queue is full",
		})
		return
	}

	h.dlq.Remove(id)

	c.JSON(http.StatusOK, gin.H{
		"message": "job requeued successfully",
		"id":      id,
	})

}