package handler

import (
	"errors"
	"github.com/gin-gonic/gin"
	"goflow/internal/model"
	"goflow/internal/service"
	"goflow/internal/store"
	"net/http"
)

type JobHandler struct {
	dlq        store.Store
	jobService *service.JobService
}

func NewJobHandler(jobService *service.JobService, dlq store.Store) *JobHandler {
	return &JobHandler{
		dlq:        dlq,
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
	jobs := h.dlq.DLQGetAll()

	c.JSON(http.StatusOK, jobs)
}

func (h *JobHandler) RetryDLQJob(c *gin.Context) {
	id := c.Param("id")

	job, found := h.dlq.DLQClaim(id)
	if !found {
		c.JSON(http.StatusNotFound, gin.H{
			"error": "job not found in DLQ",
		})
		return
	}

	if !h.jobService.RetryJob(job) {
		h.dlq.DLQAdd(job)

		c.JSON(http.StatusServiceUnavailable, gin.H{
			"error": "job queue is full",
		})
		return
	}

	c.JSON(http.StatusOK, gin.H{
		"message": "job requeued successfully",
		"id":      id,
	})
}

// RetryAllDLQ claims the entire DLQ, tries to enqueue every job, and
// puts any that could not fit (queue full) back in the DLQ. The
// response reports both numbers so the UI can show a toast like
// "48 requeued, 2 rejected (queue full)".
func (h *JobHandler) RetryAllDLQ(c *gin.Context) {
	jobs := h.dlq.DLQClaimAll()

	var requeued, rejected int
	for _, j := range jobs {
		if h.jobService.RetryJob(j) {
			requeued++
			continue
		}
		h.dlq.DLQAdd(j)
		rejected++
	}

	c.JSON(http.StatusOK, gin.H{
		"requeued": requeued,
		"rejected": rejected,
	})
}

func (h *JobHandler) PurgeDLQ(c *gin.Context) {
	jobs := h.dlq.DLQClaimAll()
	c.JSON(http.StatusOK, gin.H{"purged": len(jobs)})
}

func (h *JobHandler) DeleteDLQJob(c *gin.Context) {
	id := c.Param("id")
	if _, found := h.dlq.DLQClaim(id); !found {
		c.JSON(http.StatusNotFound, gin.H{"error": "job not found in DLQ"})
		return
	}
	c.JSON(http.StatusOK, gin.H{"deleted": id})
}

func (h *JobHandler) ListJobs(c *gin.Context) {
	status := c.Query("status")
	jobType := c.Query("type")

	jobs := h.jobService.ListJobs()
	if status != "" || jobType != "" {
		filtered := jobs[:0]
		for _, j := range jobs {
			if status != "" && j.Status != status {
				continue
			}
			if jobType != "" && j.Type != jobType {
				continue
			}
			filtered = append(filtered, j)
		}
		jobs = filtered
	}

	c.JSON(http.StatusOK, jobs)
}
