package handler

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"goflow/model"
	"goflow/service"
	"goflow/store"

	"github.com/gin-gonic/gin"
)

func setupJobHandler() (*gin.Engine, chan model.Job) {
	gin.SetMode(gin.TestMode)

	jobs := make(chan model.Job, 10)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()

	jobService := service.NewJobService(jobs, jobStore)
	jobHandler := NewJobHandler(jobService, dlq)

	router := gin.New()
	router.POST("/jobs", jobHandler.CreateJob)
	router.GET("/jobs/:id", jobHandler.GetJob)
	router.GET("/dlq", jobHandler.GetDLQ)
	router.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)

	return router, jobs
}

func TestCreateJobHandler(t *testing.T) {
	router, jobs := setupJobHandler()

	body := `{"type":"email","payload":"send welcome email"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "handler-test-123")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusCreated, recorder.Code, recorder.Body.String())
	}

	var createdJob model.Job
	if err := json.Unmarshal(recorder.Body.Bytes(), &createdJob); err != nil {
		t.Fatalf("failed to decode response: %v", err)
	}

	if createdJob.ID == "" {
		t.Fatal("expected response to contain a job ID")
	}
	if createdJob.Status != model.JobPending {
		t.Fatalf("expected status %q, got %q",
			model.JobPending, createdJob.Status)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected 1 queued job, got %d", len(jobs))
	}
}

func TestCreateJobMissingIdempotencyKey(t *testing.T) {
	router, jobs := setupJobHandler()

	body := `{"type":"email","payload":"send welcome email"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d",
			http.StatusBadRequest, recorder.Code)
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no queued jobs, got %d", len(jobs))
	}
}

func TestCreateJobInvalidJSON(t *testing.T) {
	router, jobs := setupJobHandler()

	req := httptest.NewRequest(
		http.MethodPost,
		"/jobs",
		bytes.NewBufferString(`{"type":`),
	)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "invalid-json-key")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusBadRequest {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusBadRequest, recorder.Code, recorder.Body.String())
	}
	if len(jobs) != 0 {
		t.Fatalf("expected no queued jobs, got %d", len(jobs))
	}
}

func TestCreateJobQueueFullHandler(t *testing.T) {
	router, jobs := setupJobHandler()

	// Fill the queue so the next creation fails immediately.
	for i := 0; i < cap(jobs); i++ {
		jobs <- model.Job{ID: "existing-job"}
	}

	body := `{"type":"email","payload":"send welcome email"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "queue-full-key")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}

	if len(jobs) != cap(jobs) {
		t.Fatalf("expected queue to remain full (%d jobs), got %d",
			cap(jobs), len(jobs))
	}
}

func TestGetJobHandler(t *testing.T) {
	router, jobs := setupJobHandler()

	body := `{"type":"email","payload":"send welcome email"}`
	req := httptest.NewRequest(http.MethodPost, "/jobs", bytes.NewBufferString(body))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Idempotency-Key", "get-job-test")

	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusCreated {
		t.Fatalf("expected creation status %d, got %d",
			http.StatusCreated, recorder.Code)
	}

	var createdJob model.Job
	if err := json.Unmarshal(recorder.Body.Bytes(), &createdJob); err != nil {
		t.Fatalf("failed to decode created job: %v", err)
	}

	getReq := httptest.NewRequest(
		http.MethodGet,
		"/jobs/"+createdJob.ID,
		nil,
	)
	getRecorder := httptest.NewRecorder()
	router.ServeHTTP(getRecorder, getReq)

	if getRecorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusOK, getRecorder.Code, getRecorder.Body.String())
	}

	var fetchedJob model.Job
	if err := json.Unmarshal(getRecorder.Body.Bytes(), &fetchedJob); err != nil {
		t.Fatalf("failed to decode fetched job: %v", err)
	}

	if fetchedJob.ID != createdJob.ID {
		t.Fatalf("expected job ID %q, got %q", createdJob.ID, fetchedJob.ID)
	}
	if fetchedJob.Type != createdJob.Type {
		t.Fatalf("expected type %q, got %q", createdJob.Type, fetchedJob.Type)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected 1 queued job, got %d", len(jobs))
	}
}

func TestGetMissingJobHandler(t *testing.T) {
	router, _ := setupJobHandler()

	req := httptest.NewRequest(http.MethodGet, "/jobs/nonexistent-job", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusNotFound, recorder.Code, recorder.Body.String())
	}
}

func TestGetDLQHandler(t *testing.T) {
	router, _ := setupJobHandler()

	dlq := store.NewDLQ()
	dlq.Add(model.Job{
		ID:      "failed-job",
		Type:    "email",
		Payload: "send email",
		Status:  model.JobFailed,
	})

	jobService := service.NewJobService(make(chan model.Job, 1), store.NewJobStore())
	jobHandler := NewJobHandler(jobService, dlq)

	router = gin.New()
	router.GET("/dlq", jobHandler.GetDLQ)

	req := httptest.NewRequest(http.MethodGet, "/dlq", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d", http.StatusOK, recorder.Code)
	}

	var jobs []model.Job
	if err := json.Unmarshal(recorder.Body.Bytes(), &jobs); err != nil {
		t.Fatalf("failed to decode DLQ response: %v", err)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected 1 DLQ job, got %d", len(jobs))
	}
	if jobs[0].ID != "failed-job" {
		t.Fatalf("expected failed-job, got %q", jobs[0].ID)
	}
}

func TestRetryDLQJobHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jobs := make(chan model.Job, 1)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()

	failedJob := model.Job{
		ID:       "failed-job",
		Type:     "email",
		Payload:  "send email",
		Status:   model.JobFailed,
		Attempts: 3,
	}
	dlq.Add(failedJob)

	jobService := service.NewJobService(jobs, jobStore)
	jobHandler := NewJobHandler(jobService, dlq)

	router := gin.New()
	router.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)

	req := httptest.NewRequest(http.MethodPost, "/dlq/failed-job/retry", nil)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusOK {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusOK, recorder.Code, recorder.Body.String())
	}

	if len(dlq.GetAll()) != 0 {
		t.Fatal("expected job to be removed from DLQ after retry")
	}

	retriedJob := <-jobs
	if retriedJob.ID != failedJob.ID {
		t.Fatalf("expected job ID %q, got %q", failedJob.ID, retriedJob.ID)
	}
	if retriedJob.Status != model.JobPending {
		t.Fatalf("expected status %q, got %q", model.JobPending, retriedJob.Status)
	}
	if retriedJob.Attempts != 0 {
		t.Fatalf("expected attempts to reset to 0, got %d", retriedJob.Attempts)
	}
}

func TestRetryMissingDLQJobHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jobs := make(chan model.Job, 1)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()

	jobService := service.NewJobService(jobs, jobStore)
	jobHandler := NewJobHandler(jobService, dlq)

	router := gin.New()
	router.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)

	req := httptest.NewRequest(
		http.MethodPost,
		"/dlq/missing-job/retry",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusNotFound {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusNotFound, recorder.Code, recorder.Body.String())
	}
}

func TestRetryDLQJobQueueFullHandler(t *testing.T) {
	gin.SetMode(gin.TestMode)

	jobs := make(chan model.Job, 1)
	jobs <- model.Job{ID: "existing-job"}

	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()
	dlq.Add(model.Job{
		ID:       "failed-job",
		Type:     "email",
		Payload:  "send email",
		Status:   model.JobFailed,
		Attempts: 3,
	})

	jobService := service.NewJobService(jobs, jobStore)
	jobHandler := NewJobHandler(jobService, dlq)

	router := gin.New()
	router.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)

	req := httptest.NewRequest(
		http.MethodPost,
		"/dlq/failed-job/retry",
		nil,
	)
	recorder := httptest.NewRecorder()
	router.ServeHTTP(recorder, req)

	if recorder.Code != http.StatusServiceUnavailable {
		t.Fatalf("expected status %d, got %d: %s",
			http.StatusServiceUnavailable, recorder.Code, recorder.Body.String())
	}

	if len(dlq.GetAll()) != 1 {
		t.Fatal("expected job to remain in DLQ when requeue fails")
	}
	if len(jobs) != 1 {
		t.Fatal("expected existing queue item to remain unchanged")
	}
}
