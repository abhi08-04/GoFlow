package service

import (
	"fmt"
	"goflow/metrics"
	"goflow/model"
	"goflow/store"
	"sync"
	"testing"
)

func TestCreateJob(t *testing.T) {
	jobs := make(chan model.Job, 2)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	req := model.CreateJobRequest{
		Type:    "email",
		Payload: "send welcome email",
	}

	job, created, err := jobService.CreateJob(req, "request-123")
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if !created {
		t.Fatal("expected a new job to be created")
	}
	if job.ID == "" {
		t.Fatal("expected job to have an ID")
	}
	if job.Status != model.JobPending {
		t.Fatalf("expected status %q, got %q", model.JobPending, job.Status)
	}
	if job.Attempts != 0 {
		t.Fatalf("expected 0 attempts, got %d", job.Attempts)
	}

	queuedJob := <-jobs
	if queuedJob.ID != job.ID {
		t.Fatalf("expected queued job ID %q, got %q", job.ID, queuedJob.ID)
	}

	savedJob, exists := jobService.GetJob(job.ID)
	if !exists {
		t.Fatal("expected job to exist in the store")
	}
	if savedJob.ID != job.ID {
		t.Fatalf("expected stored job ID %q, got %q", job.ID, savedJob.ID)
	}
}

func TestCreateJobIdempotency(t *testing.T) {
	jobs := make(chan model.Job, 2)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	req := model.CreateJobRequest{
		Type:    "email",
		Payload: "send welcome email",
	}

	firstJob, firstCreated, err := jobService.CreateJob(req, "request-123")
	if err != nil {
		t.Fatalf("first creation returned an error: %v", err)
	}
	if !firstCreated {
		t.Fatal("expected the first request to create a job")
	}

	secondJob, secondCreated, err := jobService.CreateJob(req, "request-123")
	if err != nil {
		t.Fatalf("duplicate request returned an error: %v", err)
	}
	if secondCreated {
		t.Fatal("expected duplicate request not to create another job")
	}
	if secondJob.ID != firstJob.ID {
		t.Fatalf("expected the same job ID %q, got %q", firstJob.ID, secondJob.ID)
	}
	if len(jobs) != 1 {
		t.Fatalf("expected exactly 1 queued job, got %d", len(jobs))
	}
}

func TestCreateJobQueueFull(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobs <- model.Job{ID: "existing-job"}

	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	req := model.CreateJobRequest{
		Type:    "email",
		Payload: "send welcome email",
	}

	job, created, err := jobService.CreateJob(req, "request-full")

	if err == nil {
		t.Fatal("expected queue-full error")
	}
	if err != store.ErrQueueFull {
		t.Fatalf("expected ErrQueueFull, got %v", err)
	}
	if created {
		t.Fatal("expected job not to be created")
	}
	if job.ID != "" {
		t.Fatalf("expected empty job on failure, got ID %q", job.ID)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected existing queue item to remain, got %d items", len(jobs))
	}

	// Free the queue and retry with the same idempotency key.
	<-jobs

	job, created, err = jobService.CreateJob(req, "request-full")
	if err != nil {
		t.Fatalf("expected retry to succeed after freeing queue: %v", err)
	}
	if !created {
		t.Fatal("expected a new job after queue capacity became available")
	}
	if job.ID == "" {
		t.Fatal("expected a job ID")
	}
}

func TestCreateJobIdempotencyKeyConflict(t *testing.T) {
	jobs := make(chan model.Job, 2)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	firstReq := model.CreateJobRequest{
		Type:    "email",
		Payload: "send welcome email",
	}

	_, created, err := jobService.CreateJob(firstReq, "request-123")
	if err != nil {
		t.Fatalf("first creation returned an error: %v", err)
	}
	if !created {
		t.Fatal("expected first request to create a job")
	}

	conflictingReq := model.CreateJobRequest{
		Type:    "email",
		Payload: "send password reset email",
	}

	_, created, err = jobService.CreateJob(conflictingReq, "request-123")
	if err == nil {
		t.Fatal("expected an error when reusing the key with different content")
	}
	if created {
		t.Fatal("expected conflicting request not to create a job")
	}
	if len(jobs) != 1 {
		t.Fatalf("expected exactly 1 queued job, got %d", len(jobs))
	}
}

func TestConcurrentIdempotentRequests(t *testing.T) {
	const requests = 20

	jobs := make(chan model.Job, requests)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	req := model.CreateJobRequest{
		Type:    "email",
		Payload: "send welcome email",
	}

	var wg sync.WaitGroup
	var mu sync.Mutex
	var createdCount int
	var firstID string
	errs := make(chan error, requests)

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			job, created, err := jobService.CreateJob(req, "shared-key")
			if err != nil {
				errs <- err
				return
			}

			mu.Lock()
			defer mu.Unlock()

			if created {
				createdCount++
			}
			if firstID == "" {
				firstID = job.ID
			} else if job.ID != firstID {
				errs <- fmt.Errorf("expected job ID %q, got %q", firstID, job.ID)
			}
		}()
	}

	wg.Wait()
	close(errs)

	for err := range errs {
		t.Error(err)
	}

	if createdCount != 1 {
		t.Fatalf("expected exactly 1 created job, got %d", createdCount)
	}
	if got := len(jobs); got != 1 {
		t.Fatalf("expected exactly 1 queued job, got %d", got)
	}
}

func TestGetJobMissing(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	job, exists := jobService.GetJob("non-existent-id")

	if exists {
		t.Fatal("expected job not to exist")
	}

	if job.ID != "" {
		t.Fatalf("expected empty job, got ID %q", job.ID)
	}
}

func TestRetryJobSuccess(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	job := model.Job{
		ID:       "job-123",
		Type:     "email",
		Payload:  "send welcome email",
		Status:   model.JobFailed,
		Attempts: 3,
	}

	jobStore.Update(job)

	if !jobService.RetryJob(job) {
		t.Fatal("expected RetryJob to succeed")
	}

	queuedJob := <-jobs

	if queuedJob.ID != job.ID {
		t.Fatalf("expected job ID %q, got %q", job.ID, queuedJob.ID)
	}
	if queuedJob.Status != model.JobPending {
		t.Fatalf("expected status %q, got %q", model.JobPending, queuedJob.Status)
	}
	if queuedJob.Attempts != 0 {
		t.Fatalf("expected 0 attempts, got %d", queuedJob.Attempts)
	}

	savedJob, exists := jobService.GetJob(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}
	if savedJob.Status != model.JobPending {
		t.Fatalf("expected stored status %q, got %q", model.JobPending, savedJob.Status)
	}
	if savedJob.Attempts != 0 {
		t.Fatalf("expected stored attempts to be 0, got %d", savedJob.Attempts)
	}
}

func TestRetryJobQueueFull(t *testing.T) {
	jobs := make(chan model.Job, 1)
	existingJob := model.Job{ID: "existing-job"}
	jobs <- existingJob

	jobStore := store.NewMemoryStore()
	jobService := NewJobService(jobs, jobStore, metrics.New())

	job := model.Job{
		ID:       "job-123",
		Type:     "email",
		Payload:  "send welcome email",
		Status:   model.JobFailed,
		Attempts: 3,
	}
	jobStore.Update(job)

	if jobService.RetryJob(job) {
		t.Fatal("expected RetryJob to fail when queue is full")
	}

	if len(jobs) != 1 {
		t.Fatalf("expected queue to retain 1 job, got %d", len(jobs))
	}

	queuedJob := <-jobs
	if queuedJob.ID != existingJob.ID {
		t.Fatalf("expected existing job %q to remain queued, got %q", existingJob.ID, queuedJob.ID)
	}

	savedJob, exists := jobService.GetJob(job.ID)
	if !exists {
		t.Fatal("expected original job to remain in store")
	}
	if savedJob.Status != model.JobFailed {
		t.Fatalf("expected stored status to remain %q, got %q", model.JobFailed, savedJob.Status)
	}
	if savedJob.Attempts != 3 {
		t.Fatalf("expected stored attempts to remain 3, got %d", savedJob.Attempts)
	}
}
