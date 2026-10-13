package store

import (
	"errors"
	"sync"
	"testing"

	"goflow/model"
)

func TestJobStoreCreate(t *testing.T) {
	s := NewJobStore()
	jobs := make(chan model.Job, 1)

	job := model.Job{
		ID:      "job-1",
		Type:    "email",
		Payload: "test@example.com",
		Status:  model.JobPending,
	}

	got, created, err := s.Create(job, "key-1", jobs)

	if err != nil {
		t.Fatalf("Create() error = %v", err)
	}
	if !created {
		t.Fatal("Create() created = false, want true")
	}
	if got != job {
		t.Fatalf("Create() job = %+v, want %+v", got, job)
	}

	queued := <-jobs
	if queued != job {
		t.Fatalf("queued job = %+v, want %+v", queued, job)
	}

	stored, exists := s.Get(job.ID)
	if !exists {
		t.Fatal("job should exist in store")
	}
	if stored != job {
		t.Fatalf("stored job = %+v, want %+v", stored, job)
	}
}

func TestJobStoreGetMissing(t *testing.T) {
	s := NewJobStore()

	_, exists := s.Get("missing")

	if exists {
		t.Fatal("Get() exists = true, want false")
	}
}

func TestJobStoreUpdate(t *testing.T) {
	s := NewJobStore()

	job := model.Job{
		ID:     "job-1",
		Type:   "email",
		Status: model.JobPending,
	}
	s.Update(job)

	job.Status = model.JobRunning
	job.Attempts = 1
	s.Update(job)

	got, exists := s.Get(job.ID)
	if !exists {
		t.Fatal("updated job should exist")
	}
	if got.Status != model.JobRunning || got.Attempts != 1 {
		t.Fatalf("updated job = %+v", got)
	}
}

func TestJobStoreCreateIdempotent(t *testing.T) {
	s := NewJobStore()
	jobs := make(chan model.Job, 2)

	job := model.Job{
		ID:      "job-1",
		Type:    "email",
		Payload: "test@example.com",
		Status:  model.JobPending,
	}

	first, created, err := s.Create(job, "key-1", jobs)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}
	if !created {
		t.Fatal("first Create() should create the job")
	}

	duplicate := job
	duplicate.ID = "job-2"

	got, created, err := s.Create(duplicate, "key-1", jobs)
	if err != nil {
		t.Fatalf("duplicate Create() error = %v", err)
	}
	if created {
		t.Fatal("duplicate Create() should return created = false")
	}
	if got != first {
		t.Fatalf("duplicate job = %+v, want original %+v", got, first)
	}
	if len(jobs) != 1 {
		t.Fatalf("queue length = %d, want 1", len(jobs))
	}
}

func TestJobStoreCreateIdempotencyConflict(t *testing.T) {
	s := NewJobStore()
	jobs := make(chan model.Job, 2)

	original := model.Job{
		ID:      "job-1",
		Type:    "email",
		Payload: "first@example.com",
		Status:  model.JobPending,
	}

	_, _, err := s.Create(original, "key-1", jobs)
	if err != nil {
		t.Fatalf("first Create() error = %v", err)
	}

	different := original
	different.ID = "job-2"
	different.Payload = "second@example.com"

	_, created, err := s.Create(different, "key-1", jobs)
	if err == nil {
		t.Fatal("expected idempotency conflict error")
	}
	if created {
		t.Fatal("conflicting request must not create a job")
	}
	if len(jobs) != 1 {
		t.Fatalf("queue length = %d, want 1", len(jobs))
	}
}

func TestJobStoreCreateQueueFullRollback(t *testing.T) {
	s := NewJobStore()
	jobs := make(chan model.Job)

	job := model.Job{
		ID:      "job-1",
		Type:    "email",
		Payload: "test@example.com",
		Status:  model.JobPending,
	}

	_, created, err := s.Create(job, "key-1", jobs)

	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("Create() error = %v, want %v", err, ErrQueueFull)
	}
	if created {
		t.Fatal("Create() created = true, want false")
	}

	if _, exists := s.Get(job.ID); exists {
		t.Fatal("job should have been rolled back")
	}

	// The same key should be reusable after rollback.
	jobs = make(chan model.Job, 1)
	_, created, err = s.Create(job, "key-1", jobs)

	if err != nil {
		t.Fatalf("Create() after rollback error = %v", err)
	}
	if !created {
		t.Fatal("Create() after rollback should create the job")
	}
}

func TestJobStoreConcurrentIdempotentCreate(t *testing.T) {
	s := NewJobStore()
	jobs := make(chan model.Job, 20)

	const requests = 20

	var wg sync.WaitGroup
	var mu sync.Mutex
	createdCount := 0
	errorsFound := 0

	for i := 0; i < requests; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()

			job := model.Job{
				ID:      "job-1",
				Type:    "email",
				Payload: "test@example.com",
				Status:  model.JobPending,
			}

			_, created, err := s.Create(job, "shared-key", jobs)

			mu.Lock()
			defer mu.Unlock()

			if err != nil {
				errorsFound++
				return
			}
			if created {
				createdCount++
			}
		}()
	}

	wg.Wait()

	if errorsFound != 0 {
		t.Fatalf("got %d unexpected errors", errorsFound)
	}
	if createdCount != 1 {
		t.Fatalf("created count = %d, want 1", createdCount)
	}
	if len(jobs) != 1 {
		t.Fatalf("queue length = %d, want 1", len(jobs))
	}
}

func TestEnqueueRetrySuccess(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := NewJobStore()

	job := model.Job{
		ID:       "job-123",
		Type:     "email",
		Payload:  "send welcome email",
		Status:   model.JobPending,
		Attempts: 0,
	}

	if !jobStore.EnqueueRetry(job, jobs) {
		t.Fatal("expected retry enqueue to succeed")
	}

	if len(jobs) != 1 {
		t.Fatalf("expected 1 queued job, got %d", len(jobs))
	}

	queuedJob := <-jobs
	if queuedJob.ID != job.ID {
		t.Fatalf("expected queued ID %q, got %q", job.ID, queuedJob.ID)
	}

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}
	if savedJob.Status != model.JobPending {
		t.Fatalf("expected status %q, got %q", model.JobPending, savedJob.Status)
	}
}

func TestEnqueueRetryQueueFull(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobs <- model.Job{ID: "existing-job"}

	jobStore := NewJobStore()

	originalJob := model.Job{
		ID:       "job-123",
		Type:     "email",
		Payload:  "send welcome email",
		Status:   model.JobFailed,
		Attempts: 3,
	}
	jobStore.Update(originalJob)

	retryJob := originalJob
	retryJob.Status = model.JobPending
	retryJob.Attempts = 0

	if jobStore.EnqueueRetry(retryJob, jobs) {
		t.Fatal("expected enqueue to fail when queue is full")
	}

	savedJob, exists := jobStore.Get(originalJob.ID)
	if !exists {
		t.Fatal("expected original job to remain in store")
	}
	if savedJob.Status != model.JobFailed {
		t.Fatalf("expected status to remain %q, got %q", model.JobFailed, savedJob.Status)
	}
	if savedJob.Attempts != 3 {
		t.Fatalf("expected attempts to remain 3, got %d", savedJob.Attempts)
	}

	if len(jobs) != 1 {
		t.Fatalf("expected existing queue item to remain, got %d", len(jobs))
	}
}

func TestCreateIdempotencyKeyJobMissing(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := NewJobStore()

	jobStore.idempotencyKeys["request-123"] = "missing-job"

	job := model.Job{
		ID:       "new-job",
		Type:     "email",
		Payload:  "send welcome email",
		Status:   model.JobPending,
		Attempts: 0,
	}

	_, created, err := jobStore.Create(job, "request-123", jobs)

	if err == nil {
		t.Fatal("expected error when idempotency key points to a missing job")
	}
	if created {
		t.Fatal("expected created to be false")
	}
	if len(jobs) != 0 {
		t.Fatalf("expected queue to remain empty, got %d jobs", len(jobs))
	}
}
