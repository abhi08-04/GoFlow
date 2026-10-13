package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"goflow/metrics"
	"goflow/model"
	"goflow/retry"
	"goflow/store"
)

func TestCancelledRetryMarksJobFailedAndAddsToDLQ(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	scheduler := retry.NewScheduler(context.Background(), jobs, metrics.New())
	defer scheduler.Stop()

	job := model.Job{
		ID:     "test-job",
		Status: model.JobPending,
	}

	jobStore.Update(job)

	w := NewWorker(
		1,
		jobStore,
		scheduler,
		1,
		time.Hour,
		time.Second,
		metrics.New(),
	)

	w.execute = func(model.Job) error {
		return errors.New("simulated failure")
	}

	w.processJob(job)

	scheduler.Stop()

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}

	if savedJob.Status != model.JobFailed {
		t.Fatalf("expected status %v, got %v",
			model.JobFailed, savedJob.Status)
	}

	dlqJobs := jobStore.DLQGetAll()
	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(dlqJobs))
	}

	if dlqJobs[0].ID != job.ID {
		t.Fatalf("expected DLQ job ID %s, got %s",
			job.ID, dlqJobs[0].ID)
	}

	select {
	case received := <-jobs:
		t.Fatalf("expected cancelled retry not to be enqueued, got %s",
			received.ID)
	default:
	}
}

func TestSuccessfulJobExecution(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	scheduler := retry.NewScheduler(context.Background(), jobs, metrics.New())
	defer scheduler.Stop()

	job := model.Job{
		ID:     "successful-job",
		Status: model.JobPending,
	}
	jobStore.Update(job)

	w := NewWorker(
		1,
		jobStore,
		scheduler,
		3,
		time.Second,
		time.Second,
		metrics.New(),
	)

	// Simulate successful execution without waiting seven seconds.
	w.execute = func(model.Job) error {
		return nil
	}

	w.processJob(job)

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}

	if savedJob.Status != model.JobCompleted {
		t.Fatalf("expected status %v, got %v",
			model.JobCompleted, savedJob.Status)
	}

	if savedJob.Attempts != 1 {
		t.Fatalf("expected 1 attempt, got %d", savedJob.Attempts)
	}

	if len(jobStore.DLQGetAll()) != 0 {
		t.Fatal("expected no jobs in DLQ after successful execution")
	}

	if len(jobs) != 0 {
		t.Fatal("expected no retry to be enqueued after success")
	}
}

func TestRetryExhaustionAddsJobToDLQ(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	scheduler := retry.NewScheduler(context.Background(), jobs, metrics.New())
	defer scheduler.Stop()

	job := model.Job{
		ID:       "exhausted-job",
		Status:   model.JobRunning,
		Attempts: 1,
	}
	jobStore.Update(job)

	w := NewWorker(
		1,
		jobStore,
		scheduler,
		1,
		time.Second,
		time.Second,
		metrics.New(),
	)

	w.execute = func(model.Job) error {
		return errors.New("simulated final failure")
	}

	w.processJob(job)

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}

	if savedJob.Status != model.JobFailed {
		t.Fatalf("expected status %v, got %v",
			model.JobFailed, savedJob.Status)
	}

	if savedJob.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", savedJob.Attempts)
	}

	dlqJobs := jobStore.DLQGetAll()
	if len(dlqJobs) != 1 {
		t.Fatalf("expected 1 job in DLQ, got %d", len(dlqJobs))
	}

	if dlqJobs[0].ID != job.ID {
		t.Fatalf("expected DLQ job ID %s, got %s",
			job.ID, dlqJobs[0].ID)
	}

	if len(jobs) != 0 {
		t.Fatal("expected no retry to be enqueued after retry exhaustion")
	}
}

func TestFailedJobRetryIsEnqueued(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewMemoryStore()
	scheduler := retry.NewScheduler(context.Background(), jobs, metrics.New())
	defer scheduler.Stop()

	job := model.Job{
		ID:     "retry-job",
		Status: model.JobPending,
	}
	jobStore.Update(job)

	w := NewWorker(
		1,
		jobStore,
		scheduler,
		3,
		10*time.Millisecond,
		time.Second,
		metrics.New(),
	)

	w.execute = func(model.Job) error {
		return errors.New("simulated failure")
	}

	w.processJob(job)

	select {
	case retriedJob := <-jobs:
		if retriedJob.ID != job.ID {
			t.Fatalf("expected job ID %s, got %s",
				job.ID, retriedJob.ID)
		}

		if retriedJob.Status != model.JobPending {
			t.Fatalf("expected status %v, got %v",
				model.JobPending, retriedJob.Status)
		}

		if retriedJob.Attempts != 1 {
			t.Fatalf("expected 1 attempt before retry, got %d",
				retriedJob.Attempts)
		}

	case <-time.After(time.Second):
		t.Fatal("timed out waiting for retry to be enqueued")
	}

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}

	if savedJob.Status != model.JobPending {
		t.Fatalf("expected stored status %v, got %v",
			model.JobPending, savedJob.Status)
	}

	if len(jobStore.DLQGetAll()) != 0 {
		t.Fatal("expected job not to be in DLQ while retries remain")
	}
}

func TestRetryEventuallyCompletesJob(t *testing.T) {
	jobs := make(chan model.Job, 2)
	jobStore := store.NewMemoryStore()
	scheduler := retry.NewScheduler(context.Background(), jobs, metrics.New())
	defer scheduler.Stop()

	job := model.Job{
		ID:     "eventual-success-job",
		Status: model.JobPending,
	}
	jobStore.Update(job)

	w := NewWorker(
		1,
		jobStore,
		scheduler,
		3,
		10*time.Millisecond,
		time.Second,
		metrics.New(),
	)

	var executions int
	w.execute = func(model.Job) error {
		executions++
		if executions == 1 {
			return errors.New("temporary failure")
		}
		return nil
	}

	// First attempt fails and schedules a retry.
	w.processJob(job)

	select {
	case retriedJob := <-jobs:
		// The scheduled retry is now processed as a second attempt.
		w.processJob(retriedJob)
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for scheduled retry")
	}

	savedJob, exists := jobStore.Get(job.ID)
	if !exists {
		t.Fatal("expected job to exist in store")
	}
	if savedJob.Status != model.JobCompleted {
		t.Fatalf("expected status %q, got %q",
			model.JobCompleted, savedJob.Status)
	}
	if savedJob.Attempts != 2 {
		t.Fatalf("expected 2 attempts, got %d", savedJob.Attempts)
	}
	if executions != 2 {
		t.Fatalf("expected 2 executions, got %d", executions)
	}
	if len(jobStore.DLQGetAll()) != 0 {
		t.Fatal("expected completed job not to be in DLQ")
	}
}
