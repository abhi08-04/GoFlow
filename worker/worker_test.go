package worker

import (
	"context"
	"errors"
	"testing"
	"time"

	"goflow/model"
	"goflow/retry"
	"goflow/store"
)

func TestCancelledRetryMarksJobFailedAndAddsToDLQ(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()
	scheduler := retry.NewScheduler(context.Background(), jobs)
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
		dlq,
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

	dlqJobs := dlq.GetAll()
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
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()
	scheduler := retry.NewScheduler(context.Background(), jobs)
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
		dlq,
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

	if len(dlq.GetAll()) != 0 {
		t.Fatal("expected no jobs in DLQ after successful execution")
	}

	if len(jobs) != 0 {
		t.Fatal("expected no retry to be enqueued after success")
	}
}

func TestRetryExhaustionAddsJobToDLQ(t *testing.T) {
	jobs := make(chan model.Job, 1)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()
	scheduler := retry.NewScheduler(context.Background(), jobs)
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
		dlq,
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

	dlqJobs := dlq.GetAll()
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
