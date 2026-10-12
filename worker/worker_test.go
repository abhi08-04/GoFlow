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
