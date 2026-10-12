package retry


import (
	"sync"
	"fmt"
	"testing"
	"time"
	"context"
	"goflow/model"
)

func TestStopCancelsPendingRetry(t *testing.T) {
	jobs := make(chan model.Job, 1)

	scheduler := NewScheduler(context.Background(), jobs)

	job := model.Job{
		ID:     "test-job",
		Status: model.JobPending,
	}

	scheduler.Schedule(job, 5*time.Second)
	scheduler.Stop()

	select {
	case received := <-jobs:
		t.Fatalf("expected no retry, but received job %s", received.ID)
	default:
	}
}


func TestScheduledRetryIsEnqueued(t *testing.T) {
	jobs := make(chan model.Job, 1)
	scheduler := NewScheduler(context.Background(), jobs)

	job := model.Job{
		ID:     "test-job",
		Status: model.JobRunning,
	}

	scheduler.Schedule(job, 10*time.Millisecond)

	select {
	case received := <-jobs:
		if received.ID != job.ID {
			t.Fatalf("expected job ID %s, got %s", job.ID, received.ID)
		}

		if received.Status != model.JobPending {
			t.Fatalf("expected status pending, got %s", received.Status)
		}

	case <-time.After(1 * time.Second):
		t.Fatal("timed out waiting for scheduled retry")
	}

	scheduler.Stop()
}


func TestConcurrentScheduleAndStop(t *testing.T) {
	jobs := make(chan model.Job, 100)
	scheduler := NewScheduler(context.Background(), jobs)

	var callers sync.WaitGroup

	for i := 0; i < 100; i++ {
		callers.Add(1)

		go func(i int) {
			defer callers.Done()

			scheduler.Schedule(
				model.Job{
					ID:     fmt.Sprintf("job-%d", i),
					Status: model.JobPending,
				},
				time.Second,
			)
		}(i)
	}

	callers.Wait()
	scheduler.Stop()
}
