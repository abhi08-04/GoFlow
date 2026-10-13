package retry

import (
	"context"
	"fmt"
	"goflow/metrics"
	"goflow/model"
	"sync"
	"testing"
	"time"
)

func TestStopCancelsPendingRetry(t *testing.T) {
	jobs := make(chan model.Job, 1)
	scheduler := NewScheduler(context.Background(), jobs, metrics.New())

	callbackCalled := make(chan struct{})

	job := model.Job{
		ID:     "test-job",
		Status: model.JobPending,
	}

	scheduled := scheduler.Schedule(job, 5*time.Second, func() {
		close(callbackCalled)
	})

	if !scheduled {
		t.Fatal("expected scheduler to accept retry")
	}

	scheduler.Stop()

	select {
	case <-callbackCalled:
	case <-time.After(time.Second):
		t.Fatal("expected cancellation callback to be called")
	}

	select {
	case received := <-jobs:
		t.Fatalf("expected no retry, but received job %s", received.ID)
	default:
	}
}

func TestScheduledRetryIsEnqueued(t *testing.T) {
	jobs := make(chan model.Job, 1)
	scheduler := NewScheduler(context.Background(), jobs, metrics.New())

	job := model.Job{
		ID:     "test-job",
		Status: model.JobRunning,
	}

	scheduler.Schedule(job, 10*time.Millisecond, func() {})

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
	scheduler := NewScheduler(context.Background(), jobs, metrics.New())

	var callers sync.WaitGroup
	start := make(chan struct{})

	for i := 0; i < 100; i++ {
		callers.Add(1)

		go func(i int) {
			defer callers.Done()
			<-start

			scheduler.Schedule(
				model.Job{
					ID:     fmt.Sprintf("job-%d", i),
					Status: model.JobPending,
				},
				time.Second,
				func() {},
			)
		}(i)
	}

	close(start)

	go scheduler.Stop()

	callers.Wait()
	scheduler.Stop()
}

func TestConcurrentStop(t *testing.T) {
	jobs := make(chan model.Job, 100)
	scheduler := NewScheduler(context.Background(), jobs, metrics.New())

	for i := 0; i < 100; i++ {
		scheduler.Schedule(
			model.Job{
				ID:     fmt.Sprintf("job-%d", i),
				Status: model.JobPending,
			},
			time.Second,
			func() {},
		)
	}

	var wg sync.WaitGroup

	for i := 0; i < 10; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			scheduler.Stop()
		}()
	}

	wg.Wait()
}
