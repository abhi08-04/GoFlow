package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goflow/model"
	"goflow/store"
	"goflow/retry"
)

type Worker struct {
	ID int
	store *store.JobStore
	scheduler *retry.Scheduler
	maxRetries  int
	baseBackoff time.Duration
	jobTimeout  time.Duration
	dlq *store.DLQ
}

func NewWorker (id int, jobStore *store.JobStore, scheduler *retry.Scheduler, maxRetries int, baseBackoff time.Duration, jobTimeout time.Duration, dlq *store.DLQ) *Worker {
	return &Worker {
		ID : id,
		store : jobStore,
		scheduler : scheduler,
		maxRetries : maxRetries,
		baseBackoff : baseBackoff,
		jobTimeout : jobTimeout,
		dlq	: dlq,
	}
}


func (w *Worker) Start(jobs <-chan model.Job, wg *sync.WaitGroup) {
	defer wg.Done()

	for job := range jobs {
		w.processJob(job)
	}
}

func (w *Worker) executeJob(job model.Job) error {
	ctx, cancel := context.WithTimeout (
		context.Background(),
		w.jobTimeout,
	)

	defer cancel()

	select {
		case <- time.After(7 * time.Second):
			fmt.Println("Job completed :", job.ID)
			return nil

		case <- ctx.Done():
			return ctx.Err()
	}
	
}


func (w *Worker) processJob(job model.Job) {
	job.Attempts++

	job.Status = model.JobRunning
	w.store.Update(job)

	fmt.Println(
		"Worker :", w.ID,
		"Processing :", job.ID,
		"Attempt :", job.Attempts,
	)

	err := w.executeJob(job)

	if err == nil {
		job.Status = model.JobCompleted
		w.store.Update(job)

		fmt.Println(
			"Worker :", w.ID,
			"Finished :", job.ID,
		)

		return
	}

	fmt.Println(
		"Worker :", w.ID,
		"Failed :", job.ID,
		"Attempt :", job.Attempts,
		err,
	)

	if job.Attempts <= w.maxRetries {
		delay := w.baseBackoff * time.Duration(1<<(job.Attempts-1))

		job.Status = model.JobPending
		w.store.Update(job)

		scheduled := w.scheduler.Schedule(job, delay, func() {
			job.Status = model.JobFailed
			w.store.Update(job)
			w.dlq.Add(job)

			fmt.Println("Retry cancelled during shutdown:", job.ID)
		})

		if scheduled {
			fmt.Println("Retry scheduled:", job.ID, "after", delay)
			return
		}

		job.Status = model.JobFailed
		w.store.Update(job)
		w.dlq.Add(job)

		fmt.Println("Retry rejected; scheduler stopping:", job.ID)
		return
	}

	job.Status = model.JobFailed
	w.store.Update(job)
	w.dlq.Add(job)

	fmt.Println(
		"Job permanently failed :",
		job.ID,
	)
}