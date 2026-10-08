package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goflow/model"
	"goflow/store"
)

type Worker struct {
	ID int
	store *store.JobStore
}

func NewWorker (id int, jobStore *store.JobStore) *Worker {
	return &Worker {
		ID : id,
		store : jobStore,
	}
}


func (w *Worker) Start (jobs <- chan model.Job, wg *sync.WaitGroup){
	defer wg.Done()

	for job := range jobs {
		job.Status = model.JobRunning
		w.store.Update(job)

		fmt.Println("Worker :", w.ID, "Processing :", job.ID)

		err := w.executeJob(job)
		if err != nil {
			job.Status = model.JobFailed
			w.store.Update(job)
			fmt.Println("Worker :", w.ID, "Failed :", job.ID, err)
			continue
		}

		job.Status = model.JobCompleted
		w.store.Update(job)

		fmt.Println("Worker :", w.ID, "Finished :", job.ID)
	}
}

func (w *Worker) executeJob(job model.Job) error {
	ctx, cancel := context.WithTimeout (
		context.Background(),
		5 * time.Second,
	)

	defer cancel()

	select {
		case <- time.After(4 * time.Second):
			fmt.Println("Job completed :", job.ID)
			return nil

		case <- ctx.Done():
			return ctx.Err()
	}
	
}