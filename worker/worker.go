package worker

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goflow/model"
)

type Worker struct {
	ID int
}

func NewWorker (id int) *Worker {
	return &Worker {
		ID : id,
	}
}


func (w *Worker) Start (jobs <- chan model.Job, wg *sync.WaitGroup){
	defer wg.Done()

	for job := range jobs {
		fmt.Println("Worker :", w.ID, "Processing :", job.ID)

		err := w.executeJob(job)
		if err != nil {
			fmt.Println("Worker :", w.ID, "Failed :", job.ID, err)
			continue
		}

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
		case <- time.After(3 * time.Second):
			fmt.Println("Job completed :", job.ID)
			return nil

		case <- ctx.Done():
			return ctx.Err()
	}
	
}