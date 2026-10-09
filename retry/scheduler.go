package retry

import (
	"context"
	"time"

	"goflow/model"
)

type Scheduler struct {
	jobs chan<- model.Job
	ctx  context.Context
}

func NewScheduler(ctx context.Context, jobs chan<- model.Job) *Scheduler {
	return &Scheduler{
		jobs: jobs,
		ctx:  ctx,
	}
}

func (s *Scheduler) Schedule(job model.Job, delay time.Duration) {
	go func() {
		timer := time.NewTimer(delay)
		defer timer.Stop()

		select {
		case <-timer.C:
			job.Status = model.JobPending

			select {
			case s.jobs <- job:
			case <-s.ctx.Done():
				return
			}

		case <-s.ctx.Done():
			return
		}
	}()
}