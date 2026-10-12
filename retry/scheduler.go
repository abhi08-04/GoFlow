package retry

import (
	"context"
	"sync"
	"time"
	"fmt"
	"goflow/model"
)

type Scheduler struct {
	jobs chan<- model.Job
	ctx  context.Context

	cancel context.CancelFunc
	mu sync.Mutex
	wg sync.WaitGroup
	stopped bool
}

func NewScheduler(ctx context.Context, jobs chan<- model.Job) *Scheduler {
	schedulerCtx, cancel := context.WithCancel(ctx)

	return &Scheduler{
		jobs: jobs,
		ctx:  schedulerCtx,
		cancel: cancel,
	}
}

func (s *Scheduler) Schedule(job model.Job, delay time.Duration) {
	s.mu.Lock()

	if s.stopped {
		s.mu.Unlock()
		return
	}

	s.wg.Add(1)
	s.mu.Unlock()

	go func() {

		defer s.wg.Done()
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

func (s *Scheduler) Stop() {
	s.mu.Lock()
	s.stopped = true
	s.cancel()
	s.mu.Unlock()
	s.wg.Wait()
	fmt.Println("Scheduler: all retry goroutines stopped")
}