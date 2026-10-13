package retry

import (
	"context"
	"fmt"
	"goflow/internal/metrics"
	"goflow/internal/model"
	"sync"
	"time"
)

type Scheduler struct {
	jobs     chan<- model.Job
	ctx      context.Context
	registry *metrics.Registry

	cancel   context.CancelFunc
	mu       sync.Mutex
	wg       sync.WaitGroup
	stopped  bool
	stopOnce sync.Once
}

func NewScheduler(ctx context.Context, jobs chan<- model.Job, registry *metrics.Registry) *Scheduler {
	schedulerCtx, cancel := context.WithCancel(ctx)

	return &Scheduler{
		jobs:     jobs,
		ctx:      schedulerCtx,
		cancel:   cancel,
		registry: registry,
	}
}

func (s *Scheduler) Schedule(job model.Job, delay time.Duration, onCancel func()) bool {
	s.mu.Lock()
	if s.stopped {
		s.mu.Unlock()
		return false
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
				s.registry.RecordRetried()
			case <-s.ctx.Done():
				onCancel()
			}

		case <-s.ctx.Done():
			onCancel()
		}
	}()

	return true
}

func (s *Scheduler) Stop() {
	s.stopOnce.Do(func() {
		s.mu.Lock()
		s.stopped = true
		s.cancel()
		s.mu.Unlock()
		s.wg.Wait()
		fmt.Println("Scheduler: all retry goroutines stopped")
	})
}
