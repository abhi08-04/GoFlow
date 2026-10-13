package retry

import (
	"context"
	"fmt"
	"sync"
	"time"

	"goflow/internal/metrics"
	"goflow/internal/model"
)

// Requeuer is the subset of store.Store the scheduler needs. Kept as a
// local interface to avoid importing store (which would create a
// dependency cycle through broker).
type Requeuer interface {
	EnqueueRetry(job model.Job) error
}

// Scheduler delays-and-requeues jobs after a Nack or lease expiry.
// One goroutine per in-flight retry; all are cancelled by Stop.
type Scheduler struct {
	store    Requeuer
	ctx      context.Context
	registry *metrics.Registry

	cancel   context.CancelFunc
	mu       sync.Mutex
	wg       sync.WaitGroup
	stopped  bool
	stopOnce sync.Once
}

func NewScheduler(ctx context.Context, store Requeuer, registry *metrics.Registry) *Scheduler {
	schedulerCtx, cancel := context.WithCancel(ctx)
	return &Scheduler{
		store:    store,
		ctx:      schedulerCtx,
		cancel:   cancel,
		registry: registry,
	}
}

// Schedule parks a goroutine until `delay`, then re-enqueues the job
// via EnqueueRetry. On shutdown (ctx cancel) onCancel runs — callers
// use that to send the job to the DLQ instead.
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
			if err := s.store.EnqueueRetry(job); err != nil {
				onCancel()
				return
			}
			s.registry.RecordRetried()
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
