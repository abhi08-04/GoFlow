package metrics

import (
	"sync"
	"sync/atomic"
	"time"
)

// Registry is the central place where every package records runtime
// activity. Safe for concurrent use.
//
// Counters are atomic.Int64 (single-instruction LOCK XADD on x86), not
// mutex-protected: increments happen on every job and must be lock-free
// on the hot path. Per-type counters live in a sync.Map because the key
// set (job types) is small and stable — the sync.Map fast path then
// avoids the write-lock cost that a plain map + RWMutex would pay on
// every lookup.
type Registry struct {
	jobsCreated   atomic.Int64
	jobsCompleted atomic.Int64
	jobsFailed    atomic.Int64
	jobsRetried   atomic.Int64
	jobsDLQd      atomic.Int64
	jobsRequeued  atomic.Int64

	queueDepth atomic.Int64
	dlqSize    atomic.Int64
	inFlight   atomic.Int64

	byType  sync.Map // map[string]*typeCounters
	latency *histogram
}

type typeCounters struct {
	Created   atomic.Int64
	Completed atomic.Int64
	Failed    atomic.Int64
}

func New() *Registry {
	return &Registry{latency: newHistogram()}
}

func (r *Registry) RecordCreated(jobType string) {
	r.jobsCreated.Add(1)
	r.perType(jobType).Created.Add(1)
}

func (r *Registry) RecordStart() { r.inFlight.Add(1) }

func (r *Registry) RecordCompleted(jobType string, took time.Duration) {
	r.jobsCompleted.Add(1)
	r.inFlight.Add(-1)
	r.perType(jobType).Completed.Add(1)
	r.latency.observe(took)
}

func (r *Registry) RecordFailed(jobType string) {
	r.jobsFailed.Add(1)
	r.inFlight.Add(-1)
	r.perType(jobType).Failed.Add(1)
}

func (r *Registry) RecordRetried()  { r.jobsRetried.Add(1) }
func (r *Registry) RecordDLQd()     { r.jobsDLQd.Add(1) }
func (r *Registry) RecordRequeued() { r.jobsRequeued.Add(1) }

// Gauges are set by the sampler each tick from authoritative sources
// (len(chan), DLQ.Size()) so counter-skew bugs can't drift them.
func (r *Registry) SetQueueDepth(n int64) { r.queueDepth.Store(n) }
func (r *Registry) SetDLQSize(n int64)    { r.dlqSize.Store(n) }

func (r *Registry) perType(t string) *typeCounters {
	if v, ok := r.byType.Load(t); ok {
		return v.(*typeCounters)
	}
	actual, _ := r.byType.LoadOrStore(t, &typeCounters{})
	return actual.(*typeCounters)
}

type Snapshot struct {
	JobsCreated   int64                   `json:"jobs_created"`
	JobsCompleted int64                   `json:"jobs_completed"`
	JobsFailed    int64                   `json:"jobs_failed"`
	JobsRetried   int64                   `json:"jobs_retried"`
	JobsDLQd      int64                   `json:"jobs_dlqd"`
	JobsRequeued  int64                   `json:"jobs_requeued"`
	QueueDepth    int64                   `json:"queue_depth"`
	DLQSize       int64                   `json:"dlq_size"`
	InFlight      int64                   `json:"in_flight"`
	ByType        map[string]TypeSnapshot `json:"by_type"`
	Latency       LatencySnapshot         `json:"latency_ms"`
	Timestamp     time.Time               `json:"timestamp"`
}

type TypeSnapshot struct {
	Created   int64 `json:"created"`
	Completed int64 `json:"completed"`
	Failed    int64 `json:"failed"`
}

type LatencySnapshot struct {
	P50   float64 `json:"p50"`
	P95   float64 `json:"p95"`
	P99   float64 `json:"p99"`
	Count int64   `json:"count"`
}

// Snapshot takes an atomic.Load on each field. There is no cross-field
// coordination, so two counters may be a few nanoseconds out of sync —
// acceptable for a dashboard, and far cheaper than taking a global lock.
func (r *Registry) Snapshot() Snapshot {
	s := Snapshot{
		JobsCreated:   r.jobsCreated.Load(),
		JobsCompleted: r.jobsCompleted.Load(),
		JobsFailed:    r.jobsFailed.Load(),
		JobsRetried:   r.jobsRetried.Load(),
		JobsDLQd:      r.jobsDLQd.Load(),
		JobsRequeued:  r.jobsRequeued.Load(),
		QueueDepth:    r.queueDepth.Load(),
		DLQSize:       r.dlqSize.Load(),
		InFlight:      r.inFlight.Load(),
		ByType:        make(map[string]TypeSnapshot),
		Latency:       r.latency.snapshot(),
		Timestamp:     time.Now(),
	}
	r.byType.Range(func(k, v any) bool {
		tc := v.(*typeCounters)
		s.ByType[k.(string)] = TypeSnapshot{
			Created:   tc.Created.Load(),
			Completed: tc.Completed.Load(),
			Failed:    tc.Failed.Load(),
		}
		return true
	})
	return s
}
