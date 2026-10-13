// Package broker owns the gRPC-side state that didn't exist in the
// learning codebase: lease tracking (who has which job and until when)
// and the sweeper that reclaims expired leases.
//
// The broker DOES NOT change store.JobStore, service.JobService, or
// retry.Scheduler — it reuses them. The shared jobs chan is still the
// queue; the broker just reads from it on Claim instead of workers
// reading from it on Start.
package broker

import (
	"sync"
	"time"

	"github.com/google/uuid"

	"goflow/model"
)

// Lease records that a job has been handed to a consumer and is
// expected to be acked / nacked before ExpiresAt.
type Lease struct {
	JobID     string
	Token     string
	Job       model.Job
	ExpiresAt time.Time
}

// LeaseStore maps job-id → Lease. Lives entirely inside the broker
// package; nothing in store/ or service/ knows about it.
type LeaseStore struct {
	mu     sync.Mutex
	byJob  map[string]*Lease
}

func NewLeaseStore() *LeaseStore {
	return &LeaseStore{byJob: make(map[string]*Lease)}
}

// Issue creates a lease for the given job and stores it. The returned
// Lease carries the server-generated token the client must echo on
// Ack / Nack.
func (ls *LeaseStore) Issue(job model.Job, duration time.Duration) *Lease {
	ls.mu.Lock()
	defer ls.mu.Unlock()

	lease := &Lease{
		JobID:     job.ID,
		Token:     uuid.New().String(),
		Job:       job,
		ExpiresAt: time.Now().Add(duration),
	}
	ls.byJob[job.ID] = lease
	return lease
}

// Validate returns the current lease iff the token matches, else nil.
// Does not remove the entry — Release does that.
func (ls *LeaseStore) Validate(jobID, token string) *Lease {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	lease, ok := ls.byJob[jobID]
	if !ok || lease.Token != token {
		return nil
	}
	return lease
}

// Release removes and returns the lease iff the token matches.
func (ls *LeaseStore) Release(jobID, token string) *Lease {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	lease, ok := ls.byJob[jobID]
	if !ok || lease.Token != token {
		return nil
	}
	delete(ls.byJob, jobID)
	return lease
}

// Expired returns and removes every lease whose deadline is <= now.
// Called by the sweeper on each tick.
func (ls *LeaseStore) Expired(now time.Time) []*Lease {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	var out []*Lease
	for id, lease := range ls.byJob {
		if !now.Before(lease.ExpiresAt) {
			out = append(out, lease)
			delete(ls.byJob, id)
		}
	}
	return out
}

// Size — for metrics / debugging only.
func (ls *LeaseStore) Size() int {
	ls.mu.Lock()
	defer ls.mu.Unlock()
	return len(ls.byJob)
}
