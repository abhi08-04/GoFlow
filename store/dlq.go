package store

import (
	"goflow/model"
	"sync"
)

type DLQ struct {
	mu   sync.RWMutex
	jobs map[string]model.Job
}

func NewDLQ() *DLQ {
	return &DLQ{
		jobs: make(map[string]model.Job),
	}
}

func (d *DLQ) Add(job model.Job) {
	d.mu.Lock()
	defer d.mu.Unlock()

	d.jobs[job.ID] = job
}

func (d *DLQ) GetAll() []model.Job {
	d.mu.RLock()
	defer d.mu.RUnlock()

	jobs := make([]model.Job, 0, len(d.jobs))

	for _, job := range d.jobs {
		jobs = append(jobs, job)
	}

	return jobs
}

func (d *DLQ) Claim(id string) (model.Job, bool) {
	d.mu.Lock()
	defer d.mu.Unlock()

	job, exists := d.jobs[id]
	if !exists {
		return model.Job{}, false
	}

	delete(d.jobs, id)
	return job, true
}

func (d *DLQ) Remove(id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete(d.jobs, id)
}
