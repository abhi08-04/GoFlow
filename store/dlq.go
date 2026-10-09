package store

import (
	"sync"
	"goflow/model"
)

type DLQ struct {
	mu   sync.RWMutex
	jobs map[string]model.Job

}

func NewDLQ () *DLQ {
	return &DLQ {
		jobs : make(map[string]model.Job),
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

func (d *DLQ) Remove (id string) {
	d.mu.Lock()
	defer d.mu.Unlock()

	delete (d.jobs, id)
}

