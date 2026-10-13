package store

import (
	"sync"
	"testing"

	"goflow/model"
)

func TestDLQAddAndGetAll(t *testing.T) {
	dlq := NewDLQ()

	job1 := model.Job{
		ID:     "job-1",
		Type:   "email",
		Status: model.JobFailed,
	}
	job2 := model.Job{
		ID:     "job-2",
		Type:   "webhook",
		Status: model.JobFailed,
	}

	dlq.Add(job1)
	dlq.Add(job2)

	got := dlq.GetAll()

	if len(got) != 2 {
		t.Fatalf("GetAll() returned %d jobs, want 2", len(got))
	}

	found := map[string]model.Job{}
	for _, job := range got {
		found[job.ID] = job
	}

	if found[job1.ID] != job1 {
		t.Errorf("job-1 = %+v, want %+v", found[job1.ID], job1)
	}
	if found[job2.ID] != job2 {
		t.Errorf("job-2 = %+v, want %+v", found[job2.ID], job2)
	}
}

func TestDLQGetAllEmpty(t *testing.T) {
	dlq := NewDLQ()

	got := dlq.GetAll()

	if got == nil {
		t.Fatal("GetAll() returned nil, want an empty slice")
	}
	if len(got) != 0 {
		t.Fatalf("GetAll() returned %d jobs, want 0", len(got))
	}
}

func TestDLQAddSameIDUpdatesJob(t *testing.T) {
	dlq := NewDLQ()

	job := model.Job{
		ID:     "job-1",
		Type:   "email",
		Status: model.JobFailed,
	}
	dlq.Add(job)

	job.Attempts = 3
	dlq.Add(job)

	got := dlq.GetAll()

	if len(got) != 1 {
		t.Fatalf("GetAll() returned %d jobs, want 1", len(got))
	}
	if got[0].Attempts != 3 {
		t.Fatalf("Attempts = %d, want 3", got[0].Attempts)
	}
}

func TestDLQRemove(t *testing.T) {
	dlq := NewDLQ()

	dlq.Add(model.Job{ID: "job-1"})
	dlq.Add(model.Job{ID: "job-2"})

	dlq.Remove("job-1")

	got := dlq.GetAll()

	if len(got) != 1 {
		t.Fatalf("GetAll() returned %d jobs, want 1", len(got))
	}
	if got[0].ID != "job-2" {
		t.Fatalf("remaining job ID = %q, want job-2", got[0].ID)
	}
}

func TestDLQRemoveMissing(t *testing.T) {
	dlq := NewDLQ()
	dlq.Add(model.Job{ID: "job-1"})

	// Removing a nonexistent ID should be safe.
	dlq.Remove("missing")

	if got := dlq.GetAll(); len(got) != 1 {
		t.Fatalf("GetAll() returned %d jobs, want 1", len(got))
	}
}

func TestDLQConcurrentAccess(t *testing.T) {
	dlq := NewDLQ()

	const count = 100

	var wg sync.WaitGroup

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func(i int) {
			defer wg.Done()

			id := string(rune('A' + i))
			dlq.Add(model.Job{ID: id})
			dlq.GetAll()

			if i%2 == 0 {
				dlq.Remove(id)
			}
		}(i)
	}

	wg.Wait()
}

func TestDLQConcurrentClaimOnlyOneSucceeds(t *testing.T) {
	dlq := NewDLQ()
	dlq.Add(model.Job{
		ID:     "job-1",
		Type:   "email",
		Status: model.JobFailed,
	})

	const count = 100

	var wg sync.WaitGroup
	var successCount int
	var mu sync.Mutex

	start := make(chan struct{})

	for i := 0; i < count; i++ {
		wg.Add(1)

		go func() {
			defer wg.Done()
			<-start

			_, claimed := dlq.Claim("job-1")
			if claimed {
				mu.Lock()
				successCount++
				mu.Unlock()
			}
		}()
	}

	close(start)
	wg.Wait()

	if successCount != 1 {
		t.Fatalf("successful claims = %d, want 1", successCount)
	}

	if got := dlq.GetAll(); len(got) != 0 {
		t.Fatalf("DLQ contains %d jobs, want 0", len(got))
	}
}
