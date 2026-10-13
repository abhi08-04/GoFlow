package metrics

import (
	"sync"
	"testing"
	"time"
)

func TestRecordCreatedIsRaceFree(t *testing.T) {
	r := New()

	const goroutines = 20
	const perGoroutine = 1000

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < perGoroutine; i++ {
				r.RecordCreated("email")
			}
		}()
	}
	wg.Wait()

	snap := r.Snapshot()
	want := int64(goroutines * perGoroutine)
	if snap.JobsCreated != want {
		t.Fatalf("JobsCreated = %d, want %d", snap.JobsCreated, want)
	}
	if snap.ByType["email"].Created != want {
		t.Fatalf("ByType[email].Created = %d, want %d", snap.ByType["email"].Created, want)
	}
}

func TestInFlightBalancesToZero(t *testing.T) {
	r := New()

	const goroutines = 10
	const iterations = 500

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			for i := 0; i < iterations; i++ {
				r.RecordStart()
				r.RecordCompleted("webhook", time.Millisecond)
			}
		}()
	}
	wg.Wait()

	snap := r.Snapshot()
	if snap.InFlight != 0 {
		t.Fatalf("InFlight = %d, want 0", snap.InFlight)
	}
	if snap.JobsCompleted != int64(goroutines*iterations) {
		t.Fatalf("JobsCompleted = %d, want %d", snap.JobsCompleted, goroutines*iterations)
	}
}

func TestPerTypeMapIsConsistentUnderNewKeys(t *testing.T) {
	// Multiple goroutines create brand-new keys concurrently. The
	// sync.Map LoadOrStore path must return the same pointer to all
	// of them so counters don't fragment.
	r := New()

	const goroutines = 50
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func() {
			defer wg.Done()
			r.RecordCreated("sms")
		}()
	}
	wg.Wait()

	snap := r.Snapshot()
	if snap.ByType["sms"].Created != int64(goroutines) {
		t.Fatalf("ByType[sms].Created = %d, want %d", snap.ByType["sms"].Created, goroutines)
	}
}

func TestHistogramObservesCounts(t *testing.T) {
	r := New()

	r.RecordStart()
	r.RecordCompleted("x", 3*time.Millisecond)
	r.RecordStart()
	r.RecordCompleted("x", 7*time.Millisecond)
	r.RecordStart()
	r.RecordCompleted("x", 1500*time.Millisecond)

	snap := r.Snapshot()
	if snap.Latency.Count != 3 {
		t.Fatalf("Latency.Count = %d, want 3", snap.Latency.Count)
	}
	if snap.Latency.P50 <= 0 {
		t.Fatalf("Latency.P50 = %v, want > 0", snap.Latency.P50)
	}
}

func TestRingBufferReturnsChronologicalOrder(t *testing.T) {
	rb := NewRingBuffer(3)

	rb.Add(Snapshot{JobsCreated: 1})
	rb.Add(Snapshot{JobsCreated: 2})

	got := rb.All()
	if len(got) != 2 {
		t.Fatalf("got %d snapshots, want 2", len(got))
	}
	if got[0].JobsCreated != 1 || got[1].JobsCreated != 2 {
		t.Fatalf("order wrong: %+v", got)
	}

	// Wrap around: oldest (1) is overwritten.
	rb.Add(Snapshot{JobsCreated: 3})
	rb.Add(Snapshot{JobsCreated: 4})

	got = rb.All()
	if len(got) != 3 {
		t.Fatalf("got %d snapshots, want 3 (full buffer)", len(got))
	}
	if got[0].JobsCreated != 2 || got[1].JobsCreated != 3 || got[2].JobsCreated != 4 {
		t.Fatalf("wrap order wrong: %+v", got)
	}
}

func TestRingBufferConcurrentWriterReader(t *testing.T) {
	rb := NewRingBuffer(100)

	done := make(chan struct{})

	// Writer: adds 10k snapshots as fast as possible.
	go func() {
		defer close(done)
		for i := 0; i < 10000; i++ {
			rb.Add(Snapshot{JobsCreated: int64(i)})
		}
	}()

	// Readers: hammer All(). Must never see a torn Snapshot (the
	// race detector flags it if we do).
	var wg sync.WaitGroup
	for g := 0; g < 4; g++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				select {
				case <-done:
					return
				default:
					_ = rb.All()
				}
			}
		}()
	}
	<-done
	wg.Wait()
}
