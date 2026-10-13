package metrics

import "sync"

// RingBuffer stores the last N snapshots for time-series rendering.
//
// One writer (the sampler goroutine) and many readers (HTTP handlers).
// Writes happen at ticker cadence (seconds apart); reads happen on
// dashboard refresh. An RWMutex fits: the write is a short slice-index
// assignment, reads need a consistent view of the whole buffer.
type RingBuffer struct {
	mu   sync.RWMutex
	data []Snapshot
	head int
	size int
	full bool
}

func NewRingBuffer(size int) *RingBuffer {
	return &RingBuffer{data: make([]Snapshot, size), size: size}
}

func (rb *RingBuffer) Add(s Snapshot) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	rb.data[rb.head] = s
	rb.head = (rb.head + 1) % rb.size
	if rb.head == 0 {
		rb.full = true
	}
}

// All returns snapshots in chronological order (oldest first).
func (rb *RingBuffer) All() []Snapshot {
	rb.mu.RLock()
	defer rb.mu.RUnlock()

	if !rb.full {
		out := make([]Snapshot, rb.head)
		copy(out, rb.data[:rb.head])
		return out
	}
	out := make([]Snapshot, 0, rb.size)
	out = append(out, rb.data[rb.head:]...)
	out = append(out, rb.data[:rb.head]...)
	return out
}
