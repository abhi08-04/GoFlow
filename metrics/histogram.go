package metrics

import (
	"sort"
	"sync/atomic"
	"time"
)

// histBoundsMs — upper bounds (ms) for latency buckets. An implicit
// +Inf bucket at the end catches outliers.
var histBoundsMs = []float64{1, 2, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000, 10000}

// histogram is a lock-free bucket histogram. Each bucket is an
// atomic.Int64, so observations from many workers never block each
// other.
type histogram struct {
	buckets []atomic.Int64
	count   atomic.Int64
	sumMs   atomic.Int64
}

func newHistogram() *histogram {
	return &histogram{buckets: make([]atomic.Int64, len(histBoundsMs)+1)}
}

func (h *histogram) observe(d time.Duration) {
	ms := float64(d) / float64(time.Millisecond)
	idx := sort.SearchFloat64s(histBoundsMs, ms)
	h.buckets[idx].Add(1)
	h.count.Add(1)
	h.sumMs.Add(int64(ms))
}

func (h *histogram) snapshot() LatencySnapshot {
	total := h.count.Load()
	if total == 0 {
		return LatencySnapshot{}
	}
	counts := make([]int64, len(h.buckets))
	for i := range h.buckets {
		counts[i] = h.buckets[i].Load()
	}
	return LatencySnapshot{
		P50:   histPercentile(counts, total, 0.50),
		P95:   histPercentile(counts, total, 0.95),
		P99:   histPercentile(counts, total, 0.99),
		Count: total,
	}
}

// histPercentile returns the upper bound of the bucket that contains
// the qth percentile. Approximate but cheap and lock-free.
func histPercentile(counts []int64, total int64, q float64) float64 {
	target := int64(float64(total) * q)
	if target == 0 {
		target = 1
	}
	var cum int64
	for i, c := range counts {
		cum += c
		if cum >= target {
			if i >= len(histBoundsMs) {
				return histBoundsMs[len(histBoundsMs)-1]
			}
			return histBoundsMs[i]
		}
	}
	return histBoundsMs[len(histBoundsMs)-1]
}
