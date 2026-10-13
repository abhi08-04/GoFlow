package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"goflow/config"
	"goflow/metrics"
)

type MetricsHandler struct {
	registry *metrics.Registry
	ring     *metrics.RingBuffer
	cfg      config.Config
	workers  int
}

func NewMetricsHandler(r *metrics.Registry, ring *metrics.RingBuffer, cfg config.Config) *MetricsHandler {
	return &MetricsHandler{registry: r, ring: ring, cfg: cfg, workers: cfg.NumWorkers}
}

// Snapshot — current counters + gauges. Used by the dashboard tiles,
// polled every ~2s via HTMX.
func (h *MetricsHandler) Snapshot(c *gin.Context) {
	c.JSON(http.StatusOK, h.registry.Snapshot())
}

// TimeSeries — the full ring buffer. Reduced on the client to the
// throughput chart.
func (h *MetricsHandler) TimeSeries(c *gin.Context) {
	c.JSON(http.StatusOK, h.ring.All())
}

// ConfigView — read-only runtime config for the Config page.
func (h *MetricsHandler) ConfigView(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"port":                     h.cfg.Port,
		"num_workers":              h.cfg.NumWorkers,
		"job_queue_size":           h.cfg.JobQueueSize,
		"job_timeout_seconds":      int(h.cfg.JobTimeout.Seconds()),
		"max_retries":              h.cfg.MaxRetries,
		"base_backoff_seconds":     int(h.cfg.BaseBackoff.Seconds()),
		"shutdown_timeout_seconds": int(h.cfg.ShutdownTimeout.Seconds()),
	})
}
