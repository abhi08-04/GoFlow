package handler

import (
	"net/http"

	"github.com/gin-gonic/gin"

	"goflow/internal/config"
	"goflow/internal/metrics"
)

type MetricsHandler struct {
	registry *metrics.Registry
	ring     *metrics.RingBuffer
	cfg      config.Config
}

func NewMetricsHandler(r *metrics.Registry, ring *metrics.RingBuffer, cfg config.Config) *MetricsHandler {
	return &MetricsHandler{registry: r, ring: ring, cfg: cfg}
}

func (h *MetricsHandler) Snapshot(c *gin.Context) {
	c.JSON(http.StatusOK, h.registry.Snapshot())
}

func (h *MetricsHandler) TimeSeries(c *gin.Context) {
	c.JSON(http.StatusOK, h.ring.All())
}

func (h *MetricsHandler) ConfigView(c *gin.Context) {
	c.JSON(http.StatusOK, gin.H{
		"port":                            h.cfg.Port,
		"grpc_port":                       h.cfg.GrpcPort,
		"job_queue_size":                  h.cfg.JobQueueSize,
		"max_retries":                     h.cfg.MaxRetries,
		"base_backoff_seconds":            int(h.cfg.BaseBackoff.Seconds()),
		"shutdown_timeout_seconds":        int(h.cfg.ShutdownTimeout.Seconds()),
		"default_lease_seconds":           int(h.cfg.DefaultLease.Seconds()),
		"max_lease_seconds":               int(h.cfg.MaxLease.Seconds()),
		"lease_sweep_interval_seconds":    int(h.cfg.LeaseSweepInterval.Seconds()),
		"max_prefetch":                    h.cfg.MaxPrefetch,
	})
}
