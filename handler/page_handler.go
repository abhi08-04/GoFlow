package handler

import (
	"net/http"
	"sort"

	"github.com/gin-gonic/gin"

	"goflow/config"
	"goflow/metrics"
	"goflow/model"
	"goflow/service"
	"goflow/store"
	"goflow/web"
)

type PageHandler struct {
	tmpl       *web.Templates
	jobService *service.JobService
	dlq        *store.DLQ
	registry   *metrics.Registry
	ring       *metrics.RingBuffer
	cfg        config.Config
}

func NewPageHandler(t *web.Templates, j *service.JobService, dlq *store.DLQ, r *metrics.Registry, ring *metrics.RingBuffer, cfg config.Config) *PageHandler {
	return &PageHandler{tmpl: t, jobService: j, dlq: dlq, registry: r, ring: ring, cfg: cfg}
}

// baseVM assembles the fields every page's layout needs (sidebar
// highlight, DLQ badge count, port).
type baseVM struct {
	Title    string
	Subtitle string
	Nav      string
	DLQCount int
	Port     string

	PageScript string // overridden per-page for Chart.js wiring
}

func (h *PageHandler) base(nav, title, subtitle string) baseVM {
	return baseVM{
		Title: title, Subtitle: subtitle, Nav: nav,
		DLQCount: h.dlq.Size(), Port: h.cfg.Port,
	}
}

// ---------- Dashboard ----------

type typeRow struct {
	Type         string
	Created      int64
	Completed    int64
	Failed       int64
	CompletedPct int
}

type dashboardVM struct {
	baseVM
	Snap             metrics.Snapshot
	ByType           []typeRow
	RecentJobs       []model.Job
	ThroughputPerSec float64
	SuccessRate      float64
	TotalTerminal    int64
	QueueCap         int
	Workers          int
}

func (h *PageHandler) buildDashboardVM() dashboardVM {
	snap := h.registry.Snapshot()
	byType := make([]typeRow, 0, len(snap.ByType))
	for name, t := range snap.ByType {
		pct := 0
		if t.Created > 0 {
			pct = int(float64(t.Completed) * 100 / float64(t.Created))
		}
		byType = append(byType, typeRow{
			Type: name, Created: t.Created, Completed: t.Completed, Failed: t.Failed, CompletedPct: pct,
		})
	}
	sort.Slice(byType, func(i, j int) bool { return byType[i].Created > byType[j].Created })

	// Throughput over last ~60s window from ring buffer.
	series := h.ring.All()
	var tput float64
	if len(series) >= 2 {
		last := series[len(series)-1]
		var ref metrics.Snapshot
		target := last.Timestamp.Add(-60 * 1e9) // ~60s window via wall time
		for i := len(series) - 1; i >= 0; i-- {
			if series[i].Timestamp.Before(target) {
				ref = series[i]
				break
			}
		}
		if ref.Timestamp.IsZero() {
			ref = series[0]
		}
		dur := last.Timestamp.Sub(ref.Timestamp).Seconds()
		if dur > 0 {
			tput = float64(last.JobsCompleted-ref.JobsCompleted) / dur
		}
	}

	totalTerm := snap.JobsCompleted + snap.JobsFailed
	success := 100.0
	if totalTerm > 0 {
		success = float64(snap.JobsCompleted) * 100 / float64(totalTerm)
	}

	recent := h.jobService.ListJobs()
	// Prefer most-recent-looking: sort by attempts desc then ID so the table isn't jumping.
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].Status != recent[j].Status {
			// running first, then pending, completed, failed
			order := map[string]int{"running": 0, "pending": 1, "completed": 2, "failed": 3}
			return order[recent[i].Status] < order[recent[j].Status]
		}
		return recent[i].ID < recent[j].ID
	})
	if len(recent) > 8 {
		recent = recent[:8]
	}

	return dashboardVM{
		baseVM:           h.base("dashboard", "Dashboard", "Live view of your queue"),
		Snap:             snap,
		ByType:           byType,
		RecentJobs:       recent,
		ThroughputPerSec: tput,
		SuccessRate:      success,
		TotalTerminal:    totalTerm,
		QueueCap:         h.cfg.JobQueueSize,
		Workers:          h.cfg.NumWorkers,
	}
}

func (h *PageHandler) Dashboard(c *gin.Context) {
	vm := h.buildDashboardVM()
	vm.PageScript = "dashboard.js"
	_ = h.tmpl.Render(c.Writer, "dashboard", vm)
}

func (h *PageHandler) PartialTiles(c *gin.Context) {
	vm := h.buildDashboardVM()
	_ = h.tmpl.Render(c.Writer, "partial:tiles", vm)
}

func (h *PageHandler) PartialActivity(c *gin.Context) {
	vm := h.buildDashboardVM()
	_ = h.tmpl.Render(c.Writer, "partial:activity_rows", vm)
}

// ---------- Jobs ----------

type jobsVM struct {
	baseVM
	Jobs      []model.Job
	TotalJobs int
	Status    string
	Type      string
}

func (h *PageHandler) buildJobsVM(c *gin.Context) jobsVM {
	status := c.Query("status")
	jobType := c.Query("type")

	all := h.jobService.ListJobs()
	jobs := make([]model.Job, 0, len(all))
	for _, j := range all {
		if status != "" && j.Status != status {
			continue
		}
		if jobType != "" && j.Type != jobType {
			continue
		}
		jobs = append(jobs, j)
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })

	return jobsVM{
		baseVM:    h.base("jobs", "Jobs", "Browse, filter, and create jobs"),
		Jobs:      jobs,
		TotalJobs: len(all),
		Status:    status,
		Type:      jobType,
	}
}

func (h *PageHandler) Jobs(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "jobs", h.buildJobsVM(c))
}

func (h *PageHandler) PartialJobs(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "partial:jobs_rows", h.buildJobsVM(c))
}

func (h *PageHandler) JobDetail(c *gin.Context) {
	id := c.Param("id")
	job, exists := h.jobService.GetJob(id)
	if !exists {
		c.Status(http.StatusNotFound)
		c.Writer.WriteString("Job not found")
		return
	}
	_ = h.tmpl.Render(c.Writer, "job_detail", struct {
		baseVM
		Job model.Job
	}{
		baseVM: h.base("jobs", "Job detail", id[:8]),
		Job:    job,
	})
}

// ---------- DLQ ----------

type dlqVM struct {
	baseVM
	DLQJobs []model.Job
}

func (h *PageHandler) buildDLQVM() dlqVM {
	jobs := h.dlq.GetAll()
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return dlqVM{
		baseVM:  h.base("dlq", "Dead Letter Queue", "Jobs that exhausted their retry budget"),
		DLQJobs: jobs,
	}
}

func (h *PageHandler) DLQ(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "dlq", h.buildDLQVM())
}

func (h *PageHandler) PartialDLQ(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "partial:dlq_rows", h.buildDLQVM())
}

// ---------- Config ----------

type configVM struct {
	baseVM
	Cfg struct {
		Port                   string
		NumWorkers             int
		JobQueueSize           int
		JobTimeoutSeconds      int
		MaxRetries             int
		BaseBackoffSeconds     int
		ShutdownTimeoutSeconds int
	}
}

func (h *PageHandler) Config(c *gin.Context) {
	vm := configVM{baseVM: h.base("config", "Configuration", "Runtime values this process was started with")}
	vm.Cfg.Port = h.cfg.Port
	vm.Cfg.NumWorkers = h.cfg.NumWorkers
	vm.Cfg.JobQueueSize = h.cfg.JobQueueSize
	vm.Cfg.JobTimeoutSeconds = int(h.cfg.JobTimeout.Seconds())
	vm.Cfg.MaxRetries = h.cfg.MaxRetries
	vm.Cfg.BaseBackoffSeconds = int(h.cfg.BaseBackoff.Seconds())
	vm.Cfg.ShutdownTimeoutSeconds = int(h.cfg.ShutdownTimeout.Seconds())
	_ = h.tmpl.Render(c.Writer, "config", vm)
}
