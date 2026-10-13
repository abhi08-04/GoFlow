package handler

import (
	"net/http"
	"sort"
	"time"

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
// highlight, DLQ badge count, port, and the queue-type filter chip).
type baseVM struct {
	Title    string
	Subtitle string
	Nav      string
	DLQCount int
	Port     string

	// Queue type filter (empty = all queues combined).
	Queue        string
	AllQueues    []string

	PageScript string
}

// knownQueues collects every job type GoFlow has ever seen (from live
// metrics + DLQ) so the dropdown lists them all even if some are idle.
func (h *PageHandler) knownQueues() []string {
	snap := h.registry.Snapshot()
	set := map[string]struct{}{}
	for name := range snap.ByType {
		set[name] = struct{}{}
	}
	for _, j := range h.dlq.GetAll() {
		set[j.Type] = struct{}{}
	}
	for _, j := range h.jobService.ListJobs() {
		set[j.Type] = struct{}{}
	}
	out := make([]string, 0, len(set))
	for name := range set {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

func (h *PageHandler) base(c *gin.Context, nav, title, subtitle string) baseVM {
	return baseVM{
		Title: title, Subtitle: subtitle, Nav: nav,
		DLQCount:  h.dlq.Size(),
		Port:      h.cfg.Port,
		Queue:     c.Query("queue"),
		AllQueues: h.knownQueues(),
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

type tilesVM struct {
	Queue            string
	ThroughputPerSec float64
	SuccessRate      float64
	TotalTerminal    int64
	QueueCap         int
	Workers          int

	// Per-type numbers when a queue is selected; global otherwise.
	Created   int64
	Completed int64
	Failed    int64
	InFlight  int64
	DLQSize   int64
	P95Ms     float64
	P99Ms     float64
	LatencyN  int64
	Retried   int64
}

type dashboardVM struct {
	baseVM
	Tiles      tilesVM
	ByType     []typeRow
	RecentJobs []model.Job
}

func (h *PageHandler) buildDashboardVM(c *gin.Context) dashboardVM {
	snap := h.registry.Snapshot()
	queue := c.Query("queue")

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

	// Compute throughput + per-queue numbers.
	tiles := tilesVM{
		Queue:    queue,
		QueueCap: h.cfg.JobQueueSize,
		Workers:  h.cfg.NumWorkers,
		P95Ms:    snap.Latency.P95,
		P99Ms:    snap.Latency.P99,
		LatencyN: snap.Latency.Count,
	}

	if queue == "" {
		// Global view.
		tiles.Created = snap.JobsCreated
		tiles.Completed = snap.JobsCompleted
		tiles.Failed = snap.JobsFailed
		tiles.InFlight = snap.InFlight
		tiles.DLQSize = snap.DLQSize
		tiles.Retried = snap.JobsRetried
		tiles.ThroughputPerSec = h.globalThroughput()
	} else {
		// Per-queue view — pull from snap.ByType and count the DLQ for this type.
		if t, ok := snap.ByType[queue]; ok {
			tiles.Created = t.Created
			tiles.Completed = t.Completed
			tiles.Failed = t.Failed
		}
		// InFlight / Retried aren't tracked per-type by the registry;
		// approximate via list-and-count for now (small working set).
		for _, j := range h.jobService.ListJobs() {
			if j.Type != queue {
				continue
			}
			if j.Status == model.JobRunning {
				tiles.InFlight++
			}
		}
		for _, j := range h.dlq.GetAll() {
			if j.Type == queue {
				tiles.DLQSize++
			}
		}
		tiles.ThroughputPerSec = h.queueThroughput(queue)
	}

	totalTerm := tiles.Completed + tiles.Failed
	success := 100.0
	if totalTerm > 0 {
		success = float64(tiles.Completed) * 100 / float64(totalTerm)
	}
	tiles.SuccessRate = success
	tiles.TotalTerminal = totalTerm

	// If a queue is selected, scope the ByType breakdown to that row.
	if queue != "" {
		filtered := byType[:0]
		for _, r := range byType {
			if r.Type == queue {
				filtered = append(filtered, r)
			}
		}
		byType = filtered
	}

	// Recent jobs — filter by queue if selected.
	recent := h.jobService.ListJobs()
	if queue != "" {
		keep := recent[:0]
		for _, j := range recent {
			if j.Type == queue {
				keep = append(keep, j)
			}
		}
		recent = keep
	}
	sort.Slice(recent, func(i, j int) bool {
		if recent[i].Status != recent[j].Status {
			order := map[string]int{"running": 0, "pending": 1, "completed": 2, "failed": 3}
			return order[recent[i].Status] < order[recent[j].Status]
		}
		return recent[i].ID < recent[j].ID
	})
	if len(recent) > 8 {
		recent = recent[:8]
	}

	return dashboardVM{
		baseVM:     h.base(c, "dashboard", "Dashboard", "Live view of your queue"),
		Tiles:      tiles,
		ByType:     byType,
		RecentJobs: recent,
	}
}

// globalThroughput: completed-count delta across the ring over ~60s.
func (h *PageHandler) globalThroughput() float64 {
	series := h.ring.All()
	return deltaRate(series, func(s metrics.Snapshot) int64 { return s.JobsCompleted })
}

// queueThroughput: per-type completed delta over the ring (~60s).
func (h *PageHandler) queueThroughput(queue string) float64 {
	series := h.ring.All()
	return deltaRate(series, func(s metrics.Snapshot) int64 {
		if t, ok := s.ByType[queue]; ok {
			return t.Completed
		}
		return 0
	})
}

func deltaRate(series []metrics.Snapshot, counter func(metrics.Snapshot) int64) float64 {
	if len(series) < 2 {
		return 0
	}
	last := series[len(series)-1]
	target := last.Timestamp.Add(-60 * time.Second)
	ref := series[0]
	for i := len(series) - 1; i >= 0; i-- {
		if series[i].Timestamp.Before(target) {
			ref = series[i]
			break
		}
	}
	dur := last.Timestamp.Sub(ref.Timestamp).Seconds()
	if dur <= 0 {
		return 0
	}
	return float64(counter(last)-counter(ref)) / dur
}

func (h *PageHandler) Dashboard(c *gin.Context) {
	vm := h.buildDashboardVM(c)
	vm.PageScript = "dashboard.js"
	_ = h.tmpl.Render(c.Writer, "dashboard", vm)
}

func (h *PageHandler) PartialTiles(c *gin.Context) {
	vm := h.buildDashboardVM(c)
	_ = h.tmpl.Render(c.Writer, "partial:tiles", vm)
}

func (h *PageHandler) PartialActivity(c *gin.Context) {
	vm := h.buildDashboardVM(c)
	_ = h.tmpl.Render(c.Writer, "partial:activity_rows", vm)
}

// ---------- Jobs ----------

type jobsVM struct {
	baseVM
	Jobs      []model.Job
	TotalJobs int
	Status    string
	Type      string // kept so the /jobs?type= legacy query still works
}

func (h *PageHandler) buildJobsVM(c *gin.Context) jobsVM {
	status := c.Query("status")
	// Prefer the baseline `queue` filter; fall back to the legacy `type` param.
	jobType := c.Query("queue")
	if jobType == "" {
		jobType = c.Query("type")
	}

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
		baseVM:    h.base(c, "jobs", "Jobs", "Browse, filter, and create jobs"),
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
		baseVM: h.base(c, "jobs", "Job detail", id[:8]),
		Job:    job,
	})
}

// ---------- DLQ ----------

type dlqVM struct {
	baseVM
	DLQJobs []model.Job
}

func (h *PageHandler) buildDLQVM(c *gin.Context) dlqVM {
	jobs := h.dlq.GetAll()
	queue := c.Query("queue")
	if queue != "" {
		keep := jobs[:0]
		for _, j := range jobs {
			if j.Type == queue {
				keep = append(keep, j)
			}
		}
		jobs = keep
	}
	sort.Slice(jobs, func(i, j int) bool { return jobs[i].ID < jobs[j].ID })
	return dlqVM{
		baseVM:  h.base(c, "dlq", "Dead Letter Queue", "Jobs that exhausted their retry budget"),
		DLQJobs: jobs,
	}
}

func (h *PageHandler) DLQ(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "dlq", h.buildDLQVM(c))
}

func (h *PageHandler) PartialDLQ(c *gin.Context) {
	_ = h.tmpl.Render(c.Writer, "partial:dlq_rows", h.buildDLQVM(c))
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
	vm := configVM{baseVM: h.base(c, "config", "Configuration", "Runtime values this process was started with")}
	vm.Cfg.Port = h.cfg.Port
	vm.Cfg.NumWorkers = h.cfg.NumWorkers
	vm.Cfg.JobQueueSize = h.cfg.JobQueueSize
	vm.Cfg.JobTimeoutSeconds = int(h.cfg.JobTimeout.Seconds())
	vm.Cfg.MaxRetries = h.cfg.MaxRetries
	vm.Cfg.BaseBackoffSeconds = int(h.cfg.BaseBackoff.Seconds())
	vm.Cfg.ShutdownTimeoutSeconds = int(h.cfg.ShutdownTimeout.Seconds())
	_ = h.tmpl.Render(c.Writer, "config", vm)
}
