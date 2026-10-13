// goflowd — the standalone broker service.
//
// This binary REUSES the learning codebase as-is:
//   - store.JobStore / store.DLQ  — unchanged
//   - service.JobService          — unchanged (handles idempotency, chan enqueue)
//   - retry.Scheduler             — unchanged (exponential backoff → chan)
//   - metrics.Registry/Sampler    — unchanged (dashboard tiles still work)
//   - handler/*                   — unchanged (HTTP dashboard + JSON API)
//   - web/*                       — unchanged (embedded templates + static)
//
// What's added:
//   - gRPC listener on :GRPC_PORT with the Broker service (any language can connect)
//   - broker.LeaseSweeper goroutine (reclaims expired leases)
//
// What's dropped vs the original main.go:
//   - No worker.Worker goroutines. Remote consumers do the work by
//     opening a Consume stream.
package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"goflow/broker"
	"goflow/config"
	"goflow/handler"
	"goflow/metrics"
	"goflow/model"
	"goflow/retry"
	"goflow/rpc"
	"goflow/service"
	"goflow/store"
	"goflow/web"

	goflowv1 "goflow/rpc/gen/goflow/v1"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	grpcPort := getEnvDefault("GRPC_PORT", "9090")
	defaultLease := time.Duration(getIntEnvDefault("DEFAULT_LEASE_SECONDS", 60)) * time.Second
	maxLease := time.Duration(getIntEnvDefault("MAX_LEASE_SECONDS", 300)) * time.Second
	maxWait := time.Duration(getIntEnvDefault("MAX_WAIT_SECONDS", 30)) * time.Second
	sweepInterval := time.Duration(getIntEnvDefault("LEASE_SWEEP_INTERVAL_SECONDS", 1)) * time.Second
	maxPrefetch := uint32(getIntEnvDefault("MAX_PREFETCH", 64))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// --- Reused learning-code wiring (same shape as original main.go) ---
	jobs := make(chan model.Job, cfg.JobQueueSize)
	jobStore := store.NewJobStore()
	dlq := store.NewDLQ()

	registry := metrics.New()
	ring := metrics.NewRingBuffer(720)
	sampler := metrics.NewSampler(registry, ring, jobs, dlq, 5*time.Second)
	go sampler.Run(ctx)

	retryScheduler := retry.NewScheduler(ctx, jobs, registry)

	jobService := service.NewJobService(jobs, jobStore, registry)

	// --- New: broker layer + gRPC + lease sweeper ---
	br := broker.NewBroker(jobs, jobStore, dlq, retryScheduler, registry, cfg.MaxRetries, cfg.BaseBackoff)
	sweeper := broker.NewLeaseSweeper(br, sweepInterval)
	go sweeper.Run(ctx)

	// --- HTTP side (dashboard + JSON API, unchanged routes) ---
	r := gin.Default()
	jobHandler := handler.NewJobHandler(jobService, dlq)
	metricsHandler := handler.NewMetricsHandler(registry, ring, cfg)

	tmpl, err := web.Parse()
	if err != nil {
		log.Fatalf("Failed to parse templates: %v", err)
	}
	pageHandler := handler.NewPageHandler(tmpl, jobService, dlq, registry, ring, cfg)

	r.GET("/ping", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"message": "pong"}) })

	r.GET("/", pageHandler.Dashboard)
	r.GET("/jobs", pageHandler.Jobs)
	r.GET("/jobs/:id/view", pageHandler.JobDetail)
	r.GET("/dlq", pageHandler.DLQ)
	r.GET("/config", pageHandler.Config)

	r.GET("/partials/tiles", pageHandler.PartialTiles)
	r.GET("/partials/activity", pageHandler.PartialActivity)
	r.GET("/partials/jobs", pageHandler.PartialJobs)
	r.GET("/partials/dlq", pageHandler.PartialDLQ)

	r.StaticFS("/static", web.StaticFS())

	r.POST("/jobs", jobHandler.CreateJob)
	r.GET("/api/jobs", jobHandler.ListJobs)
	r.GET("/api/jobs/:id", jobHandler.GetJob)
	r.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)
	r.DELETE("/dlq/:id", jobHandler.DeleteDLQJob)
	r.POST("/dlq/retry-all", jobHandler.RetryAllDLQ)
	r.POST("/dlq/purge", jobHandler.PurgeDLQ)
	r.GET("/api/dlq", jobHandler.GetDLQ)

	api := r.Group("/api")
	api.GET("/metrics/snapshot", metricsHandler.Snapshot)
	api.GET("/metrics/timeseries", metricsHandler.TimeSeries)
	api.GET("/config", metricsHandler.ConfigView)

	httpServer := &http.Server{Addr: ":" + cfg.Port, Handler: r}
	httpErr := make(chan error, 1)
	go func() {
		fmt.Println("HTTP listening on :" + cfg.Port)
		httpErr <- httpServer.ListenAndServe()
	}()

	// --- gRPC listener ---
	grpcLis, err := net.Listen("tcp", ":"+grpcPort)
	if err != nil {
		log.Fatalf("gRPC listen on :%s failed: %v", grpcPort, err)
	}

	grpcServer := grpc.NewServer()
	brokerSrv := rpc.NewServer(jobService, jobStore, dlq, br, registry, rpc.Config{
		DefaultLease: defaultLease,
		MaxLease:     maxLease,
		MaxWait:      maxWait,
		MaxPrefetch:  maxPrefetch,
	})
	goflowv1.RegisterBrokerServer(grpcServer, brokerSrv)
	reflection.Register(grpcServer)

	grpcErr := make(chan error, 1)
	go func() {
		fmt.Println("gRPC listening on :" + grpcPort)
		grpcErr <- grpcServer.Serve(grpcLis)
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	select {
	case sig := <-quit:
		fmt.Println("Received signal:", sig)
	case err := <-httpErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
	case err := <-grpcErr:
		if err != nil {
			log.Printf("gRPC server failed: %v", err)
		}
	}

	shutdownCtx, done := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer done()

	// gRPC first — drain streaming consumers before yanking HTTP.
	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
		fmt.Println("gRPC server stopped gracefully")
	case <-shutdownCtx.Done():
		fmt.Println("gRPC shutdown timeout; forcing stop")
		grpcServer.Stop()
	}

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Graceful HTTP shutdown failed:", err)
		if closeErr := httpServer.Close(); closeErr != nil {
			fmt.Println("HTTP server close error:", closeErr)
		}
	}

	cancel()
	retryScheduler.Stop()
	fmt.Println("Shutdown complete")
}

func getEnvDefault(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
func getIntEnvDefault(key string, fallback int) int {
	v := os.Getenv(key)
	if v == "" {
		return fallback
	}
	n, err := strconv.Atoi(v)
	if err != nil {
		return fallback
	}
	return n
}
