package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"goflow/internal/config"
	"goflow/internal/handler"
	"goflow/internal/metrics"
	"goflow/internal/model"
	"goflow/internal/retry"
	"goflow/internal/rpc"
	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
	"goflow/internal/service"
	"goflow/internal/store"
	"goflow/internal/web"
	"goflow/internal/worker"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	r := gin.Default()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan model.Job, cfg.JobQueueSize)
	st := store.NewMemoryStore()

	// Metrics: one registry shared by every goroutine. 720 ring slots
	// at a 5s sampling interval = exactly one hour of history.
	registry := metrics.New()
	ring := metrics.NewRingBuffer(720)
	sampler := metrics.NewSampler(registry, ring, jobs, st, 5*time.Second)
	go sampler.Run(ctx)

	retryScheduler := retry.NewScheduler(ctx, jobs, registry)
	var wg sync.WaitGroup

	for i := 1; i <= cfg.NumWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, st, retryScheduler, cfg.MaxRetries, cfg.BaseBackoff, cfg.JobTimeout, registry)
		go w.Start(jobs, &wg)
	}

	jobService := service.NewJobService(jobs, st, registry)
	jobHandler := handler.NewJobHandler(jobService, st)
	metricsHandler := handler.NewMetricsHandler(registry, ring, cfg)

	tmpl, err := web.Parse()
	if err != nil {
		log.Fatalf("Failed to parse templates: %v", err)
	}
	pageHandler := handler.NewPageHandler(tmpl, jobService, st, registry, ring, cfg)

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	// HTML pages (serve the UI on same port as the API — monolith).
	r.GET("/", pageHandler.Dashboard)
	r.GET("/jobs", pageHandler.Jobs)
	r.GET("/jobs/:id/view", pageHandler.JobDetail)
	r.GET("/dlq", pageHandler.DLQ)
	r.GET("/config", pageHandler.Config)

	// HTMX partials.
	r.GET("/partials/tiles", pageHandler.PartialTiles)
	r.GET("/partials/activity", pageHandler.PartialActivity)
	r.GET("/partials/jobs", pageHandler.PartialJobs)
	r.GET("/partials/dlq", pageHandler.PartialDLQ)

	// Static assets, served from the embedded web/static directory.
	r.StaticFS("/static", web.StaticFS())

	// JSON API — keeping the content-negotiation simple by putting job
	// JSON under /api so /jobs can be the HTML page.
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

	// --- HTTP (dashboard + legacy JSON) ---
	httpServer := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	httpErr := make(chan error, 1)
	go func() {
		fmt.Println("HTTP listening on :" + cfg.Port)
		httpErr <- httpServer.ListenAndServe()
	}()

	// --- gRPC (broker protocol) ---
	grpcLis, err := net.Listen("tcp", ":"+cfg.GrpcPort)
	if err != nil {
		log.Fatalf("gRPC listen on :%s failed: %v", cfg.GrpcPort, err)
	}

	grpcServer := grpc.NewServer()
	brokerSrv := rpc.NewServer(st, jobService, registry)
	goflowv1.RegisterBrokerServer(grpcServer, brokerSrv)
	// Reflection lets grpcurl / Postman list services without a .proto file.
	// Cheap, read-only, no auth implications on an unauthenticated port.
	reflection.Register(grpcServer)

	grpcErr := make(chan error, 1)
	go func() {
		fmt.Println("gRPC listening on :" + cfg.GrpcPort)
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

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer shutdownCancel()

	// Stop accepting new RPCs; wait for in-flight unary + streaming
	// calls to finish within shutdownCtx. If the context expires we
	// fall back to Stop() which closes every connection immediately.
	grpcStopped := make(chan struct{})
	go func() {
		grpcServer.GracefulStop()
		close(grpcStopped)
	}()
	select {
	case <-grpcStopped:
		fmt.Println("gRPC server stopped gracefully")
	case <-shutdownCtx.Done():
		fmt.Println("gRPC shutdown timeout exceeded; forcing stop")
		grpcServer.Stop()
	}

	if err := httpServer.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Graceful HTTP shutdown failed:", err)

		if closeErr := httpServer.Close(); closeErr != nil {
			fmt.Println("HTTP server close error:", closeErr)
		}
	}

	// Stop retry scheduling before closing the jobs channel.
	cancel()
	retryScheduler.Stop()
	close(jobs)

	workersDone := make(chan struct{})

	go func() {
		wg.Wait()
		close(workersDone)
	}()

	select {
	case <-workersDone:
		fmt.Println("All workers stopped gracefully")

	case <-shutdownCtx.Done():
		fmt.Println("Shutdown timeout exceeded; workers may not have finished")
	}

}
