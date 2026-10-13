package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/gin-gonic/gin"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"

	"goflow/internal/broker"
	"goflow/internal/config"
	"goflow/internal/handler"
	"goflow/internal/metrics"
	"goflow/internal/retry"
	"goflow/internal/rpc"
	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
	"goflow/internal/service"
	"goflow/internal/store"
	"goflow/internal/web"
)

func main() {
	cfg, err := config.Load()
	if err != nil {
		log.Fatalf("Failed to load config: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	st := store.NewMemoryStore()
	st.QueueCap = cfg.JobQueueSize

	registry := metrics.New()
	ring := metrics.NewRingBuffer(720)
	sampler := metrics.NewSampler(registry, ring, st, 5*time.Second)
	go sampler.Run(ctx)

	jobService := service.NewJobService(st, registry, cfg.MaxRetries)
	retryScheduler := retry.NewScheduler(ctx, st, registry)

	leaseWatcher := broker.NewLeaseWatcher(st, retryScheduler, registry, cfg.LeaseSweepInterval, cfg.BaseBackoff)
	go leaseWatcher.Run(ctx)

	// --- HTTP side (dashboard + legacy JSON) ---
	r := gin.Default()
	jobHandler := handler.NewJobHandler(jobService, st)
	metricsHandler := handler.NewMetricsHandler(registry, ring, cfg)

	tmpl, err := web.Parse()
	if err != nil {
		log.Fatalf("Failed to parse templates: %v", err)
	}
	pageHandler := handler.NewPageHandler(tmpl, jobService, st, registry, ring, cfg)

	r.GET("/ping", func(c *gin.Context) { c.JSON(http.StatusOK, gin.H{"message": "pong"}) })

	// HTML pages.
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

	r.StaticFS("/static", web.StaticFS())

	// JSON API (same as before — the dashboard calls these).
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

	// --- gRPC side ---
	grpcLis, err := net.Listen("tcp", ":"+cfg.GrpcPort)
	if err != nil {
		log.Fatalf("gRPC listen on :%s failed: %v", cfg.GrpcPort, err)
	}

	grpcServer := grpc.NewServer()
	brokerSrv := rpc.NewServer(st, jobService, registry, retryScheduler, rpc.Config{
		DefaultLease: cfg.DefaultLease,
		MaxLease:     cfg.MaxLease,
		MaxPrefetch:  uint32(cfg.MaxPrefetch),
		BaseBackoff:  cfg.BaseBackoff,
	})
	goflowv1.RegisterBrokerServer(grpcServer, brokerSrv)
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

	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), cfg.ShutdownTimeout)
	defer shutdownCancel()

	// gRPC first — streaming consumers die cleanly before we yank HTTP.
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

	cancel()
	retryScheduler.Stop()
	fmt.Println("Shutdown complete")
}
