package main

import (
	"context"
	"fmt"
	"github.com/gin-gonic/gin"
	"goflow/config"
	"goflow/handler"
	"goflow/model"
	"goflow/retry"
	"goflow/service"
	"goflow/store"
	"goflow/worker"
	"log"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
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
	dlq := store.NewDLQ()
	jobStore := store.NewJobStore()
	retryScheduler := retry.NewScheduler(ctx, jobs)
	var wg sync.WaitGroup

	for i := 1; i <= cfg.NumWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, jobStore, retryScheduler, cfg.MaxRetries, cfg.BaseBackoff, cfg.JobTimeout, dlq)
		go w.Start(jobs, &wg)
	}

	jobService := service.NewJobService(jobs, jobStore)
	jobHandler := handler.NewJobHandler(jobService, dlq)

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.POST("/jobs", jobHandler.CreateJob)
	r.POST("/dlq/:id/retry", jobHandler.RetryDLQJob)

	r.GET("/jobs/:id", jobHandler.GetJob)
	r.GET("/dlq", jobHandler.GetDLQ)

	server := &http.Server{
		Addr:    ":" + cfg.Port,
		Handler: r,
	}

	serverErr := make(chan error, 1)

	go func() {
		serverErr <- server.ListenAndServe()
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	defer signal.Stop(quit)

	select {
	case sig := <-quit:
		fmt.Println("Received signal:", sig)

	case err := <-serverErr:
		if err != nil && err != http.ErrServerClosed {
			log.Printf("HTTP server failed: %v", err)
		}
		return
	}

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer shutdownCancel()

	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Graceful HTTP shutdown failed:", err)

		if closeErr := server.Close(); closeErr != nil {
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
