package main

import (
	"context"
	"fmt"
	"goflow/config"
	"goflow/handler"
	"goflow/model"
	"goflow/retry"
	"goflow/service"
	"goflow/store"
	"goflow/worker"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"github.com/gin-gonic/gin"
)

func main() {
	cfg := config.Load()

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
	go server.ListenAndServe()

	quit := make(chan os.Signal, 1)

	signal.Notify(
		quit,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<-quit

	cancel()

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		cfg.ShutdownTimeout,
	)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Server shutdown error:", err)
	}

	retryScheduler.Stop()
	close(jobs)

	wg.Wait()
}
