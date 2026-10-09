package main

import (
	"context"
	"fmt"
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
	"time"

	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	jobs := make(chan model.Job, 10)
	dlq := store.NewDLQ()
	jobStore := store.NewJobStore()
	retryScheduler := retry.NewScheduler(ctx, jobs)
	var wg sync.WaitGroup

	const numWorkers = 3
	for i := 1; i <= numWorkers; i++ {
		wg.Add(1)
		w := worker.NewWorker(i, jobStore, retryScheduler, 3, 1*time.Second, dlq)
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
		Addr:    ":8080",
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
		10 * time.Second,
	)
	defer shutdownCancel()
	if err := server.Shutdown(shutdownCtx); err != nil {
		fmt.Println("Server shutdown error:", err)
	}

	close(jobs)

	wg.Wait()
}
