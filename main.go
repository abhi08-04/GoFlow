package main
import (
	"context"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"
	"goflow/handler"
	"goflow/service"
	"goflow/worker"
	"goflow/model"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	jobs := make(chan model.Job, 10)
	var wg sync.WaitGroup
	wg.Add(3)

	for i := 1; i < 3; i++ {
		w := worker.NewWorker(i)
		go w.Start(jobs, &wg)
	}

	jobService := service.NewJobService(jobs)
	jobHandler := handler.NewJobHandler(jobService)

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.POST("/jobs", jobHandler.CreateJob)

	server := &http.Server {
		Addr : ":8080",
		Handler : r,
	}
	go server.ListenAndServe()

	quit := make(chan os.Signal, 1)

	signal.Notify(
		quit,
		syscall.SIGINT,
		syscall.SIGTERM,
	)

	<- quit

	shutdownCtx, shutdownCancel := context.WithTimeout(
		context.Background(),
		10 * time.Second,
	)
	defer shutdownCancel()

	server.Shutdown(shutdownCtx)

	close(jobs)

	wg.Wait()
}