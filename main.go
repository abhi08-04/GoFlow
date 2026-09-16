package main
import (
	"net/http"
	"goflow/handler"
	"goflow/service"
	"github.com/gin-gonic/gin"
)

func main() {
	r := gin.Default()

	jobService := service.NewJobService()
	jobHandler := handler.NewJobHandler(jobService)

	r.GET("/ping", func(c *gin.Context) {
		c.JSON(http.StatusOK, gin.H{
			"message": "pong",
		})
	})

	r.POST("/jobs", jobHandler.CreateJob)

	r.Run(":8080")
}