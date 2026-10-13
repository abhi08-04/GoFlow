// examples/consumer — subscribes to "email" jobs and prints the
// payload. 10% of jobs are nacked to exercise the retry path.
//
// Usage: GOFLOW_URL=localhost:9090 go run ./examples/consumer
package main

import (
	"context"
	"errors"
	"log"
	"math/rand"
	"os"
	"os/signal"
	"syscall"
	"time"

	"goflow/client"
)

func main() {
	target := os.Getenv("GOFLOW_URL")
	if target == "" {
		target = "localhost:9090"
	}

	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()

	consumer := client.NewConsumer(target, []string{"email"},
		func(ctx context.Context, job client.Job) error {
			log.Printf("got %s type=%s attempts=%d payload=%s",
				job.ID[:8], job.Type, job.Attempts, string(job.Payload))

			// Simulate work.
			time.Sleep(200 * time.Millisecond)

			// 10% simulated failure to exercise retry + DLQ.
			if rand.Intn(10) == 0 {
				return errors.New("simulated downstream failure")
			}
			return nil
		},
		client.WithLeaseSeconds(30),
		client.WithPrefetch(4),
	)

	log.Printf("consuming from %s", target)
	if err := consumer.Run(ctx); err != nil {
		log.Printf("consumer stopped: %v", err)
	}
}
