// examples/producer — sends 20 "email" jobs to a GoFlow broker.
//
// Usage: GOFLOW_URL=localhost:9090 go run ./examples/producer
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"time"

	"goflow/client"
)

func main() {
	target := os.Getenv("GOFLOW_URL")
	if target == "" {
		target = "localhost:9090"
	}

	pub, err := client.NewPublisher(target)
	if err != nil {
		log.Fatalf("dial: %v", err)
	}
	defer pub.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	for i := 1; i <= 20; i++ {
		idemKey := fmt.Sprintf("example-%d-%d", time.Now().Unix(), i)
		payload := []byte(fmt.Sprintf(`{"to":"user%d@example.com","body":"hello"}`, i))

		job, err := pub.Enqueue(ctx, client.Job{
			Type:    "email",
			Payload: payload,
		}, idemKey, 3)
		if err != nil {
			log.Printf("enqueue %d: %v", i, err)
			continue
		}
		fmt.Printf("enqueued %s\n", job.ID)
	}
}
