// Package client is the Go SDK for talking to a GoFlow broker.
//
// Publisher.Enqueue is the producer side; Consumer.Run opens a
// bidirectional stream and drives one of your handlers per job. Both
// hide the generated gRPC types behind friendly structs.
package client

import (
	"context"
	"time"
)

type Job struct {
	ID        string
	Type      string
	Payload   []byte
	Status    string
	Attempts  int
	LastError string
	CreatedAt time.Time
	UpdatedAt time.Time
}

// Handler processes a single job delivery. Returning nil acks the
// job; returning an error nacks with that error message (which the
// server stores and uses for the retry / DLQ decision).
type Handler func(ctx context.Context, job Job) error
