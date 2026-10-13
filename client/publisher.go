package client

import (
	"context"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
)

// Publisher owns a long-lived connection to a GoFlow broker. Safe for
// concurrent use.
type Publisher struct {
	conn *grpc.ClientConn
	stub goflowv1.BrokerClient
}

// NewPublisher dials target and returns a ready Publisher.
// target is host:port, e.g. "localhost:9090" or "goflow:9090" when
// running inside a Docker network.
func NewPublisher(target string, opts ...grpc.DialOption) (*Publisher, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Publisher{conn: conn, stub: goflowv1.NewBrokerClient(conn)}, nil
}

// Enqueue sends a produce RPC. idempotencyKey is required by the
// broker — repeat sends with the same key return the original job.
// maxRetries is a per-job override; pass 0 to use the broker default.
func (p *Publisher) Enqueue(ctx context.Context, j Job, idempotencyKey string, maxRetries uint32) (Job, error) {
	resp, err := p.stub.Enqueue(ctx, &goflowv1.EnqueueRequest{
		Type:           j.Type,
		Payload:        j.Payload,
		IdempotencyKey: idempotencyKey,
		MaxRetries:     maxRetries,
	})
	if err != nil {
		return Job{}, err
	}
	return protoToJob(resp), nil
}

// GetJob looks up the current state of a job by id.
func (p *Publisher) GetJob(ctx context.Context, id string) (Job, error) {
	resp, err := p.stub.GetJob(ctx, &goflowv1.GetJobRequest{Id: id})
	if err != nil {
		return Job{}, err
	}
	return protoToJob(resp), nil
}

func (p *Publisher) Close() error { return p.conn.Close() }

func protoToJob(j *goflowv1.Job) Job {
	out := Job{
		ID:        j.Id,
		Type:      j.Type,
		Payload:   j.Payload,
		Status:    j.Status,
		Attempts:  int(j.Attempts),
		LastError: j.LastError,
	}
	if j.CreatedAt != nil {
		out.CreatedAt = j.CreatedAt.AsTime()
	}
	if j.UpdatedAt != nil {
		out.UpdatedAt = j.UpdatedAt.AsTime()
	}
	return out
}
