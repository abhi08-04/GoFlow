// Package client is the Go SDK for talking to a GoFlow broker.
//
// Design goal: look like `amqplib` or `kafkajs` — users never see
// protobuf types, lease tokens, or stream frames. Three method calls
// is the whole API: Dial, Publish, Consume.
//
//	c, _ := client.Dial("localhost:9090")
//	defer c.Close()
//
//	// Produce
//	c.Publish(ctx, "email",
//	    map[string]string{"to": "a@b.com", "subject": "hi"},
//	    "idem-k1")
//
//	// Consume (blocks; handler fires per delivery)
//	c.Consume(ctx, "email", func(ctx context.Context, job client.Job) error {
//	    var email struct{ To, Subject string }
//	    job.Decode(&email)
//	    return sendEmail(email)
//	}, client.ConsumeOpts{Prefetch: 4})
package client

import (
	"context"
	"encoding/json"
	"sync"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	goflowv1 "goflow/rpc/gen/goflow/v1"
)

// Job is the user-facing delivery shape. Payload is kept as raw bytes
// so you can treat it however suits — Decode is a convenience for the
// common JSON case.
type Job struct {
	ID       string
	Type     string
	Status   string
	Attempts int
	Payload  []byte
}

// Decode unmarshals Payload into v as JSON.
func (j Job) Decode(v any) error { return json.Unmarshal(j.Payload, v) }

// Handler is invoked once per delivery. Return nil to ack; return
// a non-nil error to nack with that message (which triggers the
// broker's retry → DLQ pipeline).
type Handler func(ctx context.Context, job Job) error

// Client is both producer and consumer. Safe for concurrent use.
type Client struct {
	conn *grpc.ClientConn
	stub goflowv1.BrokerClient
}

// Dial opens a long-lived connection to target (host:port). The
// returned Client is ready for Publish and Consume immediately.
func Dial(target string, opts ...grpc.DialOption) (*Client, error) {
	if len(opts) == 0 {
		opts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	conn, err := grpc.NewClient(target, opts...)
	if err != nil {
		return nil, err
	}
	return &Client{conn: conn, stub: goflowv1.NewBrokerClient(conn)}, nil
}

func (c *Client) Close() error { return c.conn.Close() }

// Publish enqueues a job. payload accepts []byte, string, nil, or
// anything JSON-serializable (map, struct, slice, …).
//
// idempotencyKey must be unique per logical request; sending the
// same key again returns the original job without re-enqueueing.
func (c *Client) Publish(ctx context.Context, jobType string, payload any, idempotencyKey string) (Job, error) {
	buf, err := encodePayload(payload)
	if err != nil {
		return Job{}, err
	}
	resp, err := c.stub.Publish(ctx, &goflowv1.PublishRequest{
		Type:           jobType,
		Payload:        buf,
		IdempotencyKey: idempotencyKey,
	})
	if err != nil {
		return Job{}, err
	}
	return protoToJob(resp), nil
}

// ConsumeOpts controls stream parameters.
type ConsumeOpts struct {
	LeaseSeconds   uint32        // per-delivery lease; 0 = broker default
	Prefetch       uint32        // in-flight deliveries; 0 → 1 (strict)
	ReconnectDelay time.Duration // between stream reconnects; 0 → 2s
}

// Consume opens a bidi stream, dispatches each delivery to handler,
// and ack/nacks for you. Blocks until ctx cancels. On stream errors
// it reconnects with ReconnectDelay backoff.
func (c *Client) Consume(ctx context.Context, jobType string, handler Handler, opts ConsumeOpts) error {
	if opts.LeaseSeconds == 0 {
		opts.LeaseSeconds = 60
	}
	if opts.Prefetch == 0 {
		opts.Prefetch = 1
	}
	if opts.ReconnectDelay == 0 {
		opts.ReconnectDelay = 2 * time.Second
	}

	for {
		if ctx.Err() != nil {
			return nil
		}
		_ = c.consumeOnce(ctx, jobType, handler, opts)
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(opts.ReconnectDelay):
		}
	}
}

func (c *Client) consumeOnce(ctx context.Context, jobType string, handler Handler, opts ConsumeOpts) error {
	stream, err := c.stub.Consume(ctx)
	if err != nil {
		return err
	}

	types := []string{}
	if jobType != "" {
		types = []string{jobType}
	}
	if err := stream.Send(&goflowv1.ClientFrame{Kind: &goflowv1.ClientFrame_Start{
		Start: &goflowv1.StartConsume{
			Types:        types,
			LeaseSeconds: opts.LeaseSeconds,
			Prefetch:     opts.Prefetch,
		},
	}}); err != nil {
		return err
	}

	// gRPC streams are not safe for concurrent Send, so one dispatcher
	// goroutine owns Send. Handler goroutines push onto sendCh.
	sendCh := make(chan *goflowv1.ClientFrame, opts.Prefetch)
	sendDone := make(chan struct{})
	go func() {
		for frame := range sendCh {
			_ = stream.Send(frame)
		}
		close(sendDone)
	}()

	var wg sync.WaitGroup

	for {
		frame, err := stream.Recv()
		if err != nil {
			wg.Wait()
			close(sendCh)
			<-sendDone
			return err
		}
		d := frame.GetDelivery()
		if d == nil {
			continue
		}
		wg.Add(1)
		go func(d *goflowv1.JobDelivery) {
			defer wg.Done()
			job := protoToJob(d.Job)
			herr := handler(ctx, job)
			if herr == nil {
				sendCh <- &goflowv1.ClientFrame{Kind: &goflowv1.ClientFrame_Ack{Ack: &goflowv1.Ack{
					JobId: job.ID, LeaseToken: d.LeaseToken,
				}}}
				return
			}
			sendCh <- &goflowv1.ClientFrame{Kind: &goflowv1.ClientFrame_Nack{Nack: &goflowv1.Nack{
				JobId: job.ID, LeaseToken: d.LeaseToken, Error: herr.Error(),
			}}}
		}(d)
	}
}

func encodePayload(payload any) ([]byte, error) {
	switch p := payload.(type) {
	case nil:
		return []byte{}, nil
	case []byte:
		return p, nil
	case string:
		return []byte(p), nil
	default:
		return json.Marshal(payload)
	}
}

func protoToJob(j *goflowv1.Job) Job {
	return Job{
		ID:       j.Id,
		Type:     j.Type,
		Status:   j.Status,
		Attempts: int(j.Attempts),
		Payload:  j.Payload,
	}
}
