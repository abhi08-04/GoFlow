package client

import (
	"context"
	"fmt"
	"sync"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
)

// Consumer long-polls the broker on a bidi stream, invokes the user
// handler per delivery, and sends Ack/Nack back up the same stream.
//
// gRPC streams are not safe for concurrent Send, so the consumer uses
// one dispatcher goroutine that owns Send; handler goroutines push
// their ack/nack results onto an internal channel.
type Consumer struct {
	target    string
	jobTypes  []string
	handler   Handler
	lease     uint32 // seconds, 0 = broker default
	prefetch  uint32 // concurrent in-flight deliveries
	dialOpts  []grpc.DialOption
}

// Opt customizes a Consumer. All opts are applied before Run.
type Opt func(*Consumer)

func WithLeaseSeconds(n uint32) Opt { return func(c *Consumer) { c.lease = n } }
func WithPrefetch(n uint32) Opt     { return func(c *Consumer) { c.prefetch = n } }
func WithDialOptions(opts ...grpc.DialOption) Opt {
	return func(c *Consumer) { c.dialOpts = opts }
}

// NewConsumer subscribes to one or more job types. Pass an empty
// types slice to receive any job type.
func NewConsumer(target string, types []string, h Handler, opts ...Opt) *Consumer {
	c := &Consumer{
		target: target, jobTypes: types, handler: h, prefetch: 1,
	}
	for _, o := range opts {
		o(c)
	}
	if len(c.dialOpts) == 0 {
		c.dialOpts = []grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}
	}
	return c
}

// Run opens the stream and blocks until ctx cancels or the stream
// errors. Returns the first non-nil error encountered.
func (c *Consumer) Run(ctx context.Context) error {
	conn, err := grpc.NewClient(c.target, c.dialOpts...)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	defer conn.Close()

	stub := goflowv1.NewBrokerClient(conn)
	stream, err := stub.Consume(ctx)
	if err != nil {
		return fmt.Errorf("consume: %w", err)
	}

	// Opening frame: subscribe + lease + prefetch.
	if err := stream.Send(&goflowv1.ClientFrame{
		Kind: &goflowv1.ClientFrame_Start{Start: &goflowv1.StartConsume{
			Types:        c.jobTypes,
			LeaseSeconds: c.lease,
			Prefetch:     c.prefetch,
		}},
	}); err != nil {
		return fmt.Errorf("send start: %w", err)
	}

	// One dispatcher goroutine owns Send. Handlers write results here.
	sendCh := make(chan *goflowv1.ClientFrame, c.prefetch)
	sendDone := make(chan struct{})
	go func() {
		for frame := range sendCh {
			if err := stream.Send(frame); err != nil {
				return
			}
		}
		close(sendDone)
	}()

	var wg sync.WaitGroup

	for {
		frame, err := stream.Recv()
		if err != nil {
			// Drain outstanding handlers before returning so their
			// acks/nacks have a chance to go out before the stream
			// context collapses.
			wg.Wait()
			close(sendCh)
			<-sendDone
			if ctx.Err() != nil {
				return ctx.Err()
			}
			return err
		}

		delivery := frame.GetDelivery()
		if delivery == nil {
			continue
		}

		wg.Add(1)
		go func(d *goflowv1.JobDelivery) {
			defer wg.Done()
			job := protoToJob(d.Job)
			handlerErr := c.handler(ctx, job)
			if handlerErr == nil {
				sendCh <- &goflowv1.ClientFrame{
					Kind: &goflowv1.ClientFrame_Ack{Ack: &goflowv1.Ack{
						JobId: job.ID, LeaseToken: d.LeaseToken,
					}},
				}
				return
			}
			sendCh <- &goflowv1.ClientFrame{
				Kind: &goflowv1.ClientFrame_Nack{Nack: &goflowv1.Nack{
					JobId: job.ID, LeaseToken: d.LeaseToken, Error: handlerErr.Error(),
				}},
			}
		}(delivery)
	}
}
