// Package rpc hosts the gRPC Broker service implementation. It is a
// thin adapter that:
//
//   - delegates Publish to the existing service.JobService (which
//     already handles idempotency + chan enqueue)
//   - delegates Consume to broker.Claim / Ack / Nack
//   - delegates admin RPCs to broker + store.DLQ
//
// Nothing in service/, store/, retry/, metrics/ is modified.
package rpc

import (
	"context"
	"errors"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"goflow/broker"
	"goflow/metrics"
	"goflow/model"
	"goflow/service"
	"goflow/store"

	goflowv1 "goflow/rpc/gen/goflow/v1"
)

// Config carries the clamps the server applies to client-supplied
// lease / prefetch values.
type Config struct {
	DefaultLease time.Duration
	MaxLease     time.Duration
	MaxWait      time.Duration
	MaxPrefetch  uint32
}

type Server struct {
	goflowv1.UnimplementedBrokerServer

	jobService *service.JobService
	store      *store.JobStore
	dlq        *store.DLQ
	broker     *broker.Broker
	registry   *metrics.Registry
	cfg        Config
}

func NewServer(
	js *service.JobService,
	st *store.JobStore,
	dlq *store.DLQ,
	br *broker.Broker,
	reg *metrics.Registry,
	cfg Config,
) *Server {
	return &Server{
		jobService: js,
		store:      st,
		dlq:        dlq,
		broker:     br,
		registry:   reg,
		cfg:        cfg,
	}
}

// --- Publish / GetJob ---

func (s *Server) Publish(ctx context.Context, req *goflowv1.PublishRequest) (*goflowv1.Job, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}
	job, _, err := s.jobService.CreateJob(model.CreateJobRequest{
		Type:    req.Type,
		Payload: string(req.Payload),
	}, req.IdempotencyKey)
	if err != nil {
		if errors.Is(err, store.ErrQueueFull) {
			return nil, status.Error(codes.ResourceExhausted, "queue is full")
		}
		return nil, status.Error(codes.FailedPrecondition, err.Error())
	}
	return jobToProto(job), nil
}

func (s *Server) GetJob(ctx context.Context, req *goflowv1.GetJobRequest) (*goflowv1.Job, error) {
	job, ok := s.store.Get(req.Id)
	if !ok {
		return nil, status.Error(codes.NotFound, "job not found")
	}
	return jobToProto(job), nil
}

// --- Consume (bidi stream) ---

func (s *Server) Consume(stream goflowv1.Broker_ConsumeServer) error {
	first, err := stream.Recv()
	if err != nil {
		return err
	}
	startFrame := first.GetStart()
	if startFrame == nil {
		return status.Error(codes.InvalidArgument, "first frame must be StartConsume")
	}

	lease := time.Duration(startFrame.LeaseSeconds) * time.Second
	if lease <= 0 {
		lease = s.cfg.DefaultLease
	}
	if lease > s.cfg.MaxLease {
		lease = s.cfg.MaxLease
	}
	prefetch := startFrame.Prefetch
	if prefetch == 0 {
		prefetch = 1
	}
	if prefetch > s.cfg.MaxPrefetch {
		prefetch = s.cfg.MaxPrefetch
	}

	// Prefetch semaphore — caps un-acked in-flight deliveries per consumer.
	sem := make(chan struct{}, prefetch)

	// Reader goroutine: pulls Ack / Nack frames from the client.
	ackErrCh := make(chan error, 1)
	var tracked sync.Map // in-flight job IDs this stream has outstanding
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				ackErrCh <- err
				return
			}
			switch k := frame.Kind.(type) {
			case *goflowv1.ClientFrame_Ack:
				_ = s.broker.Ack(k.Ack.JobId, k.Ack.LeaseToken)
				tracked.Delete(k.Ack.JobId)
			case *goflowv1.ClientFrame_Nack:
				_ = s.broker.Nack(k.Nack.JobId, k.Nack.LeaseToken, k.Nack.Error)
				tracked.Delete(k.Nack.JobId)
			}
			// Release a prefetch slot regardless of outcome.
			select {
			case <-sem:
			default:
			}
		}
	}()

	// Writer loop: claim from the broker, push a JobDelivery frame.
	//
	// Each Claim blocks up to MaxWait; when the stream context cancels
	// (client disconnected or server shutdown), Claim returns ctx.Err
	// and we exit cleanly.
	waitWindow := s.cfg.MaxWait
	if waitWindow <= 0 {
		waitWindow = 30 * time.Second
	}

	for {
		select {
		case sem <- struct{}{}:
		case err := <-ackErrCh:
			return err
		case <-stream.Context().Done():
			return nil
		}

		leaseEntry, got, err := s.broker.Claim(stream.Context(), startFrame.Types, waitWindow, lease)
		if err != nil {
			if stream.Context().Err() != nil {
				return nil
			}
			return err
		}
		if !got {
			// Window elapsed with nothing available. Release the
			// prefetch slot and loop so we don't starve on long waits.
			select {
			case <-sem:
			default:
			}
			continue
		}

		tracked.Store(leaseEntry.Job.ID, struct{}{})

		if err := stream.Send(&goflowv1.ServerFrame{
			Kind: &goflowv1.ServerFrame_Delivery{Delivery: &goflowv1.JobDelivery{
				Job:        jobToProto(leaseEntry.Job),
				LeaseToken: leaseEntry.Token,
				ExpiresAt:  timestamppb.New(leaseEntry.ExpiresAt),
			}},
		}); err != nil {
			// Send failed — nack so the lease sweeper doesn't have to.
			_ = s.broker.Nack(leaseEntry.Job.ID, leaseEntry.Token, "stream send failed")
			tracked.Delete(leaseEntry.Job.ID)
			return err
		}
	}
}

// --- Admin ---

func (s *Server) ListDLQ(ctx context.Context, _ *goflowv1.ListDLQRequest) (*goflowv1.ListDLQResponse, error) {
	jobs := s.dlq.GetAll()
	out := make([]*goflowv1.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToProto(j))
	}
	return &goflowv1.ListDLQResponse{Jobs: out}, nil
}

func (s *Server) RetryDLQ(ctx context.Context, req *goflowv1.RetryDLQRequest) (*goflowv1.RetryDLQResponse, error) {
	var toRetry []model.Job
	if len(req.Ids) == 0 {
		// All.
		for _, j := range s.dlq.GetAll() {
			if claimed, ok := s.dlq.Claim(j.ID); ok {
				toRetry = append(toRetry, claimed)
			}
		}
	} else {
		for _, id := range req.Ids {
			if j, ok := s.dlq.Claim(id); ok {
				toRetry = append(toRetry, j)
			}
		}
	}

	var requeued, rejected uint32
	for _, j := range toRetry {
		if s.jobService.RetryJob(j) {
			requeued++
			continue
		}
		s.dlq.Add(j)
		rejected++
	}
	return &goflowv1.RetryDLQResponse{Requeued: requeued, Rejected: rejected}, nil
}

func (s *Server) PurgeDLQ(ctx context.Context, _ *goflowv1.PurgeDLQRequest) (*goflowv1.PurgeDLQResponse, error) {
	count := uint32(0)
	for _, j := range s.dlq.GetAll() {
		if _, ok := s.dlq.Claim(j.ID); ok {
			count++
		}
	}
	return &goflowv1.PurgeDLQResponse{Purged: count}, nil
}

// --- conversion ---

func jobToProto(j model.Job) *goflowv1.Job {
	return &goflowv1.Job{
		Id:       j.ID,
		Type:     j.Type,
		Payload:  []byte(j.Payload),
		Status:   j.Status,
		Attempts: uint32(j.Attempts),
		Timestamp: timestamppb.Now(),
	}
}

// Compile-time check the server implements the generated interface
// (helps catch proto drift at build time).
var _ goflowv1.BrokerServer = (*Server)(nil)
