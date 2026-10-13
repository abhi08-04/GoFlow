// Package rpc hosts the gRPC Broker service implementation.
package rpc

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"time"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"

	"goflow/internal/metrics"
	"goflow/internal/model"
	"goflow/internal/retry"
	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
	"goflow/internal/service"
	"goflow/internal/store"
)

// Config carries the knobs the server needs to clamp client-supplied
// values (lease duration, prefetch).
type Config struct {
	DefaultLease time.Duration
	MaxLease     time.Duration
	MaxPrefetch  uint32
	BaseBackoff  time.Duration
}

type Server struct {
	goflowv1.UnimplementedBrokerServer

	store      store.Store
	jobService *service.JobService
	registry   *metrics.Registry
	scheduler  *retry.Scheduler
	cfg        Config
}

func NewServer(st store.Store, js *service.JobService, reg *metrics.Registry, sched *retry.Scheduler, cfg Config) *Server {
	return &Server{
		store: st, jobService: js, registry: reg, scheduler: sched, cfg: cfg,
	}
}

// --- Enqueue / GetJob ---

func (s *Server) Enqueue(ctx context.Context, req *goflowv1.EnqueueRequest) (*goflowv1.Job, error) {
	if req.Type == "" {
		return nil, status.Error(codes.InvalidArgument, "type is required")
	}
	if req.IdempotencyKey == "" {
		return nil, status.Error(codes.InvalidArgument, "idempotency_key is required")
	}

	job, _, err := s.jobService.CreateJob(model.CreateJobRequest{
		Type:       req.Type,
		Payload:    string(req.Payload),
		MaxRetries: int(req.MaxRetries),
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

// claimedTimes carries per-delivery metadata the ack path needs so we
// can observe latency as (ack time - delivery time).
type claimedState struct {
	mu      sync.Mutex
	claimed map[string]time.Time
}

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

	// Prefetch semaphore — bounds how many un-acked deliveries the
	// writer may push before waiting for acks.
	sem := make(chan struct{}, prefetch)

	cs := &claimedState{claimed: make(map[string]time.Time)}

	// Reader goroutine: pulls Ack/Nack frames from the client.
	ackErrCh := make(chan error, 1)
	go func() {
		for {
			frame, err := stream.Recv()
			if err != nil {
				ackErrCh <- err
				return
			}
			switch k := frame.Kind.(type) {
			case *goflowv1.ClientFrame_Ack:
				s.handleAck(k.Ack, cs)
			case *goflowv1.ClientFrame_Nack:
				s.handleNack(k.Nack, cs)
			default:
				// StartConsume after the first frame is a protocol
				// violation; ignore rather than killing the stream.
			}
			// Release one prefetch slot regardless of ack/nack.
			select {
			case <-sem:
			default:
			}
		}
	}()

	// Writer loop: claim from the store, push JobDelivery frames.
	defer func() {
		// On stream exit, release any still-leased jobs we claimed.
		// They will come back on the next ExpiredLeases sweep, but a
		// prompt best-effort release gives faster redelivery.
		s.releaseAllHeld(cs)
	}()

	for {
		select {
		case sem <- struct{}{}:
		case err := <-ackErrCh:
			return err
		case <-stream.Context().Done():
			return nil
		}

		job, token, err := s.store.ClaimOne(stream.Context(), startFrame.Types, lease)
		if err != nil {
			if stream.Context().Err() != nil {
				return nil
			}
			return err
		}

		expiresAt := time.Now().Add(lease)
		cs.mu.Lock()
		cs.claimed[job.ID] = time.Now()
		cs.mu.Unlock()

		s.registry.RecordStart()

		if err := stream.Send(&goflowv1.ServerFrame{
			Kind: &goflowv1.ServerFrame_Delivery{
				Delivery: &goflowv1.JobDelivery{
					Job:        jobToProto(job),
					LeaseToken: string(token),
					ExpiresAt:  timestamppb.New(expiresAt),
				},
			},
		}); err != nil {
			// Stream died mid-send — undo our bookkeeping so the lease
			// expiry watcher will reclaim the job cleanly.
			s.store.Nack(job.ID, token, "stream send failed")
			cs.mu.Lock()
			delete(cs.claimed, job.ID)
			cs.mu.Unlock()
			return err
		}
	}
}

func (s *Server) handleAck(ack *goflowv1.Ack, cs *claimedState) {
	cs.mu.Lock()
	start, had := cs.claimed[ack.JobId]
	delete(cs.claimed, ack.JobId)
	cs.mu.Unlock()

	if err := s.store.Ack(ack.JobId, store.LeaseToken(ack.LeaseToken)); err != nil {
		return
	}
	job, ok := s.store.Get(ack.JobId)
	if ok {
		if had {
			s.registry.RecordCompleted(job.Type, time.Since(start))
		} else {
			s.registry.RecordCompleted(job.Type, 0)
		}
	}
}

func (s *Server) handleNack(nack *goflowv1.Nack, cs *claimedState) {
	cs.mu.Lock()
	delete(cs.claimed, nack.JobId)
	cs.mu.Unlock()

	job, _ := s.store.Get(nack.JobId)
	shouldRetry, _, err := s.store.Nack(nack.JobId, store.LeaseToken(nack.LeaseToken), nack.Error)
	if err != nil {
		return
	}
	s.registry.RecordFailed(job.Type)

	if shouldRetry {
		// backoff = base * 2^(attempt - 1)
		attempts := 1
		if job.Attempts > 0 {
			attempts = job.Attempts
		}
		delay := s.cfg.BaseBackoff * time.Duration(1<<(attempts-1))
		if ok := s.scheduler.Schedule(job, delay, func() {
			s.store.DLQAdd(job)
			s.registry.RecordDLQd()
		}); !ok {
			s.store.DLQAdd(job)
			s.registry.RecordDLQd()
		}
		return
	}
	s.store.DLQAdd(job)
	s.registry.RecordDLQd()
}

func (s *Server) releaseAllHeld(cs *claimedState) {
	cs.mu.Lock()
	ids := make([]string, 0, len(cs.claimed))
	for id := range cs.claimed {
		ids = append(ids, id)
	}
	cs.claimed = map[string]time.Time{}
	cs.mu.Unlock()

	// For each still-held job we don't know the lease token (we don't
	// store it stream-side). The lease watcher will reclaim them when
	// it next ticks.
	_ = ids
}

// --- Admin RPCs ---

func (s *Server) ListDLQ(ctx context.Context, _ *goflowv1.ListDLQRequest) (*goflowv1.ListDLQResponse, error) {
	jobs := s.store.DLQGetAll()
	out := make([]*goflowv1.Job, 0, len(jobs))
	for _, j := range jobs {
		out = append(out, jobToProto(j))
	}
	return &goflowv1.ListDLQResponse{Jobs: out}, nil
}

func (s *Server) RetryDLQ(ctx context.Context, req *goflowv1.RetryDLQRequest) (*goflowv1.RetryDLQResponse, error) {
	var toRetry []model.Job
	if len(req.Ids) == 0 {
		toRetry = s.store.DLQClaimAll()
	} else {
		for _, id := range req.Ids {
			if j, ok := s.store.DLQClaim(id); ok {
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
		s.store.DLQAdd(j)
		rejected++
	}
	return &goflowv1.RetryDLQResponse{Requeued: requeued, Rejected: rejected}, nil
}

func (s *Server) PurgeDLQ(ctx context.Context, _ *goflowv1.PurgeDLQRequest) (*goflowv1.PurgeDLQResponse, error) {
	jobs := s.store.DLQClaimAll()
	return &goflowv1.PurgeDLQResponse{Purged: uint32(len(jobs))}, nil
}

func (s *Server) DeleteDLQJob(ctx context.Context, req *goflowv1.DeleteDLQJobRequest) (*goflowv1.DeleteDLQJobResponse, error) {
	if _, ok := s.store.DLQClaim(req.Id); !ok {
		return nil, status.Error(codes.NotFound, fmt.Sprintf("job %s not in DLQ", req.Id))
	}
	return &goflowv1.DeleteDLQJobResponse{Id: req.Id}, nil
}

// --- conversions ---

func jobToProto(j model.Job) *goflowv1.Job {
	return &goflowv1.Job{
		Id:        j.ID,
		Type:      j.Type,
		Payload:   []byte(j.Payload),
		Status:    j.Status,
		Attempts:  uint32(j.Attempts),
		LastError: j.LastError,
		CreatedAt: timestamppb.New(j.CreatedAt),
		UpdatedAt: timestamppb.New(j.UpdatedAt),
	}
}
