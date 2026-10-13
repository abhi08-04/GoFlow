// Package rpc hosts the gRPC Broker service implementation. Methods
// are stubbed out at rollout 4 and filled in by subsequent rollouts
// (Enqueue + GetJob in step 5; Consume streaming + lease state in
// step 6; admin RPCs in step 9).
package rpc

import (
	"goflow/internal/metrics"
	goflowv1 "goflow/internal/rpc/gen/goflow/v1"
	"goflow/internal/service"
	"goflow/internal/store"
)

// Server implements goflowv1.BrokerServer. UnimplementedBrokerServer
// is embedded so any RPC we have not written yet responds with
// codes.Unimplemented — the client sees a clear error instead of a
// panic, and new methods added to the proto do not break the build
// here (forward-compatibility knob set by
// require_unimplemented_servers=true in buf.gen.yaml).
type Server struct {
	goflowv1.UnimplementedBrokerServer

	store      store.Store
	jobService *service.JobService
	registry   *metrics.Registry
}

func NewServer(st store.Store, js *service.JobService, reg *metrics.Registry) *Server {
	return &Server{
		store:      st,
		jobService: js,
		registry:   reg,
	}
}
