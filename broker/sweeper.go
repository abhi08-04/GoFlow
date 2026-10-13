package broker

import (
	"context"
	"time"
)

// LeaseSweeper reclaims expired leases on a ticker. One goroutine for
// the whole broker; it never contends with the hot path (Claim / Ack /
// Nack) beyond briefly holding the LeaseStore mutex during each tick.
type LeaseSweeper struct {
	broker   *Broker
	interval time.Duration
}

func NewLeaseSweeper(b *Broker, interval time.Duration) *LeaseSweeper {
	return &LeaseSweeper{broker: b, interval: interval}
}

// Run blocks until ctx cancels. Intended to be launched as its own
// goroutine from main.
func (s *LeaseSweeper) Run(ctx context.Context) {
	t := time.NewTicker(s.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case now := <-t.C:
			for _, lease := range s.broker.leases.Expired(now) {
				s.broker.handleExpiredLease(lease)
			}
		}
	}
}
