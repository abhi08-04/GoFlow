# GoFlow

A RabbitMQ-style job queue broker written in Go. Producers call
`Enqueue` over gRPC; consumers open a bidirectional stream, receive
pushed jobs with a lease, and ack / nack them in-stream. Includes a
live HTML dashboard served from the same binary.

- Transport: **gRPC** (bidi streaming) for the broker protocol +
  **HTTP** for the dashboard and legacy JSON endpoints
- Storage: in-memory (pluggable behind a `Store` interface)
- Concurrency: atomic counters, `sync.Cond`-based queue, lease
  watcher goroutine
- Observability: live counters + 1-hour ring-buffer time series + per-type breakdown
- Shipping shape: single binary (`cmd/goflowd`) + Docker image

---

## Quickstart

### Local

```bash
make proto    # generate gRPC Go code (first time only)
make run      # go run ./cmd/goflowd
```

Dashboard: <http://localhost:8000/> · gRPC: `localhost:9090`

### Docker

```bash
docker compose up --build
```

Spins up the broker on `:8000` / `:9090` plus a sample consumer that
subscribes to `email` jobs. Produce from your host:

```bash
go run ./examples/producer
```

### Produce / consume in your own Go code

```go
import "goflow/client"

// Producer
pub, _ := client.NewPublisher("localhost:9090")
pub.Enqueue(ctx, client.Job{Type: "email", Payload: []byte(`{...}`)},
    "idem-key-123", 3)

// Consumer
c := client.NewConsumer("localhost:9090", []string{"email"},
    func(ctx context.Context, job client.Job) error {
        // do work; return nil -> ack, non-nil -> nack w/ retry
        return nil
    },
    client.WithPrefetch(4), client.WithLeaseSeconds(60),
)
c.Run(ctx) // blocks: streams, dispatches, ack/nacks for you
```

The client package wraps `goflow.v1.Broker` gRPC calls — users never
see the generated protos.

---

## HTTP endpoints (dashboard side)

| Path | What |
|---|---|
| `/` | Dashboard — live tiles, throughput chart, breakdown |
| `/jobs` | Jobs browser with filter + "New job" modal |
| `/dlq` | Dead-letter manager — multi-select, bulk requeue, purge |
| `/config` | Read-only view of effective config |
| `/api/metrics/snapshot` | Current counters/gauges/latency (JSON) |
| `/api/metrics/timeseries` | Last 60 min, 5s cadence (JSON) |

---

## gRPC surface

`proto/goflow/v1/broker.proto` is the source of truth. Highlights:

```proto
service Broker {
  rpc Enqueue(EnqueueRequest) returns (Job);
  rpc GetJob(GetJobRequest) returns (Job);
  rpc Consume(stream ClientFrame) returns (stream ServerFrame);  // bidi
  rpc ListDLQ   / RetryDLQ / PurgeDLQ / DeleteDLQJob             // admin
}
```

Non-Go clients: `buf generate` with your language's `protoc-gen-*`
plugin against the proto in this repo.

---

## Configuration

All via environment, see `.env.example`. Highlights:

| Var | Default | Purpose |
|---|---|---|
| `PORT` | 8000 | HTTP dashboard + JSON |
| `GRPC_PORT` | 9090 | Broker protocol |
| `JOB_QUEUE_SIZE` | 1000 | Pending-queue soft cap |
| `MAX_RETRIES` | 3 | Default attempt budget |
| `BASE_BACKOFF_SECONDS` | 1 | `delay = base * 2^attempt` |
| `DEFAULT_LEASE_SECONDS` | 60 | Lease when client omits |
| `MAX_LEASE_SECONDS` | 300 | Hard cap on client requests |
| `LEASE_SWEEP_INTERVAL_SECONDS` | 1 | Expiry watcher tick |
| `MAX_PREFETCH` | 64 | Per-consumer in-flight cap |

---

## Repo layout

```
cmd/goflowd/          server entrypoint
proto/goflow/v1/      protocol contract (.proto)
internal/
  rpc/                gRPC Broker service impl + generated code
  broker/             lease-expiry watcher
  retry/              scheduler (backoff + requeue)
  store/              Store interface + in-memory impl
  metrics/            atomic counters + ring buffer + sampler
  service/            producer-side façade
  handler/            dashboard HTTP + legacy JSON
  web/                embedded templates + static
  model/              Job struct, constants
  config/             env loading + validation
client/               Go SDK (Publisher, Consumer)
examples/
  producer/
  consumer/
```

---

## Development

```bash
make proto         # regenerate internal/rpc/gen from proto/
make build         # go build ./...
make test          # go test ./...
make race          # go test -race ./...
make run           # go run ./cmd/goflowd
```

Codegen tools (buf, protoc-gen-go, protoc-gen-go-grpc) are installed
lazily by `make proto` via `go install` when missing.

### Testing gRPC with `grpcurl`

```bash
grpcurl -plaintext :9090 list
grpcurl -plaintext :9090 list goflow.v1.Broker
grpcurl -plaintext -d '{"type":"email","payload":"aGVsbG8=","idempotency_key":"k1"}' \
  :9090 goflow.v1.Broker/Enqueue
```

---

## License

MIT. See [LICENSE](LICENSE).
