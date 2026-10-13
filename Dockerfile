# syntax=docker/dockerfile:1.7
# Multi-stage build. Generated gRPC code is committed to the repo so
# no protoc / buf is needed inside the image.

FROM golang:1.25-alpine AS build
ENV GOTOOLCHAIN=auto
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/goflowd ./cmd/goflowd

FROM gcr.io/distroless/static-debian12
COPY --from=build /out/goflowd /goflowd

ENV PORT=8000 \
    GRPC_PORT=9090 \
    NUM_WORKERS=1 \
    JOB_QUEUE_SIZE=1000 \
    JOB_TIMEOUT_SECONDS=30 \
    MAX_RETRIES=3 \
    BASE_BACKOFF_SECONDS=1 \
    SHUTDOWN_TIMEOUT_SECONDS=10 \
    DEFAULT_LEASE_SECONDS=60 \
    MAX_LEASE_SECONDS=300 \
    MAX_WAIT_SECONDS=30 \
    LEASE_SWEEP_INTERVAL_SECONDS=1 \
    MAX_PREFETCH=64

EXPOSE 8000 9090
ENTRYPOINT ["/goflowd"]
