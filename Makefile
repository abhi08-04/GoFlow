# GoFlow — development targets.
#
# `make proto` regenerates the gRPC Go code from proto/goflow/v1/*.proto.
# `make build` and `make test` are the usual go wrappers.

GOBIN   := $(shell go env GOPATH)/bin
BUF     := $(GOBIN)/buf

.PHONY: all build test race run proto proto-tools proto-lint fmt tidy clean

all: build

proto-tools:
	@command -v $(BUF) >/dev/null 2>&1 || go install github.com/bufbuild/buf/cmd/buf@latest
	@command -v $(GOBIN)/protoc-gen-go >/dev/null 2>&1 || go install google.golang.org/protobuf/cmd/protoc-gen-go@latest
	@command -v $(GOBIN)/protoc-gen-go-grpc >/dev/null 2>&1 || go install google.golang.org/grpc/cmd/protoc-gen-go-grpc@latest

proto: proto-tools
	PATH=$(GOBIN):$$PATH $(BUF) generate

proto-lint: proto-tools
	PATH=$(GOBIN):$$PATH $(BUF) lint

build:
	go build ./...

test:
	go test ./...

race:
	go test -race ./...

run:
	go run ./cmd/goflowd

fmt:
	go fmt ./...

tidy:
	go mod tidy

clean:
	rm -rf internal/rpc/gen/goflow
