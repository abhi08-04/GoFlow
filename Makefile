# GoFlow broker — development targets.

GOBIN   := $(shell go env GOPATH)/bin
BUF     := $(GOBIN)/buf

.PHONY: all build test race proto proto-tools proto-lint run docker docker-run clean

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

docker:
	docker build -t goflow:dev .

docker-run: docker
	docker run --rm --name goflow -p 8000:8000 -p 9090:9090 goflow:dev

clean:
	rm -rf rpc/gen/goflow
