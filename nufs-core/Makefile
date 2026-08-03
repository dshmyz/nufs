.PHONY: build test vet clean docker docker-up docker-down fmt lint help test-storage-p0 test-storage-crash

# Binary output directory
BIN_DIR := bin

# Go build flags
GOFLAGS := -trimpath
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "unknown")
LDFLAGS := -s -w \
	-X github.com/example/dfs/internal/version.Version=$(VERSION) \
	-X github.com/example/dfs/internal/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/example/dfs/internal/version.BuildTime=$(BUILD_TIME)

# All service binaries
BINS := metad datanode nufs-s3 nufs-fuse nufs-cli

help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-20s\033[0m %s\n", $$1, $$2}'

build: ## Build all binaries
	@mkdir -p $(BIN_DIR)
	@for bin in $(BINS); do \
		echo "Building $$bin..."; \
		go build $(GOFLAGS) -ldflags '$(LDFLAGS)' -o $(BIN_DIR)/$$bin ./cmd/$$bin; \
	done
	@echo "Build complete: $(BIN_DIR)/"

build-linux: ## Cross-compile for Linux
	@mkdir -p $(BIN_DIR)/linux-amd64
	@for bin in $(BINS); do \
		echo "Building $$bin (linux/amd64)..."; \
		CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build $(GOFLAGS) -ldflags '$(LDFLAGS)' \
			-o $(BIN_DIR)/linux-amd64/$$bin ./cmd/$$bin; \
	done

test: ## Run all tests
	go test -race -timeout 120s ./...

test-verbose: ## Run all tests with verbose output
	go test -race -v -timeout 120s ./...

test-cover: ## Run tests with coverage report
	@mkdir -p coverage
	go test -race -coverprofile=coverage/cover.out -timeout 120s ./...
	go tool cover -html=coverage/cover.out -o coverage/cover.html
	@echo "Coverage report: coverage/cover.html"

test-storage-p0: ## P0 storage correctness gate (race, 20x): close/range/recovery
	go test -race -count=20 -timeout 120s ./datanode/storage/segment/... ./datanode/storage/recovery/...

test-storage-crash: ## P0 abrupt-process-crash (SIGKILL) recovery gate (50x)
	go test -count=50 -timeout 10m ./datanode/storage/segment -run TestProcessCrash_AcknowledgedMutationsRecover

vet: ## Run go vet
	go vet ./...

fmt: ## Format Go source files
	gofmt -s -w .

lint: ## Run golangci-lint
	golangci-lint run ./...

docker: ## Build Docker images
	docker build --target metad    -t dfs-metad    .
	docker build --target datanode -t dfs-datanode .
	docker build --target nufs-s3  -t dfs-nufs-s3  .
	docker build --target nufs-fuse -t dfs-nufs-fuse .
	docker build --target nufs-cli -t dfs-nufs-cli .

docker-up: ## Start DFS cluster with docker-compose
	docker-compose up -d --build

docker-down: ## Stop DFS cluster
	docker-compose down

docker-logs: ## Tail logs of all services
	docker-compose logs -f

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage
	go clean -testcache

tidy: ## Run go mod tidy
	go mod tidy
