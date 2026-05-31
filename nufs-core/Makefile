.PHONY: build test vet clean docker docker-up docker-down fmt lint help

# Binary output directory
BIN_DIR := bin

# Go build flags
GOFLAGS := -trimpath
LDFLAGS := -s -w

# All service binaries
BINS := metad datanode s3gw fusegw ctl

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

vet: ## Run go vet
	go vet ./...

fmt: ## Format Go source files
	gofmt -s -w .

lint: ## Run golangci-lint
	golangci-lint run ./...

docker: ## Build Docker images
	docker build --target metad    -t dfs-metad    .
	docker build --target datanode -t dfs-datanode .
	docker build --target s3gw     -t dfs-s3gw     .
	docker build --target fusegw   -t dfs-fusegw   .
	docker build --target ctl      -t dfs-ctl      .

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
