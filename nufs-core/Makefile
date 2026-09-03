.PHONY: build test vet clean docker docker-up docker-down fmt lint help test-storage-p0 test-storage-crash verify verify-docker install install-mount-helper deploy package

# Binary output directory
BIN_DIR := bin

# Distribution tarball directory
DIST_DIR := dist

# Go build flags
GOFLAGS := -trimpath
VERSION ?= $(shell git describe --tags --always --dirty 2>/dev/null || echo "dev")
GIT_COMMIT ?= $(shell git rev-parse HEAD 2>/dev/null || echo "unknown")
BUILD_TIME ?= $(shell date -u +%Y-%m-%dT%H:%M:%SZ 2>/dev/null || echo "unknown")
LDFLAGS := -s -w \
	-X github.com/dshmyz/nufs/nufs-core/internal/version.Version=$(VERSION) \
	-X github.com/dshmyz/nufs/nufs-core/internal/version.GitCommit=$(GIT_COMMIT) \
	-X github.com/dshmyz/nufs/nufs-core/internal/version.BuildTime=$(BUILD_TIME)

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

package: ## 构建并打包（当前平台 + linux/amd64）：bin/ + admin 管理台 + deploy.sh + 配置模板
	$(MAKE) build
	$(MAKE) build-linux
	# admin-server（每平台一份）+ 前端 web（一次构建，两平台共用）
	@mkdir -p $(BIN_DIR)/admin $(BIN_DIR)/linux-amd64/admin
	go build -o $(BIN_DIR)/admin/admin-server ./../nufs-admin/cmd/admin-server
	CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -o $(BIN_DIR)/linux-amd64/admin/admin-server ./../nufs-admin/cmd/admin-server
	@cd ../nufs-admin/web && npm run build >/dev/null
	@rm -rf $(DIST_DIR)
	@for plat in native linux; do \
		case $$plat in \
		  native) NAME="$(shell go env GOOS)-$(shell go env GOARCH)"; SRCDIR=$(BIN_DIR);; \
		  linux)  NAME=linux-amd64; SRCDIR=$(BIN_DIR)/linux-amd64;; \
		esac; \
		STAGE=$(DIST_DIR)/nufs-$(VERSION)-$$NAME; \
		mkdir -p $$STAGE/bin $$STAGE/config $$STAGE/admin/web; \
		for b in $(BINS); do [ -f $$SRCDIR/$$b ] && cp $$SRCDIR/$$b $$STAGE/bin/; done; \
		cp $$SRCDIR/admin/admin-server $$STAGE/admin/; \
		cp -R ../nufs-admin/web/dist $$STAGE/admin/web/dist; \
		cp deploy/vm/deploy.sh deploy/vm/cluster.env.example $$STAGE/; \
		( cd $$STAGE && ./deploy.sh gen-default-config ); \
		tar -czf $(DIST_DIR)/nufs-$(VERSION)-$$NAME.tar.gz -C $(DIST_DIR) nufs-$(VERSION)-$$NAME; \
		rm -rf $$STAGE; \
	done
	@echo "Done:"
	@ls -lh $(DIST_DIR)

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
	go test -race -short -count=20 -timeout 900s ./datanode/storage/segment/... ./datanode/storage/recovery/...

test-storage-crash: ## P0 abrupt-process-crash (SIGKILL) recovery gate (50x)
	# Each iteration spawns a helper subprocess, writes ~272 mutations, SIGKILLs
	# it, and replays the segment log: ~10-12s per run, so 50 runs land near
	# 500-600s. The timeout is 20m to leave headroom on a slow box rather than
	# failing at the boundary.
	go test -count=50 -timeout 20m ./datanode/storage/segment -run TestProcessCrash_AcknowledgedMutationsRecover

verify: ## Production-readiness regression gate: make verify [LEVEL=fast|drill|full]
	@./scripts/verify.sh --level "$(if $(LEVEL),$(LEVEL),fast)"

verify-docker: ## Same gate, run inside a Linux container (macOS host: -count=2 port-exhaustion fix)
	@./scripts/verify-docker.sh --level "$(if $(LEVEL),$(LEVEL),fast)"

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
	docker build -t nufs-netem:latest -f ../deploy/netem/Dockerfile .

docker-up: ## Start DFS cluster with docker-compose
	docker-compose up -d --build

docker-down: ## Stop DFS cluster
	docker-compose down

docker-logs: ## Tail logs of all services
	docker-compose logs -f

clean: ## Remove build artifacts
	rm -rf $(BIN_DIR) coverage
	go clean -testcache

install: build ## Install binaries to /usr/local/bin
	install -d /usr/local/bin
	install -m 0755 bin/* /usr/local/bin/
	@echo "installed: /usr/local/bin/{metad,datanode,nufs-s3,nufs-fuse,nufs-cli}"

install-mount-helper: ## Install mount helper to /sbin (root): enables `mount -t nufs ...`
	install -m 0755 deploy/mount.nufs /sbin/mount.nufs
	@echo "installed: /sbin/mount.nufs"
	@echo "用法:  mount -t nufs none /mnt/nufs-fuse [-o meta=host:port,log=level]"
	@echo "       umount /mnt/nufs-fuse"

deploy: ## One-click: install binaries + mount helper (root). Wraps deploy/install.sh
	@[ "$$(id -u)" -eq 0 ] || { echo "需要 root:  sudo make deploy  (或 sudo ./deploy/install.sh)"; exit 1; }
	@./deploy/install.sh

tidy: ## Run go mod tidy
	go mod tidy
