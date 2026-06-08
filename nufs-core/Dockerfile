# ========== Build Stage ==========
FROM golang:1.23-bookworm AS builder

WORKDIR /src

# Copy go.mod and go.sum first for better caching
COPY go.mod go.sum ./
RUN go mod download

# Copy source code
COPY . .

# Build all binaries
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/metad    ./cmd/metad
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/datanode ./cmd/datanode
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/nufs-s3   ./cmd/nufs-s3
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/nufs-fuse ./cmd/nufs-fuse
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/nufs-cli  ./cmd/nufs-cli

# ========== Metadata Service ==========
FROM gcr.io/distroless/static-debian12 AS metad
COPY --from=builder /bin/metad /usr/local/bin/metad
EXPOSE 2379 2380
ENTRYPOINT ["/usr/local/bin/metad"]

# ========== Data Node ==========
FROM gcr.io/distroless/static-debian12 AS datanode
COPY --from=builder /bin/datanode /usr/local/bin/datanode
VOLUME ["/data"]
EXPOSE 9001
ENTRYPOINT ["/usr/local/bin/datanode"]

# ========== S3 Gateway ==========
FROM gcr.io/distroless/static-debian12 AS nufs-s3
COPY --from=builder /bin/nufs-s3 /usr/local/bin/nufs-s3
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/nufs-s3"]

# ========== FUSE Gateway (requires FUSE libraries) ==========
FROM debian:bookworm-slim AS nufs-fuse
RUN apt-get update && apt-get install -y --no-install-recommends fuse ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/nufs-fuse /usr/local/bin/nufs-fuse
VOLUME ["/mnt/dfs"]
ENTRYPOINT ["/usr/local/bin/nufs-fuse"]

# ========== Admin CLI ==========
FROM gcr.io/distroless/static-debian12 AS nufs-cli
COPY --from=builder /bin/nufs-cli /usr/local/bin/nufs-cli
ENTRYPOINT ["/usr/local/bin/nufs-cli"]
