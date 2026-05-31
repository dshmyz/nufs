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
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/s3gw     ./cmd/s3gw
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/fusegw   ./cmd/fusegw
RUN CGO_ENABLED=0 GOOS=linux go build -o /bin/ctl      ./cmd/ctl

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
FROM gcr.io/distroless/static-debian12 AS s3gw
COPY --from=builder /bin/s3gw /usr/local/bin/s3gw
EXPOSE 8080
ENTRYPOINT ["/usr/local/bin/s3gw"]

# ========== FUSE Gateway (requires FUSE libraries) ==========
FROM debian:bookworm-slim AS fusegw
RUN apt-get update && apt-get install -y --no-install-recommends fuse ca-certificates && rm -rf /var/lib/apt/lists/*
COPY --from=builder /bin/fusegw /usr/local/bin/fusegw
VOLUME ["/mnt/dfs"]
ENTRYPOINT ["/usr/local/bin/fusegw"]

# ========== Admin CLI ==========
FROM gcr.io/distroless/static-debian12 AS ctl
COPY --from=builder /bin/ctl /usr/local/bin/ctl
ENTRYPOINT ["/usr/local/bin/ctl"]
