FROM docker.m.daocloud.io/library/golang:1.25-alpine AS builder
RUN apk add --no-cache git
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN CGO_ENABLED=0 go build -o /out/metad ./cmd/metad && \
    CGO_ENABLED=0 go build -o /out/datanode ./cmd/datanode && \
    CGO_ENABLED=0 go build -o /out/nufs-s3 ./cmd/nufs-s3

FROM docker.m.daocloud.io/library/alpine:3.21
RUN apk add --no-cache ca-certificates tzdata fuse
COPY --from=builder /out/* /usr/local/bin/
EXPOSE 8091 9100 9101 8080
VOLUME ["/var/lib/dfs"]
ENTRYPOINT ["/usr/local/bin/nufs-s3"]
