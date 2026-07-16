# Build frontend
FROM node:20-alpine AS frontend-builder
WORKDIR /web
COPY web/package.json web/package-lock.json* ./
RUN npm install
COPY web/ .
RUN npm run build

# Build backend
FROM golang:1.22-alpine AS backend-builder
WORKDIR /app
COPY go.mod go.sum* ./
RUN go mod download
COPY . .
COPY --from=frontend-builder /web/dist ./web/dist
RUN CGO_ENABLED=0 go build -o bin/admin-server ./cmd/admin-server

# Final image
FROM alpine:3.20
WORKDIR /app
COPY --from=backend-builder /app/bin/admin-server .
COPY deploy/clusters.yaml deploy/users.yaml ./deploy/
EXPOSE 8090
CMD ["./bin/admin-server"]