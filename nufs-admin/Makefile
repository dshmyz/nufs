.PHONY: build-web build-server build-all clean

# 前端构建
build-web:
	cd web && npm install && npm run build

# 后端构建（embed 前端）
build-server:
	go build -o bin/admin-server ./cmd/admin-server

# 全量构建
build-all: build-web build-server

# 清理
clean:
	rm -rf web/dist bin/admin-server

# 开发模式（前端 dev server + 后端）
dev:
	cd web && npm run dev &
	go run ./cmd/admin-server

# 测试
test:
	go test -v ./...