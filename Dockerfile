# 多阶段构建 - 后端
FROM golang:1.21-alpine AS backend-builder
RUN apk add --no-cache gcc musl-dev sqlite-dev

WORKDIR /app
COPY go.mod ./
RUN go mod download

COPY cmd/ ./cmd/
COPY internal/ ./internal/
RUN CGO_ENABLED=1 GOOS=linux go build -o server ./cmd/server/main.go

# 多阶段构建 - 前端
FROM node:18-alpine AS frontend-builder
WORKDIR /app
COPY web/package*.json ./
RUN npm install
COPY web/ ./
RUN npm run build

# 最终镜像
FROM alpine:3.19
RUN apk add --no-cache ca-certificates sqlite-libs

WORKDIR /app

# 复制后端二进制
COPY --from=backend-builder /app/server ./

# 复制前端静态文件
COPY --from=frontend-builder /app/dist ./web/dist

# 数据目录
RUN mkdir -p /app/data

EXPOSE 8080

ENV GIN_MODE=release
ENV DATA_DIR=/app/data

CMD ["./server"]
