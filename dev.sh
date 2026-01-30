#!/bin/bash
# 开发模式启动脚本

set -e

# 启动后端（开发模式）
echo "Starting backend..."
go run ./cmd/server -port 8080 -data ./data -workers 4 &
BACKEND_PID=$!

# 等待后端启动
sleep 2

# 启动前端开发服务器
echo "Starting frontend dev server..."
cd web
npm run dev &
FRONTEND_PID=$!

echo ""
echo "=========================================="
echo "  Development servers started!"
echo "  Backend:  http://localhost:8080"
echo "  Frontend: http://localhost:3000"
echo "=========================================="
echo ""
echo "Press Ctrl+C to stop all servers"

# 捕获信号并清理
cleanup() {
    echo "Stopping servers..."
    kill $BACKEND_PID 2>/dev/null || true
    kill $FRONTEND_PID 2>/dev/null || true
    exit 0
}

trap cleanup SIGINT SIGTERM

# 等待
wait
