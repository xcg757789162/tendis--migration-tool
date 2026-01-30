#!/bin/bash
# Tendis Migration Tool 启动脚本

set -e

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
NC='\033[0m'

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Tendis Migration Tool v1.4${NC}"
echo -e "${BLUE}========================================${NC}"

# 检查Go环境
if ! command -v go &> /dev/null; then
    echo "Error: Go is not installed. Please install Go 1.20+"
    exit 1
fi

# 检查Node环境
if ! command -v npm &> /dev/null; then
    echo "Error: npm is not installed. Please install Node.js 18+"
    exit 1
fi

# 创建数据目录
mkdir -p data

# 安装Go依赖
echo -e "${GREEN}Installing Go dependencies...${NC}"
go mod tidy

# 构建后端
echo -e "${GREEN}Building backend...${NC}"
go build -o tendis-migrate ./cmd/server

# 安装前端依赖
echo -e "${GREEN}Installing frontend dependencies...${NC}"
cd web
npm install

# 构建前端
echo -e "${GREEN}Building frontend...${NC}"
npm run build
cd ..

# 启动服务
echo -e "${GREEN}Starting Tendis Migration Tool...${NC}"
echo -e "${BLUE}Web UI: http://localhost:8080${NC}"
./tendis-migrate -port 8080 -data ./data -workers 4
