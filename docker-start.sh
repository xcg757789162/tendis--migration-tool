#!/bin/bash

# Tendis 迁移工具 Docker 一键启动脚本
# 用法: ./docker-start.sh [dev|prod|stop|logs|build]

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

log_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

log_warn() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

log_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# 检查 Docker 是否安装
check_docker() {
    if ! command -v docker &> /dev/null; then
        log_error "Docker 未安装，请先安装 Docker"
        echo ""
        echo "安装方法："
        echo "  macOS:   brew install --cask docker"
        echo "  Linux:   curl -fsSL https://get.docker.com | sh"
        echo "  Windows: https://docs.docker.com/desktop/windows/install/"
        exit 1
    fi

    if ! docker info &> /dev/null; then
        log_error "Docker 服务未运行，请启动 Docker Desktop"
        exit 1
    fi

    log_info "Docker 检查通过 ✓"
}

# 创建必要目录
init_dirs() {
    mkdir -p data config
    log_info "目录初始化完成"
}

# 生产模式启动（合并镜像）
start_prod() {
    log_info "启动生产模式..."
    docker compose up -d tendis-migrate
    log_info "服务已启动"
    echo ""
    echo -e "${BLUE}访问地址: http://localhost:8080${NC}"
    echo ""
    log_info "查看日志: ./docker-start.sh logs"
}

# 开发模式启动（前后端分离）
start_dev() {
    log_info "启动开发模式..."
    docker compose --profile dev up -d backend-dev frontend-dev
    log_info "服务已启动"
    echo ""
    echo -e "${BLUE}后端地址: http://localhost:8080${NC}"
    echo -e "${BLUE}前端地址: http://localhost:3000${NC}"
    echo ""
    log_info "查看日志: ./docker-start.sh logs"
}

# 停止服务
stop_services() {
    log_info "停止所有服务..."
    docker compose --profile dev down
    log_info "服务已停止"
}

# 查看日志
show_logs() {
    docker compose logs -f --tail=100
}

# 构建镜像
build_image() {
    log_info "构建 Docker 镜像..."
    docker compose build --no-cache
    log_info "构建完成"
}

# 显示帮助
show_help() {
    echo "Tendis 迁移工具 Docker 启动脚本"
    echo ""
    echo "用法: ./docker-start.sh [命令]"
    echo ""
    echo "命令:"
    echo "  prod    启动生产模式（默认，单容器）"
    echo "  dev     启动开发模式（前后端分离，支持热重载）"
    echo "  stop    停止所有服务"
    echo "  logs    查看日志"
    echo "  build   重新构建镜像"
    echo "  help    显示帮助"
    echo ""
    echo "示例:"
    echo "  ./docker-start.sh          # 启动生产模式"
    echo "  ./docker-start.sh dev      # 启动开发模式"
    echo "  ./docker-start.sh stop     # 停止服务"
}

# 主函数
main() {
    check_docker
    init_dirs

    case "${1:-prod}" in
        prod)
            start_prod
            ;;
        dev)
            start_dev
            ;;
        stop)
            stop_services
            ;;
        logs)
            show_logs
            ;;
        build)
            build_image
            ;;
        help|--help|-h)
            show_help
            ;;
        *)
            log_error "未知命令: $1"
            show_help
            exit 1
            ;;
    esac
}

main "$@"
