#!/bin/bash

#######################################
# Tendis Migration Tool 远程部署脚本
# 支持通过 SSH 部署到远程服务器
#######################################

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
CYAN='\033[0;36m'
NC='\033[0m'

# 默认配置
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
PACKAGE_DIR="tendis-migrate-package"
REMOTE_PATH="/home"
KEEP_DATA=true
BUILD_OS="linux"
BUILD_ARCH="amd64"
PACKAGE_FILE=""

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# 显示帮助
show_help() {
    echo ""
    echo -e "${CYAN}Tendis Migration Tool 远程部署脚本${NC}"
    echo ""
    echo "用法: $0 [选项] <目标服务器>"
    echo ""
    echo "目标服务器格式:"
    echo "  user@host              使用默认 22 端口"
    echo "  user@host:port         指定端口"
    echo "  host                   使用默认用户和端口"
    echo ""
    echo "选项:"
    echo "  -k, --keep-data        保留数据目录（默认）"
    echo "  -c, --clean            不保留数据，全新部署"
    echo "  -p, --path <path>      远程部署路径（默认: /home）"
    echo "  -h, --help             显示此帮助"
    echo ""
    echo "示例:"
    echo "  $0 root@192.168.1.100                  # 保留数据部署"
    echo "  $0 -c root@192.168.1.100               # 全新部署（不保留数据）"
    echo "  $0 root@8.137.20.144:8822              # 指定端口部署"
    echo "  $0 -k -p /opt root@server              # 保留数据，指定路径"
    echo ""
    echo "预设环境:"
    echo "  $0 env-a               # 测试环境 A（8.137.20.144:8822）"
    echo "  $0 env-b               # 测试环境 B（140.143.218.100:5542）"
    echo "  $0 env-c               # 测试环境 C（10.248.37.11:22）"
    echo "  $0 home                # 家里环境（192.168.1.19，本地不需要部署）"
    echo ""
}

# 解析参数
parse_args() {
    while [[ $# -gt 0 ]]; do
        case $1 in
            -k|--keep-data)
                KEEP_DATA=true
                shift
                ;;
            -c|--clean)
                KEEP_DATA=false
                shift
                ;;
            -p|--path)
                REMOTE_PATH="$2"
                shift 2
                ;;
            -h|--help)
                show_help
                exit 0
                ;;
            env-a|ENV-A)
                SSH_TARGET="root@8.137.20.144"
                SSH_PORT="8822"
                REMOTE_PATH="/home"
                shift
                ;;
            env-b|ENV-B)
                SSH_TARGET="root@140.143.218.100"
                SSH_PORT="5542"
                REMOTE_PATH="/home"
                shift
                ;;
            env-c|ENV-C)
                SSH_TARGET="root@10.248.37.11"
                SSH_PORT="22"
                REMOTE_PATH="/home"
                shift
                ;;
            home|HOME)
                SSH_TARGET="xiechenguo@192.168.1.19"
                SSH_PORT="22"
                REMOTE_PATH="/tmp"
                BUILD_OS="darwin"
                BUILD_ARCH="arm64"
                shift
                ;;
            -*)
                log_error "未知选项: $1"
                show_help
                exit 1
                ;;
            *)
                # 解析目标服务器 user@host:port
                if [[ "$1" == *":"* ]]; then
                    SSH_TARGET="${1%:*}"
                    SSH_PORT="${1##*:}"
                else
                    SSH_TARGET="$1"
                    SSH_PORT="22"
                fi
                shift
                ;;
        esac
    done
    
    if [[ -z "$SSH_TARGET" ]]; then
        log_error "请指定目标服务器"
        show_help
        exit 1
    fi
}

# 编译
build() {
    log_step "编译应用..."
    cd "$SCRIPT_DIR"
    
    log_info "目标平台: ${BUILD_OS}/${BUILD_ARCH}"
    GOOS=$BUILD_OS GOARCH=$BUILD_ARCH go build -o tendis-migrate ./cmd/simple
    
    ls -lh tendis-migrate
    log_info "编译完成"
}

# 打包（结果保存到全局变量 PACKAGE_FILE，不通过 stdout 返回）
package() {
    log_step "打包应用..."
    cd "$SCRIPT_DIR"
    
    local timestamp
    timestamp=$(date +%Y%m%d%H%M%S)
    PACKAGE_FILE="tendis-migrate-${BUILD_OS}-${timestamp}.tar.gz"
    
    # 清理并创建打包目录
    rm -rf "$PACKAGE_DIR"
    mkdir -p "$PACKAGE_DIR/logs" "$PACKAGE_DIR/data" "$PACKAGE_DIR/web"
    
    # 复制文件
    cp tendis-migrate run.sh stop.sh INSTALL.txt "$PACKAGE_DIR/"
    cp -r web/dist "$PACKAGE_DIR/web/"
    
    # 打包（排除 macOS 特殊文件）
    COPYFILE_DISABLE=1 tar --no-xattrs -czf "$PACKAGE_FILE" "$PACKAGE_DIR"
    
    ls -lh "$PACKAGE_FILE"
    log_info "打包完成: $PACKAGE_FILE"
}

# 上传
upload() {
    log_step "上传到 ${SSH_TARGET}:${SSH_PORT}..."
    
    scp -P "$SSH_PORT" -o StrictHostKeyChecking=no "$PACKAGE_FILE" "${SSH_TARGET}:${REMOTE_PATH}/"
    
    log_info "上传完成"
}

# 远程部署
remote_deploy() {
    local package_name
    package_name=$(basename "$PACKAGE_FILE")
    
    log_step "远程部署..."
    
    if [[ "$KEEP_DATA" == "true" ]]; then
        log_info "模式: 保留 data 和 logs 目录"
        ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no "$SSH_TARGET" bash -s "$REMOTE_PATH" "$PACKAGE_DIR" "$package_name" <<'DEPLOY_EOF'
REMOTE_PATH="$1"
PACKAGE_DIR="$2"
PACKAGE_NAME="$3"
cd "$REMOTE_PATH"

echo "=== 优雅停止旧服务 ==="
if [ -f "${PACKAGE_DIR}/stop.sh" ]; then
    GRACEFUL_TIMEOUT=30 ./${PACKAGE_DIR}/stop.sh || true
else
    echo "旧服务未安装"
fi

echo "=== 备份 data 和 logs 目录 ==="
if [ -d "${PACKAGE_DIR}/data" ]; then
    cp -r "${PACKAGE_DIR}/data" "${PACKAGE_DIR}-data-backup"
    echo "data 目录已备份"
fi
if [ -d "${PACKAGE_DIR}/logs" ]; then
    cp -r "${PACKAGE_DIR}/logs" "${PACKAGE_DIR}-logs-backup"
    echo "logs 目录已备份"
fi

echo "=== 删除旧目录 ==="
rm -rf "${PACKAGE_DIR}"

echo "=== 解压新版本 ==="
tar -xzf "${PACKAGE_NAME}"

echo "=== 恢复 data 和 logs 目录 ==="
if [ -d "${PACKAGE_DIR}-data-backup" ]; then
    cp -r "${PACKAGE_DIR}-data-backup"/* "${PACKAGE_DIR}/data/" 2>/dev/null || true
    rm -rf "${PACKAGE_DIR}-data-backup"
    echo "data 目录已恢复"
fi
if [ -d "${PACKAGE_DIR}-logs-backup" ]; then
    cp -r "${PACKAGE_DIR}-logs-backup"/* "${PACKAGE_DIR}/logs/" 2>/dev/null || true
    rm -rf "${PACKAGE_DIR}-logs-backup"
    echo "logs 目录已恢复"
fi

echo "=== 清理安装包 ==="
rm -f "${PACKAGE_NAME}"

echo "=== 设置权限并启动 ==="
cd "${PACKAGE_DIR}"
chmod +x tendis-migrate run.sh stop.sh
./run.sh

sleep 3
echo ""
echo "=== 检查进程 ==="
ps aux | grep tendis-migrate | grep -v grep || echo "警告: 进程未找到"

echo ""
echo "=== 健康检查 ==="
curl -s http://localhost:8088/api/v1/health 2>/dev/null || echo "健康检查失败"

echo ""
echo "=== 检查任务恢复情况 ==="
curl -s http://localhost:8088/api/v1/tasks 2>/dev/null | python3 -c "
import sys, json
try:
    data = json.load(sys.stdin)
    tasks = data.get('data', {}).get('tasks', [])
    print(f'恢复任务数: {len(tasks)}')
    for t in tasks:
        print(f'  [{t.get(\"status\",\"?\")}] {t.get(\"name\",\"?\")} (进度: {t.get(\"progress\",0):.1f}%)')
except:
    print('无法解析任务列表')
" 2>/dev/null || echo "无法获取任务列表"
DEPLOY_EOF
    else
        log_info "模式: 全新部署（不保留数据）"
        ssh -p "$SSH_PORT" -o StrictHostKeyChecking=no "$SSH_TARGET" bash -s "$REMOTE_PATH" "$PACKAGE_DIR" "$package_name" <<'DEPLOY_EOF'
REMOTE_PATH="$1"
PACKAGE_DIR="$2"
PACKAGE_NAME="$3"
cd "$REMOTE_PATH"

echo "=== 优雅停止旧服务 ==="
if [ -f "${PACKAGE_DIR}/stop.sh" ]; then
    GRACEFUL_TIMEOUT=30 ./${PACKAGE_DIR}/stop.sh || true
else
    echo "旧服务未安装"
fi

echo "=== 删除旧目录（包括数据）==="
rm -rf "${PACKAGE_DIR}"
rm -rf "${PACKAGE_DIR}-data-backup"
rm -rf "${PACKAGE_DIR}-logs-backup"

echo "=== 解压新版本 ==="
tar -xzf "${PACKAGE_NAME}"

echo "=== 清理安装包 ==="
rm -f "${PACKAGE_NAME}"

echo "=== 设置权限并启动 ==="
cd "${PACKAGE_DIR}"
chmod +x tendis-migrate run.sh stop.sh
./run.sh

sleep 2
echo ""
echo "=== 检查进程 ==="
ps aux | grep tendis-migrate | grep -v grep || echo "警告: 进程未找到"

echo ""
echo "=== 健康检查 ==="
curl -s http://localhost:8088/api/v1/health 2>/dev/null || echo "健康检查失败"
DEPLOY_EOF
    fi
}

# 显示结果
show_result() {
    echo ""
    echo -e "${GREEN}========================================${NC}"
    echo -e "${GREEN}   部署完成！${NC}"
    echo -e "${GREEN}========================================${NC}"
    echo ""
    echo -e "目标服务器: ${CYAN}${SSH_TARGET}:${SSH_PORT}${NC}"
    echo -e "部署路径:   ${CYAN}${REMOTE_PATH}/${PACKAGE_DIR}/${NC}"
    echo -e "数据保留:   ${CYAN}${KEEP_DATA}${NC}"
    echo -e "Web UI:     ${CYAN}http://${SSH_TARGET#*@}:8088${NC}"
    echo ""
    echo "SSH 连接:"
    echo "  ssh -p ${SSH_PORT} ${SSH_TARGET}"
    echo ""
    echo "查看日志:"
    echo "  ssh -p ${SSH_PORT} ${SSH_TARGET} 'tail -f ${REMOTE_PATH}/${PACKAGE_DIR}/logs/tendis-migrate-\$(date +%Y-%m-%d).log'"
    echo ""
    echo -e "${GREEN}========================================${NC}"
}

# 主函数
main() {
    echo ""
    echo -e "${CYAN}╔════════════════════════════════════════╗${NC}"
    echo -e "${CYAN}║   Tendis Migration Tool 远程部署       ║${NC}"
    echo -e "${CYAN}╚════════════════════════════════════════╝${NC}"
    echo ""
    
    parse_args "$@"
    
    echo -e "目标服务器: ${YELLOW}${SSH_TARGET}:${SSH_PORT}${NC}"
    echo -e "远程路径:   ${YELLOW}${REMOTE_PATH}${NC}"
    echo -e "保留数据:   ${YELLOW}${KEEP_DATA}${NC}"
    echo -e "构建目标:   ${YELLOW}${BUILD_OS}/${BUILD_ARCH}${NC}"
    echo ""
    
    build
    package
    upload
    remote_deploy
    
    # 清理本地打包目录和文件
    rm -rf "$PACKAGE_DIR"
    rm -f "$PACKAGE_FILE"
    
    show_result
}

main "$@"
