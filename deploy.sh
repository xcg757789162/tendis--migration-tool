#!/bin/bash

#######################################
# Tendis Migration Tool 部署脚本
# 支持 Linux (CentOS/Ubuntu) 和 macOS
#######################################

set -e

# 颜色定义
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m'

# 配置
APP_NAME="tendis-migrate"
APP_PORT=8088
INSTALL_DIR="/opt/tendis-migrate"
LOG_DIR="/opt/tendis-migrate/logs"
GO_VERSION="1.21.6"
NODE_VERSION="20.11.0"

log_info() { echo -e "${GREEN}[INFO]${NC} $1"; }
log_warn() { echo -e "${YELLOW}[WARN]${NC} $1"; }
log_error() { echo -e "${RED}[ERROR]${NC} $1"; }
log_step() { echo -e "${BLUE}[STEP]${NC} $1"; }

# 检测操作系统
detect_os() {
    if [[ "$OSTYPE" == "darwin"* ]]; then
        OS="macos"
        ARCH=$(uname -m)
        if [[ "$ARCH" == "arm64" ]]; then
            GO_ARCH="darwin-arm64"
            NODE_ARCH="darwin-arm64"
        else
            GO_ARCH="darwin-amd64"
            NODE_ARCH="darwin-x64"
        fi
    elif [[ -f /etc/centos-release ]] || [[ -f /etc/redhat-release ]]; then
        OS="centos"
        GO_ARCH="linux-amd64"
        NODE_ARCH="linux-x64"
    elif [[ -f /etc/debian_version ]]; then
        OS="ubuntu"
        GO_ARCH="linux-amd64"
        NODE_ARCH="linux-x64"
    else
        OS="linux"
        GO_ARCH="linux-amd64"
        NODE_ARCH="linux-x64"
    fi
    log_info "检测到操作系统: $OS ($GO_ARCH)"
}

# 检查并安装 Go
install_go() {
    if command -v go &> /dev/null; then
        GO_INSTALLED=$(go version | grep -oE '[0-9]+\.[0-9]+\.[0-9]+' | head -1)
        log_info "Go 已安装: $GO_INSTALLED"
        return 0
    fi

    log_step "安装 Go $GO_VERSION..."
    
    cd /tmp
    GO_TAR="go${GO_VERSION}.${GO_ARCH}.tar.gz"
    
    if [[ ! -f "$GO_TAR" ]]; then
        wget -q --show-progress "https://go.dev/dl/${GO_TAR}" || {
            log_error "下载 Go 失败，尝试备用地址..."
            wget -q --show-progress "https://golang.google.cn/dl/${GO_TAR}"
        }
    fi
    
    sudo rm -rf /usr/local/go
    sudo tar -C /usr/local -xzf "$GO_TAR"
    
    # 配置环境变量
    if ! grep -q '/usr/local/go/bin' ~/.bashrc 2>/dev/null; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.bashrc
    fi
    if ! grep -q '/usr/local/go/bin' ~/.zshrc 2>/dev/null; then
        echo 'export PATH=$PATH:/usr/local/go/bin' >> ~/.zshrc 2>/dev/null || true
    fi
    
    export PATH=$PATH:/usr/local/go/bin
    log_info "Go $GO_VERSION 安装完成"
}

# 检查并安装 Node.js
install_node() {
    if command -v node &> /dev/null; then
        NODE_INSTALLED=$(node --version)
        log_info "Node.js 已安装: $NODE_INSTALLED"
        return 0
    fi

    log_step "安装 Node.js $NODE_VERSION..."
    
    cd /tmp
    NODE_TAR="node-v${NODE_VERSION}-${NODE_ARCH}.tar.xz"
    
    if [[ ! -f "$NODE_TAR" ]]; then
        wget -q --show-progress "https://nodejs.org/dist/v${NODE_VERSION}/${NODE_TAR}" || {
            log_error "下载 Node.js 失败，尝试镜像..."
            wget -q --show-progress "https://npmmirror.com/mirrors/node/v${NODE_VERSION}/${NODE_TAR}"
        }
    fi
    
    sudo rm -rf /usr/local/node
    sudo mkdir -p /usr/local/node
    sudo tar -C /usr/local/node --strip-components=1 -xf "$NODE_TAR"
    
    # 配置环境变量
    if ! grep -q '/usr/local/node/bin' ~/.bashrc 2>/dev/null; then
        echo 'export PATH=$PATH:/usr/local/node/bin' >> ~/.bashrc
    fi
    if ! grep -q '/usr/local/node/bin' ~/.zshrc 2>/dev/null; then
        echo 'export PATH=$PATH:/usr/local/node/bin' >> ~/.zshrc 2>/dev/null || true
    fi
    
    export PATH=$PATH:/usr/local/node/bin
    log_info "Node.js $NODE_VERSION 安装完成"
}

# 构建应用
build_app() {
    log_step "构建应用..."
    
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    cd "$SCRIPT_DIR"
    
    # 构建后端
    log_info "编译 Go 服务..."
    export PATH=$PATH:/usr/local/go/bin
    go build -o simple-server ./cmd/simple/main.go
    
    # 构建前端
    log_info "构建前端..."
    export PATH=$PATH:/usr/local/node/bin
    cd web
    npm install --registry=https://registry.npmmirror.com 2>/dev/null || npm install
    npm run build
    cd ..
    
    log_info "构建完成"
}

# 部署应用
deploy_app() {
    log_step "部署应用到 $INSTALL_DIR..."
    
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    
    # 停止现有服务
    if systemctl is-active --quiet $APP_NAME 2>/dev/null; then
        log_info "停止现有服务..."
        sudo systemctl stop $APP_NAME
    fi
    
    # 创建目录
    sudo mkdir -p $INSTALL_DIR
    sudo mkdir -p $LOG_DIR
    
    # 复制文件
    sudo cp "$SCRIPT_DIR/simple-server" $INSTALL_DIR/
    sudo cp -r "$SCRIPT_DIR/web/dist" $INSTALL_DIR/web/
    sudo mkdir -p $INSTALL_DIR/logs
    
    # 设置权限
    sudo chmod +x $INSTALL_DIR/simple-server
    
    log_info "文件部署完成"
}

# 创建 systemd 服务 (Linux)
create_systemd_service() {
    if [[ "$OS" == "macos" ]]; then
        create_launchd_service
        return
    fi

    log_step "创建 systemd 服务..."
    
    sudo tee /etc/systemd/system/${APP_NAME}.service > /dev/null <<EOF
[Unit]
Description=Tendis Migration Tool
After=network.target

[Service]
Type=simple
WorkingDirectory=$INSTALL_DIR
ExecStart=$INSTALL_DIR/simple-server
Restart=always
RestartSec=5
StandardOutput=append:$LOG_DIR/stdout.log
StandardError=append:$LOG_DIR/stderr.log

# 资源限制
LimitNOFILE=65535
LimitNPROC=65535

[Install]
WantedBy=multi-user.target
EOF

    sudo systemctl daemon-reload
    sudo systemctl enable $APP_NAME
    sudo systemctl start $APP_NAME
    
    log_info "服务创建并启动完成"
}

# 创建 launchd 服务 (macOS)
create_launchd_service() {
    log_step "创建 launchd 服务..."
    
    PLIST_PATH="$HOME/Library/LaunchAgents/com.tendis.migrate.plist"
    
    mkdir -p "$HOME/Library/LaunchAgents"
    
    cat > "$PLIST_PATH" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>com.tendis.migrate</string>
    <key>ProgramArguments</key>
    <array>
        <string>$INSTALL_DIR/simple-server</string>
    </array>
    <key>WorkingDirectory</key>
    <string>$INSTALL_DIR</string>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <true/>
    <key>StandardOutPath</key>
    <string>$LOG_DIR/stdout.log</string>
    <key>StandardErrorPath</key>
    <string>$LOG_DIR/stderr.log</string>
</dict>
</plist>
EOF

    launchctl unload "$PLIST_PATH" 2>/dev/null || true
    launchctl load "$PLIST_PATH"
    
    log_info "launchd 服务创建完成"
}

# 显示状态
show_status() {
    echo ""
    echo "=========================================="
    echo -e "${GREEN}部署完成！${NC}"
    echo "=========================================="
    echo ""
    echo "访问地址: http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo 'localhost'):$APP_PORT"
    echo "日志页面: http://$(hostname -I 2>/dev/null | awk '{print $1}' || echo 'localhost'):$APP_PORT/logs"
    echo ""
    echo "常用命令:"
    if [[ "$OS" == "macos" ]]; then
        echo "  启动: launchctl load ~/Library/LaunchAgents/com.tendis.migrate.plist"
        echo "  停止: launchctl unload ~/Library/LaunchAgents/com.tendis.migrate.plist"
        echo "  日志: tail -f $LOG_DIR/stdout.log"
    else
        echo "  启动: sudo systemctl start $APP_NAME"
        echo "  停止: sudo systemctl stop $APP_NAME"
        echo "  状态: sudo systemctl status $APP_NAME"
        echo "  日志: sudo journalctl -u $APP_NAME -f"
    fi
    echo ""
    echo "日志目录: $LOG_DIR"
    echo "安装目录: $INSTALL_DIR"
    echo "=========================================="
}

# 快速启动（不安装为服务）
quick_start() {
    log_step "快速启动模式..."
    
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    cd "$SCRIPT_DIR"
    
    # 检查是否已编译
    if [[ ! -f "simple-server" ]]; then
        log_info "首次运行，需要编译..."
        install_go
        install_node
        build_app
    fi
    
    # 检查前端是否已构建
    if [[ ! -d "web/dist" ]]; then
        log_info "构建前端..."
        export PATH=$PATH:/usr/local/node/bin
        cd web && npm install && npm run build && cd ..
    fi
    
    # 停止已有进程
    pkill -f "simple-server" 2>/dev/null || true
    sleep 1
    
    # 启动
    mkdir -p logs
    nohup ./simple-server > logs/stdout.log 2>&1 &
    
    sleep 2
    
    if pgrep -f "simple-server" > /dev/null; then
        echo ""
        echo "=========================================="
        echo -e "${GREEN}启动成功！${NC}"
        echo "=========================================="
        echo ""
        echo "访问地址: http://localhost:$APP_PORT"
        echo "日志页面: http://localhost:$APP_PORT/logs"
        echo ""
        echo "查看日志: tail -f logs/stdout.log"
        echo "停止服务: pkill -f simple-server"
        echo "=========================================="
    else
        log_error "启动失败，查看日志: cat logs/stdout.log"
        exit 1
    fi
}

# 打包发布
package_release() {
    log_step "打包发布版本..."
    
    SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
    cd "$SCRIPT_DIR"
    
    VERSION=$(date +%Y%m%d%H%M%S)
    RELEASE_DIR="release-$VERSION"
    
    mkdir -p "$RELEASE_DIR"
    
    # 复制必要文件
    cp simple-server "$RELEASE_DIR/"
    cp -r web/dist "$RELEASE_DIR/web/"
    cp deploy.sh "$RELEASE_DIR/"
    mkdir -p "$RELEASE_DIR/logs"
    
    # 创建启动脚本
    cat > "$RELEASE_DIR/start.sh" <<'EOF'
#!/bin/bash
cd "$(dirname "$0")"
mkdir -p logs
pkill -f simple-server 2>/dev/null || true
sleep 1
nohup ./simple-server > logs/stdout.log 2>&1 &
echo "服务已启动: http://localhost:8088"
echo "日志页面: http://localhost:8088/logs"
EOF
    chmod +x "$RELEASE_DIR/start.sh"
    
    # 创建停止脚本
    cat > "$RELEASE_DIR/stop.sh" <<'EOF'
#!/bin/bash
pkill -f simple-server && echo "服务已停止" || echo "服务未运行"
EOF
    chmod +x "$RELEASE_DIR/stop.sh"
    
    # 打包
    tar -czvf "tendis-migrate-$VERSION.tar.gz" "$RELEASE_DIR"
    rm -rf "$RELEASE_DIR"
    
    log_info "打包完成: tendis-migrate-$VERSION.tar.gz"
    echo ""
    echo "部署步骤:"
    echo "  1. 上传 tendis-migrate-$VERSION.tar.gz 到目标服务器"
    echo "  2. tar -xzf tendis-migrate-$VERSION.tar.gz"
    echo "  3. cd release-$VERSION && ./start.sh"
}

# 帮助信息
show_help() {
    echo "Tendis Migration Tool 部署脚本"
    echo ""
    echo "用法: $0 [命令]"
    echo ""
    echo "命令:"
    echo "  install     完整安装（安装依赖+构建+部署为系统服务）"
    echo "  start       快速启动（编译后直接运行，不安装为服务）"
    echo "  build       仅构建（不部署）"
    echo "  package     打包发布版本（用于迁移到其他服务器）"
    echo "  status      查看服务状态"
    echo "  stop        停止服务"
    echo "  logs        查看实时日志"
    echo "  help        显示此帮助"
    echo ""
    echo "示例:"
    echo "  $0 start    # 快速启动（推荐测试环境）"
    echo "  $0 install  # 完整安装为系统服务"
    echo "  $0 package  # 打包后迁移到其他服务器"
}

# 主函数
main() {
    case "${1:-start}" in
        install)
            detect_os
            install_go
            install_node
            build_app
            deploy_app
            create_systemd_service
            show_status
            ;;
        start)
            detect_os
            quick_start
            ;;
        build)
            detect_os
            install_go
            install_node
            build_app
            log_info "构建完成，可执行文件: ./simple-server"
            ;;
        package)
            detect_os
            install_go
            install_node
            build_app
            package_release
            ;;
        status)
            if [[ "$OSTYPE" == "darwin"* ]]; then
                launchctl list | grep tendis || echo "服务未运行"
            else
                sudo systemctl status $APP_NAME 2>/dev/null || echo "服务未安装"
            fi
            ;;
        stop)
            pkill -f simple-server && log_info "服务已停止" || log_warn "服务未运行"
            ;;
        logs)
            tail -f logs/stdout.log 2>/dev/null || tail -f $LOG_DIR/stdout.log 2>/dev/null || log_error "日志文件不存在"
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
