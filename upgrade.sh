#!/bin/bash
# Tendis Migration Tool 升级脚本（保留数据目录）
# 用法: ./upgrade.sh <new-package.tar.gz>
# 
# 与全新部署不同，升级会保留 data/ 和 logs/ 目录，确保任务状态不丢失
# 升级模式只替换：二进制文件、前端资源、脚本文件

set -e

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

INSTALL_DIR="/home/tendis-migrate-package"

# 检查参数
if [ $# -lt 1 ]; then
    echo -e "${RED}用法: $0 <new-package.tar.gz>${NC}"
    echo -e "  示例: $0 tendis-migrate-linux-20260211.tar.gz"
    exit 1
fi

PACKAGE="$1"
if [ ! -f "$PACKAGE" ]; then
    echo -e "${RED}包文件不存在: $PACKAGE${NC}"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Tendis Migration Tool 升级${NC}"
echo -e "${BLUE}========================================${NC}"

# 1. 优雅停止旧服务（会保存所有任务状态）
echo -e "${YELLOW}[1/5] 停止旧服务（优雅关闭，保存任务状态）...${NC}"
if [ -f "$INSTALL_DIR/stop.sh" ]; then
    "$INSTALL_DIR/stop.sh" || true
    sleep 2
else
    echo -e "${YELLOW}  旧服务未找到，跳过${NC}"
fi

# 2. 备份数据目录
echo -e "${YELLOW}[2/5] 备份数据目录...${NC}"
if [ -d "$INSTALL_DIR/data" ]; then
    BACKUP_NAME="data-backup-$(date +%Y%m%d%H%M%S)"
    cp -r "$INSTALL_DIR/data" "/tmp/$BACKUP_NAME"
    echo -e "${GREEN}  数据已备份到 /tmp/$BACKUP_NAME${NC}"
else
    echo -e "${YELLOW}  无历史数据目录${NC}"
fi

# 3. 解压新包到临时目录
echo -e "${YELLOW}[3/5] 解压新包...${NC}"
TEMP_DIR=$(mktemp -d)
tar -xzf "$PACKAGE" -C "$TEMP_DIR"

# 找到解压后的目录（可能是 tendis-migrate-package 或其他名称）
NEW_DIR=$(find "$TEMP_DIR" -maxdepth 1 -type d ! -name "$(basename "$TEMP_DIR")" | head -1)
if [ -z "$NEW_DIR" ]; then
    echo -e "${RED}解压失败，未找到目录${NC}"
    rm -rf "$TEMP_DIR"
    exit 1
fi
echo -e "${GREEN}  解压成功: $(basename "$NEW_DIR")${NC}"

# 4. 升级：只替换二进制、前端、脚本，保留 data/ 和 logs/
echo -e "${YELLOW}[4/5] 升级文件（保留 data/ 和 logs/）...${NC}"

# 确保安装目录存在
mkdir -p "$INSTALL_DIR"

# 替换二进制文件
if [ -f "$NEW_DIR/tendis-migrate" ]; then
    cp "$NEW_DIR/tendis-migrate" "$INSTALL_DIR/tendis-migrate"
    chmod +x "$INSTALL_DIR/tendis-migrate"
    echo -e "${GREEN}  ✓ 二进制文件已更新${NC}"
fi

# 替换脚本文件
for script in run.sh stop.sh INSTALL.txt; do
    if [ -f "$NEW_DIR/$script" ]; then
        cp "$NEW_DIR/$script" "$INSTALL_DIR/$script"
        chmod +x "$INSTALL_DIR/$script" 2>/dev/null || true
        echo -e "${GREEN}  ✓ $script 已更新${NC}"
    fi
done

# 替换前端资源
if [ -d "$NEW_DIR/web/dist" ]; then
    rm -rf "$INSTALL_DIR/web/dist"
    mkdir -p "$INSTALL_DIR/web"
    cp -r "$NEW_DIR/web/dist" "$INSTALL_DIR/web/"
    echo -e "${GREEN}  ✓ 前端资源已更新${NC}"
fi

# 确保 data/ 和 logs/ 目录存在
mkdir -p "$INSTALL_DIR/data"
mkdir -p "$INSTALL_DIR/logs"

# 检查数据文件是否存在
if [ -f "$INSTALL_DIR/data/tasks-state.json" ]; then
    TASK_COUNT=$(python3 -c "import json;d=json.load(open('$INSTALL_DIR/data/tasks-state.json'));print(len(d))" 2>/dev/null || echo "?")
    echo -e "${GREEN}  ✓ 数据目录保留完好（${TASK_COUNT} 个任务）${NC}"
else
    echo -e "${YELLOW}  ⚠ 无历史任务数据${NC}"
fi

# 清理临时目录
rm -rf "$TEMP_DIR"

# 5. 启动新服务
echo -e "${YELLOW}[5/5] 启动新服务...${NC}"
cd "$INSTALL_DIR"
./run.sh

echo ""
echo -e "${GREEN}========================================${NC}"
echo -e "${GREEN}   升级完成！${NC}"
echo -e "${GREEN}========================================${NC}"
echo -e "  历史任务状态已保留"
echo -e "  数据备份: /tmp/$BACKUP_NAME"
echo -e ""
