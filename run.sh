#!/bin/bash
# Tendis Migration Tool 启动脚本 (生产环境)

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
cd "$SCRIPT_DIR"

# 颜色定义
GREEN='\033[0;32m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# 配置
PORT=${PORT:-8088}
DATA_DIR=${DATA_DIR:-./data}
WORKERS=${WORKERS:-4}
PID_FILE="$SCRIPT_DIR/tendis-migrate.pid"
LOG_FILE="$SCRIPT_DIR/logs/tendis-migrate.log"

# 创建必要目录
mkdir -p "$DATA_DIR"
mkdir -p "$SCRIPT_DIR/logs"

# 检查是否已经运行
if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${RED}Tendis Migration Tool is already running (PID: $PID)${NC}"
        exit 1
    else
        rm -f "$PID_FILE"
    fi
fi

# 检查可执行文件
if [ ! -f "$SCRIPT_DIR/tendis-migrate" ]; then
    echo -e "${RED}Error: tendis-migrate binary not found!${NC}"
    exit 1
fi

echo -e "${BLUE}========================================${NC}"
echo -e "${BLUE}   Tendis Migration Tool${NC}"
echo -e "${BLUE}========================================${NC}"
echo -e "${GREEN}Starting service...${NC}"
echo -e "  Port: $PORT"
echo -e "  Data: $DATA_DIR"
echo -e "  Workers: $WORKERS"
echo -e "  Log: $LOG_FILE"

# 后台启动
nohup "$SCRIPT_DIR/tendis-migrate" -port "$PORT" -data "$DATA_DIR" -workers "$WORKERS" >> "$LOG_FILE" 2>&1 &
PID=$!
echo $PID > "$PID_FILE"

sleep 1

# 检查是否启动成功
if ps -p "$PID" > /dev/null 2>&1; then
    echo -e "${GREEN}Started successfully (PID: $PID)${NC}"
    echo -e "${BLUE}Web UI: http://localhost:$PORT${NC}"
else
    echo -e "${RED}Failed to start. Check log: $LOG_FILE${NC}"
    rm -f "$PID_FILE"
    exit 1
fi
