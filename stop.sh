#!/bin/bash
# Tendis Migration Tool 停止脚本

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PID_FILE="$SCRIPT_DIR/tendis-migrate.pid"

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}Stopping Tendis Migration Tool (PID: $PID)...${NC}"
        kill "$PID"
        
        # 等待进程结束
        for i in {1..10}; do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                break
            fi
            sleep 1
        done
        
        # 如果还没结束，强制终止
        if ps -p "$PID" > /dev/null 2>&1; then
            echo -e "${YELLOW}Force killing...${NC}"
            kill -9 "$PID"
        fi
        
        rm -f "$PID_FILE"
        echo -e "${GREEN}Stopped successfully${NC}"
    else
        echo -e "${YELLOW}Process not running, cleaning up PID file${NC}"
        rm -f "$PID_FILE"
    fi
else
    echo -e "${RED}PID file not found. Service may not be running.${NC}"
    
    # 尝试查找进程
    PIDS=$(pgrep -f "tendis-migrate" 2>/dev/null)
    if [ -n "$PIDS" ]; then
        echo -e "${YELLOW}Found running processes: $PIDS${NC}"
        echo -e "${YELLOW}Use 'kill $PIDS' to stop them manually${NC}"
    fi
fi
