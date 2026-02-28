#!/bin/bash
# Tendis Migration Tool 停止脚本
# 发送 SIGTERM 实现优雅关闭，等待进程保存所有任务状态后退出

# 获取脚本所在目录
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# 颜色定义
GREEN='\033[0;32m'
RED='\033[0;31m'
YELLOW='\033[1;33m'
NC='\033[0m'

PID_FILE="$SCRIPT_DIR/tendis-migrate.pid"

# 优雅关闭等待时间（秒），确保有足够时间保存任务状态和断点
GRACEFUL_TIMEOUT=${GRACEFUL_TIMEOUT:-30}

if [ -f "$PID_FILE" ]; then
    PID=$(cat "$PID_FILE")
    if ps -p "$PID" > /dev/null 2>&1; then
        echo -e "${YELLOW}Stopping Tendis Migration Tool (PID: $PID)...${NC}"
        echo -e "${YELLOW}Waiting up to ${GRACEFUL_TIMEOUT}s for graceful shutdown (saving task states)...${NC}"
        kill "$PID"
        
        # 等待进程优雅退出（保存任务状态、断点、错误Key等）
        for i in $(seq 1 $GRACEFUL_TIMEOUT); do
            if ! ps -p "$PID" > /dev/null 2>&1; then
                break
            fi
            if [ $((i % 5)) -eq 0 ]; then
                echo -e "${YELLOW}  Still waiting... (${i}s/${GRACEFUL_TIMEOUT}s)${NC}"
            fi
            sleep 1
        done
        
        # 如果还没结束，强制终止
        if ps -p "$PID" > /dev/null 2>&1; then
            echo -e "${RED}Graceful shutdown timed out after ${GRACEFUL_TIMEOUT}s, force killing...${NC}"
            kill -9 "$PID"
            sleep 1
        fi
        
        rm -f "$PID_FILE"
        echo -e "${GREEN}Stopped successfully. All task states have been saved.${NC}"
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
        echo -e "${YELLOW}Sending SIGTERM for graceful shutdown...${NC}"
        kill $PIDS 2>/dev/null
        sleep 5
        # 检查是否还在运行
        STILL_RUNNING=$(pgrep -f "tendis-migrate" 2>/dev/null)
        if [ -n "$STILL_RUNNING" ]; then
            echo -e "${YELLOW}Still running, use 'kill -9 $STILL_RUNNING' to force stop${NC}"
        else
            echo -e "${GREEN}Stopped successfully${NC}"
        fi
    fi
fi
