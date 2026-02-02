#!/bin/bash
# V2.0 端到端测试脚本

set -e

echo "========================================="
echo "   Tendis-Migrate V2.0 E2E Test"
echo "========================================="

# 配置
TASK_ID="test-v2-$(date +%s)"
SOURCE_CLUSTER="192.168.1.23:7001,192.168.1.23:7002,192.168.1.23:7003"
TARGET_CLUSTER="192.168.1.23:8001,192.168.1.23:8002,192.168.1.23:8003"
NUM_WORKERS=8
MASTER_PORT=8088
SOCKET_PATH="/tmp/tendis-migrate-master.sock"

# 清理旧数据
echo ""
echo "1. 清理旧数据..."
rm -rf ./data/*.db ./data/queue_* ./logs/*.log
rm -f $SOCKET_PATH

# 初始化数据库
echo ""
echo "2. 初始化 SQLite 数据库..."
./tendis-migrate-master \
  -task-id=$TASK_ID \
  -source=$SOURCE_CLUSTER \
  -target=$TARGET_CLUSTER \
  -num-workers=$NUM_WORKERS \
  -port=$MASTER_PORT \
  -init-only &

MASTER_PID=$!
sleep 3

# 检查 Master 是否启动
if ! kill -0 $MASTER_PID 2>/dev/null; then
    echo "❌ Master 启动失败！"
    exit 1
fi

echo "✅ Master 启动成功（PID: $MASTER_PID）"
echo ""
echo "3. 等待全量迁移完成..."
echo "   - 监听端口: $MASTER_PORT"
echo "   - IPC Socket: $SOCKET_PATH"
echo "   - Worker 数量: $NUM_WORKERS"
echo "   - 预计时间: 根据数据量而定"
echo ""

# 监控进度
while true; do
    sleep 10
    
    # 检查 Master 进程
    if ! kill -0 $MASTER_PID 2>/dev/null; then
        echo ""
        echo "Master 进程已退出"
        break
    fi
    
    # 从数据库查询进度
    sqlite3 ./data/tasks.db "SELECT COUNT(*) FROM slot_status WHERE task_id='$TASK_ID' AND status='completed';" | \
        xargs -I {} echo "   进度: {}/16384 Slots"
done

echo ""
echo "4. 检查结果..."

# 查询最终统计
sqlite3 ./data/tasks.db <<EOF
SELECT 
    COUNT(CASE WHEN status = 'completed' THEN 1 END) as completed,
    COUNT(CASE WHEN status = 'in_progress' THEN 1 END) as in_progress,
    COUNT(CASE WHEN status = 'failed' THEN 1 END) as failed,
    COUNT(CASE WHEN status = 'pending' THEN 1 END) as pending
FROM slot_status 
WHERE task_id = '$TASK_ID';
EOF

echo ""
echo "========================================="
echo "   测试完成！"
echo "========================================="
echo ""
echo "日志位置:"
echo "  - Master: ./logs/master_${TASK_ID}.log"
echo "  - Workers: ./logs/worker_${TASK_ID}_*.log"
echo ""
echo "数据库:"
echo "  - SQLite: ./data/tasks.db"
echo "  - LevelDB: ./data/queue_*"
echo ""
