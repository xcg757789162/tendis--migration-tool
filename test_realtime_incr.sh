#!/bin/bash

# 测试实时增量同步

API="http://localhost:8088/api/v1"

echo "=== 创建增量同步任务 ==="
RESPONSE=$(curl -s -X POST "$API/tasks" -H "Content-Type: application/json" -d '{
  "name": "realtime-incr-test",
  "source_cluster": {
    "addrs": ["127.0.0.1:7001"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8001"]
  },
  "migration_mode": "full_and_incremental",
  "key_filter": "rt_test:*",
  "workers": 4,
  "batch_size": 1000,
  "conflict_policy": "replace"
}')

echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

TASK_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['id'])" 2>/dev/null)

if [ -z "$TASK_ID" ]; then
  echo "Failed to create task"
  exit 1
fi

echo ""
echo "Task ID: $TASK_ID"
echo ""
echo "=== 启动任务 ==="
curl -s -X POST "$API/tasks/$TASK_ID/start"
echo ""
echo ""
echo "=== 等待全量阶段完成并进入增量阶段 ==="
sleep 15

# 检查状态
STATUS=$(curl -s "$API/tasks/$TASK_ID" | python3 -c "import sys,json; d=json.load(sys.stdin)['data']; print(f\"Phase: {d['phase']}, Status: {d['status']}\")" 2>/dev/null)
echo "$STATUS"

echo ""
echo "Task ID for reference: $TASK_ID"
