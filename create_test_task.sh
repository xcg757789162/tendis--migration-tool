#!/bin/bash

API="http://localhost:8088/api/v1"

# 创建任务
RESPONSE=$(curl -s -X POST "$API/tasks" -H "Content-Type: application/json" -d '{
  "name": "realtime-test-v2",
  "source_cluster": {"addrs": ["127.0.0.1:7001"]},
  "target_cluster": {"addrs": ["127.0.0.1:8001"]},
  "migration_mode": "full_and_incremental",
  "workers": 4,
  "batch_size": 1000,
  "conflict_policy": "replace"
}')

echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

TASK_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; print(json.load(sys.stdin).get('data',{}).get('task_id',''))" 2>/dev/null)
echo ""
echo "Task ID: $TASK_ID"

# 启动任务
if [ -n "$TASK_ID" ]; then
  echo "Starting task..."
  curl -s -X POST "$API/tasks/$TASK_ID/start"
  echo ""
fi
