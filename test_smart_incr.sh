#!/bin/bash

# 测试智能增量同步

API="http://localhost:8088/api/v1"

echo "=== 创建智能增量同步任务 ==="
RESPONSE=$(curl -s -X POST "$API/tasks" -H "Content-Type: application/json" -d '{
  "name": "smart-incr-test",
  "source_cluster": {
    "addrs": ["127.0.0.1:7001"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8001"]
  },
  "migration_mode": "full_and_incremental",
  "key_filter": "smart_test:*",
  "workers": 4,
  "batch_size": 1000,
  "conflict_policy": "replace"
}')

echo "$RESPONSE" | python3 -m json.tool 2>/dev/null || echo "$RESPONSE"

TASK_ID=$(echo "$RESPONSE" | python3 -c "import sys,json; d=json.load(sys.stdin); print(d.get('data',{}).get('task_id','') or d.get('data',{}).get('id',''))" 2>/dev/null)

if [ -z "$TASK_ID" ]; then
  echo "Failed to get task ID, trying alternative..."
  TASK_ID=$(echo "$RESPONSE" | grep -o '"task_id":"[^"]*"' | cut -d'"' -f4)
fi

echo ""
echo "Task ID: $TASK_ID"
