#!/bin/bash
# 创建增量迁移任务

cat > /tmp/incr_task.json << 'EOF'
{
  "name": "incremental-sync-test",
  "source_cluster": {
    "addrs": ["127.0.0.1:7001"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8001"]
  },
  "migration_mode": "full_and_incremental",
  "options": {
    "worker_count": 4,
    "scan_batch_size": 500,
    "conflict_policy": "overwrite",
    "key_filter": {
      "mode": "prefix",
      "prefixes": ["incr_test:"]
    }
  }
}
EOF

echo "=== Creating Incremental Task ==="
TASK_ID=$(curl -s -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d @/tmp/incr_task.json | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['task_id'])")
echo "Task ID: $TASK_ID"

echo ""
echo "=== Starting Task ==="
curl -s -X POST "http://localhost:8088/api/v1/tasks/$TASK_ID/start"
echo ""

echo "$TASK_ID" > /tmp/incr_task_id
