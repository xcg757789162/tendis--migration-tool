#!/bin/bash
# 迁移剩余节点的数据（节点2和节点3）

echo "=== 迁移节点 2 (7002 -> 8002) ==="
cat > /tmp/task2.json << 'EOF'
{
  "name": "migrate-node2-7002",
  "source_cluster": {
    "addrs": ["127.0.0.1:7002"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8002"]
  },
  "migration_mode": "full",
  "options": {
    "worker_count": 8,
    "scan_batch_size": 1000,
    "conflict_policy": "overwrite",
    "key_filter": {
      "mode": "prefix",
      "prefixes": ["testkey:"]
    }
  }
}
EOF

TASK2_ID=$(curl -s -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d @/tmp/task2.json | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['task_id'])")
echo "Task 2 ID: $TASK2_ID"

echo "=== 迁移节点 3 (7003 -> 8003) ==="
cat > /tmp/task3.json << 'EOF'
{
  "name": "migrate-node3-7003",
  "source_cluster": {
    "addrs": ["127.0.0.1:7003"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8003"]
  },
  "migration_mode": "full",
  "options": {
    "worker_count": 8,
    "scan_batch_size": 1000,
    "conflict_policy": "overwrite",
    "key_filter": {
      "mode": "prefix",
      "prefixes": ["testkey:"]
    }
  }
}
EOF

TASK3_ID=$(curl -s -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d @/tmp/task3.json | python3 -c "import sys,json; print(json.load(sys.stdin)['data']['task_id'])")
echo "Task 3 ID: $TASK3_ID"

echo ""
echo "=== 启动任务 ==="
curl -s -X POST "http://localhost:8088/api/v1/tasks/$TASK2_ID/start"
echo ""
curl -s -X POST "http://localhost:8088/api/v1/tasks/$TASK3_ID/start"
echo ""

echo ""
echo "=== 任务已启动 ==="
echo "Task IDs: $TASK2_ID, $TASK3_ID"

# 保存 task IDs 到文件
echo "$TASK2_ID" > /tmp/task2_id
echo "$TASK3_ID" > /tmp/task3_id
