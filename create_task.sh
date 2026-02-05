#!/bin/bash
# 创建迁移任务 - 使用正确的 API 格式

cat > /tmp/task.json << 'EOF'
{
  "name": "test-1gb-full-migration",
  "source_cluster": {
    "addrs": ["127.0.0.1:7001", "127.0.0.1:7002", "127.0.0.1:7003"]
  },
  "target_cluster": {
    "addrs": ["127.0.0.1:8001", "127.0.0.1:8002", "127.0.0.1:8003"]
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

echo "=== Task JSON ==="
cat /tmp/task.json

echo ""
echo "=== Creating Task ==="
curl -s -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d @/tmp/task.json
