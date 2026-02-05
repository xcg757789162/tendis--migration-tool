#!/bin/bash
# 测试 binlog 解析 bug 修复

# 创建测试任务
echo "Creating migration task..."
curl -s -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "test-binlog-fix-'$(date +%Y%m%d%H%M%S)'",
    "migration_mode": "full_and_incremental",
    "source_cluster": {
      "addrs": ["10.248.37.11:8901", "10.248.37.11:8902", "10.248.37.11:8903"]
    },
    "target_cluster": {
      "addrs": ["10.31.165.39:8901", "10.31.165.39:8902", "10.31.165.39:8903"]
    },
    "key_filter": {
      "prefixes": "testkey"
    },
    "options": {
      "workers": 4,
      "scan_count": 1000,
      "conflict_policy": "skip"
    }
  }' | tee /tmp/task_result.json

echo ""
echo "Task created. Result saved to /tmp/task_result.json"
