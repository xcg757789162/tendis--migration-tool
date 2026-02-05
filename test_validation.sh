#!/bin/bash

# 测试 createTaskHandler 参数验证修复

echo "========================================="
echo "测试 createTaskHandler 参数验证"
echo "========================================="

BASE_URL="http://10.248.37.11:8088/api/v1"

echo ""
echo "测试1: 空JSON创建任务（应返回400）"
curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d '{}' | jq '.code, .message'

echo ""
echo "测试2: 只传name创建任务（应返回400缺少source_cluster）"
curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d '{"name":"test"}' | jq '.code, .message'

echo ""
echo "测试3: 有name和source_cluster但缺少target_cluster（应返回400）"
curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d '{"name":"test","source_cluster":{"addrs":["10.248.37.11:8901"]}}' | jq '.code, .message'

echo ""
echo "测试4: 正确的完整参数（应返回200）"
curl -s -X POST "${BASE_URL}/tasks" \
  -H "Content-Type: application/json" \
  -d '{
    "name": "validation-test",
    "migration_mode": "full_and_incremental",
    "source_cluster": {"addrs": ["10.248.37.11:8901"]},
    "target_cluster": {"addrs": ["10.31.165.39:8901"]},
    "key_filter": {"prefixes": "test"},
    "options": {"workers": 4}
  }' | jq '.code, .message, .data.task_id'

echo ""
echo "========================================="
echo "测试完成"
echo "========================================="
