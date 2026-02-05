#!/bin/bash
HOST="10.248.37.11"
PORT="8901"
TARGET_HOST="10.31.165.39"

echo "=== 1. 创建前缀测试数据 (prefix_test:) ==="
for i in $(seq 1 100); do
  redis-cli -h $HOST -p $PORT -c SET "prefix_test:key:$i" "value_$i" EX 3600 > /dev/null
done
echo "前缀测试数据: 100 keys"

echo "=== 2. 创建排除前缀测试数据 (exclude_me:) ==="
for i in $(seq 1 50); do
  redis-cli -h $HOST -p $PORT -c SET "exclude_me:key:$i" "should_skip_$i" > /dev/null
done
echo "排除前缀测试数据: 50 keys"

echo "=== 3. 创建正则匹配测试数据 (user:*:profile/session) ==="
for i in $(seq 1 30); do
  redis-cli -h $HOST -p $PORT -c SET "user:123:profile:$i" "profile_data_$i" > /dev/null
  redis-cli -h $HOST -p $PORT -c SET "user:456:session:$i" "session_data_$i" > /dev/null
done
echo "正则测试数据: 60 keys"

echo "=== 4. 创建大Key测试数据 ==="
# 大Hash (1000 fields)
for i in $(seq 1 1000); do
  redis-cli -h $HOST -p $PORT -c HSET "bigkey:hash:test" "field_$i" "value_$i" > /dev/null
done
echo "大Hash: 1000 fields"

# 大Set (500 members)
for i in $(seq 1 500); do
  redis-cli -h $HOST -p $PORT -c SADD "bigkey:set:test" "member_$i" > /dev/null
done
echo "大Set: 500 members"

# 大ZSet (500 members)
for i in $(seq 1 500); do
  redis-cli -h $HOST -p $PORT -c ZADD "bigkey:zset:test" $i "member_$i" > /dev/null
done
echo "大ZSet: 500 members"

# 大List (500 elements)
for i in $(seq 1 500); do
  redis-cli -h $HOST -p $PORT -c RPUSH "bigkey:list:test" "element_$i" > /dev/null
done
echo "大List: 500 elements"

echo "=== 5. 创建冲突测试数据 ==="
# 先在目标端创建
for i in $(seq 1 20); do
  redis-cli -h $TARGET_HOST -p 8901 -c SET "conflict:key:$i" "target_value_$i" > /dev/null
done
# 再在源端创建同名key
for i in $(seq 1 20); do
  redis-cli -h $HOST -p $PORT -c SET "conflict:key:$i" "source_value_$i" > /dev/null
done
echo "冲突测试数据: 20 keys (两端同名不同值)"

echo "=== 6. 创建TTL测试数据 ==="
redis-cli -h $HOST -p $PORT -c SET "ttl:test:60s" "expire_60s" EX 60 > /dev/null
redis-cli -h $HOST -p $PORT -c SET "ttl:test:300s" "expire_300s" EX 300 > /dev/null
redis-cli -h $HOST -p $PORT -c SET "ttl:test:3600s" "expire_3600s" EX 3600 > /dev/null
redis-cli -h $HOST -p $PORT -c SET "ttl:test:persist" "no_expire" > /dev/null
echo "TTL测试数据: 4 keys"

echo "=== 7. 创建指定Key列表测试数据 ==="
for i in $(seq 1 10); do
  redis-cli -h $HOST -p $PORT -c SET "keylist:item:$i" "keylist_value_$i" > /dev/null
done
echo "指定Key列表测试数据: 10 keys"

echo ""
echo "=== 测试数据创建完成 ==="
echo "源端新增测试Key总数: 约 250+ keys"
