#!/bin/bash
# 生成约500MB测试数据 (简化版)

HOST="10.248.37.11"
PORT="8901"

echo "=== 开始生成500MB测试数据 ==="
START=$(date +%s)

# 生成4KB的随机数据 (重复使用以提高效率)
DATA4K=$(head -c 4000 /dev/urandom | base64 | head -c 4000)
DATA1M=$(head -c 1000000 /dev/urandom | base64 | head -c 1000000)
DATA5M=$(head -c 5000000 /dev/urandom | base64 | head -c 5000000)

# 1. 生成普通string keys (约400MB)
echo "[1/4] 生成100000个string keys (约400MB)..."
for batch in $(seq 1 100); do
    for i in $(seq 1 1000); do
        idx=$((($batch - 1) * 1000 + $i))
        echo "SET str_key_$idx $DATA4K"
    done | redis-cli -c -h $HOST -p $PORT --pipe > /dev/null 2>&1
    echo -ne "  进度: $batch% ($((batch * 1000)) keys)\r"
done
echo -e "\n  string keys 完成"

# 2. 生成大string keys (约50MB)
echo "[2/4] 生成10个大key (约50MB, 每个5MB)..."
for i in $(seq 1 10); do
    redis-cli -c -h $HOST -p $PORT SET "large_str_$i" "$DATA5M" > /dev/null
    echo "  large_str_$i 完成"
done

# 3. 生成hash keys (约30MB)
echo "[3/4] 生成3000个hash keys (约30MB)..."
for batch in $(seq 1 30); do
    for i in $(seq 1 100); do
        idx=$((($batch - 1) * 100 + $i))
        echo "HSET hash_key_$idx f1 val1 f2 val2 f3 val3 f4 val4 f5 val5 f6 val6 f7 val7 f8 val8 f9 val9 f10 $(head -c 1000 /dev/urandom | base64 | head -c 1000)"
    done | redis-cli -c -h $HOST -p $PORT --pipe > /dev/null 2>&1
done
echo "  hash keys 完成"

# 4. 生成list/set keys (约20MB)
echo "[4/4] 生成2000个list/set keys (约20MB)..."
for batch in $(seq 1 20); do
    for i in $(seq 1 100); do
        idx=$((($batch - 1) * 100 + $i))
        echo "RPUSH list_key_$idx item1 item2 item3 item4 item5 item6 item7 item8 item9 item10"
        echo "SADD set_key_$idx m1 m2 m3 m4 m5 m6 m7 m8 m9 m10"
    done | redis-cli -c -h $HOST -p $PORT --pipe > /dev/null 2>&1
done
echo "  list/set keys 完成"

END=$(date +%s)
ELAPSED=$((END - START))

echo ""
echo "=== 数据生成完成 ==="
echo "耗时: ${ELAPSED}秒"
echo ""
echo "=== 验证数据 ==="
echo "节点 8901:"
redis-cli -h $HOST -p 8901 DBSIZE
redis-cli -h $HOST -p 8901 INFO memory | grep used_memory_human
echo "节点 8902:"
redis-cli -h $HOST -p 8902 DBSIZE
redis-cli -h $HOST -p 8902 INFO memory | grep used_memory_human
echo "节点 8903:"
redis-cli -h $HOST -p 8903 DBSIZE
redis-cli -h $HOST -p 8903 INFO memory | grep used_memory_human
