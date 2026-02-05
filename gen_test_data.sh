#!/bin/bash
# 生成 1GB 测试数据到 Tendis 集群
# 在 Docker 服务器上执行

set -e

# 配置
DOCKER=/usr/local/bin/docker
CONTAINER="tendis-src-7001"
PORT=7001
KEY_PREFIX="testkey:"
TARGET_SIZE_MB=1024  # 1GB
BIG_KEY_COUNT=50
BIG_KEY_SIZE_MB=5    # 每个大 key 约 5MB

echo "=============================================="
echo "Tendis 测试数据生成器"
echo "=============================================="
echo "目标: ${TARGET_SIZE_MB} MB"
echo "大 Key: ${BIG_KEY_COUNT} 个，每个约 ${BIG_KEY_SIZE_MB} MB"

# 生成随机字符串函数
gen_string() {
    local size=$1
    cat /dev/urandom | LC_ALL=C tr -dc 'a-zA-Z0-9' | head -c $size
}

# 获取当前集群状态
echo ""
echo "=== 写入前集群状态 ==="
$DOCKER exec $CONTAINER redis-cli -p $PORT -c info memory | grep used_memory_human
$DOCKER exec $CONTAINER redis-cli -p $PORT -c dbsize

written_mb=0

# 1. 写入大 Key (约 250MB)
echo ""
echo "=== 开始写入大 Key ==="
for i in $(seq 1 $BIG_KEY_COUNT); do
    # 随机选择类型
    types=("string" "hash" "list" "set" "zset")
    type=${types[$((RANDOM % 5))]}
    key="${KEY_PREFIX}bigkey:${type}:${i}"
    
    case $type in
        string)
            # 生成 5MB 字符串
            value=$(gen_string $((BIG_KEY_SIZE_MB * 1024 * 1024)))
            $DOCKER exec $CONTAINER redis-cli -p $PORT -c set "$key" "$value" > /dev/null
            ;;
        hash)
            # 生成 500 个 field，每个 10KB
            for j in $(seq 1 500); do
                field_value=$(gen_string 10240)
                $DOCKER exec $CONTAINER redis-cli -p $PORT -c hset "$key" "field_$j" "$field_value" > /dev/null
            done
            ;;
        list)
            # 生成 5000 个元素，每个 1KB
            for j in $(seq 1 50); do
                # 每次 100 个
                elements=""
                for k in $(seq 1 100); do
                    elements="$elements $(gen_string 1024)"
                done
                $DOCKER exec $CONTAINER redis-cli -p $PORT -c rpush "$key" $elements > /dev/null
            done
            ;;
        set)
            # 生成 5000 个成员，每个 1KB
            for j in $(seq 1 50); do
                members=""
                for k in $(seq 1 100); do
                    members="$members $(gen_string 1024)"
                done
                $DOCKER exec $CONTAINER redis-cli -p $PORT -c sadd "$key" $members > /dev/null
            done
            ;;
        zset)
            # 生成 5000 个成员，每个 1KB
            for j in $(seq 1 50); do
                for k in $(seq 1 100); do
                    member=$(gen_string 1024)
                    score=$((RANDOM % 10000))
                    $DOCKER exec $CONTAINER redis-cli -p $PORT -c zadd "$key" $score "$member" > /dev/null
                done
            done
            ;;
    esac
    
    echo "  [$i/$BIG_KEY_COUNT] $key ($type)"
    written_mb=$((written_mb + BIG_KEY_SIZE_MB))
done

echo "大 Key 写入完成: ${written_mb} MB"

# 2. 写入普通 Key (剩余约 750MB)
echo ""
echo "=== 开始写入普通 Key ==="
remaining_mb=$((TARGET_SIZE_MB - written_mb))
echo "需要写入: ${remaining_mb} MB"

# 使用 pipeline 批量写入
batch_size=1000
key_id=0
string_count=0
hash_count=0
list_count=0
set_count=0
zset_count=0

while [ $written_mb -lt $TARGET_SIZE_MB ]; do
    # 生成批量命令
    cmds=""
    batch_bytes=0
    
    for i in $(seq 1 $batch_size); do
        # 80% string, 5% 其他类型
        rand=$((RANDOM % 100))
        
        if [ $rand -lt 80 ]; then
            # String
            key="${KEY_PREFIX}string:${string_count}"
            size=$((RANDOM % 10000 + 100))  # 100-10100 字节
            value=$(gen_string $size)
            cmds="${cmds}SET $key $value\n"
            string_count=$((string_count + 1))
            batch_bytes=$((batch_bytes + size))
        elif [ $rand -lt 85 ]; then
            # Hash
            key="${KEY_PREFIX}hash:${hash_count}"
            for j in $(seq 1 10); do
                fsize=$((RANDOM % 450 + 50))
                fval=$(gen_string $fsize)
                cmds="${cmds}HSET $key f$j $fval\n"
                batch_bytes=$((batch_bytes + fsize))
            done
            hash_count=$((hash_count + 1))
        elif [ $rand -lt 90 ]; then
            # List
            key="${KEY_PREFIX}list:${list_count}"
            for j in $(seq 1 20); do
                esize=$((RANDOM % 450 + 50))
                elem=$(gen_string $esize)
                cmds="${cmds}RPUSH $key $elem\n"
                batch_bytes=$((batch_bytes + esize))
            done
            list_count=$((list_count + 1))
        elif [ $rand -lt 95 ]; then
            # Set
            key="${KEY_PREFIX}set:${set_count}"
            for j in $(seq 1 20); do
                msize=$((RANDOM % 450 + 50))
                member=$(gen_string $msize)
                cmds="${cmds}SADD $key $member\n"
                batch_bytes=$((batch_bytes + msize))
            done
            set_count=$((set_count + 1))
        else
            # ZSet
            key="${KEY_PREFIX}zset:${zset_count}"
            for j in $(seq 1 20); do
                msize=$((RANDOM % 450 + 50))
                member=$(gen_string $msize)
                score=$((RANDOM % 10000))
                cmds="${cmds}ZADD $key $score $member\n"
                batch_bytes=$((batch_bytes + msize))
            done
            zset_count=$((zset_count + 1))
        fi
    done
    
    # 执行批量命令
    echo -e "$cmds" | $DOCKER exec -i $CONTAINER redis-cli -p $PORT -c --pipe > /dev/null 2>&1
    
    batch_mb=$((batch_bytes / 1024 / 1024))
    written_mb=$((written_mb + batch_mb + 1))  # 估算
    
    total_keys=$((string_count + hash_count + list_count + set_count + zset_count))
    echo "  进度: ~${written_mb}/${TARGET_SIZE_MB} MB | Keys: ${total_keys} | String: ${string_count}"
done

echo ""
echo "=== 写入完成 ==="
echo "Key 分布:"
echo "  String: $string_count"
echo "  Hash: $hash_count"
echo "  List: $list_count"
echo "  Set: $set_count"
echo "  ZSet: $zset_count"
echo "  BigKey: $BIG_KEY_COUNT"

echo ""
echo "=== 写入后集群状态 ==="
$DOCKER exec $CONTAINER redis-cli -p $PORT -c info memory | grep used_memory_human
$DOCKER exec $CONTAINER redis-cli -p $PORT -c dbsize

echo ""
echo "=============================================="
echo "完成!"
echo "=============================================="
