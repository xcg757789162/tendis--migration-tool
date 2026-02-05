#!/usr/bin/env python3
"""
生成 1GB 测试数据到 Tendis 集群
- 80% String 类型
- 20% 其他类型（Hash, List, Set, ZSet）
- 包含大 Key
"""

import redis
from redis.cluster import RedisCluster, ClusterNode
import random
import string
import time
import sys

# 集群配置
CLUSTER_NODES = [
    ClusterNode("192.168.1.23", 7001),
    ClusterNode("192.168.1.23", 7002),
    ClusterNode("192.168.1.23", 7003),
]

# 单节点配置（用于获取统计信息）
NODE_CONFIGS = [
    {"host": "192.168.1.23", "port": 7001},
    {"host": "192.168.1.23", "port": 7002},
    {"host": "192.168.1.23", "port": 7003},
]

# 数据配置
TARGET_SIZE_GB = 1  # 目标数据量 GB
TARGET_SIZE_BYTES = TARGET_SIZE_GB * 1024 * 1024 * 1024

# Key 前缀
KEY_PREFIX = "testkey:"

# 数据分布
STRING_RATIO = 0.80  # 80% string
HASH_RATIO = 0.05    # 5% hash
LIST_RATIO = 0.05    # 5% list
SET_RATIO = 0.05     # 5% set
ZSET_RATIO = 0.05    # 5% zset

# 大 Key 配置
BIG_KEY_COUNT = 50   # 大 key 数量
BIG_KEY_MIN_SIZE = 1 * 1024 * 1024   # 最小 1MB
BIG_KEY_MAX_SIZE = 10 * 1024 * 1024  # 最大 10MB

# 普通 Key 配置
NORMAL_VALUE_MIN = 100      # 最小 100 字节
NORMAL_VALUE_MAX = 10240    # 最大 10KB
BATCH_SIZE = 1000           # 批量写入大小


def generate_random_string(length):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))


def generate_random_value(min_size, max_size):
    """生成随机值"""
    size = random.randint(min_size, max_size)
    return generate_random_string(size)


def connect_cluster():
    """连接集群"""
    try:
        rc = RedisCluster(
            startup_nodes=CLUSTER_NODES,
            decode_responses=True,
            skip_full_coverage_check=True
        )
        rc.ping()
        print("✅ 成功连接到 Tendis 集群")
        return rc
    except Exception as e:
        print(f"❌ 连接集群失败: {e}")
        sys.exit(1)


def write_big_keys(rc):
    """写入大 Key"""
    print(f"\n📦 开始写入 {BIG_KEY_COUNT} 个大 Key...")
    total_size = 0
    
    for i in range(BIG_KEY_COUNT):
        key_type = random.choice(['string', 'hash', 'list', 'set', 'zset'])
        key_name = f"{KEY_PREFIX}bigkey:{key_type}:{i}"
        size = random.randint(BIG_KEY_MIN_SIZE, BIG_KEY_MAX_SIZE)
        
        try:
            if key_type == 'string':
                value = generate_random_string(size)
                rc.set(key_name, value)
                total_size += size
                
            elif key_type == 'hash':
                # 生成多个 field，每个 field 的值较大
                field_count = random.randint(100, 500)
                field_size = size // field_count
                hash_data = {f"field_{j}": generate_random_string(field_size) for j in range(field_count)}
                rc.hset(key_name, mapping=hash_data)
                total_size += size
                
            elif key_type == 'list':
                # 生成多个元素
                elem_count = random.randint(1000, 5000)
                elem_size = size // elem_count
                for j in range(0, elem_count, 100):
                    batch = [generate_random_string(elem_size) for _ in range(min(100, elem_count - j))]
                    rc.rpush(key_name, *batch)
                total_size += size
                
            elif key_type == 'set':
                # 生成多个成员
                member_count = random.randint(1000, 5000)
                member_size = size // member_count
                for j in range(0, member_count, 100):
                    batch = [generate_random_string(member_size) for _ in range(min(100, member_count - j))]
                    rc.sadd(key_name, *batch)
                total_size += size
                
            elif key_type == 'zset':
                # 生成多个成员和分数
                member_count = random.randint(1000, 5000)
                member_size = size // member_count
                for j in range(0, member_count, 100):
                    batch = {generate_random_string(member_size): random.random() * 1000 for _ in range(min(100, member_count - j))}
                    rc.zadd(key_name, batch)
                total_size += size
            
            print(f"  [{i+1}/{BIG_KEY_COUNT}] {key_name} ({key_type}) - {size/1024/1024:.2f} MB")
            
        except Exception as e:
            print(f"  ❌ 写入 {key_name} 失败: {e}")
    
    print(f"✅ 大 Key 写入完成，总大小: {total_size/1024/1024:.2f} MB")
    return total_size


def write_normal_keys(rc, remaining_size):
    """写入普通 Key"""
    print(f"\n📝 开始写入普通 Key，目标大小: {remaining_size/1024/1024:.2f} MB")
    
    total_written = 0
    key_counts = {'string': 0, 'hash': 0, 'list': 0, 'set': 0, 'zset': 0}
    batch_count = 0
    start_time = time.time()
    last_report_time = start_time
    
    while total_written < remaining_size:
        # 决定数据类型
        rand = random.random()
        if rand < STRING_RATIO:
            key_type = 'string'
        elif rand < STRING_RATIO + HASH_RATIO:
            key_type = 'hash'
        elif rand < STRING_RATIO + HASH_RATIO + LIST_RATIO:
            key_type = 'list'
        elif rand < STRING_RATIO + HASH_RATIO + LIST_RATIO + SET_RATIO:
            key_type = 'set'
        else:
            key_type = 'zset'
        
        key_id = key_counts[key_type]
        key_name = f"{KEY_PREFIX}{key_type}:{key_id}"
        
        try:
            if key_type == 'string':
                value = generate_random_value(NORMAL_VALUE_MIN, NORMAL_VALUE_MAX)
                rc.set(key_name, value)
                total_written += len(value)
                
            elif key_type == 'hash':
                field_count = random.randint(5, 50)
                field_size = random.randint(50, 500)
                hash_data = {f"f{j}": generate_random_string(field_size) for j in range(field_count)}
                rc.hset(key_name, mapping=hash_data)
                total_written += field_count * field_size
                
            elif key_type == 'list':
                elem_count = random.randint(10, 100)
                elem_size = random.randint(50, 500)
                elements = [generate_random_string(elem_size) for _ in range(elem_count)]
                rc.rpush(key_name, *elements)
                total_written += elem_count * elem_size
                
            elif key_type == 'set':
                member_count = random.randint(10, 100)
                member_size = random.randint(50, 500)
                members = [generate_random_string(member_size) for _ in range(member_count)]
                rc.sadd(key_name, *members)
                total_written += member_count * member_size
                
            elif key_type == 'zset':
                member_count = random.randint(10, 100)
                member_size = random.randint(50, 500)
                members = {generate_random_string(member_size): random.random() * 1000 for _ in range(member_count)}
                rc.zadd(key_name, members)
                total_written += member_count * member_size
            
            key_counts[key_type] += 1
            batch_count += 1
            
            # 每 5 秒报告一次进度
            current_time = time.time()
            if current_time - last_report_time >= 5:
                elapsed = current_time - start_time
                speed = total_written / elapsed / 1024 / 1024  # MB/s
                progress = total_written / remaining_size * 100
                print(f"  进度: {progress:.1f}% | 已写入: {total_written/1024/1024:.2f} MB | "
                      f"速度: {speed:.2f} MB/s | Key 数量: {sum(key_counts.values())}")
                last_report_time = current_time
                
        except Exception as e:
            print(f"  ⚠️ 写入 {key_name} 失败: {e}")
            time.sleep(0.1)
    
    elapsed = time.time() - start_time
    print(f"\n✅ 普通 Key 写入完成")
    print(f"   总大小: {total_written/1024/1024:.2f} MB")
    print(f"   耗时: {elapsed:.1f} 秒")
    print(f"   平均速度: {total_written/elapsed/1024/1024:.2f} MB/s")
    print(f"   Key 分布:")
    for k, v in key_counts.items():
        print(f"     - {k}: {v} 个")
    
    return total_written, key_counts


def get_cluster_stats(rc):
    """获取集群统计信息"""
    print("\n📊 集群统计信息:")
    total_keys = 0
    total_memory = 0
    
    for node in NODE_CONFIGS:
        try:
            r = redis.Redis(host=node["host"], port=node["port"], decode_responses=True)
            info = r.info("memory")
            dbsize = r.dbsize()
            memory_used = info.get("used_memory", 0)
            total_keys += dbsize
            total_memory += memory_used
            print(f"   节点 {node['port']}: {dbsize} keys, {memory_used/1024/1024:.2f} MB")
        except Exception as e:
            print(f"   节点 {node['port']}: 获取信息失败 - {e}")
    
    print(f"   总计: {total_keys} keys, {total_memory/1024/1024:.2f} MB")
    return total_keys, total_memory


def main():
    print("=" * 60)
    print("🚀 Tendis 集群测试数据生成器")
    print("=" * 60)
    print(f"目标数据量: {TARGET_SIZE_GB} GB")
    print(f"数据分布: String {STRING_RATIO*100}%, Hash {HASH_RATIO*100}%, "
          f"List {LIST_RATIO*100}%, Set {SET_RATIO*100}%, ZSet {ZSET_RATIO*100}%")
    print(f"大 Key 数量: {BIG_KEY_COUNT} 个 ({BIG_KEY_MIN_SIZE/1024/1024}-{BIG_KEY_MAX_SIZE/1024/1024} MB)")
    
    # 连接集群
    rc = connect_cluster()
    
    # 获取当前统计
    print("\n📊 写入前集群状态:")
    get_cluster_stats(rc)
    
    # 写入大 Key
    big_key_size = write_big_keys(rc)
    
    # 计算剩余需要写入的大小
    remaining_size = TARGET_SIZE_BYTES - big_key_size
    if remaining_size > 0:
        normal_size, key_counts = write_normal_keys(rc, remaining_size)
    
    # 获取最终统计
    print("\n📊 写入后集群状态:")
    get_cluster_stats(rc)
    
    print("\n" + "=" * 60)
    print("✅ 数据生成完成!")
    print("=" * 60)


if __name__ == "__main__":
    main()
