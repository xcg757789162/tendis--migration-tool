#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
高效生成约500MB Redis测试数据
80% string类型 + 大key + hash/list/set
"""
import redis
import random
import string
import time
import sys

# Redis集群节点
NODES = [
    ("10.248.37.11", 8901),
    ("10.248.37.11", 8902),
    ("10.248.37.11", 8903),
]

def generate_random_string(length):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def get_cluster_client():
    """获取集群客户端"""
    from redis.cluster import RedisCluster
    startup_nodes = [redis.cluster.ClusterNode(host, port) for host, port in NODES]
    return RedisCluster(startup_nodes=startup_nodes, decode_responses=False)

def main():
    print("=== 开始生成500MB测试数据 ===")
    print("连接Redis集群...")
    
    try:
        rc = get_cluster_client()
        rc.ping()
        print("连接成功!")
    except Exception as e:
        print(f"集群连接失败，尝试单节点模式: {e}")
        rc = redis.Redis(host=NODES[0][0], port=NODES[0][1], decode_responses=False)
    
    start_time = time.time()
    total_bytes = 0
    total_keys = 0
    
    # 1. 生成普通string keys (约400MB = 80%)
    # 每个key约1KB，需要约400,000个
    print("\n[1/5] 生成普通string keys (约400MB, 每批10000个)...")
    batch_size = 10000
    target_keys = 100000  # 减少到10万个，每个4KB = 400MB
    
    for batch in range(target_keys // batch_size):
        pipe = rc.pipeline(transaction=False)
        for i in range(batch_size):
            key_idx = batch * batch_size + i
            key = f"str_key_{key_idx}"
            value = generate_random_string(4000).encode()  # 4KB
            pipe.set(key, value)
            total_bytes += len(key) + len(value)
        pipe.execute()
        total_keys += batch_size
        print(f"  批次 {batch+1}/{target_keys // batch_size} 完成 ({total_keys} keys, {total_bytes / 1024 / 1024:.1f} MB)")
    
    # 2. 生成大string keys (约50MB)
    print("\n[2/5] 生成大string keys (10个x5MB)...")
    for i in range(10):
        key = f"large_str_{i}"
        value = generate_random_string(5 * 1024 * 1024).encode()  # 5MB
        rc.set(key, value)
        total_bytes += len(value)
        total_keys += 1
        print(f"  大key {i+1}/10 完成")
    
    # 3. 生成hash类型 (约30MB)
    print("\n[3/5] 生成hash keys (3000个)...")
    for batch in range(30):
        pipe = rc.pipeline(transaction=False)
        for i in range(100):
            key_idx = batch * 100 + i
            key = f"hash_key_{key_idx}"
            fields = {f"field_{j}": generate_random_string(1000).encode() for j in range(10)}
            pipe.hset(key, mapping=fields)
            total_bytes += 10 * 1000
        pipe.execute()
        total_keys += 100
    print(f"  3000 hash keys 完成")
    
    # 4. 生成list类型 (约10MB)
    print("\n[4/5] 生成list keys (1000个)...")
    for batch in range(10):
        pipe = rc.pipeline(transaction=False)
        for i in range(100):
            key_idx = batch * 100 + i
            key = f"list_key_{key_idx}"
            values = [generate_random_string(1000).encode() for _ in range(10)]
            pipe.rpush(key, *values)
            total_bytes += 10 * 1000
        pipe.execute()
        total_keys += 100
    print(f"  1000 list keys 完成")
    
    # 5. 生成set类型 (约10MB)
    print("\n[5/5] 生成set keys (1000个)...")
    for batch in range(10):
        pipe = rc.pipeline(transaction=False)
        for i in range(100):
            key_idx = batch * 100 + i
            key = f"set_key_{key_idx}"
            members = [generate_random_string(1000).encode() for _ in range(10)]
            pipe.sadd(key, *members)
            total_bytes += 10 * 1000
        pipe.execute()
        total_keys += 100
    print(f"  1000 set keys 完成")
    
    elapsed = time.time() - start_time
    
    print(f"\n=== 数据生成完成 ===")
    print(f"总key数: {total_keys}")
    print(f"预估数据量: {total_bytes / 1024 / 1024:.1f} MB")
    print(f"耗时: {elapsed:.1f}s")
    
    # 验证
    print("\n=== 验证数据 ===")
    for host, port in NODES:
        r = redis.Redis(host=host, port=port)
        info = r.info("memory")
        dbsize = r.dbsize()
        print(f"{host}:{port} - keys: {dbsize}, memory: {info.get('used_memory_human', 'N/A')}")

if __name__ == "__main__":
    main()
