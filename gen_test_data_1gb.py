#!/usr/bin/env python3
"""
生成1GB测试数据到Tendis集群
- 总计约220万个Key
- 80% string类型
- 其他类型随机分布（hash, list, set, zset）
- 包含大Key测试
- 按前缀分类便于测试过滤功能
"""

import redis
import random
import string
import time
import sys

# 配置 - 使用单节点连接，自动处理重定向
CLUSTER_NODES = [
    {"host": "10.248.37.11", "port": 8901},
    {"host": "10.248.37.11", "port": 8902},
    {"host": "10.248.37.11", "port": 8903},
]

# Key前缀分类（用于测试前缀过滤）
PREFIXES = {
    "user:": 0.30,      # 30% - 用户数据
    "order:": 0.25,     # 25% - 订单数据
    "product:": 0.20,   # 20% - 产品数据
    "cache:": 0.15,     # 15% - 缓存数据（测试跳过）
    "session:": 0.10,   # 10% - 会话数据（测试跳过）
}

# 数据类型分布
TYPE_DISTRIBUTION = {
    "string": 0.80,     # 80% string
    "hash": 0.08,       # 8% hash
    "list": 0.05,       # 5% list
    "set": 0.04,        # 4% set
    "zset": 0.03,       # 3% zset
}

# 目标数据量
TARGET_KEYS = 2200000
BATCH_SIZE = 500
REPORT_INTERVAL = 50000

def output(msg):
    """带刷新的输出"""
    print(msg, flush=True)

def random_string(length):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def get_prefix():
    """根据分布获取前缀"""
    r = random.random()
    cumulative = 0
    for prefix, prob in PREFIXES.items():
        cumulative += prob
        if r < cumulative:
            return prefix
    return "user:"

def get_data_type():
    """根据分布获取数据类型"""
    r = random.random()
    cumulative = 0
    for dtype, prob in TYPE_DISTRIBUTION.items():
        cumulative += prob
        if r < cumulative:
            return dtype
    return "string"

def get_slot(key):
    """计算 key 的槽位"""
    def crc16(data):
        crc = 0
        for byte in data:
            crc ^= byte << 8
            for _ in range(8):
                if crc & 0x8000:
                    crc = (crc << 1) ^ 0x1021
                else:
                    crc <<= 1
                crc &= 0xFFFF
        return crc
    return crc16(key.encode()) % 16384

def get_node_for_slot(slot, slot_map):
    """根据槽位获取对应节点"""
    for start, end, node in slot_map:
        if start <= slot <= end:
            return node
    return None

def write_key(client, key, dtype, value):
    """写入单个Key"""
    try:
        if dtype == "string":
            client.set(key, value)
        elif dtype == "hash":
            if value:
                client.hmset(key, value)
        elif dtype == "list":
            if value:
                client.rpush(key, *value)
        elif dtype == "set":
            if value:
                client.sadd(key, *value)
        elif dtype == "zset":
            if value:
                items = []
                for member, score in value.items():
                    items.extend([score, member])
                if items:
                    client.zadd(key, *items)
        return True
    except redis.exceptions.ResponseError as e:
        if "MOVED" in str(e):
            return False
        return False
    except Exception as e:
        return False

def generate_value(dtype, is_big_key=False):
    """生成对应类型的值"""
    if is_big_key:
        if dtype == "string":
            return random_string(random.randint(500000, 1000000))
        elif dtype == "hash":
            return {f"field_{i}": random_string(random.randint(100, 300)) for i in range(random.randint(1000, 2000))}
        elif dtype == "list":
            return [random_string(random.randint(50, 100)) for _ in range(random.randint(5000, 10000))]
        elif dtype == "set":
            return [random_string(random.randint(50, 100)) for _ in range(random.randint(5000, 10000))]
        elif dtype == "zset":
            return {random_string(random.randint(50, 100)): random.uniform(0, 10000) for _ in range(random.randint(5000, 10000))}
    else:
        if dtype == "string":
            return random_string(random.randint(50, 500))
        elif dtype == "hash":
            return {f"field_{i}": random_string(random.randint(10, 100)) for i in range(random.randint(5, 20))}
        elif dtype == "list":
            return [random_string(random.randint(10, 100)) for _ in range(random.randint(5, 50))]
        elif dtype == "set":
            return [random_string(random.randint(10, 100)) for _ in range(random.randint(5, 50))]
        elif dtype == "zset":
            return {random_string(random.randint(10, 100)): random.uniform(0, 1000) for _ in range(random.randint(5, 50))}

def main():
    output("=" * 60)
    output("Tendis 测试数据生成器")
    output("=" * 60)
    output(f"目标: {TARGET_KEYS:,} 个Key")
    output(f"类型分布: {TYPE_DISTRIBUTION}")
    output(f"前缀分布: {PREFIXES}")
    output("=" * 60)
    
    output("\n获取集群槽位分布...")
    slot_map = []
    clients = {}
    
    for node in CLUSTER_NODES:
        try:
            r = redis.Redis(host=node["host"], port=node["port"], decode_responses=True)
            r.ping()
            clients[f"{node['host']}:{node['port']}"] = r
            
            slots = r.cluster("slots")
            for slot_info in slots:
                start_slot = slot_info[0]
                end_slot = slot_info[1]
                master_host = slot_info[2][0]
                master_port = slot_info[2][1]
                slot_map.append((start_slot, end_slot, f"{master_host}:{master_port}"))
            output(f"  连接 {node['host']}:{node['port']} 成功")
            break
        except Exception as e:
            output(f"  连接 {node['host']}:{node['port']} 失败: {e}")
    
    if not slot_map:
        output("无法获取集群槽位信息!")
        sys.exit(1)
    
    output(f"槽位分布: {len(slot_map)} 个范围")
    for start, end, node in slot_map:
        output(f"  {start}-{end} -> {node}")
    
    all_nodes = set(node for _, _, node in slot_map)
    for node_addr in all_nodes:
        if node_addr not in clients:
            host, port = node_addr.split(":")
            clients[node_addr] = redis.Redis(host=host, port=int(port), decode_responses=True)
    
    stats = {
        "total": 0,
        "success": 0,
        "failed": 0,
        "big_keys": 0,
        "by_type": {"string": 0, "hash": 0, "list": 0, "set": 0, "zset": 0},
        "by_prefix": {p: 0 for p in PREFIXES.keys()}
    }
    
    start_time = time.time()
    
    output("\n开始生成数据...")
    
    output("\n生成大Key (50个)...")
    for i in range(50):
        prefix = get_prefix()
        dtype = get_data_type()
        key = f"{prefix}bigkey:{dtype}:{i}"
        value = generate_value(dtype, is_big_key=True)
        
        slot = get_slot(key)
        node_addr = get_node_for_slot(slot, slot_map)
        client = clients.get(node_addr)
        
        if client and write_key(client, key, dtype, value):
            stats["success"] += 1
            stats["big_keys"] += 1
            stats["by_type"][dtype] += 1
            stats["by_prefix"][prefix] += 1
        else:
            stats["failed"] += 1
        stats["total"] += 1
        
        if (i + 1) % 10 == 0:
            output(f"  大Key进度: {i+1}/50")
    
    output(f"大Key生成完成: {stats['big_keys']} 个")
    
    remaining = TARGET_KEYS - stats["total"]
    output(f"\n生成普通Key ({remaining:,} 个)...")
    
    for i in range(remaining):
        prefix = get_prefix()
        dtype = get_data_type()
        key = f"{prefix}{dtype}:{i}"
        value = generate_value(dtype, is_big_key=False)
        
        slot = get_slot(key)
        node_addr = get_node_for_slot(slot, slot_map)
        client = clients.get(node_addr)
        
        if client and write_key(client, key, dtype, value):
            stats["success"] += 1
            stats["by_type"][dtype] += 1
            stats["by_prefix"][prefix] += 1
        else:
            stats["failed"] += 1
        stats["total"] += 1
        
        if stats["total"] % REPORT_INTERVAL == 0:
            elapsed = time.time() - start_time
            speed = stats["total"] / elapsed
            eta = (TARGET_KEYS - stats["total"]) / speed if speed > 0 else 0
            output(f"  进度: {stats['total']:,}/{TARGET_KEYS:,} ({100*stats['total']/TARGET_KEYS:.1f}%) | "
                  f"速度: {speed:.0f} keys/s | ETA: {eta:.0f}s | 成功: {stats['success']:,} 失败: {stats['failed']:,}")
    
    elapsed = time.time() - start_time
    
    output("\n" + "=" * 60)
    output("数据生成完成!")
    output("=" * 60)
    output(f"总耗时: {elapsed:.1f} 秒 ({elapsed/60:.1f} 分钟)")
    output(f"平均速度: {stats['total']/elapsed:.0f} keys/s")
    output(f"\n统计:")
    output(f"  总Key数: {stats['total']:,}")
    output(f"  成功: {stats['success']:,}")
    output(f"  失败: {stats['failed']:,}")
    output(f"  大Key: {stats['big_keys']:,}")
    output(f"\n类型分布:")
    for dtype, count in stats["by_type"].items():
        pct = 100 * count / stats["success"] if stats["success"] > 0 else 0
        output(f"  {dtype}: {count:,} ({pct:.1f}%)")
    output(f"\n前缀分布:")
    for prefix, count in stats["by_prefix"].items():
        pct = 100 * count / stats["success"] if stats["success"] > 0 else 0
        output(f"  {prefix}: {count:,} ({pct:.1f}%)")
    
    output("\n验证数据...")
    total_keys = 0
    for node in CLUSTER_NODES:
        try:
            r = redis.Redis(host=node["host"], port=node["port"])
            dbsize = r.dbsize()
            total_keys += dbsize
            output(f"  {node['host']}:{node['port']}: {dbsize:,} keys")
        except Exception as e:
            output(f"  {node['host']}:{node['port']}: 错误 - {e}")
    
    output(f"\n集群总Key数: {total_keys:,}")
    output("\n" + "=" * 60)
    output("数据生成完成!")
    output("=" * 60)

if __name__ == "__main__":
    main()
