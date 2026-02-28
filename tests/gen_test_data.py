#!/usr/bin/env python3
"""
tendis-migrate 统一测试数据生成器
================================
合并了多个数据生成脚本的功能，通过命令行参数控制：

用法:
  # 生成 10K 轻量数据（回归测试用）
  python tests/gen_test_data.py --host 192.168.0.142 --ports 7001,7002 --size 10k

  # 生成 1GB 数据（功能测试用）
  python tests/gen_test_data.py --host 192.168.0.142 --ports 7001,7002 --size 1g

  # 生成 3GB 数据（压力测试用，含大 Key + 多前缀）
  python tests/gen_test_data.py --host 192.168.0.142 --ports 7001,7002 --size 3g

  # 自定义前缀和 Key 数量
  python tests/gen_test_data.py --host 10.31.36.5 --ports 8901,8902,8903 --keys 220000 --prefixes user:,order:,cache:

  # 清空后再写
  python tests/gen_test_data.py --host 192.168.0.142 --ports 7001,7002 --size 1g --flush

数据特征:
  - 80% String, 5% Hash, 5% List, 5% Set, 5% ZSet
  - 默认包含大 Key（5% 概率，1MB~5MB）
  - 按前缀分类，便于测试过滤功能
  - Pipeline 批量写入，高吞吐
"""

import redis
from redis.cluster import RedisCluster, ClusterNode
import random
import string
import time
import sys
import argparse
import traceback

# 预生成随机块（加速）
_RAND_BLOCK = ''.join(random.choices(string.ascii_letters + string.digits, k=8192))


def rand_bytes(size):
    if size <= 0:
        return b''
    if size <= 8192:
        start = random.randint(0, 8192 - size)
        return _RAND_BLOCK[start:start + size].encode()
    repeats = (size // 8192) + 1
    return (_RAND_BLOCK * repeats)[:size].encode()


def rand_str(size):
    return rand_bytes(size).decode('ascii', errors='replace')


# ============================================================
# 默认前缀配置（模拟真实业务）
# ============================================================
DEFAULT_PREFIXES = {
    "user:":    0.30,
    "order:":   0.25,
    "product:": 0.20,
    "cache:":   0.15,
    "session:": 0.10,
}

BUSINESS_PREFIXES = [
    "user:profile:", "user:session:", "user:token:",
    "order:detail:", "order:status:", "order:item:",
    "product:info:", "product:stock:", "product:price:",
    "cache:page:", "cache:api:", "cache:query:",
    "msg:inbox:", "msg:outbox:", "msg:unread:",
    "stat:daily:", "stat:hourly:", "stat:total:",
    "config:app:", "config:sys:", "config:feature:",
    "log:access:", "log:error:", "log:audit:",
    "task:queue:", "task:result:", "task:retry:",
    "geo:location:", "geo:fence:", "geo:nearby:",
]


def decide_type():
    r = random.random()
    if r < 0.80:
        return "string"
    elif r < 0.85:
        return "hash"
    elif r < 0.90:
        return "list"
    elif r < 0.95:
        return "set"
    else:
        return "zset"


def get_prefix(prefixes):
    if isinstance(prefixes, dict):
        r = random.random()
        cumulative = 0
        for prefix, prob in prefixes.items():
            cumulative += prob
            if r < cumulative:
                return prefix
        return list(prefixes.keys())[0]
    elif isinstance(prefixes, list):
        return random.choice(prefixes)
    return "testkey:"


def connect_cluster(host, ports):
    nodes = [ClusterNode(host, p) for p in ports]
    for attempt in range(10):
        try:
            rc = RedisCluster(
                startup_nodes=nodes,
                decode_responses=False,
                skip_full_coverage_check=True
            )
            rc.ping()
            return rc
        except Exception as e:
            print(f"  连接尝试 {attempt + 1} 失败: {e}")
            if attempt < 9:
                time.sleep(3)
    raise Exception("连接集群失败（10 次重试后放弃）")


def get_cluster_stats(host, ports):
    total_keys = 0
    total_memory = 0
    for port in ports:
        try:
            r = redis.Redis(host=host, port=port, decode_responses=True)
            info = r.info("memory")
            dbsize = r.dbsize()
            total_keys += dbsize
            total_memory += info.get("used_memory", 0)
            print(f"  {host}:{port}: {dbsize:,} keys, {total_memory / 1024 / 1024:.1f} MB")
        except Exception as e:
            print(f"  {host}:{port}: 获取信息失败 - {e}")
    return total_keys, total_memory


# ============================================================
# 写入函数
# ============================================================
def write_big_string(client, key):
    size = random.randint(1 * 1024 * 1024, 5 * 1024 * 1024)
    chunk_size = 512 * 1024
    client.set(key, rand_bytes(min(size, chunk_size)))
    written = min(size, chunk_size)
    while written < size:
        chunk = min(size - written, chunk_size)
        client.append(key, rand_bytes(chunk))
        written += chunk
    return written


def write_big_hash(client, key):
    total_fields = random.randint(1000, 10000)
    written = 0
    batch = 200
    for start in range(0, total_fields, batch):
        end = min(start + batch, total_fields)
        mapping = {}
        for fi in range(start, end):
            val_size = random.randint(100, 500)
            mapping[f"field_{fi}".encode()] = rand_bytes(val_size)
            written += val_size + len(f"field_{fi}")
        client.hset(key, mapping=mapping)
    return written


def write_big_list(client, key):
    total_items = random.randint(5000, 50000)
    written = 0
    batch = 500
    for start in range(0, total_items, batch):
        end = min(start + batch, total_items)
        elements = [rand_bytes(random.randint(50, 200)) for _ in range(start, end)]
        written += sum(len(e) for e in elements)
        client.rpush(key, *elements)
    return written


def write_big_set(client, key):
    total_items = random.randint(5000, 30000)
    written = 0
    batch = 500
    for start in range(0, total_items, batch):
        end = min(start + batch, total_items)
        members = [f"{si}:".encode() + rand_bytes(random.randint(50, 200)) for si in range(start, end)]
        written += sum(len(m) for m in members)
        client.sadd(key, *members)
    return written


def write_big_zset(client, key):
    total_items = random.randint(5000, 30000)
    written = 0
    batch = 500
    for start in range(0, total_items, batch):
        end = min(start + batch, total_items)
        mapping = {}
        for zi in range(start, end):
            member = f"{zi}:".encode() + rand_bytes(random.randint(50, 200))
            mapping[member] = random.random() * 10000
            written += len(member)
        client.zadd(key, mapping)
    return written


BIG_WRITERS = {
    "string": write_big_string,
    "hash":   write_big_hash,
    "list":   write_big_list,
    "set":    write_big_set,
    "zset":   write_big_zset,
}


# ============================================================
# 按数据量模式生成
# ============================================================
def generate_by_size(rc, target_bytes, prefixes, report_interval=5):
    """按目标数据量生成，使用 Pipeline 批量写入"""
    total_written = 0
    key_count = 0
    big_key_count = 0
    errors = 0
    type_counts = {"string": 0, "hash": 0, "list": 0, "set": 0, "zset": 0}
    start_time = time.time()
    last_report = start_time

    pipe = rc.pipeline(transaction=False)
    pipe_count = 0
    pipe_size = 0
    pipeline_batch = 200

    use_business = isinstance(prefixes, list) and len(prefixes) > 10

    while total_written < target_bytes:
        try:
            dtype = decide_type()
            prefix = prefixes[key_count % len(prefixes)] if use_business else get_prefix(prefixes)
            key = f"{prefix}{dtype[0]}_{key_count}".encode()

            # 5% 概率是大 Key
            is_big = random.random() > 0.95 and target_bytes > 100 * 1024 * 1024  # 仅 >100MB 时才生成大 Key

            if is_big:
                if pipe_count > 0:
                    pipe.execute()
                    pipe_count = 0
                    pipe_size = 0
                written = BIG_WRITERS[dtype](rc, key)
                total_written += written
                big_key_count += 1
                type_counts[dtype] += 1
                key_count += 1
            else:
                val_size = random.randint(100, 10240)
                if dtype == "string":
                    pipe.set(key, rand_bytes(val_size))
                elif dtype == "hash":
                    fields = max(1, val_size // 200)
                    fsize = max(10, val_size // fields)
                    mapping = {f"f{j}".encode(): rand_bytes(fsize) for j in range(fields)}
                    pipe.hset(key, mapping=mapping)
                elif dtype == "list":
                    items = max(1, val_size // 200)
                    isize = max(10, val_size // items)
                    pipe.rpush(key, *[rand_bytes(isize) for _ in range(items)])
                elif dtype == "set":
                    items = max(1, val_size // 200)
                    isize = max(10, val_size // items)
                    pipe.sadd(key, *[f"{si}:".encode() + rand_bytes(isize) for si in range(items)])
                elif dtype == "zset":
                    items = max(1, val_size // 200)
                    isize = max(10, val_size // items)
                    mapping = {f"{zi}:".encode() + rand_bytes(isize): random.random() * 10000 for zi in range(items)}
                    pipe.zadd(key, mapping)

                pipe_size += val_size
                pipe_count += 1
                total_written += val_size
                type_counts[dtype] += 1
                key_count += 1

                if pipe_count >= pipeline_batch or pipe_size > 10 * 1024 * 1024:
                    pipe.execute()
                    pipe_count = 0
                    pipe_size = 0

        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            errors += 1
            print(f"\n  [ERROR] 连接丢失: {e}，等待重连...")
            time.sleep(5)
            try:
                pipe = rc.pipeline(transaction=False)
                pipe_count = 0
                pipe_size = 0
            except Exception:
                pass
            continue
        except Exception as e:
            errors += 1
            if errors > 50:
                print(f"  错误过多({errors})，停止生成")
                break
            continue

        now = time.time()
        if now - last_report >= report_interval:
            elapsed = now - start_time
            speed = total_written / elapsed / 1024 / 1024
            pct = total_written / target_bytes * 100
            eta = (target_bytes - total_written) / (total_written / elapsed) if total_written > 0 else 0
            print(f"  [{elapsed:.0f}s] {pct:.1f}% | {total_written / 1024 / 1024:.0f}MB | "
                  f"keys: {key_count:,} (big: {big_key_count}) | {speed:.1f}MB/s | ETA: {eta:.0f}s")
            last_report = now

    if pipe_count > 0:
        try:
            pipe.execute()
        except Exception:
            pass

    return key_count, total_written, big_key_count, type_counts, errors


# ============================================================
# 按 Key 数量模式生成
# ============================================================
def generate_by_count(rc, target_keys, prefixes, include_big=True):
    """按目标 Key 数量生成"""
    type_counts = {"string": 0, "hash": 0, "list": 0, "set": 0, "zset": 0}
    big_key_count = 0
    total_written = 0
    errors = 0
    start_time = time.time()

    # 先写大 Key（如果启用）
    big_count = 50 if include_big and target_keys > 1000 else 0
    for i in range(big_count):
        dtype = decide_type()
        prefix = get_prefix(prefixes)
        key = f"{prefix}bigkey:{dtype}:{i}".encode()
        try:
            written = BIG_WRITERS[dtype](rc, key)
            total_written += written
            type_counts[dtype] += 1
            big_key_count += 1
            if (i + 1) % 10 == 0:
                print(f"  大 Key: {i + 1}/{big_count}")
        except Exception as e:
            errors += 1

    # 普通 Key
    remaining = target_keys - big_count
    pipe = rc.pipeline(transaction=False)
    pipe_count = 0

    for i in range(remaining):
        dtype = decide_type()
        prefix = get_prefix(prefixes)
        key = f"{prefix}{dtype}:{i}".encode()

        try:
            val_size = random.randint(100, 2048)
            if dtype == "string":
                pipe.set(key, rand_bytes(val_size))
            elif dtype == "hash":
                fields = random.randint(5, 20)
                fsize = random.randint(10, 100)
                mapping = {f"f{j}".encode(): rand_bytes(fsize) for j in range(fields)}
                pipe.hset(key, mapping=mapping)
            elif dtype == "list":
                elems = [rand_bytes(random.randint(10, 100)) for _ in range(random.randint(5, 50))]
                pipe.rpush(key, *elems)
            elif dtype == "set":
                members = [rand_bytes(random.randint(10, 100)) for _ in range(random.randint(5, 50))]
                pipe.sadd(key, *members)
            elif dtype == "zset":
                mapping = {rand_bytes(random.randint(10, 100)): random.random() * 1000 for _ in range(random.randint(5, 50))}
                pipe.zadd(key, mapping)

            pipe_count += 1
            total_written += val_size
            type_counts[dtype] += 1

            if pipe_count >= 500:
                pipe.execute()
                pipe_count = 0

        except Exception as e:
            errors += 1
            if errors > 50:
                break

        if (i + 1) % 50000 == 0:
            elapsed = time.time() - start_time
            speed = (i + 1 + big_count) / elapsed
            print(f"  进度: {i + 1 + big_count:,}/{target_keys:,} ({(i + 1 + big_count) / target_keys * 100:.1f}%) | {speed:.0f} keys/s")

    if pipe_count > 0:
        try:
            pipe.execute()
        except Exception:
            pass

    return target_keys, total_written, big_key_count, type_counts, errors


def parse_size(size_str):
    """解析大小字符串: 10k, 100k, 1m, 1g, 3g"""
    s = size_str.strip().lower()
    if s.endswith('k'):
        return int(float(s[:-1]) * 1000), 'keys'
    elif s.endswith('m'):
        return int(float(s[:-1]) * 1024 * 1024), 'bytes'
    elif s.endswith('g'):
        return int(float(s[:-1]) * 1024 * 1024 * 1024), 'bytes'
    else:
        return int(s), 'keys'


def main():
    parser = argparse.ArgumentParser(description='tendis-migrate 统一测试数据生成器')
    parser.add_argument('--host', default='192.168.0.142', help='Redis/Tendis 主机地址')
    parser.add_argument('--ports', default='7001,7002', help='端口列表（逗号分隔）')
    parser.add_argument('--size', default='10k', help='数据规模: 10k/100k/1m(keys) 或 500m/1g/3g/10g(bytes)')
    parser.add_argument('--keys', type=int, help='直接指定 Key 数量（覆盖 --size）')
    parser.add_argument('--prefixes', help='自定义前缀（逗号分隔），如 user:,order:,cache:')
    parser.add_argument('--business-prefixes', action='store_true', help='使用 30 种业务前缀（模拟真实场景）')
    parser.add_argument('--flush', action='store_true', help='写入前先清空所有数据')
    parser.add_argument('--no-big-keys', action='store_true', help='不生成大 Key')

    args = parser.parse_args()

    ports = [int(p.strip()) for p in args.ports.split(',')]

    # 确定前缀
    if args.prefixes:
        prefix_list = [p.strip() for p in args.prefixes.split(',')]
        prefixes = {p: 1.0 / len(prefix_list) for p in prefix_list}
    elif args.business_prefixes:
        prefixes = BUSINESS_PREFIXES  # list 形式
    else:
        prefixes = DEFAULT_PREFIXES

    print("=" * 60)
    print("tendis-migrate 测试数据生成器")
    print("=" * 60)
    print(f"目标集群: {args.host}:{args.ports}")

    # 连接
    rc = connect_cluster(args.host, ports)
    print("连接成功!")

    # 清空
    if args.flush:
        print("\n清空现有数据...")
        for port in ports:
            try:
                r = redis.Redis(host=args.host, port=port)
                r.flushall()
            except Exception:
                pass
        time.sleep(1)

    # 写入前状态
    print("\n写入前集群状态:")
    get_cluster_stats(args.host, ports)

    # 解析大小
    start_time = time.time()
    if args.keys:
        target = args.keys
        mode = 'keys'
    else:
        target, mode = parse_size(args.size)

    if mode == 'keys':
        print(f"\n目标: {target:,} 个 Key")
        key_count, total_written, big_count, type_counts, errors = generate_by_count(
            rc, target, prefixes, include_big=not args.no_big_keys
        )
    else:
        print(f"\n目标: {target / 1024 / 1024 / 1024:.2f} GB")
        key_count, total_written, big_count, type_counts, errors = generate_by_size(
            rc, target, prefixes
        )

    elapsed = time.time() - start_time

    # 报告
    print(f"\n{'=' * 60}")
    print(f"生成完成!")
    print(f"{'=' * 60}")
    print(f"耗时: {elapsed:.1f}s ({elapsed / 60:.1f} min)")
    print(f"总 Key 数: {key_count:,}")
    print(f"数据量: {total_written / 1024 / 1024:.1f} MB")
    print(f"大 Key: {big_count:,}")
    print(f"错误: {errors}")
    print(f"\n类型分布:")
    for t, c in type_counts.items():
        pct = c / key_count * 100 if key_count > 0 else 0
        print(f"  {t:8s}: {c:,} ({pct:.1f}%)")

    # 写入后状态
    print(f"\n写入后集群状态:")
    get_cluster_stats(args.host, ports)


if __name__ == "__main__":
    main()
