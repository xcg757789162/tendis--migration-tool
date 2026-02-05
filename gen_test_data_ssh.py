#!/usr/bin/env python3
"""
通过 SSH 在 Docker 服务器上生成 1GB 测试数据到 Tendis 集群
使用 redis-cli --pipe 批量导入
"""

import subprocess
import random
import string
import time
import sys
import os

# SSH 配置
SSH_HOST = "192.168.1.23"
SSH_USER = "xiechenguo"
SSH_PASS = "!QAZxsw2"

# Docker 配置
DOCKER = "/usr/local/bin/docker"
CONTAINER = "tendis-src-7001"
PORT = 7001

# 数据配置
TARGET_SIZE_MB = 1024  # 1GB
KEY_PREFIX = "testkey:"

# 大 Key 配置
BIG_KEY_COUNT = 50
BIG_KEY_MIN_MB = 1
BIG_KEY_MAX_MB = 10

# 数据分布
STRING_RATIO = 0.80
HASH_RATIO = 0.05
LIST_RATIO = 0.05
SET_RATIO = 0.05
ZSET_RATIO = 0.05

def gen_random_string(length):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

def run_ssh_cmd(cmd, timeout=300):
    """通过 SSH 执行命令"""
    full_cmd = f"""expect -c 'set timeout {timeout}; spawn ssh -o StrictHostKeyChecking=no {SSH_USER}@{SSH_HOST} "{cmd}"; expect "*assword*" {{send "{SSH_PASS}\\r"}}; expect eof'"""
    result = subprocess.run(full_cmd, shell=True, capture_output=True, text=True)
    return result.stdout

def run_docker_redis_cmd(redis_cmd, timeout=60):
    """在 Docker 容器中执行 redis-cli 命令"""
    cmd = f"{DOCKER} exec {CONTAINER} redis-cli -p {PORT} -c {redis_cmd}"
    return run_ssh_cmd(cmd, timeout)

def send_redis_protocol_data(data, timeout=300):
    """通过 redis-cli --pipe 发送 RESP 协议数据"""
    # 将数据写入临时文件
    tmp_file = "/tmp/redis_data.txt"
    
    # 先将数据上传到服务器
    with open("/tmp/local_redis_data.txt", "w") as f:
        f.write(data)
    
    # 上传文件
    scp_cmd = f"""expect -c 'set timeout 60; spawn scp /tmp/local_redis_data.txt {SSH_USER}@{SSH_HOST}:{tmp_file}; expect "*assword*" {{send "{SSH_PASS}\\r"}}; expect eof'"""
    subprocess.run(scp_cmd, shell=True)
    
    # 执行 pipe 导入
    cmd = f"cat {tmp_file} | {DOCKER} exec -i {CONTAINER} redis-cli -p {PORT} -c --pipe"
    return run_ssh_cmd(cmd, timeout)

def format_resp(*args):
    """格式化为 RESP 协议"""
    result = f"*{len(args)}\r\n"
    for arg in args:
        arg_str = str(arg)
        result += f"${len(arg_str)}\r\n{arg_str}\r\n"
    return result

def get_cluster_stats():
    """获取集群状态"""
    print("\n📊 集群状态:")
    for port in [7001, 7002, 7003]:
        result = run_ssh_cmd(f"{DOCKER} exec tendis-src-{port} redis-cli -p {port} dbsize")
        # 解析结果
        lines = result.split('\n')
        for line in lines:
            if line.strip().isdigit():
                print(f"   节点 {port}: {line.strip()} keys")
                break

def main():
    print("=" * 60)
    print("🚀 Tendis 集群测试数据生成器")
    print("=" * 60)
    print(f"目标数据量: {TARGET_SIZE_MB} MB (1 GB)")
    print(f"数据分布: String 80%, Hash 5%, List 5%, Set 5%, ZSet 5%")
    print(f"大 Key: {BIG_KEY_COUNT} 个 ({BIG_KEY_MIN_MB}-{BIG_KEY_MAX_MB} MB)")
    
    # 获取写入前状态
    print("\n📊 写入前状态:")
    get_cluster_stats()
    
    written_bytes = 0
    target_bytes = TARGET_SIZE_MB * 1024 * 1024
    
    # 1. 写入大 Key
    print(f"\n📦 开始写入 {BIG_KEY_COUNT} 个大 Key...")
    big_key_types = ['string', 'hash', 'list', 'set', 'zset']
    
    for i in range(BIG_KEY_COUNT):
        key_type = random.choice(big_key_types)
        key = f"{KEY_PREFIX}bigkey:{key_type}:{i}"
        size_mb = random.randint(BIG_KEY_MIN_MB, BIG_KEY_MAX_MB)
        size_bytes = size_mb * 1024 * 1024
        
        resp_data = ""
        
        if key_type == 'string':
            value = gen_random_string(size_bytes)
            resp_data = format_resp("SET", key, value)
        elif key_type == 'hash':
            field_count = 500
            field_size = size_bytes // field_count
            for j in range(field_count):
                resp_data += format_resp("HSET", key, f"field_{j}", gen_random_string(field_size))
        elif key_type == 'list':
            elem_count = 2000
            elem_size = size_bytes // elem_count
            for j in range(0, elem_count, 100):
                batch = [gen_random_string(elem_size) for _ in range(100)]
                resp_data += format_resp("RPUSH", key, *batch)
        elif key_type == 'set':
            member_count = 2000
            member_size = size_bytes // member_count
            for j in range(0, member_count, 100):
                batch = [gen_random_string(member_size) for _ in range(100)]
                resp_data += format_resp("SADD", key, *batch)
        elif key_type == 'zset':
            member_count = 1000
            member_size = size_bytes // member_count
            for j in range(member_count):
                member = gen_random_string(member_size)
                score = random.randint(0, 10000)
                resp_data += format_resp("ZADD", key, str(score), member)
        
        # 发送数据
        send_redis_protocol_data(resp_data)
        written_bytes += size_bytes
        print(f"  [{i+1}/{BIG_KEY_COUNT}] {key} ({key_type}) - {size_mb} MB")
    
    big_key_total = written_bytes
    print(f"✅ 大 Key 写入完成，总计: {big_key_total/1024/1024:.0f} MB")
    
    # 2. 写入普通 Key
    remaining_bytes = target_bytes - written_bytes
    print(f"\n📝 开始写入普通 Key，目标: {remaining_bytes/1024/1024:.0f} MB")
    
    key_counts = {'string': 0, 'hash': 0, 'list': 0, 'set': 0, 'zset': 0}
    batch_data = ""
    batch_bytes = 0
    batch_limit = 10 * 1024 * 1024  # 每批 10MB
    
    start_time = time.time()
    last_report = start_time
    normal_written = 0
    
    while normal_written < remaining_bytes:
        # 决定类型
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
        
        key = f"{KEY_PREFIX}{key_type}:{key_counts[key_type]}"
        key_counts[key_type] += 1
        
        if key_type == 'string':
            size = random.randint(100, 10240)
            value = gen_random_string(size)
            batch_data += format_resp("SET", key, value)
            batch_bytes += size
        elif key_type == 'hash':
            field_count = random.randint(5, 30)
            field_size = random.randint(50, 500)
            for j in range(field_count):
                batch_data += format_resp("HSET", key, f"f{j}", gen_random_string(field_size))
            batch_bytes += field_count * field_size
        elif key_type == 'list':
            elem_count = random.randint(10, 50)
            elem_size = random.randint(50, 500)
            elems = [gen_random_string(elem_size) for _ in range(elem_count)]
            batch_data += format_resp("RPUSH", key, *elems)
            batch_bytes += elem_count * elem_size
        elif key_type == 'set':
            member_count = random.randint(10, 50)
            member_size = random.randint(50, 500)
            members = [gen_random_string(member_size) for _ in range(member_count)]
            batch_data += format_resp("SADD", key, *members)
            batch_bytes += member_count * member_size
        elif key_type == 'zset':
            member_count = random.randint(10, 50)
            member_size = random.randint(50, 500)
            for _ in range(member_count):
                batch_data += format_resp("ZADD", key, str(random.randint(0,10000)), gen_random_string(member_size))
            batch_bytes += member_count * member_size
        
        # 批量发送
        if batch_bytes >= batch_limit:
            send_redis_protocol_data(batch_data)
            normal_written += batch_bytes
            
            # 进度报告
            now = time.time()
            if now - last_report >= 5:
                elapsed = now - start_time
                speed = normal_written / elapsed / 1024 / 1024
                progress = (big_key_total + normal_written) / target_bytes * 100
                total_keys = sum(key_counts.values())
                print(f"  进度: {progress:.1f}% | 已写: {(big_key_total + normal_written)/1024/1024:.0f} MB | "
                      f"速度: {speed:.2f} MB/s | Keys: {total_keys}")
                last_report = now
            
            batch_data = ""
            batch_bytes = 0
    
    # 发送剩余数据
    if batch_data:
        send_redis_protocol_data(batch_data)
        normal_written += batch_bytes
    
    # 完成统计
    elapsed = time.time() - start_time
    total_written = big_key_total + normal_written
    
    print(f"\n✅ 普通 Key 写入完成")
    print(f"   耗时: {elapsed:.1f}s")
    print(f"   速度: {normal_written/elapsed/1024/1024:.2f} MB/s")
    print(f"   Key 分布:")
    for k, v in key_counts.items():
        print(f"     - {k}: {v}")
    
    # 获取最终状态
    print("\n📊 写入后状态:")
    get_cluster_stats()
    
    print("\n" + "=" * 60)
    print(f"✅ 数据生成完成! 总计: {total_written/1024/1024:.0f} MB")
    print("=" * 60)

if __name__ == "__main__":
    main()
