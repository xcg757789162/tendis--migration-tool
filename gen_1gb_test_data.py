#!/usr/bin/env python3
"""
生成 1GB 测试数据到 Tendis 源集群
使用端口映射模式连接（localhost:端口）
- 80% string 类型
- 20% 其他类型（hash, list, set, zset）随机
- 包含大 key（大 string、大 hash、大 list 等）
"""

import redis
import random
import string
import time
import sys

# 集群节点配置 - 使用 localhost 和端口映射
NODES = [
    {"host": "127.0.0.1", "port": 7001, "slots": range(0, 5461)},
    {"host": "127.0.0.1", "port": 7002, "slots": range(5461, 10923)},
    {"host": "127.0.0.1", "port": 7003, "slots": range(10923, 16384)},
]

# 目标数据量 1GB = 1073741824 bytes
TARGET_SIZE = 1 * 1024 * 1024 * 1024  # 1GB

# 数据类型比例
STRING_RATIO = 0.80
OTHER_RATIO = 0.20

# 大 key 配置
BIG_KEY_RATIO = 0.01  # 1% 的 key 是大 key
BIG_STRING_SIZE = 1 * 1024 * 1024  # 1MB 大 string
BIG_HASH_FIELDS = 10000  # 大 hash 10000 个字段
BIG_LIST_ELEMENTS = 50000  # 大 list 50000 个元素
BIG_SET_MEMBERS = 30000  # 大 set 30000 个成员
BIG_ZSET_MEMBERS = 30000  # 大 zset 30000 个成员

# CRC16 XMODEM 表
XMODEM_CRC16_TABLE = [
    0x0000, 0x1021, 0x2042, 0x3063, 0x4084, 0x50a5, 0x60c6, 0x70e7,
    0x8108, 0x9129, 0xa14a, 0xb16b, 0xc18c, 0xd1ad, 0xe1ce, 0xf1ef,
    0x1231, 0x0210, 0x3273, 0x2252, 0x52b5, 0x4294, 0x72f7, 0x62d6,
    0x9339, 0x8318, 0xb37b, 0xa35a, 0xd3bd, 0xc39c, 0xf3ff, 0xe3de,
    0x2462, 0x3443, 0x0420, 0x1401, 0x64e6, 0x74c7, 0x44a4, 0x5485,
    0xa56a, 0xb54b, 0x8528, 0x9509, 0xe5ee, 0xf5cf, 0xc5ac, 0xd58d,
    0x3653, 0x2672, 0x1611, 0x0630, 0x76d7, 0x66f6, 0x5695, 0x46b4,
    0xb75b, 0xa77a, 0x9719, 0x8738, 0xf7df, 0xe7fe, 0xd79d, 0xc7bc,
    0x48c4, 0x58e5, 0x6886, 0x78a7, 0x0840, 0x1861, 0x2802, 0x3823,
    0xc9cc, 0xd9ed, 0xe98e, 0xf9af, 0x8948, 0x9969, 0xa90a, 0xb92b,
    0x5af5, 0x4ad4, 0x7ab7, 0x6a96, 0x1a71, 0x0a50, 0x3a33, 0x2a12,
    0xdbfd, 0xcbdc, 0xfbbf, 0xeb9e, 0x9b79, 0x8b58, 0xbb3b, 0xab1a,
    0x6ca6, 0x7c87, 0x4ce4, 0x5cc5, 0x2c22, 0x3c03, 0x0c60, 0x1c41,
    0xedae, 0xfd8f, 0xcdec, 0xddcd, 0xad2a, 0xbd0b, 0x8d68, 0x9d49,
    0x7e97, 0x6eb6, 0x5ed5, 0x4ef4, 0x3e13, 0x2e32, 0x1e51, 0x0e70,
    0xff9f, 0xefbe, 0xdfdd, 0xcffc, 0xbf1b, 0xaf3a, 0x9f59, 0x8f78,
    0x9188, 0x81a9, 0xb1ca, 0xa1eb, 0xd10c, 0xc12d, 0xf14e, 0xe16f,
    0x1080, 0x00a1, 0x30c2, 0x20e3, 0x5004, 0x4025, 0x7046, 0x6067,
    0x83b9, 0x9398, 0xa3fb, 0xb3da, 0xc33d, 0xd31c, 0xe37f, 0xf35e,
    0x02b1, 0x1290, 0x22f3, 0x32d2, 0x4235, 0x5214, 0x6277, 0x7256,
    0xb5ea, 0xa5cb, 0x95a8, 0x8589, 0xf56e, 0xe54f, 0xd52c, 0xc50d,
    0x34e2, 0x24c3, 0x14a0, 0x0481, 0x7466, 0x6447, 0x5424, 0x4405,
    0xa7db, 0xb7fa, 0x8799, 0x97b8, 0xe75f, 0xf77e, 0xc71d, 0xd73c,
    0x26d3, 0x36f2, 0x0691, 0x16b0, 0x6657, 0x7676, 0x4615, 0x5634,
    0xd94c, 0xc96d, 0xf90e, 0xe92f, 0x99c8, 0x89e9, 0xb98a, 0xa9ab,
    0x5844, 0x4865, 0x7806, 0x6827, 0x18c0, 0x08e1, 0x3882, 0x28a3,
    0xcb7d, 0xdb5c, 0xeb3f, 0xfb1e, 0x8bf9, 0x9bd8, 0xabbb, 0xbb9a,
    0x4a75, 0x5a54, 0x6a37, 0x7a16, 0x0af1, 0x1ad0, 0x2ab3, 0x3a92,
    0xfd2e, 0xed0f, 0xdd6c, 0xcd4d, 0xbdaa, 0xad8b, 0x9de8, 0x8dc9,
    0x7c26, 0x6c07, 0x5c64, 0x4c45, 0x3ca2, 0x2c83, 0x1ce0, 0x0cc1,
    0xef1f, 0xff3e, 0xcf5d, 0xdf7c, 0xaf9b, 0xbfba, 0x8fd9, 0x9ff8,
    0x6e17, 0x7e36, 0x4e55, 0x5e74, 0x2e93, 0x3eb2, 0x0ed1, 0x1ef0,
]

def crc16(data):
    """计算 CRC16 (XMODEM)"""
    crc = 0
    if isinstance(data, str):
        data = data.encode('utf-8')
    for byte in data:
        crc = ((crc << 8) ^ XMODEM_CRC16_TABLE[((crc >> 8) ^ byte) & 0xFF]) & 0xFFFF
    return crc

def key_slot(key):
    """计算 key 所属的 slot"""
    # 检查是否有 hash tag
    start = key.find('{')
    if start != -1:
        end = key.find('}', start + 1)
        if end != -1 and end != start + 1:
            key = key[start + 1:end]
    return crc16(key) % 16384

def random_string(length):
    """生成随机字符串"""
    return ''.join(random.choices(string.ascii_letters + string.digits, k=length))

class DataGenerator:
    def __init__(self):
        self.total_size = 0
        self.key_count = 0
        self.big_key_count = 0
        self.type_counts = {
            'string': 0,
            'hash': 0,
            'list': 0,
            'set': 0,
            'zset': 0
        }
        
        # 连接各个节点
        print("连接到 Tendis 源集群各节点...")
        self.clients = {}
        for node in NODES:
            try:
                client = redis.Redis(
                    host=node['host'],
                    port=node['port'],
                    decode_responses=True,
                    socket_timeout=30,
                    socket_connect_timeout=10
                )
                client.ping()
                self.clients[node['port']] = {
                    'client': client,
                    'slots': node['slots']
                }
                print(f"  节点 {node['host']}:{node['port']} 连接成功")
            except Exception as e:
                print(f"  节点 {node['host']}:{node['port']} 连接失败: {e}")
                sys.exit(1)
        
        print("所有节点连接成功!")
        sys.stdout.flush()
    
    def get_client_for_key(self, key):
        """根据 key 获取对应的客户端"""
        slot = key_slot(key)
        for port, info in self.clients.items():
            if slot in info['slots']:
                return info['client']
        # 默认返回第一个
        return list(self.clients.values())[0]['client']
    
    def generate_string(self, key, is_big=False):
        """生成 string 类型数据"""
        if is_big:
            # 大 string: 1MB
            value = random_string(BIG_STRING_SIZE)
            self.big_key_count += 1
        else:
            # 普通 string: 100-1000 bytes
            value = random_string(random.randint(100, 1000))
        
        client = self.get_client_for_key(key)
        client.set(key, value)
        size = len(key) + len(value)
        self.type_counts['string'] += 1
        return size
    
    def generate_hash(self, key, is_big=False):
        """生成 hash 类型数据"""
        if is_big:
            # 大 hash: 10000 个字段
            fields = {f"field_{i}": random_string(50) for i in range(BIG_HASH_FIELDS)}
            self.big_key_count += 1
        else:
            # 普通 hash: 5-50 个字段
            field_count = random.randint(5, 50)
            fields = {f"field_{i}": random_string(random.randint(20, 100)) for i in range(field_count)}
        
        client = self.get_client_for_key(key)
        client.hset(key, mapping=fields)
        size = len(key) + sum(len(k) + len(v) for k, v in fields.items())
        self.type_counts['hash'] += 1
        return size
    
    def generate_list(self, key, is_big=False):
        """生成 list 类型数据"""
        client = self.get_client_for_key(key)
        
        if is_big:
            # 大 list: 50000 个元素
            elements = [random_string(20) for _ in range(BIG_LIST_ELEMENTS)]
            self.big_key_count += 1
        else:
            # 普通 list: 10-100 个元素
            element_count = random.randint(10, 100)
            elements = [random_string(random.randint(10, 50)) for _ in range(element_count)]
        
        # 分批 push 避免命令太大
        batch = 1000
        for i in range(0, len(elements), batch):
            client.rpush(key, *elements[i:i+batch])
        
        size = len(key) + sum(len(e) for e in elements)
        self.type_counts['list'] += 1
        return size
    
    def generate_set(self, key, is_big=False):
        """生成 set 类型数据"""
        client = self.get_client_for_key(key)
        
        if is_big:
            # 大 set: 30000 个成员
            members = [random_string(30) for _ in range(BIG_SET_MEMBERS)]
            self.big_key_count += 1
        else:
            # 普通 set: 10-100 个成员
            member_count = random.randint(10, 100)
            members = [random_string(random.randint(10, 30)) for _ in range(member_count)]
        
        # 分批 add
        batch = 1000
        for i in range(0, len(members), batch):
            client.sadd(key, *members[i:i+batch])
        
        size = len(key) + sum(len(m) for m in members)
        self.type_counts['set'] += 1
        return size
    
    def generate_zset(self, key, is_big=False):
        """生成 zset 类型数据"""
        client = self.get_client_for_key(key)
        
        if is_big:
            # 大 zset: 30000 个成员
            members = {random_string(30): random.uniform(0, 100000) for _ in range(BIG_ZSET_MEMBERS)}
            self.big_key_count += 1
        else:
            # 普通 zset: 10-100 个成员
            member_count = random.randint(10, 100)
            members = {random_string(random.randint(10, 30)): random.uniform(0, 10000) for _ in range(member_count)}
        
        # 分批 zadd
        items = list(members.items())
        batch = 1000
        for i in range(0, len(items), batch):
            batch_dict = dict(items[i:i+batch])
            client.zadd(key, batch_dict)
        
        size = len(key) + sum(len(m) + 8 for m in members.keys())  # 8 bytes for score
        self.type_counts['zset'] += 1
        return size
    
    def generate_data(self):
        """生成测试数据"""
        print(f"\n开始生成 1GB 测试数据...")
        print(f"目标大小: {TARGET_SIZE / (1024*1024*1024):.2f} GB")
        print(f"数据类型比例: string {STRING_RATIO*100}%, 其他类型 {OTHER_RATIO*100}%")
        print(f"大 key 比例: {BIG_KEY_RATIO*100}%")
        print("-" * 60)
        sys.stdout.flush()
        
        start_time = time.time()
        last_report = time.time()
        
        while self.total_size < TARGET_SIZE:
            # 决定数据类型
            r = random.random()
            is_big = random.random() < BIG_KEY_RATIO
            
            key = f"testkey:{self.key_count}:{random_string(8)}"
            
            try:
                if r < STRING_RATIO:
                    # 80% string
                    size = self.generate_string(key, is_big)
                else:
                    # 20% 其他类型随机
                    other_type = random.choice(['hash', 'list', 'set', 'zset'])
                    if other_type == 'hash':
                        size = self.generate_hash(key, is_big)
                    elif other_type == 'list':
                        size = self.generate_list(key, is_big)
                    elif other_type == 'set':
                        size = self.generate_set(key, is_big)
                    else:
                        size = self.generate_zset(key, is_big)
                
                self.total_size += size
                self.key_count += 1
                
                # 每10秒或每1000个key报告一次进度
                if time.time() - last_report > 10 or self.key_count % 1000 == 0:
                    elapsed = time.time() - start_time
                    progress = self.total_size / TARGET_SIZE * 100
                    speed = self.total_size / elapsed / (1024*1024) if elapsed > 0 else 0
                    print(f"进度: {progress:.1f}% | 大小: {self.total_size/(1024*1024):.1f}MB | "
                          f"Keys: {self.key_count} | 大Key: {self.big_key_count} | "
                          f"速度: {speed:.1f}MB/s | 耗时: {elapsed:.0f}s")
                    last_report = time.time()
                    sys.stdout.flush()
                    
            except Exception as e:
                print(f"写入错误 (key={key}): {e}")
                import traceback
                traceback.print_exc()
                sys.stdout.flush()
                # 继续尝试
                continue
        
        elapsed = time.time() - start_time
        print("\n" + "=" * 60)
        print("数据生成完成!")
        print(f"总大小: {self.total_size / (1024*1024*1024):.2f} GB ({self.total_size} bytes)")
        print(f"总 Key 数: {self.key_count}")
        print(f"大 Key 数: {self.big_key_count}")
        print(f"总耗时: {elapsed:.1f} 秒")
        print(f"平均速度: {self.total_size / elapsed / (1024*1024):.1f} MB/s")
        print("\n各类型数量统计:")
        for t, c in self.type_counts.items():
            pct = c / self.key_count * 100 if self.key_count > 0 else 0
            print(f"  {t}: {c} ({pct:.1f}%)")
        print("=" * 60)
        sys.stdout.flush()
        
        return self.key_count, self.total_size

if __name__ == "__main__":
    generator = DataGenerator()
    generator.generate_data()
