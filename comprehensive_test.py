#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Tendis-Migrate 综合测试脚本
用于自动化执行各种迁移场景测试
"""

import redis
import requests
import json
import time
import subprocess
import sys
import os
from datetime import datetime

# ============== 配置 ==============
SOURCE_HOST = "192.168.1.19"
SOURCE_PORT = 7001
TARGET_HOST = "192.168.1.19"
TARGET_PORT = 8001
API_BASE = "http://localhost:8088/api/v1"

# 测试数据量配置
SMALL_KEY_COUNT = 1000      # 小规模测试
MEDIUM_KEY_COUNT = 10000    # 中等规模测试
LARGE_KEY_COUNT = 100000    # 大规模测试

# ============== 工具函数 ==============

def get_redis_client(host, port):
    """获取 Redis 客户端"""
    return redis.Redis(host=host, port=port, decode_responses=True)

def log(msg, level="INFO"):
    """打印日志"""
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    print(f"[{timestamp}] [{level}] {msg}")

def clear_redis(client, name=""):
    """清空 Redis 数据"""
    client.flushall()
    log(f"已清空 {name} Redis 数据")

def get_key_count(client):
    """获取 key 数量"""
    return client.dbsize()

def generate_test_data(client, prefix, count, data_types=None):
    """
    生成测试数据
    data_types: 指定生成的数据类型列表，默认只生成 string（兼容性最好）
    """
    if data_types is None:
        data_types = ['string']  # 默认只用 string，避免 Tendis 兼容性问题
    
    log(f"开始生成 {count} 个 key，前缀: {prefix}")
    
    pipe = client.pipeline()
    batch_size = 500  # 减小批次大小
    
    for i in range(count):
        key_type = data_types[i % len(data_types)]
        key = f"{prefix}{i:08d}"
        
        if key_type == 'string':
            pipe.set(key, f"value_{i}")
        elif key_type == 'hash':
            pipe.hset(key, mapping={"field1": f"value_{i}", "field2": str(i)})
        elif key_type == 'list':
            pipe.rpush(key, f"item_{i}_1", f"item_{i}_2", f"item_{i}_3")
        elif key_type == 'set':
            pipe.sadd(key, f"member_{i}_1", f"member_{i}_2", f"member_{i}_3")
        elif key_type == 'zset':
            pipe.zadd(key, {f"member_{i}": float(i)})
        
        if (i + 1) % batch_size == 0:
            try:
                pipe.execute()
            except Exception as e:
                log(f"Pipeline 执行错误 (batch {(i+1)//batch_size}): {e}", "WARN")
            pipe = client.pipeline()
            if (i + 1) % 10000 == 0:
                log(f"已生成 {i + 1} 个 key")
    
    # 执行剩余的命令
    try:
        pipe.execute()
    except Exception as e:
        log(f"Pipeline 最终执行错误: {e}", "WARN")
    
    actual_count = client.dbsize()
    log(f"数据生成完成，实际 key 数量: {actual_count}")

def generate_multi_prefix_data(client, prefixes, count_per_prefix):
    """生成多前缀测试数据"""
    total = 0
    for prefix in prefixes:
        generate_test_data(client, prefix, count_per_prefix)
        total += count_per_prefix
    log(f"多前缀数据生成完成，共 {total} 个 key")

def verify_data_consistency(source_client, target_client, prefix=None, expected_count=None):
    """
    验证数据一致性
    返回: (success, details)
    """
    source_count = get_key_count(source_client)
    target_count = get_key_count(target_client)
    
    log(f"源端 key 数量: {source_count}")
    log(f"目标端 key 数量: {target_count}")
    
    if expected_count is not None:
        log(f"预期 key 数量: {expected_count}")
    
    # 检查数量
    if expected_count is not None:
        count_match = target_count == expected_count
    else:
        count_match = target_count == source_count
    
    # 抽样检查内容一致性
    sample_size = min(100, target_count)
    content_match = True
    mismatch_keys = []
    
    if prefix:
        keys = list(target_client.scan_iter(match=f"{prefix}*", count=sample_size))[:sample_size]
    else:
        keys = list(target_client.scan_iter(count=sample_size))[:sample_size]
    
    for key in keys:
        key_type = target_client.type(key)
        
        if key_type == 'string':
            src_val = source_client.get(key)
            tgt_val = target_client.get(key)
            if src_val != tgt_val:
                content_match = False
                mismatch_keys.append(key)
        elif key_type == 'hash':
            src_val = source_client.hgetall(key)
            tgt_val = target_client.hgetall(key)
            if src_val != tgt_val:
                content_match = False
                mismatch_keys.append(key)
        elif key_type == 'list':
            src_val = source_client.lrange(key, 0, -1)
            tgt_val = target_client.lrange(key, 0, -1)
            if src_val != tgt_val:
                content_match = False
                mismatch_keys.append(key)
        elif key_type == 'set':
            src_val = source_client.smembers(key)
            tgt_val = target_client.smembers(key)
            if src_val != tgt_val:
                content_match = False
                mismatch_keys.append(key)
        elif key_type == 'zset':
            src_val = source_client.zrange(key, 0, -1, withscores=True)
            tgt_val = target_client.zrange(key, 0, -1, withscores=True)
            if src_val != tgt_val:
                content_match = False
                mismatch_keys.append(key)
    
    success = count_match and content_match
    details = {
        "source_count": source_count,
        "target_count": target_count,
        "expected_count": expected_count,
        "count_match": count_match,
        "content_match": content_match,
        "sample_size": sample_size,
        "mismatch_keys": mismatch_keys[:10]  # 最多记录10个不匹配的key
    }
    
    if success:
        log("✅ 数据一致性验证通过")
    else:
        log(f"❌ 数据一致性验证失败: count_match={count_match}, content_match={content_match}", "ERROR")
        if mismatch_keys:
            log(f"不匹配的 key 示例: {mismatch_keys[:5]}", "ERROR")
    
    return success, details

# ============== API 操作函数 ==============

def api_create_task(task_config):
    """创建迁移任务"""
    url = f"{API_BASE}/tasks"
    try:
        resp = requests.post(url, json=task_config, timeout=30)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            # 兼容两种返回格式
            task_id = result.get("data", {}).get("task_id") or result.get("data", {}).get("id")
            log(f"任务创建成功，ID: {task_id}")
            return task_id
        else:
            log(f"任务创建失败: {result.get('message')}", "ERROR")
            return None
    except Exception as e:
        log(f"创建任务异常: {e}", "ERROR")
        return None

def api_get_task(task_id):
    """获取任务详情"""
    url = f"{API_BASE}/tasks/{task_id}"
    try:
        resp = requests.get(url, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            return result.get("data")
        return None
    except Exception as e:
        log(f"获取任务详情异常: {e}", "ERROR")
        return None

def api_start_task(task_id):
    """启动任务"""
    url = f"{API_BASE}/tasks/{task_id}/start"
    try:
        resp = requests.post(url, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            log(f"任务 {task_id} 启动成功")
            return True
        else:
            log(f"任务启动失败: {result.get('message')}", "ERROR")
            return False
    except Exception as e:
        log(f"启动任务异常: {e}", "ERROR")
        return False

def api_stop_task(task_id):
    """停止任务"""
    url = f"{API_BASE}/tasks/{task_id}/stop"
    try:
        resp = requests.post(url, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            log(f"任务 {task_id} 停止成功")
            return True
        else:
            log(f"任务停止失败: {result.get('message')}", "ERROR")
            return False
    except Exception as e:
        log(f"停止任务异常: {e}", "ERROR")
        return False

def api_delete_task(task_id):
    """删除任务"""
    url = f"{API_BASE}/tasks/{task_id}"
    try:
        resp = requests.delete(url, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            log(f"任务 {task_id} 删除成功")
            return True
        else:
            log(f"任务删除失败: {result.get('message')}", "ERROR")
            return False
    except Exception as e:
        log(f"删除任务异常: {e}", "ERROR")
        return False

def api_update_task_options(task_id, options):
    """更新任务参数"""
    url = f"{API_BASE}/tasks/{task_id}/options"
    try:
        resp = requests.put(url, json=options, timeout=10)
        resp.raise_for_status()
        result = resp.json()
        if result.get("code") == 0:
            log(f"任务 {task_id} 参数更新成功: {options}")
            return True
        else:
            log(f"参数更新失败: {result.get('message')}", "ERROR")
            return False
    except Exception as e:
        log(f"更新参数异常: {e}", "ERROR")
        return False

def wait_task_complete(task_id, timeout=600, check_interval=5, mode="full"):
    """
    等待任务完成
    mode: "full" - 全量模式，等待全量迁移完成
          "full_and_incremental" - 全量+增量模式，需要手动停止增量
    """
    log(f"等待任务 {task_id} 完成，超时时间: {timeout}s, 模式: {mode}")
    start_time = time.time()
    
    while time.time() - start_time < timeout:
        task = api_get_task(task_id)
        if task:
            status = task.get("status")
            phase = task.get("phase", "")
            progress = task.get("progress", {})
            migrated = progress.get("migrated_keys", 0)
            total = progress.get("keys_to_migrate", 0) or progress.get("total_keys", 0)
            percentage = progress.get("percentage", 0)
            
            log(f"任务状态: {status}, 阶段: {phase}, 进度: {migrated}/{total} ({percentage}%)")
            
            if status == "completed":
                log(f"✅ 任务完成")
                return True, task
            elif status == "failed":
                log(f"❌ 任务失败: {task.get('error')}", "ERROR")
                return False, task
            elif status == "stopped":
                # 如果是全量模式且已迁移完成，视为成功
                if mode == "full" and migrated > 0 and migrated >= total:
                    log(f"✅ 全量迁移完成（已停止）")
                    return True, task
                log(f"任务已停止")
                return False, task
            
            # 对于全量模式，如果进入增量阶段且全量已完成，视为成功
            if mode == "full" and phase == "incremental" and percentage >= 100:
                log(f"✅ 全量迁移完成（进入增量阶段）")
                # 自动停止任务
                api_stop_task(task_id)
                return True, task
        
        time.sleep(check_interval)
    
    log(f"❌ 任务超时", "ERROR")
    return False, None

def wait_incremental_sync(task_id, duration=60, check_interval=5):
    """等待增量同步一段时间"""
    log(f"增量同步运行 {duration}s")
    start_time = time.time()
    
    while time.time() - start_time < duration:
        task = api_get_task(task_id)
        if task:
            status = task.get("status")
            phase = task.get("phase", "")
            progress = task.get("progress", {})
            
            log(f"任务状态: {status}, 阶段: {phase}, 增量同步: {progress.get('incremental_keys', 0)} keys")
            
            if status == "failed":
                log(f"❌ 任务失败: {task.get('error')}", "ERROR")
                return False, task
        
        time.sleep(check_interval)
    
    return True, api_get_task(task_id)

# ============== 测试用例 ==============

def build_task_config(name, mode, filter_mode="all", filter_value=None, 
                      shadow_mode=False, compression=False, extra_options=None):
    """构建任务配置"""
    config = {
        "name": name,
        "migration_mode": mode,
        "source_cluster": {
            "addrs": [f"{SOURCE_HOST}:{SOURCE_PORT}"]
        },
        "target_cluster": {
            "addrs": [f"{TARGET_HOST}:{TARGET_PORT}"]
        },
        "options": {
            "workers": 4,
            "scan_count": 1000,
            "conflict_policy": "replace",
            "shadow_mode": shadow_mode,
            "compression": compression,
            "key_filter": {
                "mode": filter_mode
            }
        }
    }
    
    # 设置过滤值（放在 options.key_filter 内）
    if filter_mode == "prefix" and filter_value:
        # 支持字符串（逗号分隔）或数组
        if isinstance(filter_value, str):
            config["options"]["key_filter"]["prefixes"] = [p.strip() for p in filter_value.split(",")]
        else:
            config["options"]["key_filter"]["prefixes"] = filter_value
    elif filter_mode == "pattern" and filter_value:
        if isinstance(filter_value, str):
            config["options"]["key_filter"]["patterns"] = [p.strip() for p in filter_value.split(",")]
        else:
            config["options"]["key_filter"]["patterns"] = filter_value
    elif filter_mode == "keylist" and filter_value:
        if isinstance(filter_value, str):
            config["options"]["key_filter"]["keys"] = [k.strip() for k in filter_value.split(",")]
        else:
            config["options"]["key_filter"]["keys"] = filter_value
    
    # 合并额外选项
    if extra_options:
        config["options"].update(extra_options)
    
    return config

class TestResult:
    """测试结果记录"""
    def __init__(self, test_id, test_name):
        self.test_id = test_id
        self.test_name = test_name
        self.start_time = datetime.now()
        self.end_time = None
        self.success = False
        self.details = {}
        self.errors = []
    
    def finish(self, success, details=None):
        self.end_time = datetime.now()
        self.success = success
        if details:
            self.details = details
    
    def add_error(self, error):
        self.errors.append(error)
    
    def to_dict(self):
        return {
            "test_id": self.test_id,
            "test_name": self.test_name,
            "start_time": self.start_time.isoformat(),
            "end_time": self.end_time.isoformat() if self.end_time else None,
            "duration": str(self.end_time - self.start_time) if self.end_time else None,
            "success": self.success,
            "details": self.details,
            "errors": self.errors
        }

# ============== 测试执行函数 ==============

def test_1_1_full_all():
    """测试 1.1: 全量迁移 - 过滤模式 all"""
    result = TestResult("1.1", "全量迁移 - 过滤模式 all")
    log("=" * 60)
    log("开始测试 1.1: 全量迁移 - 过滤模式 all")
    log("=" * 60)
    
    try:
        # 1. 准备
        source = get_redis_client(SOURCE_HOST, SOURCE_PORT)
        target = get_redis_client(TARGET_HOST, TARGET_PORT)
        
        clear_redis(source, "源端")
        clear_redis(target, "目标端")
        
        # 2. 生成测试数据
        generate_test_data(source, "testkey:", MEDIUM_KEY_COUNT)
        source_count = get_key_count(source)
        log(f"源端数据准备完成: {source_count} keys")
        
        # 3. 创建并启动任务
        config = build_task_config(
            name=f"test_1_1_{int(time.time())}",
            mode="full",
            filter_mode="all"
        )
        task_id = api_create_task(config)
        if not task_id:
            result.add_error("创建任务失败")
            result.finish(False)
            return result
        
        if not api_start_task(task_id):
            result.add_error("启动任务失败")
            result.finish(False)
            return result
        
        # 4. 等待完成
        success, task = wait_task_complete(task_id)
        if not success:
            result.add_error(f"任务未成功完成: {task}")
            result.finish(False)
            return result
        
        # 5. 验证数据
        verify_success, verify_details = verify_data_consistency(
            source, target, expected_count=source_count
        )
        
        result.details["task"] = task
        result.details["verification"] = verify_details
        result.finish(verify_success)
        
        # 6. 清理
        api_delete_task(task_id)
        
    except Exception as e:
        result.add_error(str(e))
        result.finish(False)
        log(f"测试异常: {e}", "ERROR")
    
    return result

def test_1_2_full_prefix():
    """测试 1.2: 全量迁移 - 过滤模式 prefix"""
    result = TestResult("1.2", "全量迁移 - 过滤模式 prefix")
    log("=" * 60)
    log("开始测试 1.2: 全量迁移 - 过滤模式 prefix")
    log("=" * 60)
    
    try:
        source = get_redis_client(SOURCE_HOST, SOURCE_PORT)
        target = get_redis_client(TARGET_HOST, TARGET_PORT)
        
        clear_redis(source, "源端")
        clear_redis(target, "目标端")
        
        # 生成多前缀数据
        prefixes = ["app1:", "app2:", "app3:", "other:"]
        count_per_prefix = SMALL_KEY_COUNT
        generate_multi_prefix_data(source, prefixes, count_per_prefix)
        
        # 只迁移 app1 和 app2 前缀
        target_prefixes = "app1:,app2:"
        expected_count = count_per_prefix * 2
        
        config = build_task_config(
            name=f"test_1_2_{int(time.time())}",
            mode="full",
            filter_mode="prefix",
            filter_value=target_prefixes
        )
        task_id = api_create_task(config)
        if not task_id:
            result.add_error("创建任务失败")
            result.finish(False)
            return result
        
        if not api_start_task(task_id):
            result.add_error("启动任务失败")
            result.finish(False)
            return result
        
        success, task = wait_task_complete(task_id)
        if not success:
            result.add_error(f"任务未成功完成: {task}")
            result.finish(False)
            return result
        
        # 验证: 目标端应该只有 app1 和 app2 的 key
        target_count = get_key_count(target)
        log(f"目标端 key 数量: {target_count}, 预期: {expected_count}")
        
        # 检查是否有 app3 或 other 的 key
        unwanted_keys = list(target.scan_iter(match="app3:*", count=10))
        unwanted_keys += list(target.scan_iter(match="other:*", count=10))
        
        if unwanted_keys:
            result.add_error(f"发现不应迁移的 key: {unwanted_keys[:5]}")
        
        verify_success = (target_count == expected_count) and (len(unwanted_keys) == 0)
        
        result.details["task"] = task
        result.details["target_count"] = target_count
        result.details["expected_count"] = expected_count
        result.details["unwanted_keys"] = unwanted_keys[:10]
        result.finish(verify_success)
        
        api_delete_task(task_id)
        
    except Exception as e:
        result.add_error(str(e))
        result.finish(False)
        log(f"测试异常: {e}", "ERROR")
    
    return result

def test_1_3_full_pattern():
    """测试 1.3: 全量迁移 - 过滤模式 pattern"""
    result = TestResult("1.3", "全量迁移 - 过滤模式 pattern")
    log("=" * 60)
    log("开始测试 1.3: 全量迁移 - 过滤模式 pattern")
    log("=" * 60)
    
    try:
        source = get_redis_client(SOURCE_HOST, SOURCE_PORT)
        target = get_redis_client(TARGET_HOST, TARGET_PORT)
        
        clear_redis(source, "源端")
        clear_redis(target, "目标端")
        
        # 生成特定模式的数据
        # 注意：当前 pattern 只支持 前缀* 或 *后缀 形式
        # user:profile:xxxx (应该匹配 user:profile:*)
        # user:settings:xxxx (不应该匹配)
        # order:xxxx (不应该匹配)
        
        for i in range(SMALL_KEY_COUNT):
            source.set(f"user:profile:{i:04d}", f"profile_value_{i}")
            source.set(f"user:settings:{i:04d}", f"settings_value_{i}")
            source.set(f"order:{i:04d}", f"order_value_{i}")
        
        source_count = get_key_count(source)
        log(f"源端数据准备完成: {source_count} keys")
        
        # 只迁移匹配 user:profile:* 的 key (前缀通配符)
        config = build_task_config(
            name=f"test_1_3_{int(time.time())}",
            mode="full",
            filter_mode="pattern",
            filter_value="user:profile:*"
        )
        task_id = api_create_task(config)
        if not task_id:
            result.add_error("创建任务失败")
            result.finish(False)
            return result
        
        if not api_start_task(task_id):
            result.add_error("启动任务失败")
            result.finish(False)
            return result
        
        success, task = wait_task_complete(task_id)
        if not success:
            result.add_error(f"任务未成功完成: {task}")
            result.finish(False)
            return result
        
        target_count = get_key_count(target)
        expected_count = SMALL_KEY_COUNT  # 只有 user:profile:*
        
        log(f"目标端 key 数量: {target_count}, 预期: {expected_count}")
        
        # 检查是否有不应该迁移的 key
        settings_keys = list(target.scan_iter(match="user:settings:*", count=10))
        order_keys = list(target.scan_iter(match="order:*", count=10))
        
        verify_success = (target_count == expected_count) and \
                        (len(settings_keys) == 0) and (len(order_keys) == 0)
        
        result.details["task"] = task
        result.details["target_count"] = target_count
        result.details["expected_count"] = expected_count
        result.details["unwanted_settings_keys"] = settings_keys[:5]
        result.details["unwanted_order_keys"] = order_keys[:5]
        result.finish(verify_success)
        
        api_delete_task(task_id)
        
    except Exception as e:
        result.add_error(str(e))
        result.finish(False)
        log(f"测试异常: {e}", "ERROR")
    
    return result

def test_1_4_full_keylist():
    """测试 1.4: 全量迁移 - 过滤模式 keylist"""
    result = TestResult("1.4", "全量迁移 - 过滤模式 keylist")
    log("=" * 60)
    log("开始测试 1.4: 全量迁移 - 过滤模式 keylist")
    log("=" * 60)
    
    try:
        source = get_redis_client(SOURCE_HOST, SOURCE_PORT)
        target = get_redis_client(TARGET_HOST, TARGET_PORT)
        
        clear_redis(source, "源端")
        clear_redis(target, "目标端")
        
        # 生成测试数据
        all_keys = []
        for i in range(SMALL_KEY_COUNT):
            key = f"key:{i:04d}"
            source.set(key, f"value_{i}")
            all_keys.append(key)
        
        # 只迁移前 100 个 key
        keys_to_migrate = all_keys[:100]
        expected_count = 100
        
        config = build_task_config(
            name=f"test_1_4_{int(time.time())}",
            mode="full",
            filter_mode="keylist",
            filter_value=",".join(keys_to_migrate)
        )
        task_id = api_create_task(config)
        if not task_id:
            result.add_error("创建任务失败")
            result.finish(False)
            return result
        
        if not api_start_task(task_id):
            result.add_error("启动任务失败")
            result.finish(False)
            return result
        
        success, task = wait_task_complete(task_id)
        if not success:
            result.add_error(f"任务未成功完成: {task}")
            result.finish(False)
            return result
        
        target_count = get_key_count(target)
        log(f"目标端 key 数量: {target_count}, 预期: {expected_count}")
        
        # 验证指定的 key 都存在
        missing_keys = []
        for key in keys_to_migrate:
            if not target.exists(key):
                missing_keys.append(key)
        
        verify_success = (target_count == expected_count) and (len(missing_keys) == 0)
        
        result.details["task"] = task
        result.details["target_count"] = target_count
        result.details["expected_count"] = expected_count
        result.details["missing_keys"] = missing_keys[:10]
        result.finish(verify_success)
        
        api_delete_task(task_id)
        
    except Exception as e:
        result.add_error(str(e))
        result.finish(False)
        log(f"测试异常: {e}", "ERROR")
    
    return result

def test_1_11_full_crash_recovery():
    """测试 1.11: 全量迁移 - 崩溃恢复"""
    result = TestResult("1.11", "全量迁移 - 崩溃恢复")
    log("=" * 60)
    log("开始测试 1.11: 全量迁移 - 崩溃恢复")
    log("=" * 60)
    
    try:
        source = get_redis_client(SOURCE_HOST, SOURCE_PORT)
        target = get_redis_client(TARGET_HOST, TARGET_PORT)
        
        clear_redis(source, "源端")
        clear_redis(target, "目标端")
        
        # 生成较大量数据以便有时间触发崩溃
        generate_test_data(source, "crash:", LARGE_KEY_COUNT)
        source_count = get_key_count(source)
        log(f"源端数据准备完成: {source_count} keys")
        
        config = build_task_config(
            name=f"test_1_11_{int(time.time())}",
            mode="full",
            filter_mode="all",
            extra_options={"workers": 2, "scan_count": 500}  # 减慢速度
        )
        task_id = api_create_task(config)
        if not task_id:
            result.add_error("创建任务失败")
            result.finish(False)
            return result
        
        if not api_start_task(task_id):
            result.add_error("启动任务失败")
            result.finish(False)
            return result
        
        # 等待一段时间后模拟崩溃
        log("等待 10 秒后模拟崩溃...")
        time.sleep(10)
        
        # 记录崩溃前进度
        task_before = api_get_task(task_id)
        progress_before = task_before.get("progress", {}).get("migrated_keys", 0) if task_before else 0
        log(f"崩溃前进度: {progress_before} keys")
        
        # 模拟崩溃 (SIGKILL)
        log("发送 SIGKILL 模拟崩溃...")
        subprocess.run(["pkill", "-9", "-f", "tendis-migrate"], capture_output=True)
        time.sleep(2)
        
        # 重启服务
        log("重启迁移服务...")
        os.chdir("/Users/chenguoxie/CodeBuddy/tendis-migrate")
        subprocess.Popen(["./tendis-migrate-darwin"], stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
        time.sleep(5)
        
        # 检查任务是否自动恢复
        log("等待任务自动恢复...")
        success, task = wait_task_complete(task_id, timeout=300)
        
        if not success:
            result.add_error(f"任务恢复后未成功完成")
            result.finish(False)
            return result
        
        # 验证数据完整性
        verify_success, verify_details = verify_data_consistency(
            source, target, expected_count=source_count
        )
        
        result.details["progress_before_crash"] = progress_before
        result.details["task"] = task
        result.details["verification"] = verify_details
        result.finish(verify_success)
        
        api_delete_task(task_id)
        
    except Exception as e:
        result.add_error(str(e))
        result.finish(False)
        log(f"测试异常: {e}", "ERROR")
    
    return result

# ============== 主函数 ==============

def run_all_tests():
    """运行所有测试"""
    all_results = []
    
    # 全量迁移测试
    tests = [
        test_1_1_full_all,
        test_1_2_full_prefix,
        test_1_3_full_pattern,
        test_1_4_full_keylist,
        # test_1_11_full_crash_recovery,  # 崩溃测试需要单独运行
    ]
    
    for test_func in tests:
        result = test_func()
        all_results.append(result)
        
        status = "✅ 通过" if result.success else "❌ 失败"
        log(f"测试 {result.test_id} {result.test_name}: {status}")
        
        if not result.success:
            for error in result.errors:
                log(f"  错误: {error}", "ERROR")
        
        log("-" * 60)
        time.sleep(2)  # 测试间隔
    
    # 输出总结
    log("=" * 60)
    log("测试总结")
    log("=" * 60)
    
    passed = sum(1 for r in all_results if r.success)
    failed = len(all_results) - passed
    
    log(f"总计: {len(all_results)} 个测试")
    log(f"通过: {passed}")
    log(f"失败: {failed}")
    
    # 保存结果到文件
    result_file = f"test_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.json"
    with open(result_file, 'w', encoding='utf-8') as f:
        json.dump([r.to_dict() for r in all_results], f, ensure_ascii=False, indent=2)
    log(f"测试结果已保存到: {result_file}")
    
    return all_results

def run_single_test(test_id):
    """运行单个测试"""
    test_map = {
        "1.1": test_1_1_full_all,
        "1.2": test_1_2_full_prefix,
        "1.3": test_1_3_full_pattern,
        "1.4": test_1_4_full_keylist,
        "1.11": test_1_11_full_crash_recovery,
    }
    
    if test_id not in test_map:
        log(f"未知测试 ID: {test_id}", "ERROR")
        log(f"可用测试: {list(test_map.keys())}")
        return None
    
    return test_map[test_id]()

if __name__ == "__main__":
    if len(sys.argv) > 1:
        test_id = sys.argv[1]
        result = run_single_test(test_id)
        if result:
            status = "✅ 通过" if result.success else "❌ 失败"
            log(f"测试结果: {status}")
    else:
        log("用法: python comprehensive_test.py [test_id]")
        log("例如: python comprehensive_test.py 1.1")
        log("运行所有测试: python comprehensive_test.py all")
        
        if len(sys.argv) > 1 and sys.argv[1] == "all":
            run_all_tests()
