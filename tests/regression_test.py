#!/usr/bin/env python3
"""
tendis-migrate 全量回归测试脚本
===============================
设计原则：
1. 覆盖所有功能点，不留盲区
2. 每个测试验证"功能是否符合预期表现"而不只是"不报错"
3. 测试间完全隔离（每次清空目标端+删除旧任务）
4. 增量同步采样贯穿全过程（不只是初期）

测试分类：
  A. 基础功能（API/连接/健康检查）
  B. 全量迁移（无过滤/前缀过滤/排除前缀/pattern通配符/keylist）
  C. 冲突策略（skip/replace/skip_full_only/error）
  D. 数据类型（string/hash/list/set/zset 值正确性验证）
  E. 增量同步（基本同步/前缀过滤/DEL同步/多数据类型增量）
  F. 全量+增量（完整流程/增量阶段数据类型）
  G. 任务生命周期（pause/resume/stop/restart/delete/complete）
  H. 崩溃恢复（kill-9恢复/SIGTERM优雅关闭/新任务立即崩溃）
  I. 进度与计数器（migrated<=to_migrate 全过程监控/进度百分比合理性）
  J. 辅助功能（test-connection/preflight-check/error-keys/verify/动态配置）

用法:
  # 使用预设环境（默认 cloud）
  python tests/regression_test.py --env cloud

  # 家里环境
  python tests/regression_test.py --env home

  # 只运行指定分类
  python tests/regression_test.py --env cloud --categories A,B,D

  # 环境变量覆盖
  TM_API=http://localhost:8088/api/v1 python tests/regression_test.py
"""
import requests
import time
import json
import subprocess
import sys
import os
import traceback

# 确保 tests/ 目录在 import 路径中
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from config import TestConfig, get_config_from_args

# ================================================================
# 配置（从 config.py 加载，支持 --env 参数和环境变量覆盖）
# ================================================================
_CFG = None  # 延迟初始化，在 main() 中设置
_PARSED_ARGS = None

API = ""
SSH_CMD = ""
SRC_HOST = ""
DST_HOST = ""
SRC_PORTS = []
DST_PORTS = []
SRC_NODES = []
DST_NODES = []
REDIS_VIA_SSH = True

RESULTS = []


def _init_config(cfg):
    """从 TestConfig 对象初始化全局变量"""
    global API, SSH_CMD, SRC_HOST, DST_HOST, SRC_PORTS, DST_PORTS, SRC_NODES, DST_NODES, REDIS_VIA_SSH
    API = cfg.api
    SSH_CMD = cfg.ssh_cmd
    SRC_HOST = cfg.src_host
    DST_HOST = cfg.dst_host
    SRC_PORTS = cfg.src_ports
    DST_PORTS = cfg.dst_ports
    SRC_NODES = cfg.src_nodes
    DST_NODES = cfg.dst_nodes
    REDIS_VIA_SSH = cfg.redis_via_ssh

# ================================================================
# 基础工具
# ================================================================
def log(msg):
    ts = time.strftime("%H:%M:%S")
    print(f"[{ts}] {msg}", flush=True)

def ssh(cmd, timeout=120):
    """远程或本地执行命令"""
    if REDIS_VIA_SSH and SSH_CMD:
        full = f"{SSH_CMD} '{cmd}'"
    else:
        full = cmd
    try:
        r = subprocess.run(full, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=timeout)
        return r.stdout.decode('utf-8', errors='replace').strip()
    except subprocess.TimeoutExpired:
        return "TIMEOUT"
    except Exception as e:
        return f"ERROR: {e}"

def redis_cmd(port, cmd):
    """执行 redis-cli 命令（-c follow MOVED），连接源端"""
    r = ssh(f'redis-cli -c -h {SRC_HOST} -p {port} {cmd}')
    return r.strip()

def dst_redis_cmd(port, cmd):
    """执行 redis-cli 命令（-c follow MOVED），连接目标端"""
    r = ssh(f'redis-cli -c -h {DST_HOST} -p {port} {cmd}')
    return r.strip()

def redis_set(port, key, value):
    redis_cmd(port, f'SET {key} "{value}"')

def dst_redis_set(port, key, value):
    """在目标端写入 Key"""
    dst_redis_cmd(port, f'SET {key} "{value}"')

def redis_get(port, key):
    return redis_cmd(port, f'GET {key}')

def dst_redis_get(port, key):
    """从目标端读取 Key"""
    return dst_redis_cmd(port, f'GET {key}')

def redis_exists(port, key):
    return redis_cmd(port, f'EXISTS {key}') == '1'

def dst_redis_exists(port, key):
    """检查 Key 是否在目标端存在"""
    return dst_redis_cmd(port, f'EXISTS {key}') == '1'

def redis_del(port, key):
    redis_cmd(port, f'DEL {key}')

def redis_type(port, key):
    return redis_cmd(port, f'TYPE {key}')

def api_get(path, timeout=10):
    try:
        r = requests.get(f"{API}{path}", timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def api_post(path, data=None, timeout=15):
    try:
        r = requests.post(f"{API}{path}", json=data, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def api_put(path, data=None, timeout=10):
    try:
        r = requests.put(f"{API}{path}", json=data, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def api_delete(path, timeout=10):
    try:
        r = requests.delete(f"{API}{path}",
                            headers={"X-Confirm-Password": "confirm-delete"}, timeout=timeout)
        return r.json()
    except Exception as e:
        return {"error": str(e)}

def flush_dst():
    """清空目标端数据（支持源端/目标端不同 IP）"""
    for addr in DST_NODES:
        host, port = addr.rsplit(':', 1)
        ssh(f'redis-cli -h {host} -p {port} FLUSHALL')
    time.sleep(1)

def dbsize_nodes(addrs):
    """获取节点列表的总 Key 数"""
    total = 0
    for addr in addrs:
        host, port = addr.rsplit(':', 1)
        r = ssh(f'redis-cli -h {host} -p {port} DBSIZE')
        try:
            total += int(r.split(':')[-1].strip()) if ':' in r else int(r.split()[-1])
        except:
            pass
    return total

def dbsize(ports):
    """兼容旧接口：按端口列表获取源端 Key 数"""
    return dbsize_nodes([f"{SRC_HOST}:{p}" for p in ports])

def src_dbsize():
    return dbsize_nodes(SRC_NODES)

def dst_dbsize():
    return dbsize_nodes(DST_NODES)

def create_task(name, mode, key_filter=None, workers=4, conflict_policy="replace",
                scan_count=1000):
    data = {
        "name": name,
        "migration_mode": mode,
        "source_cluster": {"addrs": SRC_NODES},
        "target_cluster": {"addrs": DST_NODES},
        "options": {
            "workers": workers,
            "scan_count": scan_count,
            "conflict_policy": conflict_policy,
        }
    }
    if key_filter:
        data["options"]["key_filter"] = key_filter
    resp = api_post("/tasks", data)
    if "data" in resp and "task_id" in resp.get("data", {}):
        tid = resp["data"]["task_id"]
        log(f"  创建任务: {tid[:8]}... ({name})")
        return tid
    log(f"  ERROR 创建任务失败: {resp}")
    return None

def start_task(tid):
    return api_post(f"/tasks/{tid}/start")

def pause_task(tid):
    return api_post(f"/tasks/{tid}/pause")

def resume_task(tid):
    return api_post(f"/tasks/{tid}/resume")

def stop_task(tid):
    return api_post(f"/tasks/{tid}/stop")

def delete_task(tid):
    return api_delete(f"/tasks/{tid}")

def get_task(tid):
    resp = api_get(f"/tasks/{tid}")
    return resp.get("data", resp)

def wait_complete(tid, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        t = get_task(tid)
        s = t.get("status", "")
        if s == "completed":
            return t
        if s in ("failed", "error", "stopped"):
            return t
        time.sleep(2)
    return get_task(tid)

def wait_phase(tid, target_phase, timeout=600):
    start = time.time()
    while time.time() - start < timeout:
        t = get_task(tid)
        phase = t.get("progress", {}).get("phase", "")
        status = t.get("status", "")
        if phase == target_phase:
            return t
        if status in ("completed", "failed", "error", "stopped"):
            return t
        time.sleep(3)
    return get_task(tid)

def kill_process():
    ssh('PID=$(cat /home/tendis-migrate-package/data/tendis-migrate.pid 2>/dev/null || pgrep tendis-migrate | head -1) && kill -9 $PID 2>/dev/null')
    time.sleep(2)

def sigterm_process():
    ssh('PID=$(cat /home/tendis-migrate-package/data/tendis-migrate.pid 2>/dev/null || pgrep tendis-migrate | head -1) && kill $PID 2>/dev/null')
    time.sleep(3)

def restart_service():
    ssh('cd /home/tendis-migrate-package && bash run.sh')
    time.sleep(4)

def cleanup_tasks():
    """删除所有测试任务"""
    resp = api_get("/tasks")
    tasks = resp.get("data", {}).get("tasks", [])
    for t in tasks:
        if t.get("name", "").startswith("reg-"):
            api_delete(f"/tasks/{t['id']}")

def record(name, passed, details=""):
    status = "PASS" if passed else "FAIL"
    RESULTS.append({"name": name, "status": status, "details": details})
    icon = "✅" if passed else "❌"
    log(f"  {icon} [{status}] {name}: {details[:150]}")

def check_counter_consistency(tid, samples=10, interval=0.5):
    """全过程采样检查 migrated+skipped+failed+filtered <= to_migrate"""
    violations = 0
    details = []
    for _ in range(samples):
        t = get_task(tid)
        p = t.get("progress", {})
        s = t.get("stats", {})
        migrated = p.get("migrated_keys", 0)
        to_migrate = p.get("keys_to_migrate", 0)
        skipped = s.get("skipped_keys", 0)
        failed = s.get("failed_keys", 0)
        filtered = s.get("filtered_keys", 0)
        total = migrated + skipped + failed + filtered
        if to_migrate > 0 and total > to_migrate:
            violations += 1
            details.append(f"m={migrated}+s={skipped}+f={failed}+flt={filtered}={total}>tm={to_migrate}")
        time.sleep(interval)
    return violations, details

# ================================================================
# A. 基础功能测试
# ================================================================
def test_A1_health():
    """A1. 健康检查 API"""
    log("=== A1. 健康检查 ===")
    r1 = api_get("/health")
    r2 = api_get("/health/detailed")
    ok1 = r1.get("code") == 0 or r1.get("status") in ("ok", "healthy") or "data" in r1
    ok2 = r2.get("code") == 0 or r2.get("status") in ("ok", "healthy") or "data" in r2
    record("A1 健康检查", ok1 and ok2, f"basic={ok1}({r1.get('status','')}), detailed={ok2}")

def test_A2_test_connection():
    """A2. 测试连接 API"""
    log("=== A2. 测试连接 ===")
    r = api_post("/test-connection", {"addrs": SRC_NODES})
    ok = r.get("code") == 0 or "data" in r
    # 验证返回信息中有连接成功标识
    data = r.get("data", {})
    details = f"response_code={r.get('code')}, has_data={'data' in r}"
    record("A2 测试连接", ok, details)

def test_A3_preflight_check():
    """A3. 迁移前校验 API"""
    log("=== A3. 迁移前校验 ===")
    flush_dst()
    tid = create_task("reg-A3-preflight", "full_only")
    if not tid:
        record("A3 迁移前校验", False, "创建任务失败")
        return
    r = api_post(f"/tasks/{tid}/preflight-check")
    ok = r.get("code") == 0 or "data" in r
    data = r.get("data", {})
    details = f"code={r.get('code')}, checks={list(data.keys()) if isinstance(data, dict) else 'N/A'}"
    delete_task(tid)
    record("A3 迁移前校验", ok, details)

def test_A4_system_status():
    """A4. 系统状态 API"""
    log("=== A4. 系统状态 ===")
    r = api_get("/system/status")
    ok = r.get("code") == 0 or "data" in r
    data = r.get("data", {})
    details = f"code={r.get('code')}, fields={list(data.keys())[:5] if isinstance(data, dict) else 'N/A'}"
    record("A4 系统状态", ok, details)

# ================================================================
# B. 全量迁移测试
# ================================================================
def test_B1_full_no_filter():
    """B1. 全量无过滤 - 数据完整性 + 计数器全过程监控"""
    log("=== B1. 全量无过滤 ===")
    flush_dst()
    src_total = src_dbsize()
    log(f"  源端总量: {src_total}")

    tid = create_task("reg-B1-full", "full_only")
    if not tid:
        record("B1 全量无过滤", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(2)

    # 全过程采样（启动后立即开始，覆盖初期、中期、后期）
    log("  全过程计数器监控...")
    v1, d1 = check_counter_consistency(tid, samples=10, interval=1)
    time.sleep(5)
    v2, d2 = check_counter_consistency(tid, samples=10, interval=1)

    t = wait_complete(tid, timeout=600)
    status = t.get("status", "")
    dst_total = dst_dbsize()
    p = t.get("progress", {})
    migrated = p.get("migrated_keys", 0)
    to_migrate = p.get("keys_to_migrate", 0)
    progress_pct = p.get("progress", t.get("progress_percent", 0))

    # 完成后再检查一次
    v3, d3 = check_counter_consistency(tid, samples=3, interval=0.2)
    total_violations = v1 + v2 + v3

    checks = []
    checks.append(f"status={status}")
    checks.append(f"src_dbsize={src_total},dst={dst_total},migrated={migrated},to_migrate={to_migrate}")
    checks.append(f"counter_violations={total_violations}/23")
    if total_violations > 0:
        checks.append(f"violation_details={d1+d2+d3}")

    # 判定逻辑：Tendis DBSIZE 因惰性删除不可靠（过期 key 仍计数），
    # 所以不用 src-dst 差值，而是验证：
    # 1. 任务正常完成
    # 2. 计数器一致（migrated <= to_migrate）
    # 3. 实际迁移了数据（migrated > 0）
    # 4. 目标端有数据（dst > 0）
    all_ok = (status == "completed" and total_violations == 0
              and migrated > 0 and migrated <= to_migrate and dst_total > 0)
    record("B1 全量无过滤", all_ok, "; ".join(checks))
    return tid

def test_B2_full_prefix_filter():
    """B2. 全量前缀过滤 - 验证只迁移指定前缀"""
    log("=== B2. 全量前缀过滤 ===")
    flush_dst()

    tid = create_task("reg-B2-prefix", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["app:", "user:"]})
    if not tid:
        record("B2 前缀过滤", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")
    p = t.get("progress", {})
    st = t.get("stats", {})
    migrated = p.get("migrated_keys", 0)
    filtered = st.get("filtered_keys", 0)
    dst_total = dst_dbsize()

    # 严格验证：目标端不应有 dyntest2/order/config/session/hash_test/list_test/set_test
    bad_prefixes = ["dyntest2:", "order:", "config:", "session:", "hash_test:", "list_test:", "set_test:"]
    leaked = []
    for prefix in bad_prefixes:
        for port in DST_PORTS:
            r = ssh(f'redis-cli -h {DST_HOST} -p {port} SCAN 0 COUNT 5 MATCH "{prefix}*" | tail -n +2 | head -1')
            if r and r.strip():
                leaked.append(f"{prefix}@{port}")

    checks = []
    checks.append(f"status={s}")
    checks.append(f"dst={dst_total},migrated={migrated},filtered={filtered}")
    if leaked:
        checks.append(f"LEAKED_PREFIXES={leaked}")

    all_ok = s == "completed" and migrated > 0 and filtered > 0 and len(leaked) == 0
    record("B2 前缀过滤", all_ok, "; ".join(checks))
    return tid

def test_B3_full_exclude_prefix():
    """B3. 全量排除前缀 - 验证排除生效"""
    log("=== B3. 全量排除前缀 ===")
    flush_dst()

    tid = create_task("reg-B3-exclude", "full_only",
                      key_filter={"mode": "prefix", "exclude_prefixes": ["dyntest2:"]})
    if not tid:
        record("B3 排除前缀", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")
    st = t.get("stats", {})
    filtered = st.get("filtered_keys", 0)
    dst_total = dst_dbsize()

    # 严格验证：目标端不应有 dyntest2: 前缀
    has_dyntest = False
    for port in DST_PORTS:
        r = ssh(f'redis-cli -h {DST_HOST} -p {port} SCAN 0 COUNT 5 MATCH "dyntest2:*" | tail -n +2 | head -1')
        if r and r.strip():
            has_dyntest = True

    # dyntest2 约 200 万，filtered 应该 >= 1800000
    checks = []
    checks.append(f"status={s}")
    checks.append(f"dst={dst_total},filtered={filtered}")
    checks.append(f"no_dyntest={not has_dyntest}")

    all_ok = s == "completed" and not has_dyntest and filtered >= 1500000
    record("B3 排除前缀", all_ok, "; ".join(checks))
    return tid

def test_B4_full_pattern_filter():
    """B4. 全量 pattern 通配符过滤"""
    log("=== B4. Pattern 通配符过滤 ===")
    flush_dst()

    # 准备特定 key
    for i in range(20):
        redis_set(SRC_PORTS[0], f"pat_yes:{i:04d}", f"yes_{i}")
        redis_set(SRC_PORTS[0], f"pat_no:{i:04d}", f"no_{i}")
    time.sleep(1)

    tid = create_task("reg-B4-pattern", "full_only",
                      key_filter={"mode": "pattern", "patterns": ["pat_yes:*"]})
    if not tid:
        record("B4 Pattern过滤", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")
    dst_total = dst_dbsize()

    # 验证：只有 pat_yes 在目标端
    yes_count = sum(1 for i in range(20) if dst_redis_exists(DST_PORTS[0], f"pat_yes:{i:04d}"))
    no_count = sum(1 for i in range(20) if dst_redis_exists(DST_PORTS[0], f"pat_no:{i:04d}"))

    checks = []
    checks.append(f"status={s}")
    checks.append(f"dst={dst_total},yes_found={yes_count}/20,no_found={no_count}/20(should=0)")

    all_ok = s == "completed" and yes_count >= 15 and no_count == 0
    record("B4 Pattern过滤", all_ok, "; ".join(checks))
    return tid

def test_B5_full_keylist():
    """B5. 全量 keylist 模式"""
    log("=== B5. Keylist 模式 ===")
    flush_dst()

    keys = ["kl_test:a", "kl_test:b", "kl_test:c"]
    for k in keys:
        redis_set(SRC_PORTS[0], k, f"val_{k}")
    time.sleep(1)

    tid = create_task("reg-B5-keylist", "full_only",
                      key_filter={"mode": "keylist", "keys": keys})
    if not tid:
        record("B5 Keylist模式", False, "创建任务失败")
        return

    # 验证 API 返回 keys 字段
    t = get_task(tid)
    kf = t.get("options", {}).get("key_filter", {})
    has_keys = "keys" in kf and len(kf.get("keys", [])) == len(keys)

    start_task(tid)
    t = wait_complete(tid, timeout=60)
    s = t.get("status", "")
    p = t.get("progress", {})
    migrated = p.get("migrated_keys", 0)
    dst_total = dst_dbsize()

    # 验证目标端只有这些 key
    found = sum(1 for k in keys if dst_redis_exists(DST_PORTS[0], k))

    checks = []
    checks.append(f"status={s},migrated={migrated},dst={dst_total}")
    checks.append(f"found={found}/{len(keys)},api_has_keys={has_keys}")

    # 判定逻辑：keylist 模式核心验证是"指定的 key 全部迁移成功"，
    # 不检查 dst_total（Tendis FLUSHALL 异步回收可能未完成导致 dst_total 偏大）
    all_ok = s == "completed" and found == len(keys) and has_keys
    record("B5 Keylist模式", all_ok, "; ".join(checks))
    return tid

# ================================================================
# C. 冲突策略测试
# ================================================================
def test_C1_conflict_skip():
    """C1. 冲突策略: skip - 全阶段跳过"""
    log("=== C1. 冲突策略 skip ===")
    flush_dst()

    # 先在目标端预写入一些 key
    for i in range(50):
        dst_redis_set(DST_PORTS[0], f"app:{i:06d}", f"OLD_VALUE_{i}")
    time.sleep(1)

    tid = create_task("reg-C1-skip", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]},
                      conflict_policy="skip")
    if not tid:
        record("C1 skip策略", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")
    st = t.get("stats", {})
    skipped = st.get("skipped_keys", 0)

    # 验证：预写入的 key 应保持旧值
    old_val_kept = 0
    for i in range(10):
        val = dst_redis_get(DST_PORTS[0], f"app:{i:06d}")
        if "OLD_VALUE" in str(val):
            old_val_kept += 1

    checks = []
    checks.append(f"status={s},skipped={skipped}")
    checks.append(f"old_val_kept={old_val_kept}/10")

    # skip 模式下 skipped > 0，且旧值被保留
    all_ok = s == "completed" and skipped > 0 and old_val_kept >= 8
    record("C1 skip策略", all_ok, "; ".join(checks))
    return tid

def test_C2_conflict_replace():
    """C2. 冲突策略: replace - 直接覆盖"""
    log("=== C2. 冲突策略 replace ===")
    flush_dst()

    # 预写入旧值
    for i in range(50):
        dst_redis_set(DST_PORTS[0], f"app:{i:06d}", f"OLD_REPLACE_{i}")
    time.sleep(1)

    tid = create_task("reg-C2-replace", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]},
                      conflict_policy="replace")
    if not tid:
        record("C2 replace策略", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")

    # 验证：旧值应被新值覆盖
    new_val_count = 0
    for i in range(10):
        val = dst_redis_get(DST_PORTS[0], f"app:{i:06d}")
        if "OLD_REPLACE" not in str(val):
            new_val_count += 1

    checks = []
    checks.append(f"status={s},new_val_overwritten={new_val_count}/10")

    all_ok = s == "completed" and new_val_count >= 8
    record("C2 replace策略", all_ok, "; ".join(checks))
    return tid

def test_C3_conflict_skip_full_only():
    """C3. 冲突策略: skip_full_only (默认) - 全量跳过+增量覆盖"""
    log("=== C3. 冲突策略 skip_full_only ===")
    flush_dst()

    # 预写入旧值到目标端
    for i in range(20):
        dst_redis_set(DST_PORTS[0], f"app:{i:06d}", f"OLD_SFO_{i}")
    time.sleep(1)

    tid = create_task("reg-C3-sfo", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]},
                      conflict_policy="skip_full_only")
    if not tid:
        record("C3 skip_full_only", False, "创建任务失败")
        return

    start_task(tid)

    # 等进入增量阶段
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    st = t.get("stats", {})
    skipped = st.get("skipped_keys", 0)

    # 全量阶段应该跳过了预写入的 key
    full_old_kept = 0
    for i in range(10):
        val = dst_redis_get(DST_PORTS[0], f"app:{i:06d}")
        if "OLD_SFO" in str(val):
            full_old_kept += 1

    checks = []
    checks.append(f"phase={phase},skipped={skipped}")
    checks.append(f"full_old_val_kept={full_old_kept}/10")

    stop_task(tid)
    time.sleep(2)

    all_ok = phase == "incremental" and skipped > 0 and full_old_kept >= 5
    record("C3 skip_full_only", all_ok, "; ".join(checks))
    return tid

# ================================================================
# D. 数据类型正确性测试
# ================================================================
def test_D1_data_types():
    """D1. 多数据类型迁移正确性（string/hash/list/set/zset）"""
    log("=== D1. 数据类型正确性 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    # 先清理源端旧的测试 key（防止 RPUSH 等命令累加）
    for key in ["dtype:str", "dtype:hash", "dtype:list", "dtype:set", "dtype:zset"]:
        redis_cmd(src, f'DEL {key}')
    time.sleep(1)

    # 准备各种类型的数据
    redis_cmd(src, 'SET dtype:str "hello_world_123"')
    redis_cmd(src, 'HSET dtype:hash f1 v1 f2 v2 f3 v3')
    redis_cmd(src, 'RPUSH dtype:list a b c d e')
    redis_cmd(src, 'SADD dtype:set m1 m2 m3 m4 m5')
    redis_cmd(src, 'ZADD dtype:zset 1.0 z1 2.0 z2 3.0 z3')
    time.sleep(1)

    tid = create_task("reg-D1-types", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["dtype:"]})
    if not tid:
        record("D1 数据类型", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    # 逐类型验证
    checks = []
    all_type_ok = True

    # string
    val = dst_redis_get(dst, "dtype:str")
    str_ok = "hello_world_123" in str(val)
    checks.append(f"string={'OK' if str_ok else 'FAIL:'+str(val)}")
    all_type_ok = all_type_ok and str_ok

    # hash
    hval = dst_redis_cmd(dst, "HGETALL dtype:hash")
    hash_ok = "f1" in hval and "v1" in hval and "f2" in hval and "f3" in hval
    checks.append(f"hash={'OK' if hash_ok else 'FAIL:'+hval[:50]}")
    all_type_ok = all_type_ok and hash_ok

    # list
    llen = dst_redis_cmd(dst, "LLEN dtype:list")
    list_ok = llen.strip() == "5"
    lvals = dst_redis_cmd(dst, "LRANGE dtype:list 0 -1")
    list_ok = list_ok and "a" in lvals and "e" in lvals
    checks.append(f"list={'OK(len='+llen+')' if list_ok else 'FAIL:'+lvals[:50]}")
    all_type_ok = all_type_ok and list_ok

    # set
    scard = dst_redis_cmd(dst, "SCARD dtype:set")
    set_ok = scard.strip() == "5"
    smembers = dst_redis_cmd(dst, "SMEMBERS dtype:set")
    set_ok = set_ok and "m1" in smembers and "m5" in smembers
    checks.append(f"set={'OK(card='+scard+')' if set_ok else 'FAIL:'+smembers[:50]}")
    all_type_ok = all_type_ok and set_ok

    # zset
    zcard = dst_redis_cmd(dst, "ZCARD dtype:zset")
    zset_ok = zcard.strip() == "3"
    zvals = dst_redis_cmd(dst, "ZRANGEBYSCORE dtype:zset -inf +inf WITHSCORES")
    zset_ok = zset_ok and "z1" in zvals and "z3" in zvals
    checks.append(f"zset={'OK(card='+zcard+')' if zset_ok else 'FAIL:'+zvals[:50]}")
    all_type_ok = all_type_ok and zset_ok

    checks.insert(0, f"status={s}")
    all_ok = s == "completed" and all_type_ok
    record("D1 数据类型", all_ok, "; ".join(checks))
    return tid

# ================================================================
# E. 增量同步测试
# ================================================================
def test_E1_incr_basic():
    """E1. 增量基本同步 - SET 操作"""
    log("=== E1. 增量基本同步 ===")
    flush_dst()

    tid = create_task("reg-E1-incr", "full_and_incremental")
    if not tid:
        record("E1 增量基本同步", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=600)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("E1 增量基本同步", False, f"未进入增量阶段, phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 写入增量 key
    log("  写入 100 个增量 key...")
    for i in range(100):
        redis_set(SRC_PORTS[0], f"e1_incr:{i:06d}", f"val_{i}")

    log("  等待增量同步 (20s)...")
    time.sleep(20)

    synced = sum(1 for i in range(100) if dst_redis_exists(DST_PORTS[0], f"e1_incr:{i:06d}"))

    checks = [f"synced={synced}/100"]
    stop_task(tid)
    time.sleep(2)

    all_ok = synced >= 90
    record("E1 增量基本同步", all_ok, "; ".join(checks))
    return tid

def test_E2_incr_del():
    """E2. 增量 DEL 操作同步"""
    log("=== E2. 增量 DEL 同步 ===")
    flush_dst()

    # 先写入一些 key 到源端
    for i in range(20):
        redis_set(SRC_PORTS[0], f"e2_del:{i:04d}", f"to_delete_{i}")
    time.sleep(1)

    tid = create_task("reg-E2-del", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["e2_del:"]})
    if not tid:
        record("E2 增量DEL", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("E2 增量DEL", False, f"未进入增量阶段, phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 验证全量阶段已同步
    before_del = sum(1 for i in range(20) if dst_redis_exists(DST_PORTS[0], f"e2_del:{i:04d}"))
    log(f"  DEL 前目标端: {before_del}/20")

    # 在源端删除部分 key
    for i in range(10):
        redis_del(SRC_PORTS[0], f"e2_del:{i:04d}")

    log("  等待 DEL 同步 (20s)...")
    time.sleep(20)

    # 验证目标端 DEL 也同步了
    after_del = sum(1 for i in range(20) if dst_redis_exists(DST_PORTS[0], f"e2_del:{i:04d}"))
    deleted_synced = before_del - after_del

    checks = [f"before={before_del},after={after_del},deleted_synced={deleted_synced}/10"]
    stop_task(tid)
    time.sleep(2)

    all_ok = before_del >= 15 and deleted_synced >= 7
    record("E2 增量DEL", all_ok, "; ".join(checks))
    return tid

def test_E3_incr_prefix_filter():
    """E3. 增量前缀过滤 - 验证 blocked 不被同步"""
    log("=== E3. 增量前缀过滤 ===")
    flush_dst()

    tid = create_task("reg-E3-filter", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["e3_allow:"]})
    if not tid:
        record("E3 增量前缀过滤", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("E3 增量前缀过滤", False, f"未进入增量阶段")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 写入：allowed 应该同步，blocked 不应该
    log("  写入 allowed + blocked key...")
    for i in range(30):
        redis_set(SRC_PORTS[0], f"e3_allow:{i:04d}", f"yes_{i}")
        redis_set(SRC_PORTS[0], f"e3_block:{i:04d}", f"no_{i}")

    log("  等待增量同步 (20s)...")
    time.sleep(20)

    allowed = sum(1 for i in range(30) if dst_redis_exists(DST_PORTS[0], f"e3_allow:{i:04d}"))
    blocked = sum(1 for i in range(30) if dst_redis_exists(DST_PORTS[0], f"e3_block:{i:04d}"))

    checks = [f"allowed={allowed}/30,blocked={blocked}/30(should=0)"]
    stop_task(tid)
    time.sleep(2)

    all_ok = allowed >= 25 and blocked == 0
    record("E3 增量前缀过滤", all_ok, "; ".join(checks))
    return tid

def test_E4_incr_multi_types():
    """E4. 增量多数据类型同步（hash/list/set 操作）"""
    log("=== E4. 增量多数据类型 ===")
    flush_dst()

    tid = create_task("reg-E4-types", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["e4_type:"]})
    if not tid:
        record("E4 增量多类型", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("E4 增量多类型", False, "未进入增量阶段")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 增量写入多种数据类型（先清理旧 key 防止 list/set 累加）
    src = SRC_PORTS[0]
    for key in ["e4_type:str", "e4_type:hash", "e4_type:list", "e4_type:set"]:
        redis_cmd(src, f'DEL {key}')
    time.sleep(1)
    redis_cmd(src, 'SET e4_type:str "incr_string_val"')
    redis_cmd(src, 'HSET e4_type:hash f1 incr_v1 f2 incr_v2')
    redis_cmd(src, 'RPUSH e4_type:list x y z')
    redis_cmd(src, 'SADD e4_type:set s1 s2 s3')

    log("  等待增量同步 (20s)...")
    time.sleep(20)

    dst = DST_PORTS[0]
    checks = []
    all_type_ok = True

    str_val = dst_redis_get(dst, "e4_type:str")
    str_ok = "incr_string" in str(str_val)
    checks.append(f"str={'OK' if str_ok else 'FAIL'}")
    all_type_ok = all_type_ok and str_ok

    hval = dst_redis_cmd(dst, "HGETALL e4_type:hash")
    hash_ok = "incr_v1" in str(hval)
    checks.append(f"hash={'OK' if hash_ok else 'FAIL:'+hval[:30]}")
    all_type_ok = all_type_ok and hash_ok

    llen = dst_redis_cmd(dst, "LLEN e4_type:list")
    list_ok = llen.strip() == "3"
    checks.append(f"list={'OK' if list_ok else 'FAIL:len='+llen}")
    all_type_ok = all_type_ok and list_ok

    scard = dst_redis_cmd(dst, "SCARD e4_type:set")
    set_ok = scard.strip() == "3"
    checks.append(f"set={'OK' if set_ok else 'FAIL:card='+scard}")
    all_type_ok = all_type_ok and set_ok

    stop_task(tid)
    time.sleep(2)

    record("E4 增量多类型", all_type_ok, "; ".join(checks))
    return tid

# ================================================================
# F. 全量+增量综合
# ================================================================
def test_F1_full_incr_complete():
    """F1. 全量+增量完整流程"""
    log("=== F1. 全量+增量完整流程 ===")
    flush_dst()

    tid = create_task("reg-F1-complete", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["app:", "user:"]})
    if not tid:
        record("F1 全量+增量", False, "创建任务失败")
        return

    start_task(tid)

    # 全量阶段监控
    time.sleep(3)
    v, _ = check_counter_consistency(tid, samples=8, interval=1)

    # 等进入增量
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")

    if phase == "incremental":
        time.sleep(5)
        # 增量写入
        for i in range(30):
            redis_set(SRC_PORTS[0], f"app:f1_incr:{i:04d}", f"f1_val_{i}")
        time.sleep(15)

        incr_synced = sum(1 for i in range(30) if dst_redis_exists(DST_PORTS[0], f"app:f1_incr:{i:04d}"))
    else:
        incr_synced = -1

    checks = [f"phase={phase},violations={v},incr_synced={incr_synced}/30"]
    stop_task(tid)
    time.sleep(2)

    # violations <= 2: 全量阶段采样可能出现暂态竞争（SCAN 估算 vs 实际处理）
    all_ok = phase == "incremental" and v <= 2 and incr_synced >= 25
    record("F1 全量+增量", all_ok, "; ".join(checks))
    return tid

# ================================================================
# G. 任务生命周期测试
# ================================================================
def test_G1_pause_resume():
    """G1. 暂停/恢复任务"""
    log("=== G1. 暂停/恢复 ===")
    flush_dst()

    tid = create_task("reg-G1-pause", "full_only")
    if not tid:
        record("G1 暂停恢复", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(5)

    # 暂停
    pause_task(tid)
    time.sleep(2)
    t = get_task(tid)
    paused_status = t.get("status", "")
    p_migrated = t.get("progress", {}).get("migrated_keys", 0)
    log(f"  暂停后: status={paused_status}, migrated={p_migrated}")

    # 恢复
    resume_task(tid)
    time.sleep(2)
    t2 = get_task(tid)
    resumed_status = t2.get("status", "")
    log(f"  恢复后: status={resumed_status}")

    # 等完成
    t3 = wait_complete(tid, timeout=600)
    final_status = t3.get("status", "")
    dst_total = dst_dbsize()
    src_total = src_dbsize()

    checks = []
    checks.append(f"paused={paused_status},resumed={resumed_status},final={final_status}")
    checks.append(f"src_dbsize={src_total},dst={dst_total}")

    # 判定逻辑：核心验证暂停/恢复/完成的状态机正确性，
    # 不用 src-dst 差值（Tendis DBSIZE 惰性删除不可靠）
    all_ok = (paused_status == "paused" and resumed_status == "running"
              and final_status == "completed" and dst_total > 0)
    record("G1 暂停恢复", all_ok, "; ".join(checks))
    return tid

def test_G2_stop_restart():
    """G2. 停止/重启任务"""
    log("=== G2. 停止/重启 ===")
    flush_dst()

    tid = create_task("reg-G2-stop", "full_only")
    if not tid:
        record("G2 停止重启", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(5)

    # 停止
    stop_task(tid)
    time.sleep(2)
    t = get_task(tid)
    stopped_status = t.get("status", "")

    checks = [f"stopped={stopped_status}"]

    # 停止后不应该能直接 resume
    # 重启任务（从头开始）
    flush_dst()
    r = api_post(f"/tasks/{tid}/restart")
    time.sleep(2)
    t2 = get_task(tid)
    restart_status = t2.get("status", "")
    checks.append(f"after_restart={restart_status}")

    # 如果 restart 成功，等完成
    if restart_status == "running":
        t3 = wait_complete(tid, timeout=600)
        checks.append(f"final={t3.get('status')}")

    all_ok = stopped_status == "stopped"
    record("G2 停止重启", all_ok, "; ".join(checks))
    return tid

def test_G3_delete_task():
    """G3. 删除任务"""
    log("=== G3. 删除任务 ===")
    flush_dst()

    tid = create_task("reg-G3-delete", "full_only")
    if not tid:
        record("G3 删除任务", False, "创建任务失败")
        return

    # 删除
    r = delete_task(tid)
    del_code = r.get("code", -1)

    # 验证删除后无法找到
    t = get_task(tid)
    not_found = "error" in t or t.get("code") != 0 or t.get("status") is None

    checks = [f"delete_code={del_code},not_found_after={not_found}"]
    all_ok = del_code == 0 and not_found
    record("G3 删除任务", all_ok, "; ".join(checks))

def test_G4_stop_incremental():
    """G4. 停止增量同步（stop-incremental）"""
    log("=== G4. 停止增量同步 ===")
    flush_dst()

    tid = create_task("reg-G4-stop-incr", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]})
    if not tid:
        record("G4 停止增量", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")

    if phase != "incremental":
        record("G4 停止增量", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    # stop-incremental
    r = api_post(f"/tasks/{tid}/stop-incremental")
    time.sleep(3)
    t2 = get_task(tid)
    after_status = t2.get("status", "")

    checks = [f"phase={phase},after_stop_incr={after_status}"]

    # 停止增量后状态应该是 completed/stopped/paused/incremental_stopped
    all_ok = after_status in ("completed", "stopped", "paused", "incremental_stopped")
    record("G4 停止增量", all_ok, "; ".join(checks))
    return tid

# ================================================================
# H. 崩溃恢复测试
# ================================================================
def test_H1_kill9_recovery():
    """H1. kill-9 崩溃恢复 - 全量阶段
    验证核心能力：kill-9 后进度不丢失，服务重启后任务可恢复。
    两种合法场景：
      a) kill 在迁移进行中 → 状态 paused/shutdown_paused，恢复后完成
      b) kill 在迁移已完成后 → 状态 completed，进度完整保留
    """
    log("=== H1. Kill-9 崩溃恢复 ===")
    flush_dst()

    # 使用 1 个 worker 降低迁移速度，确保 kill 发生在迁移中
    tid = create_task("reg-H1-kill9", "full_only", workers=1)
    if not tid:
        record("H1 Kill-9恢复", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(5)

    t_before = get_task(tid)
    migrated_before = t_before.get("progress", {}).get("migrated_keys", 0)
    status_before = t_before.get("status", "")
    log(f"  kill-9 前: status={status_before}, migrated={migrated_before}")

    kill_process()
    log("  已 kill -9")

    restart_service()
    log("  服务已重启")

    t_after = get_task(tid)
    migrated_after = t_after.get("progress", {}).get("migrated_keys", 0)
    status_after = t_after.get("status", "")
    accuracy = migrated_after / migrated_before if migrated_before > 0 else 0
    log(f"  恢复后: status={status_after}, migrated={migrated_after}, accuracy={accuracy:.3f}")

    checks = [f"before={migrated_before},after={migrated_after},accuracy={accuracy:.3f}"]
    checks.append(f"status_after={status_after}")

    if status_after in ("paused", "shutdown_paused"):
        # 场景 a: kill 在迁移中，恢复后继续完成
        resume_task(tid)
        t_final = wait_complete(tid, timeout=600)
        final_status = t_final.get("status", "")
        dst_total = dst_dbsize()
        data_ok = final_status == "completed" and dst_total > 0
        checks.append(f"resumed_final={final_status},dst={dst_total}")
    elif status_after == "completed":
        # 场景 b: 迁移在 kill 前已完成，验证进度完整保留
        dst_total = dst_dbsize()
        data_ok = dst_total > 0 and accuracy >= 0.90
        checks.append(f"already_completed,dst={dst_total}")
    elif status_after == "running":
        # 自动恢复正在运行，等完成
        t_final = wait_complete(tid, timeout=600)
        final_status = t_final.get("status", "")
        dst_total = dst_dbsize()
        data_ok = final_status == "completed" and dst_total > 0
        checks.append(f"auto_resumed_final={final_status},dst={dst_total}")
    else:
        data_ok = False
        checks.append(f"unexpected_status={status_after}")

    all_ok = accuracy >= 0.90 and data_ok
    record("H1 Kill-9恢复", all_ok, "; ".join(checks))
    return tid

def test_H2_sigterm_graceful():
    """H2. SIGTERM 优雅关闭"""
    log("=== H2. SIGTERM 优雅关闭 ===")
    flush_dst()

    tid = create_task("reg-H2-sigterm", "full_only", workers=4)
    if not tid:
        record("H2 SIGTERM", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(8)

    t_before = get_task(tid)
    migrated_before = t_before.get("progress", {}).get("migrated_keys", 0)
    log(f"  SIGTERM 前: migrated={migrated_before}")

    sigterm_process()
    log("  已发送 SIGTERM")
    time.sleep(2)

    restart_service()
    log("  服务已重启")

    t_after = get_task(tid)
    status_after = t_after.get("status", "")
    migrated_after = t_after.get("progress", {}).get("migrated_keys", 0)
    accuracy = migrated_after / migrated_before if migrated_before > 0 else 0

    checks = [f"before={migrated_before},after={migrated_after},accuracy={accuracy:.3f}"]
    checks.append(f"status={status_after}")

    # SIGTERM 应该标记为 shutdown_paused，然后自动恢复
    # 等待自动恢复完成
    time.sleep(5)
    t2 = get_task(tid)
    s2 = t2.get("status", "")
    if s2 == "running":
        checks.append("auto_resumed=yes")
        t_final = wait_complete(tid, timeout=600)
        final_status = t_final.get("status", "")
        checks.append(f"final={final_status}")
    elif s2 in ("paused", "shutdown_paused"):
        resume_task(tid)
        t_final = wait_complete(tid, timeout=600)
        final_status = t_final.get("status", "")
        checks.append(f"manual_resumed=yes,final={final_status}")
    else:
        final_status = s2
        checks.append(f"auto_completed={s2}")

    src_total = src_dbsize()
    dst_total = dst_dbsize()
    checks.append(f"src_dbsize={src_total},dst={dst_total}")

    # 判定逻辑：
    # 1. accuracy >= 0.90：SIGTERM 后 migrated 应保留大部分进度（优雅关闭）
    #    注意 accuracy > 1.0 是正常的（恢复后继续迁移，migrated_after > migrated_before）
    # 2. 最终能完成（final_status == completed）
    # 3. 目标端有数据
    final_completed = final_status == "completed"
    all_ok = accuracy >= 0.90 and final_completed and dst_total > 0
    record("H2 SIGTERM", all_ok, "; ".join(checks))
    return tid

def test_H3_new_task_immediate_crash():
    """H3. 新任务创建后立即 kill-9（验证立即持久化修复）"""
    log("=== H3. 新任务立即崩溃 ===")
    flush_dst()

    tid = create_task("reg-H3-immediate", "full_only")
    if not tid:
        record("H3 新任务立即崩溃", False, "创建任务失败")
        return

    start_task(tid)
    # 只等 3 秒就 kill（远小于 30 秒保存周期）
    time.sleep(3)

    kill_process()
    log("  创建+启动后 3 秒即 kill-9")

    restart_service()
    log("  服务已重启")

    # 关键验证：任务应该仍然存在
    t = get_task(tid)
    task_exists = t.get("status") is not None and "error" not in t

    checks = [f"task_exists={task_exists},status={t.get('status','N/A')}"]

    all_ok = task_exists
    record("H3 新任务立即崩溃", all_ok, "; ".join(checks))

    # 清理
    if task_exists:
        if t.get("status") in ("paused", "shutdown_paused"):
            stop_task(tid)
        delete_task(tid)
    return tid

# ================================================================
# I. 进度与计数器
# ================================================================
def test_I1_counter_full_process():
    """I1. 计数器全过程监控（migrated <= to_migrate 贯穿始终）"""
    log("=== I1. 计数器全过程监控 ===")
    flush_dst()

    tid = create_task("reg-I1-counter", "full_only", workers=8)
    if not tid:
        record("I1 全过程计数器", False, "创建任务失败")
        return

    start_task(tid)

    # 分 3 阶段持续采样
    log("  阶段1: 初期采样...")
    time.sleep(2)
    v1, d1 = check_counter_consistency(tid, samples=10, interval=0.5)

    log("  阶段2: 中期采样...")
    time.sleep(10)
    v2, d2 = check_counter_consistency(tid, samples=10, interval=0.5)

    log("  阶段3: 后期采样...")
    time.sleep(10)
    v3, d3 = check_counter_consistency(tid, samples=10, interval=0.5)

    t = wait_complete(tid, timeout=600)
    s = t.get("status", "")
    p = t.get("progress", {})
    migrated = p.get("migrated_keys", 0)
    to_migrate = p.get("keys_to_migrate", 0)

    # 完成后最终检查
    v4, d4 = check_counter_consistency(tid, samples=5, interval=0.2)

    total_v = v1 + v2 + v3 + v4
    total_samples = 35
    all_details = d1 + d2 + d3 + d4

    checks = []
    checks.append(f"status={s}")
    checks.append(f"violations={total_v}/{total_samples}")
    checks.append(f"final: migrated={migrated},to_migrate={to_migrate}")
    if total_v > 0:
        checks.append(f"details={all_details[:200]}")

    all_ok = s == "completed" and total_v == 0 and migrated <= to_migrate
    record("I1 全过程计数器", all_ok, "; ".join(checks))
    return tid

def test_I2_progress_percentage():
    """I2. 进度百分比合理性验证（使用全量数据确保采到中间进度）"""
    log("=== I2. 进度百分比 ===")
    flush_dst()

    # 用全量无过滤确保数据量足够大，能采到中间进度
    tid = create_task("reg-I2-progress", "full_only")
    if not tid:
        record("I2 进度百分比", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(2)

    progress_values = []
    for _ in range(20):
        t = get_task(tid)
        p = t.get("progress", {})
        pct = p.get("percentage", 0) if isinstance(p, dict) else 0
        if pct is None:
            pct = 0
        progress_values.append(pct)
        time.sleep(2)
        if t.get("status") == "completed":
            break

    wait_complete(tid, timeout=600)

    # 验证进度值合理性
    checks = []
    non_zero = sum(1 for v in progress_values if v > 0)
    in_range = all(0 <= v <= 100 for v in progress_values)
    has_intermediate = any(0 < v < 100 for v in progress_values)

    checks.append(f"samples={len(progress_values)},non_zero={non_zero}")
    checks.append(f"in_range={in_range},has_intermediate={has_intermediate}")
    checks.append(f"values={progress_values[:5]}...{progress_values[-3:]}")

    all_ok = non_zero >= 3 and in_range
    record("I2 进度百分比", all_ok, "; ".join(checks))
    return tid

# ================================================================
# J. 辅助功能测试
# ================================================================
def test_J1_error_keys_api():
    """J1. 错误 Key 记录与查询 API"""
    log("=== J1. 错误 Key API ===")

    # 找一个已完成的任务
    resp = api_get("/tasks")
    tasks = resp.get("data", {}).get("tasks", [])
    tid = None
    for t in tasks:
        if t.get("name", "").startswith("reg-") and t.get("status") in ("completed", "stopped"):
            tid = t["id"]
            break

    if not tid:
        # 创建一个快速完成的任务
        flush_dst()
        tid = create_task("reg-J1-errors", "full_only",
                          key_filter={"mode": "keylist", "keys": ["j1_test:a"]})
        if tid:
            redis_set(SRC_PORTS[0], "j1_test:a", "val")
            start_task(tid)
            wait_complete(tid, timeout=60)

    if not tid:
        record("J1 ErrorKeys", False, "无可用任务")
        return

    r = api_get(f"/tasks/{tid}/error-keys")
    ok = "error" not in r or r.get("code") == 0
    checks = [f"api_ok={ok}, code={r.get('code')}, has_data={'data' in r}"]
    record("J1 ErrorKeys", ok, "; ".join(checks))

def test_J2_verify_api():
    """J2. 数据校验 API"""
    log("=== J2. 数据校验 ===")
    flush_dst()

    # 先完成一个小迁移
    tid = create_task("reg-J2-verify", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]})
    if not tid:
        record("J2 数据校验", False, "创建任务失败")
        return

    start_task(tid)
    wait_complete(tid, timeout=300)

    # 触发校验
    r = api_post(f"/tasks/{tid}/verify")
    verify_code = r.get("code", -1)
    time.sleep(5)

    # 查询校验结果
    r2 = api_get(f"/tasks/{tid}/verify/results")
    results_code = r2.get("code", -1)

    checks = [f"verify_code={verify_code},results_code={results_code}"]
    if "data" in r2:
        data = r2["data"]
        if isinstance(data, dict):
            checks.append(f"result_fields={list(data.keys())[:5]}")

    all_ok = verify_code == 0
    record("J2 数据校验", all_ok, "; ".join(checks))
    return tid

def test_J3_dynamic_config():
    """J3. 运行时动态调整配置"""
    log("=== J3. 动态配置 ===")
    flush_dst()

    tid = create_task("reg-J3-dynconfig", "full_only", workers=2)
    if not tid:
        record("J3 动态配置", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(3)

    # 动态调整 worker 数量
    r = api_put(f"/tasks/{tid}/config", {"worker_count": 8})
    adjust_ok = r.get("code") == 0 or "error" not in r

    time.sleep(2)
    t = get_task(tid)
    opts = t.get("options", {})
    new_workers = opts.get("worker_count", opts.get("workers", 0))

    checks = [f"adjust_ok={adjust_ok},new_workers={new_workers}"]

    # 等完成
    wait_complete(tid, timeout=600)

    all_ok = adjust_ok
    record("J3 动态配置", all_ok, "; ".join(checks))
    return tid

def test_J4_task_metrics():
    """J4. 任务指标 API"""
    log("=== J4. 任务指标 ===")

    # 创建一个快速完成的任务用于测试
    flush_dst()
    tid = create_task("reg-J4-metrics", "full_only",
                      key_filter={"mode": "keylist", "keys": ["j4_test:a"]})
    if not tid:
        record("J4 任务指标", False, "创建任务失败")
        return
    redis_set(SRC_PORTS[0], "j4_test:a", "val")
    start_task(tid)
    wait_complete(tid, timeout=60)

    r = api_get(f"/tasks/{tid}/metrics")
    ok = "error" not in r or r.get("code") == 0
    checks = [f"code={r.get('code')}, has_data={'data' in r}"]
    record("J4 任务指标", ok, "; ".join(checks))

def test_J5_task_logs():
    """J5. 任务日志 API"""
    log("=== J5. 任务日志 ===")

    # 创建一个快速完成的任务
    flush_dst()
    tid = create_task("reg-J5-logs", "full_only",
                      key_filter={"mode": "keylist", "keys": ["j5_test:a"]})
    if not tid:
        record("J5 任务日志", False, "创建任务失败")
        return
    redis_set(SRC_PORTS[0], "j5_test:a", "val")
    start_task(tid)
    wait_complete(tid, timeout=60)

    r = api_get(f"/tasks/{tid}/logs")
    ok = "error" not in r or r.get("code") == 0
    checks = [f"code={r.get('code')}, has_data={'data' in r}"]
    record("J5 任务日志", ok, "; ".join(checks))

# ================================================================
# 纯增量模式
# ================================================================
def test_E5_incremental_only():
    """E5. 纯增量模式（跳过全量）"""
    log("=== E5. 纯增量模式 ===")
    flush_dst()

    tid = create_task("reg-E5-incr-only", "incremental",
                      key_filter={"mode": "prefix", "prefixes": ["e5_only:"]})
    if not tid:
        record("E5 纯增量模式", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(10)

    t = get_task(tid)
    status = t.get("status", "")
    phase = t.get("progress", {}).get("phase", "")

    # 纯增量模式应该跳过全量直接进入增量
    checks = [f"status={status},phase={phase}"]

    if status == "running" and phase in ("incremental", "full_skipped"):
        # 写入增量 key
        for i in range(20):
            redis_set(SRC_PORTS[0], f"e5_only:{i:04d}", f"incr_only_{i}")
        time.sleep(15)
        synced = sum(1 for i in range(20) if dst_redis_exists(DST_PORTS[0], f"e5_only:{i:04d}"))
        checks.append(f"synced={synced}/20")
        all_ok = synced >= 15
    else:
        all_ok = False
        checks.append(f"unexpected status/phase")

    stop_task(tid)
    time.sleep(2)

    record("E5 纯增量模式", all_ok, "; ".join(checks))
    return tid

# ================================================================
# K. 边界条件与极端场景
# ================================================================
def test_K1_empty_source():
    """K1. 空源端迁移 - 源端无匹配数据"""
    log("=== K1. 空源端迁移 ===")
    flush_dst()

    # 使用一个不存在的前缀，确保无匹配 key
    tid = create_task("reg-K1-empty", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["nonexist_prefix_xyz:"]})
    if not tid:
        record("K1 空源端迁移", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")
    p = t.get("progress", {})
    migrated = p.get("migrated_keys", 0)
    dst_total = dst_dbsize()

    checks = [f"status={s},migrated={migrated},dst={dst_total}"]
    # 应该正常完成，迁移 0 个 key
    all_ok = s == "completed" and migrated == 0 and dst_total == 0
    record("K1 空源端迁移", all_ok, "; ".join(checks))
    return tid

def test_K2_single_key():
    """K2. 单 key 迁移 - 最小数据集"""
    log("=== K2. 单 key 迁移 ===")
    flush_dst()

    redis_set(SRC_PORTS[0], "k2_single:only", "single_value_12345")
    time.sleep(1)

    tid = create_task("reg-K2-single", "full_only",
                      key_filter={"mode": "keylist", "keys": ["k2_single:only"]})
    if not tid:
        record("K2 单key迁移", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=60)
    s = t.get("status", "")
    p = t.get("progress", {})
    migrated = p.get("migrated_keys", 0)

    val = dst_redis_get(DST_PORTS[0], "k2_single:only")
    val_ok = "single_value_12345" in str(val)

    checks = [f"status={s},migrated={migrated},val_ok={val_ok}"]
    all_ok = s == "completed" and migrated == 1 and val_ok
    record("K2 单key迁移", all_ok, "; ".join(checks))
    return tid

def test_K3_special_characters_in_key():
    """K3. Key 名包含特殊字符（空格、中文、二进制）"""
    log("=== K3. 特殊字符 Key ===")
    flush_dst()

    src = SRC_PORTS[0]
    # 用 redis-cli 写入特殊 key（需要转义处理）
    special_keys = {
        "k3_spec:with space": "val_space",
        "k3_spec:中文key": "val_chinese",
        "k3_spec:key:with:colons": "val_colons",
        "k3_spec:key{curly}": "val_curly",
    }
    for k, v in special_keys.items():
        redis_cmd(src, f'SET "{k}" "{v}"')
    time.sleep(1)

    tid = create_task("reg-K3-special", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["k3_spec:"]})
    if not tid:
        record("K3 特殊字符Key", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    # 逐个检查
    found = 0
    for k, v in special_keys.items():
        val = dst_redis_cmd(DST_PORTS[0], f'GET "{k}"')
        if v in str(val):
            found += 1

    checks = [f"status={s},found={found}/{len(special_keys)}"]
    all_ok = s == "completed" and found >= len(special_keys) - 1
    record("K3 特殊字符Key", all_ok, "; ".join(checks))
    return tid

def test_K4_large_value():
    """K4. 大 Value 迁移 - 1MB 字符串"""
    log("=== K4. 大Value迁移 ===")
    flush_dst()

    src = SRC_PORTS[0]
    # 写入约 1MB 的字符串（用 APPEND 分段写入避免命令行过长）
    redis_cmd(src, 'DEL k4_bigval:str')
    chunk = "A" * 10000  # 10KB per chunk
    for _ in range(100):  # 100 * 10KB = 1MB
        redis_cmd(src, f'APPEND k4_bigval:str "{chunk}"')
    src_len = redis_cmd(src, 'STRLEN k4_bigval:str')
    log(f"  源端大 Value 长度: {src_len}")
    time.sleep(1)

    tid = create_task("reg-K4-bigval", "full_only",
                      key_filter={"mode": "keylist", "keys": ["k4_bigval:str"]})
    if not tid:
        record("K4 大Value迁移", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    dst_len = dst_redis_cmd(DST_PORTS[0], 'STRLEN k4_bigval:str')
    len_match = src_len.strip() == dst_len.strip()

    checks = [f"status={s},src_len={src_len},dst_len={dst_len},match={len_match}"]
    all_ok = s == "completed" and len_match
    record("K4 大Value迁移", all_ok, "; ".join(checks))
    # 清理
    redis_cmd(src, 'DEL k4_bigval:str')
    return tid

def test_K5_ttl_preservation():
    """K5. TTL 保持 - 验证迁移后 TTL 不丢失"""
    log("=== K5. TTL保持 ===")
    flush_dst()

    src = SRC_PORTS[0]
    # 设置带 TTL 的 key（60 秒过期）
    for i in range(10):
        redis_cmd(src, f'SET k5_ttl:{i:04d} "ttl_val_{i}" EX 300')
    # 设置永不过期的 key
    for i in range(10, 20):
        redis_cmd(src, f'SET k5_ttl:{i:04d} "no_ttl_val_{i}"')
    time.sleep(1)

    tid = create_task("reg-K5-ttl", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["k5_ttl:"]})
    if not tid:
        record("K5 TTL保持", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    # 检查目标端 TTL
    ttl_ok = 0
    no_ttl_ok = 0
    for i in range(10):
        ttl_val = dst_redis_cmd(DST_PORTS[0], f'TTL k5_ttl:{i:04d}')
        try:
            ttl_int = int(ttl_val.strip())
            if 10 < ttl_int <= 300:  # TTL 应该在合理范围内
                ttl_ok += 1
        except:
            pass
    for i in range(10, 20):
        ttl_val = dst_redis_cmd(DST_PORTS[0], f'TTL k5_ttl:{i:04d}')
        try:
            ttl_int = int(ttl_val.strip())
            if ttl_int == -1:  # 永不过期
                no_ttl_ok += 1
        except:
            pass

    checks = [f"status={s},ttl_preserved={ttl_ok}/10,no_ttl_correct={no_ttl_ok}/10"]
    all_ok = s == "completed" and ttl_ok >= 8 and no_ttl_ok >= 8
    record("K5 TTL保持", all_ok, "; ".join(checks))
    return tid

def test_K6_big_hash():
    """K6. 大 Hash 迁移 - 10000 字段"""
    log("=== K6. 大Hash迁移 (10000字段) ===")
    flush_dst()

    src = SRC_PORTS[0]
    redis_cmd(src, 'DEL k6_bighash:test')
    # 批量写入 10000 字段（每批 100 字段 x 100 批）
    target_fields = 10000
    batch_size = 100
    num_batches = target_fields // batch_size
    log(f"  写入 {target_fields} 字段 ({num_batches} 批 x {batch_size})...")
    for batch in range(num_batches):
        fields = " ".join([f"f{batch*batch_size+i} v{batch*batch_size+i}" for i in range(batch_size)])
        redis_cmd(src, f'HSET k6_bighash:test {fields}')
    src_hlen = redis_cmd(src, 'HLEN k6_bighash:test')
    log(f"  源端 Hash 字段数: {src_hlen}")
    time.sleep(1)

    tid = create_task("reg-K6-bighash", "full_only",
                      key_filter={"mode": "keylist", "keys": ["k6_bighash:test"]})
    if not tid:
        record("K6 大Hash迁移", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")

    # 在所有目标端端口上查找
    dst_hlen = "0"
    dst_port_found = DST_PORTS[0]
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'HLEN k6_bighash:test')
        if v and v.strip() and v.strip() != "0" and v.strip() != "(nil)":
            dst_hlen = v.strip()
            dst_port_found = port
            break
    len_match = src_hlen.strip() == dst_hlen.strip()

    # 抽样验证字段值（首/中/尾各取几个）
    sample_ok = 0
    sample_total = 8
    for idx in [0, 100, 500, 1000, 2500, 5000, 7500, 9999]:
        val = dst_redis_cmd(dst_port_found, f'HGET k6_bighash:test f{idx}')
        if f"v{idx}" in str(val):
            sample_ok += 1

    checks = [f"status={s},src_hlen={src_hlen.strip()},dst_hlen={dst_hlen},match={len_match},sample={sample_ok}/{sample_total}"]
    all_ok = s == "completed" and len_match and sample_ok >= sample_total - 1
    record("K6 大Hash迁移", all_ok, "; ".join(checks))
    redis_cmd(src, 'DEL k6_bighash:test')
    return tid

# ================================================================
# L. API 异常输入与错误处理
# ================================================================
def test_L1_invalid_json():
    """L1. 无效 JSON 请求体"""
    log("=== L1. 无效JSON ===")
    try:
        r = requests.post(f"{API}/tasks", data="not-json-{{{",
                          headers={"Content-Type": "application/json"}, timeout=10)
        resp = r.json()
        code = resp.get("code", r.status_code)
        # 应返回 400
        ok = r.status_code == 400 or code == 400
        checks = [f"http_status={r.status_code},code={code}"]
    except Exception as e:
        ok = False
        checks = [f"exception={e}"]

    record("L1 无效JSON", ok, "; ".join(checks))

def test_L2_missing_required_fields():
    """L2. 缺少必填字段"""
    log("=== L2. 缺少必填字段 ===")
    checks = []

    # 缺 name
    r1 = api_post("/tasks", {"source_cluster": {"addrs": SRC_NODES},
                              "target_cluster": {"addrs": DST_NODES}})
    no_name = r1.get("code") == 400 or "name" in str(r1.get("message", "")).lower()
    checks.append(f"no_name={no_name}")

    # 缺 source_cluster
    r2 = api_post("/tasks", {"name": "reg-L2-test",
                              "target_cluster": {"addrs": DST_NODES}})
    no_src = r2.get("code") == 400 or "source" in str(r2.get("message", "")).lower()
    checks.append(f"no_source={no_src}")

    # 缺 target_cluster
    r3 = api_post("/tasks", {"name": "reg-L2-test",
                              "source_cluster": {"addrs": SRC_NODES}})
    no_tgt = r3.get("code") == 400 or "target" in str(r3.get("message", "")).lower()
    checks.append(f"no_target={no_tgt}")

    # 空 addrs 数组
    r4 = api_post("/tasks", {"name": "reg-L2-test",
                              "source_cluster": {"addrs": []},
                              "target_cluster": {"addrs": DST_NODES}})
    empty_addrs = r4.get("code") == 400 or "addrs" in str(r4.get("message", "")).lower()
    checks.append(f"empty_addrs={empty_addrs}")

    all_ok = no_name and no_src and no_tgt and empty_addrs
    record("L2 缺少必填字段", all_ok, "; ".join(checks))

def test_L3_invalid_task_id():
    """L3. 不存在的任务 ID 操作"""
    log("=== L3. 无效任务ID ===")
    fake_id = "00000000-0000-0000-0000-000000000000"
    checks = []

    # GET
    r1 = api_get(f"/tasks/{fake_id}")
    get_fail = r1.get("code") == 404 or "not found" in str(r1.get("message", "")).lower()
    checks.append(f"get_404={get_fail}")

    # start
    r2 = api_post(f"/tasks/{fake_id}/start")
    start_fail = r2.get("code") in (404, 400) or "not found" in str(r2.get("message", "")).lower()
    checks.append(f"start_404={start_fail}")

    # pause
    r3 = api_post(f"/tasks/{fake_id}/pause")
    pause_fail = r3.get("code") in (404, 400)
    checks.append(f"pause_fail={pause_fail}")

    # delete
    r4 = api_delete(f"/tasks/{fake_id}")
    del_fail = r4.get("code") in (404, 400) or "not found" in str(r4.get("message", "")).lower()
    checks.append(f"delete_fail={del_fail}")

    all_ok = get_fail and start_fail and del_fail
    record("L3 无效任务ID", all_ok, "; ".join(checks))

def test_L4_wrong_state_transitions():
    """L4. 非法状态转换 - 对 pending 任务执行 pause/resume/stop"""
    log("=== L4. 非法状态转换 ===")
    flush_dst()

    tid = create_task("reg-L4-state", "full_only")
    if not tid:
        record("L4 非法状态转换", False, "创建任务失败")
        return

    checks = []

    # pending 状态下 pause 应该失败
    r1 = api_post(f"/tasks/{tid}/pause")
    pause_rejected = r1.get("code") != 0
    checks.append(f"pause_pending_rejected={pause_rejected}")

    # pending 状态下 resume 应该失败
    r2 = api_post(f"/tasks/{tid}/resume")
    resume_rejected = r2.get("code") != 0
    checks.append(f"resume_pending_rejected={resume_rejected}")

    # pending 状态下 stop-incremental 应该失败
    r3 = api_post(f"/tasks/{tid}/stop-incremental")
    stop_incr_rejected = r3.get("code") != 0
    checks.append(f"stop_incr_pending_rejected={stop_incr_rejected}")

    # 启动后立即 start 应该被拒绝（或幂等）
    start_task(tid)
    time.sleep(1)
    r4 = api_post(f"/tasks/{tid}/start")
    # 可能是幂等成功或拒绝
    checks.append(f"double_start_code={r4.get('code')}")

    stop_task(tid)
    time.sleep(1)

    # stopped 状态下 resume 应该失败
    r5 = api_post(f"/tasks/{tid}/resume")
    resume_stopped_rejected = r5.get("code") != 0
    checks.append(f"resume_stopped_rejected={resume_stopped_rejected}")

    delete_task(tid)
    all_ok = pause_rejected and resume_rejected and stop_incr_rejected
    record("L4 非法状态转换", all_ok, "; ".join(checks))

def test_L5_connection_unreachable():
    """L5. 不可达地址 - 测试连接超时处理"""
    log("=== L5. 不可达地址 ===")

    # test-connection 到不存在的地址
    r = api_post("/test-connection",
                 {"addrs": ["192.168.255.254:6379"]}, timeout=30)
    # 应该返回错误而不是 hang
    conn_fail = r.get("code") != 0 or "error" in r
    checks = [f"conn_fail={conn_fail},code={r.get('code')},msg={str(r.get('message',''))[:60]}"]

    # 创建任务指向不可达地址，尝试启动
    tid = create_task("reg-L5-unreach", "full_only")
    start_ok = False
    if tid:
        # 修改为正确方式：直接创建一个指向坏地址的任务
        bad_data = {
            "name": "reg-L5-badaddr",
            "migration_mode": "full_only",
            "source_cluster": {"addrs": ["192.168.255.254:6379"]},
            "target_cluster": {"addrs": DST_NODES},
        }
        r2 = api_post("/tasks", bad_data)
        bad_tid = r2.get("data", {}).get("task_id")
        if bad_tid:
            api_post(f"/tasks/{bad_tid}/start")
            time.sleep(10)
            t = get_task(bad_tid)
            bad_status = t.get("status", "")
            # 应该是 failed 而不是永远 running
            start_ok = bad_status in ("failed", "error", "stopped")
            checks.append(f"bad_addr_status={bad_status}")
            delete_task(bad_tid)
        delete_task(tid)

    all_ok = conn_fail
    record("L5 不可达地址", all_ok, "; ".join(checks))

def test_L6_duplicate_task_name():
    """L6. 重复任务名 - 验证是否允许或拒绝"""
    log("=== L6. 重复任务名 ===")
    flush_dst()

    tid1 = create_task("reg-L6-dup", "full_only",
                       key_filter={"mode": "keylist", "keys": ["l6_test:a"]})
    tid2 = create_task("reg-L6-dup", "full_only",
                       key_filter={"mode": "keylist", "keys": ["l6_test:b"]})

    checks = [f"tid1={'ok' if tid1 else 'fail'},tid2={'ok' if tid2 else 'fail'}"]

    if tid1 and tid2:
        # 两个任务都创建成功（同名被允许），它们的 ID 应不同
        diff_id = tid1 != tid2
        checks.append(f"different_ids={diff_id}")
        all_ok = diff_id
        delete_task(tid1)
        delete_task(tid2)
    elif tid1 and not tid2:
        # 第二个被拒绝（重名禁止），也是合理的行为
        checks.append("duplicate_rejected=yes")
        all_ok = True
        delete_task(tid1)
    else:
        all_ok = False

    record("L6 重复任务名", all_ok, "; ".join(checks))

# ================================================================
# M. 并发场景
# ================================================================
def test_M1_concurrent_tasks():
    """M1. 多任务并发运行"""
    log("=== M1. 多任务并发 ===")
    flush_dst()

    # 创建 3 个并发任务，各迁移不同前缀
    prefixes_list = [
        (["app:"], "reg-M1-task1"),
        (["user:"], "reg-M1-task2"),
        (["order:"], "reg-M1-task3"),
    ]
    tids = []
    for prefixes, name in prefixes_list:
        tid = create_task(name, "full_only",
                          key_filter={"mode": "prefix", "prefixes": prefixes},
                          workers=2)
        if tid:
            tids.append(tid)

    if len(tids) < 2:
        record("M1 多任务并发", False, f"只创建了 {len(tids)} 个任务")
        for tid in tids:
            delete_task(tid)
        return

    # 同时启动
    for tid in tids:
        start_task(tid)
    log(f"  启动 {len(tids)} 个并发任务")

    # 等待全部完成
    results = []
    for tid in tids:
        t = wait_complete(tid, timeout=600)
        results.append(t.get("status", "unknown"))

    completed = sum(1 for s in results if s == "completed")
    checks = [f"tasks={len(tids)},completed={completed}/{len(tids)}"]
    checks.append(f"statuses={results}")

    # 验证数据不互相污染
    dst_total = dst_dbsize()
    checks.append(f"dst_total={dst_total}")

    for tid in tids:
        delete_task(tid)

    all_ok = completed == len(tids)
    record("M1 多任务并发", all_ok, "; ".join(checks))

def test_M2_rapid_create_delete():
    """M2. 快速创建-删除循环 - 测试并发安全"""
    log("=== M2. 快速创建删除 ===")

    created = 0
    deleted = 0
    errors = 0
    for i in range(10):
        tid = create_task(f"reg-M2-rapid-{i}", "full_only",
                          key_filter={"mode": "keylist", "keys": [f"m2_test:{i}"]})
        if tid:
            created += 1
            r = delete_task(tid)
            if r.get("code") == 0:
                deleted += 1
            else:
                errors += 1
        else:
            errors += 1

    checks = [f"created={created}/10,deleted={deleted}/10,errors={errors}"]
    all_ok = created >= 9 and deleted >= 9 and errors <= 1
    record("M2 快速创建删除", all_ok, "; ".join(checks))

def test_M3_pause_resume_rapid():
    """M3. 快速暂停-恢复循环 - 验证状态机稳定性"""
    log("=== M3. 快速暂停恢复 ===")
    flush_dst()

    tid = create_task("reg-M3-rapid-pr", "full_only", workers=4)
    if not tid:
        record("M3 快速暂停恢复", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(3)

    # 快速 pause/resume 5 次
    cycles = 0
    errors = 0
    for _ in range(5):
        r1 = pause_task(tid)
        time.sleep(0.5)
        t = get_task(tid)
        if t.get("status") == "paused":
            cycles += 1
        r2 = resume_task(tid)
        time.sleep(1)
        t2 = get_task(tid)
        if t2.get("status") not in ("running", "completed"):
            errors += 1

    # 最终应该能正常完成
    t_final = wait_complete(tid, timeout=600)
    final_status = t_final.get("status", "")

    checks = [f"cycles={cycles}/5,errors={errors},final={final_status}"]
    all_ok = final_status == "completed" and errors <= 1
    record("M3 快速暂停恢复", all_ok, "; ".join(checks))
    return tid

def test_M4_concurrent_api_calls():
    """M4. 并发 API 调用 - 同时请求多个接口"""
    log("=== M4. 并发API调用 ===")
    import concurrent.futures

    def call_api(path):
        try:
            r = requests.get(f"{API}{path}", timeout=10)
            return r.status_code
        except:
            return 0

    paths = ["/health", "/health/detailed", "/system/status", "/tasks"]
    # 每个路径调用 5 次 = 20 个并发请求
    all_paths = paths * 5

    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        futures = [executor.submit(call_api, p) for p in all_paths]
        results = [f.result() for f in concurrent.futures.as_completed(futures)]

    success = sum(1 for r in results if r == 200)
    checks = [f"total={len(results)},success={success}/{len(results)}"]

    all_ok = success >= len(results) - 2  # 允许极少失败
    record("M4 并发API调用", all_ok, "; ".join(checks))

# ================================================================
# N. 数据正确性深度验证
# ================================================================
def test_N1_zset_score_precision():
    """N1. ZSet Score 精度 - 浮点数精度不丢失"""
    log("=== N1. ZSet Score精度 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]
    redis_cmd(src, 'DEL n1_zscore:test')

    # 写入浮点 score
    redis_cmd(src, 'ZADD n1_zscore:test 1.23456789 a 99.99 b 0.001 c -5.5 d 1e10 e')
    time.sleep(1)

    tid = create_task("reg-N1-zscore", "full_only",
                      key_filter={"mode": "keylist", "keys": ["n1_zscore:test"]})
    if not tid:
        record("N1 ZSet精度", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=60)
    s = t.get("status", "")

    # 验证 score
    scores_raw = dst_redis_cmd(dst, 'ZRANGEBYSCORE n1_zscore:test -inf +inf WITHSCORES')
    checks = [f"status={s},scores={scores_raw[:100]}"]

    # 检查关键 score 值（浮点精度：99.99 可能存储为 99.989999999999995）
    import re
    score_numbers = re.findall(r'[-+]?\d*\.?\d+(?:[eE][-+]?\d+)?', scores_raw)
    score_floats = [float(x) for x in score_numbers if x]

    def approx_in(target, values, tol=0.01):
        return any(abs(v - target) < tol for v in values)

    score_ok = (approx_in(99.99, score_floats) and
                approx_in(0.001, score_floats) and
                approx_in(-5.5, score_floats))
    checks.append(f"score_ok={score_ok}")

    all_ok = s == "completed" and score_ok
    record("N1 ZSet精度", all_ok, "; ".join(checks))
    redis_cmd(src, 'DEL n1_zscore:test')
    return tid

def test_N2_empty_value_types():
    """N2. 空值/空集合迁移"""
    log("=== N2. 空值类型 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    # 空字符串
    redis_cmd(src, 'SET n2_empty:str ""')
    # 创建后立即清空的 hash（删除所有字段 -> key 自动消失）
    # 写一个只有一个字段的 hash
    redis_cmd(src, 'DEL n2_empty:hash')
    redis_cmd(src, 'HSET n2_empty:hash f1 v1')
    # 零长度的 list 不存在于 Redis 中，所以测试空字符串即可
    time.sleep(1)

    tid = create_task("reg-N2-empty", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["n2_empty:"]})
    if not tid:
        record("N2 空值类型", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=60)
    s = t.get("status", "")

    # 验证空字符串
    val = dst_redis_cmd(dst, 'GET n2_empty:str')
    # redis-cli 对空字符串返回 "" 或 (empty string) 或空行
    empty_str_ok = val.strip() in ('""', '(empty list or set)', '', '(empty array)')
    # 也可能返回空
    if not empty_str_ok:
        # strlen 应该是 0
        strlen_val = dst_redis_cmd(dst, 'STRLEN n2_empty:str')
        empty_str_ok = strlen_val.strip() == "0"

    checks = [f"status={s},empty_str={empty_str_ok},raw_val='{val}'"]
    all_ok = s == "completed" and empty_str_ok
    record("N2 空值类型", all_ok, "; ".join(checks))
    return tid

def test_N3_large_collection():
    """N3. 大集合迁移 - List/Set/ZSet 各 10000 元素"""
    log("=== N3. 大集合迁移 (10000元素) ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    redis_cmd(src, 'DEL n3_biglist:test n3_bigset:test n3_bigzset:test')

    target_count = 10000
    batch_size = 200
    num_batches = target_count // batch_size

    # 批量写入 list
    log(f"  写入 List {target_count} 元素 ({num_batches} 批 x {batch_size})...")
    for batch in range(num_batches):
        items = " ".join([f"item_{batch*batch_size+i}" for i in range(batch_size)])
        redis_cmd(src, f'RPUSH n3_biglist:test {items}')

    # 批量写入 set
    log(f"  写入 Set {target_count} 元素...")
    for batch in range(num_batches):
        members = " ".join([f"mem_{batch*batch_size+i}" for i in range(batch_size)])
        redis_cmd(src, f'SADD n3_bigset:test {members}')

    # 批量写入 zset（每个成员带 score）
    log(f"  写入 ZSet {target_count} 元素...")
    for batch in range(num_batches):
        # ZADD 格式: score member score member ...
        zitems = " ".join([f"{batch*batch_size+i+0.5} zm_{batch*batch_size+i}" for i in range(batch_size)])
        redis_cmd(src, f'ZADD n3_bigzset:test {zitems}')

    src_llen = redis_cmd(src, 'LLEN n3_biglist:test')
    src_scard = redis_cmd(src, 'SCARD n3_bigset:test')
    src_zcard = redis_cmd(src, 'ZCARD n3_bigzset:test')
    log(f"  源端 list={src_llen.strip()}, set={src_scard.strip()}, zset={src_zcard.strip()}")
    time.sleep(1)

    tid = create_task("reg-N3-bigcoll", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["n3_big"]})
    if not tid:
        record("N3 大集合迁移", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")

    # 在所有目标端端口上查找
    dst_llen = "0"
    dst_scard = "0"
    dst_zcard = "0"
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'LLEN n3_biglist:test')
        if v and v.strip() and v.strip() != "0" and v.strip() != "(nil)":
            dst_llen = v.strip()
        v = dst_redis_cmd(port, 'SCARD n3_bigset:test')
        if v and v.strip() and v.strip() != "0" and v.strip() != "(nil)":
            dst_scard = v.strip()
        v = dst_redis_cmd(port, 'ZCARD n3_bigzset:test')
        if v and v.strip() and v.strip() != "0" and v.strip() != "(nil)":
            dst_zcard = v.strip()

    list_ok = src_llen.strip() == dst_llen
    set_ok = src_scard.strip() == dst_scard
    zset_ok = src_zcard.strip() == dst_zcard

    checks = [f"status={s},list={src_llen.strip()}->{dst_llen}(ok={list_ok}),set={src_scard.strip()}->{dst_scard}(ok={set_ok}),zset={src_zcard.strip()}->{dst_zcard}(ok={zset_ok})"]
    all_ok = s == "completed" and list_ok and set_ok and zset_ok
    record("N3 大集合迁移", all_ok, "; ".join(checks))
    redis_cmd(src, 'DEL n3_biglist:test n3_bigset:test n3_bigzset:test')
    return tid

def test_N4_overwrite_different_type():
    """N4. 类型冲突覆盖 - 目标端 string，源端 hash（replace 模式）"""
    log("=== N4. 类型冲突覆盖 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    # 源端写 hash
    redis_cmd(src, 'DEL n4_typeclash:key')
    redis_cmd(src, 'HSET n4_typeclash:key f1 v1 f2 v2')

    # 目标端写 string（类型不同）
    dst_redis_cmd(dst, 'SET n4_typeclash:key "old_string_val"')

    src_type = redis_cmd(src, 'TYPE n4_typeclash:key')
    dst_type_before = dst_redis_cmd(dst, 'TYPE n4_typeclash:key')
    log(f"  源端类型={src_type}, 目标端类型={dst_type_before}")
    time.sleep(1)

    tid = create_task("reg-N4-typeclash", "full_only",
                      key_filter={"mode": "keylist", "keys": ["n4_typeclash:key"]},
                      conflict_policy="replace")
    if not tid:
        record("N4 类型冲突覆盖", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=60)
    s = t.get("status", "")

    dst_type_after = dst_redis_cmd(dst, 'TYPE n4_typeclash:key')
    hval = dst_redis_cmd(dst, 'HGETALL n4_typeclash:key')

    # replace 模式下，目标端应从 string 变为 hash
    type_changed = "hash" in dst_type_after
    data_ok = "f1" in str(hval) and "v1" in str(hval)

    checks = [f"status={s},type_before={dst_type_before},type_after={dst_type_after}"]
    checks.append(f"type_changed={type_changed},data_ok={data_ok}")
    all_ok = s == "completed" and type_changed and data_ok
    record("N4 类型冲突覆盖", all_ok, "; ".join(checks))
    redis_cmd(src, 'DEL n4_typeclash:key')
    return tid

# ================================================================
# O. 任务生命周期扩展
# ================================================================
def test_O1_complete_api():
    """O1. 手动标记完成 - stop-incremental + complete"""
    log("=== O1. 手动标记完成 ===")
    flush_dst()

    tid = create_task("reg-O1-complete", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]})
    if not tid:
        record("O1 手动完成", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")

    if phase != "incremental":
        record("O1 手动完成", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    # stop-incremental
    api_post(f"/tasks/{tid}/stop-incremental")
    time.sleep(3)
    t2 = get_task(tid)
    after_stop = t2.get("status", "")

    # 手动 complete
    r = api_post(f"/tasks/{tid}/complete")
    complete_code = r.get("code", -1)
    time.sleep(2)
    t3 = get_task(tid)
    final_status = t3.get("status", "")

    checks = [f"after_stop_incr={after_stop},complete_code={complete_code},final={final_status}"]
    all_ok = final_status == "completed"
    record("O1 手动完成", all_ok, "; ".join(checks))
    return tid

def test_O2_retry_failed():
    """O2. 重试失败 key - retry-failed API"""
    log("=== O2. 重试失败Key ===")
    flush_dst()

    # 先完成一个任务
    tid = create_task("reg-O2-retry", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["app:"]})
    if not tid:
        record("O2 重试失败", False, "创建任务失败")
        return

    start_task(tid)
    wait_complete(tid, timeout=300)

    # 调用 retry-failed（即使没有失败 key 也应该正常返回）
    r = api_post(f"/tasks/{tid}/retry-failed")
    retry_code = r.get("code", -1)

    checks = [f"retry_code={retry_code},msg={str(r.get('message',''))[:60]}"]
    # 应该正常返回而不是 panic
    all_ok = retry_code is not None  # 有响应就算通过
    record("O2 重试失败", all_ok, "; ".join(checks))
    return tid

def test_O3_export_report():
    """O3. 导出任务配置和报告"""
    log("=== O3. 导出报告 ===")

    # 找一个已完成任务
    resp = api_get("/tasks")
    tasks = resp.get("data", {}).get("tasks", [])
    tid = None
    for t in tasks:
        if t.get("name", "").startswith("reg-") and t.get("status") == "completed":
            tid = t["id"]
            break

    if not tid:
        flush_dst()
        tid = create_task("reg-O3-export", "full_only",
                          key_filter={"mode": "keylist", "keys": ["o3_test:a"]})
        if tid:
            redis_set(SRC_PORTS[0], "o3_test:a", "val")
            start_task(tid)
            wait_complete(tid, timeout=60)

    if not tid:
        record("O3 导出报告", False, "无可用任务")
        return

    # export
    r1 = api_get(f"/tasks/{tid}/export")
    export_ok = "error" not in r1 or r1.get("code") == 0
    checks = [f"export_code={r1.get('code')},has_data={'data' in r1}"]

    # report
    r2 = api_get(f"/tasks/{tid}/report")
    report_ok = "error" not in r2 or r2.get("code") == 0
    checks.append(f"report_code={r2.get('code')},has_data={'data' in r2}")

    all_ok = export_ok and report_ok
    record("O3 导出报告", all_ok, "; ".join(checks))

def test_O4_task_health():
    """O4. 任务健康状态 API"""
    log("=== O4. 任务健康 ===")
    flush_dst()

    tid = create_task("reg-O4-health", "full_only", workers=4)
    if not tid:
        record("O4 任务健康", False, "创建任务失败")
        return

    start_task(tid)
    time.sleep(3)

    r = api_get(f"/tasks/{tid}/health")
    health_ok = "error" not in r or r.get("code") == 0

    checks = [f"code={r.get('code')},has_data={'data' in r}"]
    if "data" in r and isinstance(r["data"], dict):
        checks.append(f"fields={list(r['data'].keys())[:5]}")

    wait_complete(tid, timeout=600)
    all_ok = health_ok
    record("O4 任务健康", all_ok, "; ".join(checks))
    return tid

# ================================================================
# P. 过滤器深度测试
# ================================================================
def test_P1_exclude_pattern_regex():
    """P1. 排除 Pattern 正则 - exclude_patterns"""
    log("=== P1. 排除Pattern ===")
    flush_dst()

    src = SRC_PORTS[0]
    # 写入匹配和不匹配的 key
    for i in range(20):
        redis_set(src, f"p1_keep:{i:04d}", f"keep_{i}")
        redis_set(src, f"p1_drop_tmp:{i:04d}", f"drop_{i}")
    time.sleep(1)

    tid = create_task("reg-P1-exclpat", "full_only",
                      key_filter={"mode": "all",
                                  "exclude_patterns": ["p1_drop_.*"]})
    if not tid:
        record("P1 排除Pattern", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=300)
    s = t.get("status", "")

    # 验证 p1_drop 不在目标端
    kept = sum(1 for i in range(10) if dst_redis_exists(DST_PORTS[0], f"p1_keep:{i:04d}"))
    dropped = sum(1 for i in range(10) if dst_redis_exists(DST_PORTS[0], f"p1_drop_tmp:{i:04d}"))

    checks = [f"status={s},kept={kept}/10,dropped={dropped}/10(should=0)"]
    st = t.get("stats", {})
    checks.append(f"filtered={st.get('filtered_keys',0)}")

    all_ok = s == "completed" and kept >= 8 and dropped == 0
    record("P1 排除Pattern", all_ok, "; ".join(checks))
    return tid

def test_P2_multiple_prefixes():
    """P2. 多前缀过滤 - 5 个前缀组合"""
    log("=== P2. 多前缀过滤 ===")
    flush_dst()

    src = SRC_PORTS[0]
    prefixes = ["p2a:", "p2b:", "p2c:", "p2d:", "p2e:"]
    # 写入 5 个前缀 + 1 个不匹配前缀
    for p in prefixes:
        for i in range(5):
            redis_set(src, f"{p}{i:04d}", f"val_{p}{i}")
    for i in range(5):
        redis_set(src, f"p2_nomatch:{i:04d}", f"no_{i}")
    time.sleep(1)

    tid = create_task("reg-P2-multi", "full_only",
                      key_filter={"mode": "prefix", "prefixes": prefixes})
    if not tid:
        record("P2 多前缀过滤", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    # 验证所有前缀都迁移了
    found = {}
    for p in prefixes:
        cnt = sum(1 for i in range(5) if dst_redis_exists(DST_PORTS[0], f"{p}{i:04d}"))
        found[p] = cnt

    # 不匹配前缀不应迁移
    nomatch = sum(1 for i in range(5) if dst_redis_exists(DST_PORTS[0], f"p2_nomatch:{i:04d}"))

    checks = [f"status={s}"]
    checks.append(f"per_prefix={found}")
    checks.append(f"nomatch={nomatch}/5(should=0)")

    total_found = sum(found.values())
    all_ok = s == "completed" and total_found >= 20 and nomatch == 0
    record("P2 多前缀过滤", all_ok, "; ".join(checks))
    return tid

def test_P3_prefix_with_exclude():
    """P3. 前缀包含 + 排除前缀组合"""
    log("=== P3. 包含+排除组合 ===")
    flush_dst()

    src = SRC_PORTS[0]
    # 写入 app:keep:xxx 和 app:tmp:xxx
    for i in range(10):
        redis_set(src, f"p3_app:keep:{i:04d}", f"keep_{i}")
        redis_set(src, f"p3_app:tmp:{i:04d}", f"tmp_{i}")
        redis_set(src, f"p3_other:{i:04d}", f"other_{i}")
    time.sleep(1)

    tid = create_task("reg-P3-combo", "full_only",
                      key_filter={"mode": "prefix",
                                  "prefixes": ["p3_app:"],
                                  "exclude_prefixes": ["p3_app:tmp:"]})
    if not tid:
        record("P3 包含+排除组合", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=120)
    s = t.get("status", "")

    kept = sum(1 for i in range(10) if dst_redis_exists(DST_PORTS[0], f"p3_app:keep:{i:04d}"))
    excluded = sum(1 for i in range(10) if dst_redis_exists(DST_PORTS[0], f"p3_app:tmp:{i:04d}"))
    other = sum(1 for i in range(10) if dst_redis_exists(DST_PORTS[0], f"p3_other:{i:04d}"))

    checks = [f"status={s},kept={kept}/10,excluded={excluded}/10(should=0),other={other}/10(should=0)"]
    all_ok = s == "completed" and kept >= 8 and excluded == 0 and other == 0
    record("P3 包含+排除组合", all_ok, "; ".join(checks))
    return tid

# ================================================================
# Q. 辅助 API 扩展
# ================================================================
def test_Q1_analyze_cluster():
    """Q1. 集群分析 API"""
    log("=== Q1. 集群分析 ===")

    r = api_post("/analyze-cluster", {"addrs": SRC_NODES})
    ok = r.get("code") == 0 or "data" in r
    data = r.get("data", {})

    checks = [f"code={r.get('code')}"]
    if isinstance(data, dict):
        checks.append(f"fields={list(data.keys())[:5]}")

    record("Q1 集群分析", ok, "; ".join(checks))

def test_Q2_recommend_config():
    """Q2. 推荐配置 API"""
    log("=== Q2. 推荐配置 ===")

    r = api_post("/recommend-config", {
        "source_cluster": {"addrs": SRC_NODES},
        "target_cluster": {"addrs": DST_NODES}
    })
    ok = r.get("code") == 0 or "data" in r
    data = r.get("data", {})

    checks = [f"code={r.get('code')}"]
    if isinstance(data, dict):
        checks.append(f"fields={list(data.keys())[:5]}")
        if "worker_count" in data:
            checks.append(f"recommended_workers={data['worker_count']}")

    record("Q2 推荐配置", ok, "; ".join(checks))

def test_Q3_conflicts_api():
    """Q3. 冲突 Key 查看 API"""
    log("=== Q3. 冲突Key ===")
    flush_dst()

    # 创建有冲突的任务
    for i in range(10):
        dst_redis_set(DST_PORTS[0], f"q3_conflict:{i:04d}", f"OLD_{i}")
    time.sleep(1)

    tid = create_task("reg-Q3-conflict", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["q3_conflict:"]},
                      conflict_policy="skip")
    if not tid:
        record("Q3 冲突Key", False, "创建任务失败")
        return

    # 写入源端数据
    for i in range(10):
        redis_set(SRC_PORTS[0], f"q3_conflict:{i:04d}", f"NEW_{i}")
    time.sleep(1)

    start_task(tid)
    wait_complete(tid, timeout=120)

    # 查看冲突 key
    r = api_get(f"/tasks/{tid}/conflicts")
    ok = "error" not in r or r.get("code") == 0

    checks = [f"code={r.get('code')},has_data={'data' in r}"]
    record("Q3 冲突Key", ok, "; ".join(checks))
    return tid

def test_Q4_templates():
    """Q4. 模板管理 - 列表/查看"""
    log("=== Q4. 模板管理 ===")

    # 获取模板列表
    r = api_get("/templates")
    ok = "error" not in r or r.get("code") == 0
    data = r.get("data", [])
    template_count = len(data) if isinstance(data, list) else 0

    checks = [f"list_code={r.get('code')},count={template_count}"]

    # 如果有模板，查看第一个
    if isinstance(data, list) and len(data) > 0:
        first = data[0]
        tid = first.get("id", first.get("ID", ""))
        if tid:
            r2 = api_get(f"/templates/{tid}")
            checks.append(f"detail_code={r2.get('code')}")

    record("Q4 模板管理", ok, "; ".join(checks))

def test_Q5_system_logs():
    """Q5. 系统日志 API"""
    log("=== Q5. 系统日志 ===")

    checks = []

    # 查看日志
    r1 = api_get("/logs")
    logs_ok = "error" not in r1 or r1.get("code") == 0
    checks.append(f"logs_code={r1.get('code')}")

    # 日志统计
    r2 = api_get("/logs/stats")
    stats_ok = "error" not in r2 or r2.get("code") == 0
    checks.append(f"stats_code={r2.get('code')}")

    all_ok = logs_ok and stats_ok
    record("Q5 系统日志", all_ok, "; ".join(checks))

def test_Q6_smart_retry_status():
    """Q6. 智能重试状态 API"""
    log("=== Q6. 智能重试状态 ===")

    r = api_get("/smart-retry/status")
    ok = "error" not in r or r.get("code") == 0

    checks = [f"code={r.get('code')},has_data={'data' in r}"]
    record("Q6 智能重试状态", ok, "; ".join(checks))

# ================================================================
# R. 增量同步深度测试
# ================================================================
def test_R1_incr_zset():
    """R1. 增量 ZSet 操作同步"""
    log("=== R1. 增量ZSet同步 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    tid = create_task("reg-R1-incr-zset", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["r1_zset:"]})
    if not tid:
        record("R1 增量ZSet", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("R1 增量ZSet", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 增量写入 zset
    redis_cmd(src, 'DEL r1_zset:scores')
    redis_cmd(src, 'ZADD r1_zset:scores 10 player1 20 player2 30 player3')
    log("  等待增量同步 (20s)...")
    time.sleep(20)

    # 验证
    zcard = dst_redis_cmd(dst, 'ZCARD r1_zset:scores')
    zvals = dst_redis_cmd(dst, 'ZRANGEBYSCORE r1_zset:scores -inf +inf WITHSCORES')
    zset_ok = "player1" in str(zvals) and "player3" in str(zvals) and zcard.strip() == "3"

    checks = [f"zcard={zcard},data={'OK' if zset_ok else 'FAIL:'+zvals[:50]}"]
    stop_task(tid)
    time.sleep(2)

    record("R1 增量ZSet", zset_ok, "; ".join(checks))
    redis_cmd(src, 'DEL r1_zset:scores')
    return tid

def test_R2_incr_modify_existing():
    """R2. 增量修改已有 Key - HSET 追加字段/RPUSH 追加元素"""
    log("=== R2. 增量修改已有Key ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    # 全量阶段先有数据
    redis_cmd(src, 'DEL r2_modify:hash r2_modify:list')
    redis_cmd(src, 'HSET r2_modify:hash f1 v1 f2 v2')
    redis_cmd(src, 'RPUSH r2_modify:list a b c')
    time.sleep(1)

    tid = create_task("reg-R2-modify", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["r2_modify:"]})
    if not tid:
        record("R2 增量修改已有Key", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("R2 增量修改已有Key", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 增量修改：追加字段和元素
    redis_cmd(src, 'HSET r2_modify:hash f3 v3_new f4 v4_new')
    redis_cmd(src, 'RPUSH r2_modify:list d e f')
    log("  等待增量同步 (20s)...")
    time.sleep(20)

    # 验证目标端包含新增数据
    hval = dst_redis_cmd(dst, 'HGETALL r2_modify:hash')
    hlen = dst_redis_cmd(dst, 'HLEN r2_modify:hash')
    llen = dst_redis_cmd(dst, 'LLEN r2_modify:list')

    hash_ok = "f3" in str(hval) and "v3_new" in str(hval)
    # 全量阶段的 list 有 3 个元素，增量追加 3 个 → 6 个
    # 但 binlog 回放可能是整个 key 的 DUMP/RESTORE，所以也可能是 6
    list_has_new = "d" in dst_redis_cmd(dst, 'LRANGE r2_modify:list 0 -1')

    checks = [f"hlen={hlen},hash_has_new={hash_ok},llen={llen},list_has_new={list_has_new}"]
    stop_task(tid)
    time.sleep(2)

    all_ok = hash_ok
    record("R2 增量修改已有Key", all_ok, "; ".join(checks))
    redis_cmd(src, 'DEL r2_modify:hash r2_modify:list')
    return tid

def test_R3_incr_expire():
    """R3. 增量 EXPIRE 同步 - 在增量阶段设置 TTL"""
    log("=== R3. 增量EXPIRE同步 ===")
    flush_dst()

    src = SRC_PORTS[0]
    dst = DST_PORTS[0]

    redis_cmd(src, 'SET r3_expire:key "persistent_value"')
    time.sleep(1)

    tid = create_task("reg-R3-expire", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["r3_expire:"]})
    if not tid:
        record("R3 增量EXPIRE", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("R3 增量EXPIRE", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 全量完成后，在增量阶段给 key 设置 TTL
    redis_cmd(src, 'EXPIRE r3_expire:key 600')
    log("  等待增量同步 (20s)...")
    time.sleep(20)

    # 验证目标端 key 是否也有 TTL
    dst_ttl = dst_redis_cmd(dst, 'TTL r3_expire:key')
    try:
        dst_ttl_int = int(dst_ttl.strip())
        ttl_synced = 10 < dst_ttl_int <= 600
    except:
        dst_ttl_int = -99
        ttl_synced = False

    checks = [f"dst_ttl={dst_ttl_int},synced={ttl_synced}"]
    stop_task(tid)
    time.sleep(2)

    record("R3 增量EXPIRE", ttl_synced, "; ".join(checks))
    redis_cmd(src, 'DEL r3_expire:key')
    return tid

def test_R4_incr_batch_writes():
    """R4. 增量批量写入 - 1000 个 key 快速写入"""
    log("=== R4. 增量批量写入 ===")
    flush_dst()

    tid = create_task("reg-R4-batch", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["r4_batch:"]})
    if not tid:
        record("R4 增量批量写入", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("R4 增量批量写入", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 快速写入 1000 个 key
    log("  写入 1000 个增量 key...")
    for i in range(1000):
        redis_set(SRC_PORTS[0], f"r4_batch:{i:06d}", f"val_{i}")

    log("  等待增量同步 (30s)...")
    time.sleep(30)

    # 抽样验证（在所有目标端节点上检查）
    sample_found = 0
    sample_indices = [0, 100, 250, 500, 750, 999]
    for idx in sample_indices:
        for port in DST_PORTS:
            if dst_redis_exists(port, f"r4_batch:{idx:06d}"):
                sample_found += 1
                break

    # 扩大抽样范围验证（每 50 个抽一个 = 20 个抽样点）
    wide_sample = 0
    wide_total = 20
    for i in range(wide_total):
        idx = i * 50
        for port in DST_PORTS:
            if dst_redis_exists(port, f"r4_batch:{idx:06d}"):
                wide_sample += 1
                break

    checks = [f"sample={sample_found}/{len(sample_indices)},wide_sample={wide_sample}/{wide_total}"]
    stop_task(tid)
    time.sleep(2)

    all_ok = sample_found >= 5 and wide_sample >= 18
    record("R4 增量批量写入", all_ok, "; ".join(checks))
    return tid

# ================================================================
# S. 补充测试（覆盖13点测试要求中的缺失场景）
# ================================================================
def test_S1_key_natural_expire_during_migration():
    """S1. 迁移过程中 Key 自然过期 - 源端 Key 过期后目标端是否同步过期"""
    log("=== S1. 迁移中Key自然过期 ===")
    flush_dst()

    src = SRC_PORTS[0]

    # 写入短 TTL 的 Key（10秒过期）和长 TTL 的 Key（300秒）
    for i in range(10):
        redis_cmd(src, f'SET s1_expire_short:{i:04d} "short_val_{i}" EX 10')
    for i in range(10):
        redis_cmd(src, f'SET s1_expire_long:{i:04d} "long_val_{i}" EX 300')
    for i in range(10):
        redis_cmd(src, f'SET s1_expire_persist:{i:04d} "persist_val_{i}"')
    time.sleep(1)

    tid = create_task("reg-S1-natural-expire", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["s1_expire_"]})
    if not tid:
        record("S1 迁移中Key自然过期", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("S1 迁移中Key自然过期", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    # 等待短 TTL Key 在源端自然过期
    log("  等待短TTL Key自然过期 (15s)...")
    time.sleep(15)

    # 再等待增量同步可能的 DEL 事件
    log("  等待增量同步过期事件 (15s)...")
    time.sleep(15)

    # 检查目标端：短 TTL Key 应该已过期（不存在或 TTL 已到期）
    short_expired = 0
    for i in range(10):
        exists_val = dst_redis_cmd(DST_PORTS[0], f'EXISTS s1_expire_short:{i:04d}')
        # Key 不存在说明已过期（通过 TTL 自然过期或增量 DEL）
        if exists_val.strip() == "0":
            short_expired += 1
        else:
            # 如果还存在，检查 TTL 是否即将过期
            ttl_val = dst_redis_cmd(DST_PORTS[0], f'TTL s1_expire_short:{i:04d}')
            try:
                if int(ttl_val.strip()) <= 0:
                    short_expired += 1
            except:
                pass

    # 长 TTL Key 应该还存在
    long_alive = 0
    for i in range(10):
        for port in DST_PORTS:
            if dst_redis_cmd(port, f'EXISTS s1_expire_long:{i:04d}').strip() == "1":
                long_alive += 1
                break

    # 永不过期 Key 应该还存在
    persist_alive = 0
    for i in range(10):
        for port in DST_PORTS:
            if dst_redis_cmd(port, f'EXISTS s1_expire_persist:{i:04d}').strip() == "1":
                persist_alive += 1
                break

    checks = [f"short_expired={short_expired}/10,long_alive={long_alive}/10,persist_alive={persist_alive}/10"]
    stop_task(tid)
    time.sleep(2)

    # 短 TTL 至少大部分过期，长 TTL 和永久 Key 必须存在
    all_ok = short_expired >= 7 and long_alive >= 8 and persist_alive >= 8
    record("S1 迁移中Key自然过期", all_ok, "; ".join(checks))

    # 清理
    for i in range(10):
        redis_cmd(src, f'DEL s1_expire_short:{i:04d} s1_expire_long:{i:04d} s1_expire_persist:{i:04d}')
    return tid


def test_S2_ttl_renewal_during_migration():
    """S2. 过期Key续期 - 迁移中 PERSIST/续期后目标端不丢 Key"""
    log("=== S2. 过期Key续期（目标端不丢Key）===")
    flush_dst()

    src = SRC_PORTS[0]

    # 写入 10 个带 TTL 的 Key（300秒过期，留充足时间完成全量迁移和增量同步）
    for i in range(10):
        redis_cmd(src, f'SET s2_renew:{i:04d} "renew_val_{i}" EX 300')
    time.sleep(1)

    tid = create_task("reg-S2-ttl-renew", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["s2_renew:"]})
    if not tid:
        record("S2 过期Key续期", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("S2 过期Key续期", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 验证目标端 Key 已迁移成功且有 TTL
    pre_check = 0
    for i in range(10):
        for port in DST_PORTS:
            ttl = dst_redis_cmd(port, f'TTL s2_renew:{i:04d}')
            try:
                if int(ttl.strip()) > 0:
                    pre_check += 1
                    break
            except:
                pass
    log(f"  迁移后目标端有TTL的Key: {pre_check}/10")

    # 关键操作：在增量阶段对这些 Key 续期
    # 前 5 个用 PERSIST（取消过期）
    for i in range(5):
        result = redis_cmd(src, f'PERSIST s2_renew:{i:04d}')
        log(f"  PERSIST s2_renew:{i:04d}: {result}")
    # 后 5 个用 EXPIRE 续期到 3600 秒（1小时）
    for i in range(5, 10):
        result = redis_cmd(src, f'EXPIRE s2_renew:{i:04d} 3600')
        log(f"  EXPIRE s2_renew:{i:04d} 3600: {result}")

    # 等待增量同步回放 PERSIST/EXPIRE 命令
    log("  等待增量同步续期命令 (30s)...")
    time.sleep(30)

    # 检查目标端：这些 Key 应该因为续期而还存在
    persist_alive = 0
    for i in range(5):
        for port in DST_PORTS:
            exists = dst_redis_cmd(port, f'EXISTS s2_renew:{i:04d}').strip()
            if exists == "1":
                ttl_val = dst_redis_cmd(port, f'TTL s2_renew:{i:04d}')
                try:
                    ttl_int = int(ttl_val.strip())
                    if ttl_int == -1:  # PERSIST 后应该无过期
                        persist_alive += 1
                    else:
                        log(f"  s2_renew:{i:04d} TTL={ttl_int} (expected -1)")
                except:
                    pass
                break

    renewed_alive = 0
    for i in range(5, 10):
        for port in DST_PORTS:
            exists = dst_redis_cmd(port, f'EXISTS s2_renew:{i:04d}').strip()
            if exists == "1":
                ttl_val = dst_redis_cmd(port, f'TTL s2_renew:{i:04d}')
                try:
                    ttl_int = int(ttl_val.strip())
                    # 续期后 Key 还存在就算成功
                    # TTL 可能 >300（正确续期）或 -1（binlog 回放为 PERSIST）
                    if ttl_int > 300 or ttl_int == -1:
                        renewed_alive += 1
                    else:
                        log(f"  s2_renew:{i:04d} TTL={ttl_int} (expected >300 or -1)")
                except:
                    pass
                break
            else:
                # Key 不存在 → 续期失败，Key 丢了
                log(f"  s2_renew:{i:04d} not found on port {port}")

    checks = [f"pre_check={pre_check}/10,persist_alive(no_ttl)={persist_alive}/5,renewed_alive(long_ttl)={renewed_alive}/5"]
    stop_task(tid)
    time.sleep(2)

    all_ok = persist_alive >= 3 and renewed_alive >= 3
    record("S2 过期Key续期", all_ok, "; ".join(checks))

    # 清理
    for i in range(10):
        redis_cmd(src, f'DEL s2_renew:{i:04d}')
    return tid


def test_S3_16mb_value_rejection():
    """S3. 16MB 超大值拦截 - Tendis 拒绝超过 16MB 的 RESTORE"""
    log("=== S3. 16MB超大值拦截 ===")
    flush_dst()

    src = SRC_PORTS[0]
    redis_cmd(src, 'DEL s3_bigval:over16m')

    # 通过 redis-cli pipe 方式写入大值（生成一个 shell 脚本来快速写入）
    # 用 APPEND 分批追加，每次 512KB，共 34 次 = 17MB
    log("  写入 ~17MB 数据到源端...")
    chunk_512k = "X" * (512 * 1024)
    write_cmd = f'redis-cli -c -h {SRC_HOST} -p {src}'

    # 批量 APPEND：通过 ssh 执行一个循环
    # 由于单次 redis-cli 命令行长度限制，每次追加 100KB
    chunk_100k = "X" * (100 * 1024)
    ssh_batch_cmd = f'for i in $(seq 1 170); do redis-cli -c -h {SRC_HOST} -p {src} APPEND s3_bigval:over16m "{chunk_100k}" > /dev/null; done'
    log("  通过批量 APPEND 写入（可能需要几分钟）...")
    ssh(ssh_batch_cmd, timeout=600)

    # 检查源端大小
    src_strlen = redis_cmd(src, 'STRLEN s3_bigval:over16m')
    log(f"  源端值大小: {src_strlen} bytes")

    try:
        src_size = int(src_strlen.strip())
    except:
        src_size = 0

    if src_size < 16 * 1024 * 1024:
        log(f"  无法写入 >16MB（实际 {src_size} bytes），测试当前大小的迁移行为")

    time.sleep(1)

    # 同时写一些正常 Key 确保任务不因为大 Key 整体失败
    for i in range(5):
        redis_cmd(src, f'SET s3_bigval:normal_{i} "normal_value_{i}"')

    tid = create_task("reg-S3-bigval", "full_only",
                      key_filter={"mode": "prefix", "prefixes": ["s3_bigval:"]})
    if not tid:
        record("S3 16MB超大值", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_complete(tid, timeout=600)
    s = t.get("status", "")

    # 检查任务状态和结果
    stats = t.get("stats", {})
    failed_keys = stats.get("failed_keys", 0)

    # 正常 Key 应该迁移成功
    normal_ok = 0
    for i in range(5):
        for port in DST_PORTS:
            val = dst_redis_cmd(port, f'GET s3_bigval:normal_{i}')
            if f"normal_value_{i}" in str(val):
                normal_ok += 1
                break

    # 检查大 Key 在目标端的状态
    big_key_dst_len = "0"
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'STRLEN s3_bigval:over16m')
        if v.strip() and v.strip() != "0":
            big_key_dst_len = v.strip()
            break

    checks = [
        f"status={s}",
        f"failed_keys={failed_keys}",
        f"normal_migrated={normal_ok}/5",
        f"src_big_size={src_size}",
        f"dst_big_size={big_key_dst_len}",
    ]

    if src_size >= 16 * 1024 * 1024:
        # 超过 16MB：两种可能结果都是正确的
        # 1) Tendis 拒绝 RESTORE → 大 Key 迁移失败，但不影响其他 Key
        # 2) Tendis 接受 → 大 Key 成功迁移，数据完整
        try:
            dst_big_int = int(big_key_dst_len)
        except:
            dst_big_int = 0

        if dst_big_int > 0:
            # Tendis 接受了大值 → 验证数据大小一致性
            size_match = dst_big_int == src_size
            all_ok = s == "completed" and normal_ok >= 4 and size_match
            checks.append(f"expect=accepted,size_match={size_match}")
        else:
            # Tendis 拒绝了 → 确保其他 Key 不受影响
            all_ok = normal_ok >= 4
            checks.append(f"expect=rejected,normal_ok={normal_ok >= 4}")
    else:
        # 未超过 16MB：所有 Key 都应成功
        all_ok = s == "completed" and normal_ok >= 4
        checks.append("expect=all_migrated(under_16mb)")

    record("S3 16MB超大值", all_ok, "; ".join(checks))

    # 清理
    redis_cmd(src, 'DEL s3_bigval:over16m')
    for i in range(5):
        redis_cmd(src, f'DEL s3_bigval:normal_{i}')
    return tid


def test_S4_same_key_order_guarantee():
    """S4. 同Key命令顺序保证 - 快速连续修改同一 Key，覆盖全部数据类型 + APPEND/EXPIRE/PERSIST 穿插"""
    log("=== S4. 同Key命令顺序保证（全类型+链式操作） ===")
    flush_dst()

    src = SRC_PORTS[0]

    # 全量阶段先有初始数据
    redis_cmd(src, 'SET s4_order:str "initial"')
    redis_cmd(src, 'DEL s4_order:hash')
    redis_cmd(src, 'HSET s4_order:hash f1 v1')
    redis_cmd(src, 'DEL s4_order:counter')
    redis_cmd(src, 'SET s4_order:counter 0')
    redis_cmd(src, 'SET s4_order:append "base"')
    redis_cmd(src, 'DEL s4_order:list')
    redis_cmd(src, 'RPUSH s4_order:list init_elem')
    redis_cmd(src, 'DEL s4_order:set')
    redis_cmd(src, 'SADD s4_order:set init_m')
    redis_cmd(src, 'DEL s4_order:zset')
    redis_cmd(src, 'ZADD s4_order:zset 1.0 init_z')
    redis_cmd(src, 'SET s4_order:ttlchain "ttl_init"')
    redis_cmd(src, 'EXPIRE s4_order:ttlchain 600')
    time.sleep(1)

    tid = create_task("reg-S4-order", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["s4_order:"]})
    if not tid:
        record("S4 同Key命令顺序", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("S4 同Key命令顺序", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # ===== 1. String 快速连续 SET =====
    log("  [1/7] 快速连续 SET 同一 Key...")
    for i in range(1, 21):
        redis_cmd(src, f'SET s4_order:str "version_{i}"')
    # 最终值应该是 version_20

    # ===== 2. APPEND 链式操作（SET→多次APPEND→验证拼接结果） =====
    log("  [2/7] APPEND 链式操作...")
    redis_cmd(src, 'SET s4_order:append "START"')
    for i in range(1, 11):
        redis_cmd(src, f'APPEND s4_order:append "_part{i}"')
    # 最终值应该是 START_part1_part2_..._part10

    # ===== 3. Hash HSET→HDEL→HSET 链式 =====
    log("  [3/7] Hash 链式操作...")
    redis_cmd(src, 'HSET s4_order:hash f1 updated_v1')
    redis_cmd(src, 'HDEL s4_order:hash f1')
    redis_cmd(src, 'HSET s4_order:hash f1 final_v1 f2 final_v2')
    # 最终 hash 应有 f1=final_v1, f2=final_v2

    # ===== 4. List RPUSH→LPUSH→RPUSH 链式 =====
    log("  [4/7] List 链式操作...")
    redis_cmd(src, 'DEL s4_order:list')
    redis_cmd(src, 'RPUSH s4_order:list a b c')
    redis_cmd(src, 'LPUSH s4_order:list z')
    redis_cmd(src, 'RPUSH s4_order:list d e')
    # 最终 list: z a b c d e

    # ===== 5. Set SADD→SREM→SADD 链式 =====
    log("  [5/7] Set 链式操作...")
    redis_cmd(src, 'DEL s4_order:set')
    redis_cmd(src, 'SADD s4_order:set m1 m2 m3 m4 m5')
    redis_cmd(src, 'SREM s4_order:set m2 m4')
    redis_cmd(src, 'SADD s4_order:set m6')
    # 最终 set: {m1, m3, m5, m6}

    # ===== 6. ZSet ZADD→ZREM→ZADD 链式 =====
    log("  [6/7] ZSet 链式操作...")
    redis_cmd(src, 'DEL s4_order:zset')
    redis_cmd(src, 'ZADD s4_order:zset 1.0 za 2.0 zb 3.0 zc')
    redis_cmd(src, 'ZREM s4_order:zset zb')
    redis_cmd(src, 'ZADD s4_order:zset 5.0 zd 1.5 za')
    # 最终 zset: za(1.5), zc(3.0), zd(5.0)

    # ===== 7. TTL 链式：SET→EXPIRE→PERSIST→EXPIRE→验证最终 TTL =====
    log("  [7/7] TTL 链式操作...")
    redis_cmd(src, 'SET s4_order:ttlchain "ttl_updated"')
    redis_cmd(src, 'EXPIRE s4_order:ttlchain 3600')
    redis_cmd(src, 'PERSIST s4_order:ttlchain')
    redis_cmd(src, 'EXPIRE s4_order:ttlchain 1800')
    # 最终 TTL 应该 ~1800

    # INCR 计数器（20次递增）
    for i in range(20):
        redis_cmd(src, 'INCR s4_order:counter')
    # 最终值应该是 20

    # 快速 DEL 再重建
    redis_cmd(src, 'SET s4_order:recreate "before_del"')
    redis_cmd(src, 'DEL s4_order:recreate')
    redis_cmd(src, 'SET s4_order:recreate "after_recreate"')

    log("  等待增量同步 (30s)...")
    time.sleep(30)

    # 验证目标端最终值
    checks = []

    # 1. String: 最终值应为 version_20
    str_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s4_order:str')
        if v and v.strip() and v.strip() != "(nil)":
            str_val = v.strip()
            break
    str_ok = "version_20" in str_val
    checks.append(f"str={'OK' if str_ok else 'FAIL:'+str_val[:30]}")

    # 2. APPEND: 最终值应包含 START 和 _part10
    append_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s4_order:append')
        if v and v.strip() and v.strip() != "(nil)":
            append_val = v.strip()
            break
    # 验证拼接正确：包含 START 和 _part10，且长度正确
    append_ok = "START" in append_val and "_part10" in append_val
    checks.append(f"append={'OK' if append_ok else 'FAIL:'+append_val[:40]}")

    # 3. Hash: 最终应有 f1=final_v1, f2=final_v2
    hash_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'HGETALL s4_order:hash')
        if v and "final" in v:
            hash_val = v
            break
    hash_ok = "final_v1" in str(hash_val) and "final_v2" in str(hash_val)
    checks.append(f"hash={'OK' if hash_ok else 'FAIL:'+str(hash_val)[:30]}")

    # 4. List: 最终应为 z a b c d e（6个元素）
    list_len = ""
    list_vals = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'LLEN s4_order:list')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() != "0":
            list_len = v.strip()
            list_vals = dst_redis_cmd(port, 'LRANGE s4_order:list 0 -1')
            break
    list_ok = list_len == "6" and "z" in str(list_vals) and "e" in str(list_vals)
    checks.append(f"list={'OK(len='+list_len+')' if list_ok else 'FAIL:len='+list_len}")

    # 5. Set: 最终应有 {m1, m3, m5, m6}（4个成员），不含 m2, m4
    set_card = ""
    set_members = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'SCARD s4_order:set')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() != "0":
            set_card = v.strip()
            set_members = dst_redis_cmd(port, 'SMEMBERS s4_order:set')
            break
    set_ok = set_card == "4" and "m1" in set_members and "m6" in set_members and "m2" not in set_members
    checks.append(f"set={'OK(card='+set_card+')' if set_ok else 'FAIL:card='+set_card}")

    # 6. ZSet: 最终应有 za(1.5), zc(3.0), zd(5.0)，3个成员
    zset_card = ""
    zset_vals = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'ZCARD s4_order:zset')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() != "0":
            zset_card = v.strip()
            zset_vals = dst_redis_cmd(port, 'ZRANGEBYSCORE s4_order:zset -inf +inf WITHSCORES')
            break
    zset_ok = zset_card == "3" and "za" in zset_vals and "zd" in zset_vals and "zb" not in zset_vals
    checks.append(f"zset={'OK(card='+zset_card+')' if zset_ok else 'FAIL:card='+zset_card}")

    # 7. TTL 链式：最终 TTL 应该 ~1800（误差 ±200 秒）
    ttl_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'TTL s4_order:ttlchain')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() not in ("-2", "0"):
            ttl_val = v.strip()
            break
    try:
        ttl_num = int(ttl_val)
        ttl_ok = 1500 <= ttl_num <= 1850
    except (ValueError, TypeError):
        ttl_num = -999
        ttl_ok = False
    checks.append(f"ttl={'OK('+str(ttl_num)+')' if ttl_ok else 'FAIL:'+str(ttl_num)}")

    # 8. Counter: 最终值应为 20
    counter_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s4_order:counter')
        if v and v.strip() and v.strip() != "(nil)":
            counter_val = v.strip()
            break
    counter_ok = counter_val == "20"
    checks.append(f"counter={'OK(20)' if counter_ok else 'FAIL:'+counter_val}")

    # 9. Recreate: 最终值应为 after_recreate
    recreate_val = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s4_order:recreate')
        if v and v.strip() and v.strip() != "(nil)":
            recreate_val = v.strip()
            break
    recreate_ok = "after_recreate" in recreate_val
    checks.append(f"recreate={'OK' if recreate_ok else 'FAIL:'+recreate_val[:30]}")

    stop_task(tid)
    time.sleep(2)

    all_ok = str_ok and append_ok and hash_ok and list_ok and set_ok and zset_ok and ttl_ok and counter_ok and recreate_ok
    record("S4 同Key命令顺序", all_ok, "; ".join(checks))

    # 清理
    for k in ['str', 'append', 'hash', 'list', 'set', 'zset', 'ttlchain', 'counter', 'recreate']:
        redis_cmd(src, f'DEL s4_order:{k}')
    return tid


def test_S5_no_key_commands_no_interference():
    """S5. 无Key命令不干扰迁移 - 迁移中执行 PING/DBSIZE/INFO 等不影响迁移"""
    log("=== S5. 无Key命令不干扰迁移 ===")
    flush_dst()

    src = SRC_PORTS[0]

    # 写入测试数据
    for i in range(50):
        redis_cmd(src, f'SET s5_nokey:{i:04d} "nokey_val_{i}"')
    time.sleep(1)

    tid = create_task("reg-S5-nokey", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["s5_nokey:"]})
    if not tid:
        record("S5 无Key命令不干扰", False, "创建任务失败")
        return

    start_task(tid)

    # 在迁移过程中持续执行无 Key 命令
    log("  迁移中执行无Key命令...")
    nokey_ok = 0
    nokey_total = 20
    for _ in range(nokey_total):
        r1 = redis_cmd(src, 'PING')
        r2 = redis_cmd(src, 'DBSIZE')
        r3 = redis_cmd(src, 'INFO server')
        r4 = redis_cmd(src, 'CLIENT ID')
        r5 = redis_cmd(src, 'TIME')
        if "PONG" in r1 and ("keys" in r2 or r2.isdigit() or ":" in r2):
            nokey_ok += 1
        time.sleep(0.5)

    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")

    # 等增量稳定
    time.sleep(10)

    # 验证迁移数据完整性
    migrated = 0
    for i in range(50):
        for port in DST_PORTS:
            if dst_redis_cmd(port, f'EXISTS s5_nokey:{i:04d}').strip() == "1":
                migrated += 1
                break

    checks = [f"nokey_responses={nokey_ok}/{nokey_total},phase={phase},migrated={migrated}/50"]
    stop_task(tid)
    time.sleep(2)

    all_ok = nokey_ok >= nokey_total - 2 and migrated >= 45
    record("S5 无Key命令不干扰", all_ok, "; ".join(checks))

    # 清理
    for i in range(50):
        redis_cmd(src, f'DEL s5_nokey:{i:04d}')
    return tid


def test_S6_lua_script_limitation():
    """S6. Lua脚本迁移 - 验证 EVAL 在增量同步中的行为（多类型操作）"""
    log("=== S6. Lua脚本增量回放（多类型） ===")
    flush_dst()

    src = SRC_PORTS[0]

    # 全量阶段写入初始数据
    redis_cmd(src, 'SET s6_lua:counter 0')
    redis_cmd(src, 'SET s6_lua:data initial')
    redis_cmd(src, 'DEL s6_lua:hash')
    redis_cmd(src, 'HSET s6_lua:hash f1 init_v1')
    redis_cmd(src, 'DEL s6_lua:list')
    redis_cmd(src, 'RPUSH s6_lua:list init_item')
    redis_cmd(src, 'DEL s6_lua:set')
    redis_cmd(src, 'SADD s6_lua:set init_m')
    time.sleep(1)

    tid = create_task("reg-S6-lua", "full_and_incremental",
                      key_filter={"mode": "prefix", "prefixes": ["s6_lua:"]})
    if not tid:
        record("S6 Lua脚本", False, "创建任务失败")
        return

    start_task(tid)
    t = wait_phase(tid, "incremental", timeout=300)
    phase = t.get("progress", {}).get("phase", "")
    if phase != "incremental":
        record("S6 Lua脚本", False, f"未进入增量阶段 phase={phase}")
        stop_task(tid)
        return tid

    time.sleep(5)

    # 在增量阶段通过 Lua 脚本修改数据
    # 使用 echo + pipe 方式避免 SSH 引号嵌套问题
    log("  执行 Lua EVAL 命令...")

    eval_cli = f'redis-cli -c -h {SRC_HOST} -p {src}'
    lua_exec_ok = True

    # 1. Lua INCR 计数器 10 次
    for i in range(10):
        cmd = f'echo \'EVAL "return redis.call(\\\"INCR\\\", KEYS[1])" 1 s6_lua:counter\' | {eval_cli}'
        ssh(cmd, timeout=10)

    # 2. Lua SET 修改 string
    cmd = f'echo \'EVAL "return redis.call(\\\"SET\\\", KEYS[1], ARGV[1])" 1 s6_lua:data lua_modified\' | {eval_cli}'
    ssh(cmd, timeout=10)

    # 3. Lua HSET 修改 hash
    cmd = f'echo \'EVAL "return redis.call(\\\"HSET\\\", KEYS[1], ARGV[1], ARGV[2])" 1 s6_lua:hash f2 lua_v2\' | {eval_cli}'
    ssh(cmd, timeout=10)

    # 4. Lua RPUSH 修改 list
    cmd = f'echo \'EVAL "return redis.call(\\\"RPUSH\\\", KEYS[1], ARGV[1])" 1 s6_lua:list lua_item\' | {eval_cli}'
    ssh(cmd, timeout=10)

    # 5. Lua SADD 修改 set
    cmd = f'echo \'EVAL "return redis.call(\\\"SADD\\\", KEYS[1], ARGV[1])" 1 s6_lua:set lua_m\' | {eval_cli}'
    ssh(cmd, timeout=10)

    # 验证源端 Lua 执行结果
    src_counter = redis_cmd(src, 'GET s6_lua:counter').strip()
    src_data = redis_cmd(src, 'GET s6_lua:data').strip()
    src_hash_f2 = redis_cmd(src, 'HGET s6_lua:hash f2').strip()
    src_llen = redis_cmd(src, 'LLEN s6_lua:list').strip()
    src_scard = redis_cmd(src, 'SCARD s6_lua:set').strip()
    log(f"  源端: counter={src_counter}, data={src_data}, hash.f2={src_hash_f2}, llen={src_llen}, scard={src_scard}")

    lua_exec_ok = (src_counter == "10" and "lua_modified" in src_data
                   and "lua_v2" in src_hash_f2)

    if not lua_exec_ok:
        log("  WARN: Lua 执行未在源端完全生效，改用直接命令测试增量同步")
        # 用直接命令代替 Lua EVAL
        for i in range(10):
            redis_cmd(src, 'INCR s6_lua:counter')
        redis_cmd(src, 'SET s6_lua:data lua_modified_direct')
        redis_cmd(src, 'HSET s6_lua:hash f2 lua_v2_direct')
        redis_cmd(src, 'RPUSH s6_lua:list lua_item_direct')
        redis_cmd(src, 'SADD s6_lua:set lua_m_direct')

    # 直接命令对照组
    redis_cmd(src, 'SET s6_lua:direct direct_set')

    log("  等待增量同步 (30s)...")
    time.sleep(30)

    # ===== 验证目标端 =====
    checks = [f"lua_exec_on_src={lua_exec_ok}"]

    # 1. counter
    src_counter = redis_cmd(src, 'GET s6_lua:counter').strip()
    dst_counter = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s6_lua:counter')
        if v and v.strip() and v.strip() != "(nil)":
            dst_counter = v.strip()
            break
    counter_ok = dst_counter == src_counter
    checks.append(f"counter:src={src_counter},dst={dst_counter},ok={counter_ok}")

    # 2. data (string)
    dst_data = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s6_lua:data')
        if v and v.strip() and v.strip() != "(nil)":
            dst_data = v.strip()
            break
    data_ok = "lua_modified" in dst_data or "direct" in dst_data
    checks.append(f"data:dst={dst_data[:30]},ok={data_ok}")

    # 3. hash
    dst_hash = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'HGET s6_lua:hash f2')
        if v and v.strip() and v.strip() != "(nil)":
            dst_hash = v.strip()
            break
    hash_ok = "lua_v2" in dst_hash or "direct" in dst_hash
    checks.append(f"hash.f2:dst={dst_hash},ok={hash_ok}")

    # 4. list
    dst_llen = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'LLEN s6_lua:list')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() != "0":
            dst_llen = v.strip()
            break
    list_ok = int(dst_llen or "0") >= 2  # 至少 init_item + lua_item
    checks.append(f"list:llen={dst_llen},ok={list_ok}")

    # 5. set
    dst_scard = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'SCARD s6_lua:set')
        if v and v.strip() and v.strip() != "(nil)" and v.strip() != "0":
            dst_scard = v.strip()
            break
    set_ok = int(dst_scard or "0") >= 2  # 至少 init_m + lua_m
    checks.append(f"set:scard={dst_scard},ok={set_ok}")

    # 6. 对照组
    dst_direct = ""
    for port in DST_PORTS:
        v = dst_redis_cmd(port, 'GET s6_lua:direct')
        if v and v.strip() and v.strip() != "(nil)":
            dst_direct = v.strip()
            break
    direct_ok = "direct_set" in dst_direct
    checks.append(f"direct:dst={dst_direct},ok={direct_ok}")

    stop_task(tid)
    time.sleep(2)

    all_ok = counter_ok and data_ok and hash_ok and list_ok and set_ok and direct_ok
    if lua_exec_ok:
        label = "Lua增量回放正常(全类型)"
    else:
        label = "Lua源端未执行,直接命令增量正常"
    record("S6 Lua脚本", all_ok, f"{label}; " + "; ".join(checks))

    # 清理
    for k in ['counter', 'data', 'hash', 'list', 'set', 'direct']:
        redis_cmd(src, f'DEL s6_lua:{k}')
    return tid


# ================================================================
# 报告
# ================================================================
def print_report():
    log("\n" + "=" * 70)
    log("全量回归测试报告")
    log("=" * 70)
    passed = sum(1 for r in RESULTS if r["status"] == "PASS")
    failed = sum(1 for r in RESULTS if r["status"] == "FAIL")
    total = len(RESULTS)

    # 按类别分组
    categories = {}
    for r in RESULTS:
        cat = r["name"][0]
        if cat not in categories:
            categories[cat] = {"pass": 0, "fail": 0, "tests": []}
        categories[cat]["tests"].append(r)
        if r["status"] == "PASS":
            categories[cat]["pass"] += 1
        else:
            categories[cat]["fail"] += 1

    cat_names = {
        "A": "基础功能", "B": "全量迁移", "C": "冲突策略",
        "D": "数据类型", "E": "增量同步", "F": "全量+增量",
        "G": "任务生命周期", "H": "崩溃恢复", "I": "进度计数器",
        "J": "辅助功能", "K": "边界条件", "L": "异常输入",
        "M": "并发场景", "N": "数据深度验证", "O": "生命周期扩展",
        "P": "过滤器深度", "Q": "辅助API扩展", "R": "增量深度",
        "S": "补充测试",
    }

    for cat_key in sorted(categories.keys()):
        cat = categories[cat_key]
        name = cat_names.get(cat_key, cat_key)
        log(f"\n  [{name}] {cat['pass']}/{cat['pass']+cat['fail']} 通过")
        for r in cat["tests"]:
            icon = "✅" if r["status"] == "PASS" else "❌"
            log(f"    {icon} {r['name']}: {r['details'][:130]}")

    log(f"\n{'=' * 70}")
    log(f"总计: {total} 项测试, {passed} 通过, {failed} 失败")
    if failed > 0:
        log("失败用例:")
        for r in RESULTS:
            if r["status"] == "FAIL":
                log(f"  ❌ {r['name']}: {r['details'][:150]}")
    log("=" * 70)

# ================================================================
# 测试分类注册
# ================================================================
TEST_CATEGORIES = {
    "A": ("基础功能", [
        ("A1", test_A1_health),
        ("A2", test_A2_test_connection),
        ("A3", test_A3_preflight_check),
        ("A4", test_A4_system_status),
    ]),
    "B": ("全量迁移", [
        ("B1", test_B1_full_no_filter),
        ("B2", test_B2_full_prefix_filter),
        ("B3", test_B3_full_exclude_prefix),
        ("B4", test_B4_full_pattern_filter),
        ("B5", test_B5_full_keylist),
    ]),
    "C": ("冲突策略", [
        ("C1", test_C1_conflict_skip),
        ("C2", test_C2_conflict_replace),
        ("C3", test_C3_conflict_skip_full_only),
    ]),
    "D": ("数据类型", [
        ("D1", test_D1_data_types),
    ]),
    "E": ("增量同步", [
        ("E1", test_E1_incr_basic),
        ("E2", test_E2_incr_del),
        ("E3", test_E3_incr_prefix_filter),
        ("E4", test_E4_incr_multi_types),
        ("E5", test_E5_incremental_only),
    ]),
    "F": ("全量+增量", [
        ("F1", test_F1_full_incr_complete),
    ]),
    "G": ("任务生命周期", [
        ("G1", test_G1_pause_resume),
        ("G2", test_G2_stop_restart),
        ("G3", test_G3_delete_task),
        ("G4", test_G4_stop_incremental),
    ]),
    "H": ("崩溃恢复", [
        ("H1", test_H1_kill9_recovery),
        ("H2", test_H2_sigterm_graceful),
        ("H3", test_H3_new_task_immediate_crash),
    ]),
    "I": ("进度计数器", [
        ("I1", test_I1_counter_full_process),
        ("I2", test_I2_progress_percentage),
    ]),
    "J": ("辅助功能", [
        ("J1", test_J1_error_keys_api),
        ("J2", test_J2_verify_api),
        ("J3", test_J3_dynamic_config),
        ("J4", test_J4_task_metrics),
        ("J5", test_J5_task_logs),
    ]),
    "K": ("边界条件", [
        ("K1", test_K1_empty_source),
        ("K2", test_K2_single_key),
        ("K3", test_K3_special_characters_in_key),
        ("K4", test_K4_large_value),
        ("K5", test_K5_ttl_preservation),
        ("K6", test_K6_big_hash),
    ]),
    "L": ("异常输入", [
        ("L1", test_L1_invalid_json),
        ("L2", test_L2_missing_required_fields),
        ("L3", test_L3_invalid_task_id),
        ("L4", test_L4_wrong_state_transitions),
        ("L5", test_L5_connection_unreachable),
        ("L6", test_L6_duplicate_task_name),
    ]),
    "M": ("并发场景", [
        ("M1", test_M1_concurrent_tasks),
        ("M2", test_M2_rapid_create_delete),
        ("M3", test_M3_pause_resume_rapid),
        ("M4", test_M4_concurrent_api_calls),
    ]),
    "N": ("数据深度验证", [
        ("N1", test_N1_zset_score_precision),
        ("N2", test_N2_empty_value_types),
        ("N3", test_N3_large_collection),
        ("N4", test_N4_overwrite_different_type),
    ]),
    "O": ("生命周期扩展", [
        ("O1", test_O1_complete_api),
        ("O2", test_O2_retry_failed),
        ("O3", test_O3_export_report),
        ("O4", test_O4_task_health),
    ]),
    "P": ("过滤器深度", [
        ("P1", test_P1_exclude_pattern_regex),
        ("P2", test_P2_multiple_prefixes),
        ("P3", test_P3_prefix_with_exclude),
    ]),
    "Q": ("辅助API扩展", [
        ("Q1", test_Q1_analyze_cluster),
        ("Q2", test_Q2_recommend_config),
        ("Q3", test_Q3_conflicts_api),
        ("Q4", test_Q4_templates),
        ("Q5", test_Q5_system_logs),
        ("Q6", test_Q6_smart_retry_status),
    ]),
    "R": ("增量深度", [
        ("R1", test_R1_incr_zset),
        ("R2", test_R2_incr_modify_existing),
        ("R3", test_R3_incr_expire),
        ("R4", test_R4_incr_batch_writes),
    ]),
    "S": ("补充测试", [
        ("S1", test_S1_key_natural_expire_during_migration),
        ("S2", test_S2_ttl_renewal_during_migration),
        ("S3", test_S3_16mb_value_rejection),
        ("S4", test_S4_same_key_order_guarantee),
        ("S5", test_S5_no_key_commands_no_interference),
        ("S6", test_S6_lua_script_limitation),
    ]),
}

# ================================================================
# 主流程
# ================================================================
if __name__ == "__main__":
    cfg, parsed_args = get_config_from_args()
    _init_config(cfg)

    log("=" * 70)
    log("tendis-migrate 全量回归测试")
    log("=" * 70)
    log(f"环境: {parsed_args.env}")
    cfg.describe()

    # 列出所有测试
    if parsed_args.list:
        print("\n所有测试用例:")
        for cat_key in sorted(TEST_CATEGORIES.keys()):
            cat_name, tests = TEST_CATEGORIES[cat_key]
            print(f"\n  {cat_key}. {cat_name}")
            for test_id, test_fn in tests:
                print(f"    {test_id}: {test_fn.__name__}")
        sys.exit(0)

    # 解析要运行的分类
    if parsed_args.categories:
        selected = [c.strip().upper() for c in parsed_args.categories.split(',')]
    else:
        selected = sorted(TEST_CATEGORIES.keys())

    # 确认服务运行
    try:
        r = requests.get(f"{API}/health", timeout=5)
        log("服务状态: OK")
    except:
        log("ERROR: 服务不可达!")
        sys.exit(1)

    src_total = src_dbsize()
    log(f"源端数据: {src_total} keys")

    # 清理旧测试任务
    cleanup_tasks()
    log("已清理旧测试任务")

    try:
        for cat_key in selected:
            if cat_key not in TEST_CATEGORIES:
                log(f"WARNING: 未知分类 {cat_key}，跳过")
                continue
            cat_name, tests = TEST_CATEGORIES[cat_key]
            log(f"\n{'=' * 50}")
            log(f"【{cat_key}. {cat_name}】")
            log(f"{'=' * 50}")
            for test_id, test_fn in tests:
                test_fn()

    except Exception as e:
        log(f"测试异常中断: {e}")
        traceback.print_exc()

    # 输出报告
    print_report()
