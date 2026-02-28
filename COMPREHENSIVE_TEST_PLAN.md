# Tendis-Migrate 综合测试方案

**更新日期**: 2026-02-27  
**测试脚本**: `tests/regression_test.py`（76 个测试用例，18 个分类）

---

## 测试环境

### Cloud 测试环境（默认测试服务器 1.95.147.159）
- **工具运行位置**: 192.168.0.142:8088（Web UI）
- **源端 Tendis**: 192.168.0.142:7001, 192.168.0.142:7002
- **目标端 Tendis**: 192.168.0.142:8001, 192.168.0.142:8002
- **Tendis 版本**: 2.7.0, kvstorecount=2, binlog-enabled=yes

### 家里测试环境
- **工具运行位置**: 本地 Mac（localhost:8088）
- **Docker 服务器**: 192.168.1.19
- **源端 Tendis**: 192.168.1.19:7001, 192.168.1.19:7002
- **目标端 Tendis**: 192.168.1.19:8001, 192.168.1.19:8002

---

## 测试分类总览

| 分类 | 名称 | 测试数 | 说明 |
|:---:|:---|:---:|:---|
| **A** | 基础功能 | 4 | 健康检查、连接测试、迁移前校验、系统状态 |
| **B** | 全量迁移 | 5 | 无过滤、前缀过滤、排除前缀、Pattern、Keylist |
| **C** | 冲突策略 | 3 | skip、replace、skip_full_only |
| **D** | 数据类型 | 1 | string/hash/list/set/zset 全类型迁移 |
| **E** | 增量同步 | 5 | 基本SET、DEL、前缀过滤、多类型、纯增量模式 |
| **F** | 全量+增量 | 1 | 完整流程（全量→增量→验证） |
| **G** | 任务生命周期 | 4 | 暂停/恢复、停止/重启、删除、停止增量 |
| **H** | 崩溃恢复 | 3 | kill-9、SIGTERM优雅关闭、新任务立即崩溃 |
| **I** | 进度计数器 | 2 | 计数器全过程监控、百分比合理性 |
| **J** | 辅助功能 | 5 | 错误Key API、校验API、动态配置、指标、日志 |
| **K** | 边界条件 | 6 | 空源端、单Key、特殊字符、大Value、TTL保持、**大Hash(10000字段)** |
| **L** | 异常输入 | 6 | 无效JSON、缺字段、无效ID、非法状态转换、不可达地址、重复名 |
| **M** | 并发场景 | 4 | 多任务并发、快速创建删除、快速暂停恢复、并发API |
| **N** | 数据深度验证 | 4 | ZSet精度、空值类型、**大集合(List/Set/ZSet各10000元素)**、覆盖不同类型 |
| **O** | 生命周期扩展 | 4 | 手动完成、重试失败、导出报告、健康状态 |
| **P** | 过滤器深度 | 3 | 排除Pattern正则、多前缀组合、前缀+排除 |
| **Q** | 辅助API扩展 | 6 | 集群分析、推荐配置、冲突Key、模板、系统日志、智能重试 |
| **R** | 增量深度 | 4 | ZSet增量、修改已有Key、增量EXPIRE、批量写入 |
| **S** | 补充测试 | 6 | 自然过期、TTL续期、16MB拦截、**同Key全类型顺序**、无Key命令、**Lua多类型** |

**总计: 18 个分类, 76 个测试用例**

---

## 完整测试用例清单

### A. 基础功能（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| A1 | 健康检查 API | GET /api/v1/health |
| A2 | 测试连接 API | POST /api/v1/test-connection |
| A3 | 迁移前校验 API | POST /api/v1/preflight-check |
| A4 | 系统状态 API | GET /api/v1/system/status |

### B. 全量迁移（5 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| B1 | 全量无过滤 | 数据完整性 + 计数器全过程监控 |
| B2 | 全量前缀过滤 | 验证只迁移指定前缀 |
| B3 | 全量排除前缀 | 验证排除生效 |
| B4 | 全量 Pattern 过滤 | 通配符过滤 |
| B5 | 全量 Keylist | 指定 Key 列表迁移 |

### C. 冲突策略（3 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| C1 | 冲突 skip | 全阶段跳过已存在 Key |
| C2 | 冲突 replace | 直接覆盖 |
| C3 | 冲突 skip_full_only | 全量跳过 + 增量覆盖（默认策略） |

### D. 数据类型（1 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| D1 | 多数据类型迁移 | string/hash/list/set/zset 全类型正确性 |

### E. 增量同步（5 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| E1 | 增量基本 SET | 基础 SET 操作增量同步 |
| E2 | 增量 DEL | DEL 操作增量同步 |
| E3 | 增量前缀过滤 | 验证 blocked 前缀不被同步 |
| E4 | 增量多类型 | hash/list/set 操作增量同步 |
| E5 | 纯增量模式 | 跳过全量，直接增量 |

### F. 全量+增量（1 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| F1 | 完整流程 | 全量→增量→验证 |

### G. 任务生命周期（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| G1 | 暂停/恢复 | pause → resume |
| G2 | 停止/重启 | stop → restart |
| G3 | 删除任务 | delete task |
| G4 | 停止增量 | stop-incremental |

### H. 崩溃恢复（3 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| H1 | kill-9 崩溃恢复 | 全量阶段断点续传 |
| H2 | SIGTERM 优雅关闭 | 保存状态后退出 |
| H3 | 新任务立即崩溃 | 验证立即持久化修复 |

### I. 进度计数器（2 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| I1 | 计数器全过程监控 | migrated <= to_migrate 贯穿始终 |
| I2 | 进度百分比合理性 | 使用全量数据确保采到中间进度 |

### J. 辅助功能（5 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| J1 | 错误 Key API | 错误 Key 记录与查询 |
| J2 | 数据校验 API | verify-tasks |
| J3 | 动态配置 | 运行时调整配置 |
| J4 | 任务指标 API | metrics 接口 |
| J5 | 任务日志 API | logs 接口 |

### K. 边界条件（6 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| K1 | 空源端 | 源端无匹配数据 |
| K2 | 单 Key | 最小数据集 |
| K3 | 特殊字符 Key | 空格、中文、二进制 |
| K4 | 大 Value | 1MB 字符串 |
| K5 | TTL 保持 | 迁移后 TTL 不丢失 |
| **K6** | **大 Hash (10000 字段)** | **100 批 × 100 字段，8 点采样验证** |

### L. 异常输入（6 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| L1 | 无效 JSON | 请求体格式错误 |
| L2 | 缺少必填字段 | 必填字段缺失 |
| L3 | 无效任务 ID | 不存在的 ID |
| L4 | 非法状态转换 | pending 任务执行 pause/resume/stop |
| L5 | 不可达地址 | 连接超时处理 |
| L6 | 重复任务名 | 是否允许 |

### M. 并发场景（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| M1 | 多任务并发 | 同时运行多个迁移任务 |
| M2 | 快速创建删除 | 并发安全 |
| M3 | 快速暂停恢复 | 状态机稳定性 |
| M4 | 并发 API | 同时请求多个接口 |

### N. 数据深度验证（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| N1 | ZSet Score 精度 | 浮点数精度不丢失 |
| N2 | 空值/空集合 | 空 string/hash/list/set/zset 迁移 |
| **N3** | **大集合 (10000 元素)** | **List/Set/ZSet 各 10000 元素，50 批 × 200** |
| N4 | 覆盖不同类型 | replace 策略下类型变换 |

### O. 生命周期扩展（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| O1 | 手动标记完成 | stop-incremental + complete |
| O2 | 重试失败 Key | retry-failed API |
| O3 | 导出报告 | 任务配置和报告导出 |
| O4 | 任务健康状态 | health API |

### P. 过滤器深度（3 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| P1 | 排除 Pattern 正则 | exclude_patterns |
| P2 | 多前缀组合 | 5 个前缀组合 |
| P3 | 前缀包含+排除 | prefixes + exclude_prefixes |

### Q. 辅助 API 扩展（6 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| Q1 | 集群分析 API | analyze-cluster |
| Q2 | 推荐配置 API | recommend-config |
| Q3 | 冲突 Key API | conflicts 查看 |
| Q4 | 模板管理 | 列表/查看 |
| Q5 | 系统日志 API | system/logs |
| Q6 | 智能重试状态 | smart-retry-status |

### R. 增量深度（4 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| R1 | 增量 ZSet | ZADD/ZREM 增量同步 |
| R2 | 增量修改已有 Key | HSET 追加字段/RPUSH 追加元素 |
| R3 | 增量 EXPIRE | 增量阶段设置 TTL |
| R4 | 增量批量写入 | 1000 Key 快速写入 |

### S. 补充测试（6 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| S1 | Key 自然过期 | 源端 Key 过期后目标端同步过期 |
| S2 | TTL 续期 | PERSIST/续期后目标端不丢 Key |
| S3 | 16MB 超大值拦截 | Tendis 拒绝超过 16MB 的 RESTORE |
| **S4** | **同 Key 全类型顺序** | **9 类链式操作：SET×20/APPEND×10/Hash/List/Set/ZSet/TTL链/INCR/DEL重建** |
| S5 | 无 Key 命令 | PING/DBSIZE/INFO 不影响迁移 |
| **S6** | **Lua 脚本多类型** | **EVAL 5 种类型：INCR/SET/HSET/RPUSH/SADD** |

---

## 重点场景详细说明

### S4. 同 Key 命令顺序保证（全类型 + 链式操作）

**测试目的**: 验证针对同一个 Key 的快速连续操作，目标端最终值与源端一致。

**覆盖 9 类链式操作**:

| # | 操作 | 验证内容 | 状态 |
|:---:|:---|:---|:---:|
| 1 | **String 快速 SET 20 次** | 最终值 = version_20 | ✅ |
| 2 | **APPEND 链式** (SET→APPEND x10) | 拼接结果含 START + _part10 | ✅ |
| 3 | **Hash HSET→HDEL→HSET** | 最终 f1=final_v1, f2=final_v2 | ✅ |
| 4 | **List RPUSH→LPUSH→RPUSH** | 最终 6 元素 (z a b c d e)，顺序正确 | ✅ |
| 5 | **Set SADD→SREM→SADD** | 最终 {m1,m3,m5,m6}，不含 m2,m4 | ✅ |
| 6 | **ZSet ZADD→ZREM→ZADD** | 最终 3 成员 (za=1.5,zc=3.0,zd=5.0)，zb 已删除 | ✅ |
| 7 | **TTL 链式** (EXPIRE→PERSIST→EXPIRE) | 最终 TTL ≈1800 (±200s) | ✅ |
| 8 | **INCR 20 次** | counter = 20 | ✅ |
| 9 | **DEL→重建** | 最终值 = after_recreate | ✅ |

**测试流程**: 创建 full_and_incremental 任务 → 等待进入增量阶段 → 快速执行全部链式操作 → 等待同步 30s → 逐项验证

---

### K6. 大 Hash 迁移（10000 字段）

**测试目的**: 验证超大 Hash 的全量迁移完整性。

| 项目 | 值 |
|:---|:---|
| **字段数** | 10,000 |
| **写入方式** | 100 批 × 100 字段/批 |
| **验证方式** | HLEN 比对 + 8 个采样点 (f0, f100, f500, f1000, f2500, f5000, f7500, f9999) |
| **迁移模式** | full_only + keylist |
| **测试结果** | ✅ src=10000, dst=10000, sample=8/8 |

---

### N3. 大集合迁移（List/Set/ZSet 各 10000 元素）

**测试目的**: 验证三种集合类型在 10000 元素规模下的全量迁移完整性。

| 类型 | 元素数 | 写入方式 | 验证方式 | 结果 |
|:---|:---:|:---|:---|:---:|
| **List** | 10,000 | 50 批 × 200 RPUSH | LLEN 比对 | ✅ |
| **Set** | 10,000 | 50 批 × 200 SADD | SCARD 比对 | ✅ |
| **ZSet** | 10,000 | 50 批 × 200 ZADD (带 score) | ZCARD 比对 | ✅ |

---

### S6. Lua 脚本增量回放（多类型操作）

**测试目的**: 验证通过 EVAL 执行的 Lua 脚本修改在增量同步中正确回放。

| # | Lua 操作 | 验证内容 | 状态 |
|:---:|:---|:---|:---:|
| 1 | `EVAL redis.call("INCR", KEYS[1])` × 10 次 | counter = 10 | ✅ |
| 2 | `EVAL redis.call("SET", KEYS[1], ARGV[1])` | data = lua_modified | ✅ |
| 3 | `EVAL redis.call("HSET", KEYS[1], ARGV[1], ARGV[2])` | hash.f2 = lua_v2 | ✅ |
| 4 | `EVAL redis.call("RPUSH", KEYS[1], ARGV[1])` | list llen ≥ 2 | ✅ |
| 5 | `EVAL redis.call("SADD", KEYS[1], ARGV[1])` | set scard ≥ 2 | ✅ |

**注**: 使用 `echo | redis-cli` pipe 方式避免 SSH 引号嵌套问题。若 Lua 源端执行失败（引号问题），自动 fallback 到直接命令验证增量同步。

---

## 数据类型全覆盖矩阵

| 数据类型 | 全量测试 | 增量测试 | 大 Key 测试 | 顺序操作 |
|:---|:---|:---|:---|:---|
| **String** | D1, B1, K2, K4 | E1, E2, R4 | K4 (1MB), S3 (16MB) | S4 (SET×20, APPEND×10) |
| **Hash** | D1, K6 | E4, R2 | **K6 (10000字段)** | S4 (HSET→HDEL→HSET) |
| **List** | D1, N3 | E4 | **N3 (10000元素)** | S4 (RPUSH→LPUSH→RPUSH) |
| **Set** | D1, N3 | E4 | **N3 (10000元素)** | S4 (SADD→SREM→SADD) |
| **ZSet** | D1, N1, N3 | R1 | **N3 (10000元素)** | S4 (ZADD→ZREM→ZADD) |
| **Lua 脚本** | - | **S6 (5种类型)** | - | - |
| **过期 Key** | S1, S2, K5 | R3 | - | S4 (EXPIRE→PERSIST→EXPIRE) |

---

## 运行方式

```bash
# 运行全部 76 项测试
python3 regression_test.py --env cloud-local

# 运行指定分类
python3 regression_test.py --env cloud-local --categories S,K,N

# 列出所有测试
python3 regression_test.py --list
```

---

## 最近测试结果（2026-02-27，cloud-local 环境）

### S 分类（补充测试）全部通过

| 测试 | 结果 | 关键验证 |
|:---|:---:|:---|
| S1 自然过期 | ✅ | 源端过期 → 目标端同步过期 |
| S2 TTL 续期 | ✅ | PERSIST/续期后目标端不丢 Key |
| S3 16MB 拦截 | ✅ | 超大值被正确拦截 |
| **S4 同Key顺序** | ✅ | **9 项全部通过**: str/append/hash/list/set/zset/ttl/counter/recreate |
| S5 无Key命令 | ✅ | PING/DBSIZE/INFO 不影响迁移 |
| **S6 Lua脚本** | ✅ | **Lua 源端成功执行，5 种类型增量回放全部一致** |

### K 分类（边界条件）全部通过

| 测试 | 结果 | 关键验证 |
|:---|:---:|:---|
| K1-K5 | ✅ | 空源端/单Key/特殊字符/大Value/TTL保持 |
| **K6 大Hash** | ✅ | **10000 字段，src=10000→dst=10000，sample=8/8** |

### N 分类（数据深度验证）全部通过

| 测试 | 结果 | 关键验证 |
|:---|:---:|:---|
| N1-N2 | ✅ | ZSet精度/空值类型 |
| **N3 大集合** | ✅ | **List/Set/ZSet 各 10000 元素，全部一致** |
| N4 | ✅ | 覆盖不同类型 |
