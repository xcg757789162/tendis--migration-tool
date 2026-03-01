# Tendis-Migrate 综合测试方案

**更新日期**: 2026-02-28  
**测试脚本**: `tests/regression_test.py`（97 个测试用例，20 个分类）

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
| **T** | **OOM 保护** | **9** | **Key清单上传预览、错误Key限流下载、校验overflow、限速配置** |
| **U** | **历史问题回归** | **12** | **TROUBLESHOOTING 场景覆盖：字段名错误、增量Pattern、系统Key、增量暂停恢复、TTL精度** |

**总计: 20 个分类, 97 个测试用例**

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

### T. OOM 保护（9 项）

| ID | 测试名称 | 说明 |
|:---|:---|:---|
| **T1** | **小文件上传预览** | 50 Key TXT 文件上传，验证 code=0, total=50, truncated=false |
| **T2** | **大文件截断预览** | **150 万 Key 文件上传，验证 code=0（不报错！）, truncated=true, total≥140万** |
| T3 | CSV 格式上传 | CSV Key 清单正确解析 |
| T4 | JSON 格式上传 | JSON Key 清单正确解析 |
| T5 | 内容解析 API | parse-keylist 去重验证 |
| **T6** | **错误 Key 限流+下载** | error-keys API 分页限流 + CSV/ZIP 流式下载 |
| **T7** | **校验 overflow 标记** | 验证校验结果包含 mismatch_overflow 字段 |
| T8 | 错误 Key 统计 | metrics 中 error_keys 统计信息 |
| T9 | 限速配置 | 创建任务时设置 source_qps/target_qps 限速 |

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

### T2. 大文件截断预览（150 万 Key）

**测试目的**: 验证 Key 清单文件超过 100 万 Key 上限时，不报错，而是截断预览 + 提示。

**背景（2026-02-28 改进）**: 之前 `LoadKeyListFromFile` 超过 100 万 Key 直接返回 400 错误，阻止用户上传。但实际迁移走 `StreamKeyListFromFile`（流式处理，无上限），所以报错是不合理的。

| 项目 | 值 |
|:---|:---|
| **文件规模** | 150 万 Key（约 30MB TXT） |
| **预期行为** | code=0, truncated=true, total=1500000 |
| **之前行为** | ❌ code=400, "key list has 1500000 keys (> 1000000 limit)" |
| **实际迁移** | 不受影响，走 StreamKeyListFromFile 流式处理 |

**三层保护策略**：

| 文件大小 | Key 数量 | 处理方式 |
|:---|:---|:---|
| ≤ 200MB | ≤ 100 万 | 全量解析 + 完整预览 |
| ≤ 200MB | > 100 万 | 全量解析 + 截断预览到 100 万 |
| > 200MB | - | 采样前 4MB 估算总量 + 前 10 条预览 |

---

### T6. 错误 Key 限流 + 流式下载

**测试目的**: 验证 error-keys API 的 OOM 保护机制。

| 保护措施 | 说明 |
|:---|:---|
| **分页查询限流** | `GET /error-keys?page=1&page_size=100`，返回 `actual_total` 和 `truncated` |
| **CSV 流式下载** | `GET /error-keys/download`，大数据量时流式输出不全量加载 |
| **removedErrorKeys 落盘** | 每任务超 100 万条已移除 Key 时自动落盘到文件 |

---

### T7. 校验 mismatch_overflow 标记

**测试目的**: 验证数据校验的不一致 Key 有 100 万上限保护，超出后设置 `mismatch_overflow=true`。

| 项目 | 值 |
|:---|:---|
| **上限** | maxMismatchKeys = 1,000,000 |
| **超出行为** | 停止收集新的不一致，设置 mismatchOverflow 标记 |
| **测试方式** | 小数据集制造 1 个不一致，验证字段存在 |

---

### T9. 限速配置与参数协同

**测试目的**: 验证 source_qps/target_qps 限速配置能正确传入并生效。

**参数协同关系**：

```
SCAN 生产端（不限速）        Worker 消费端（限速在这里）
┌───────────────────┐       ┌─────────────────────────────────┐
│ Node1/2/3 SCAN    │──→ keyChan ──→│  全局共享限速器       │
│ COUNT=scan_batch   │  (workers×100) │  Worker-0 ... Worker-N│
└───────────────────┘       └─────────────────────────────────┘
```

| 参数 | 控制什么 | 粒度 |
|:---|:---|:---|
| source_qps | 全局源端 QPS 上限 | 每个 Key 算 1 次 |
| target_qps | 全局目标端 QPS 上限 | 每个 Key 算 1 次 |
| workers | 消费端并发数 | 所有 Worker 共享限速器 |
| scan_batch_size | SCAN COUNT 参数 | 不受限速器控制 |

---

### U. 历史问题回归（12 项）

基于 `TROUBLESHOOTING_GUIDE.md` 中记录的历史 Bug 和问题，确保这些问题不会回归。

| ID | 测试名称 | 对应 TROUBLESHOOTING 编号 | 说明 |
|:---|:---|:---|:---|
| **U1** | **错误字段名拒绝** | §2.1 | 用错误字段名 source/target/addresses 创建任务，应被拒绝 |
| **U2** | **增量计数器字段位置** | §2.2, §3.2 | incr_keys_synced 在 stats 中可获取 + heartbeats 检测 |
| **U3** | **增量 Pattern 过滤** | §3.1 | 增量阶段 matchSimplePattern 通配符匹配（非 strings.Contains） |
| **U4** | **系统 Key 过滤** | §12 BUG-1 | stat:total/daily/hourly 等系统内部 Key 不被迁移 |
| **U5** | **增量阶段暂停恢复** | §12.1 (P0) | 增量阶段暂停后恢复，不应重新执行全量迁移 |
| **U6** | **Stop API 路由** | §14.1 | `/tasks/{id}/stop` 路由可用，返回 code=0 |
| **U7** | **空请求体拒绝** | §14.2 | 空 JSON `{}` 或只有 name 创建任务，应被拒绝 |
| **U8** | **启动不存在任务** | §14.3 | 启动不存在的 task_id，应返回错误而非 success |
| **U9** | **待迁移数不为 0** | §9.6 | 全量迁移中 keys_to_migrate 应 > 0 |
| **U10** | **动态限速不卡死** | BUG-5 | 运行中修改 QPS 限速后任务不卡死，正常完成 |
| **U11** | **增量 TTL 毫秒精度** | §7.4 | EXPIRE/PEXPIRE/PERSIST 增量同步后 TTL 一致 |
| **U12** | **优雅关闭自动恢复** | BUG-3 | SIGTERM 后 ShutdownPaused 任务重启自动恢复 |

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

## OOM 保护覆盖矩阵

| 保护点 | 机制 | 测试用例 | 100 亿 Key 适用 |
|:---|:---|:---|:---|
| **Key 清单上传** | 200MB 采样 + 100万截断 | T1, T2, T3, T4 | ✅ 流式迁移不受影响 |
| **Key 清单解析** | 去重 + 预览截断 | T5 | ✅ |
| **错误 Key 查询** | 分页限流 + actual_total | T6 | ✅ 不全量加载 |
| **错误 Key 下载** | 流式 CSV/ZIP | T6 | ✅ |
| **removedErrorKeys** | 100万/任务 + 落盘 | (间接) T6 | ✅ 自动落盘 |
| **校验不一致 Key** | 100万上限 + overflow 标记 | T7 | ✅ 停止收集 |
| **错误 Key 统计** | getErrorKeysStats | T8 | ✅ 原子计数器 |
| **限速保护** | 全局令牌桶 + 背压 | T9 | ✅ 精确控速 |

---

## TROUBLESHOOTING 问题覆盖矩阵

| TROUBLESHOOTING 编号 | 问题描述 | 测试用例 | 覆盖状态 |
|:---|:---|:---|:---|
| §2.1 | API 字段名错误(source/addresses) | **U1** | ✅ 直接覆盖 |
| §2.2 | incr_keys_synced 字段位置 | **U2** | ✅ 直接覆盖 |
| §3.1 | Pattern 匹配 strings.Contains bug | **U3**, B4, P1 | ✅ 增量+全量 |
| §3.2 | FakeSlave heartbeats 检测 | **U2** | ✅ 直接覆盖 |
| §3.3 | 增量计数不更新 | E1-E5, **U2** | ✅ 多角度 |
| §7.4 | TTL 一致性(PTTL精度/PERSIST) | K5, R3, S2, **U11** | ✅ 增量+全量 |
| §8.3/8.4 | incremental 纯增量模式 | E5 | ✅ |
| §8.6 | 集群拓扑缓存 :0 | _(无法自动测)_ | ⚠️ 需异常集群 |
| §8.7 | DBSIZE 超时 totalKeys | **U9** (间接) | ⚠️ 验证不为0 |
| §9.6 | 待迁移Key数显示为0 | **U9**, I1, I2 | ✅ |
| §12.1 (P0) | 增量恢复后重新全量 | **U5** | ✅ 直接覆盖 |
| §12.2 (P0) | 增量阶段并发新全量 | **U5** (间接) | ⚠️ |
| §12.3 (P1) | 断点恢复 cursor=0 | H1, H3 | ✅ |
| §14.1 | Stop API 404 | **U6** | ✅ 直接覆盖 |
| §14.2 | 空参数创建任务 | **U7**, L2 | ✅ |
| §14.3 | 启动不存在任务返回 success | **U8**, L3 | ✅ |
| BUG-1(§12) | 系统 key 被迁移 | **U4** | ✅ 直接覆盖 |
| BUG-3 | ShutdownPaused 自动恢复 | **U12** | ✅ 直接覆盖 |
| BUG-4 | 限速 WaitN 不生效 | T9 | ✅ |
| BUG-5 | 动态调整限速卡死 | **U10** | ✅ 直接覆盖 |
| BUG-6 | 多Worker限速退化 | T9 (间接) | ⚠️ 需大数据量 |

---

## 运行方式

```bash
# 运行全部 97 项测试
python3 regression_test.py --env cloud-local

# 运行指定分类
python3 regression_test.py --env cloud-local --categories S,K,N,T,U

# 只运行 OOM 保护测试
python3 regression_test.py --env cloud-local --categories T

# 只运行历史问题回归测试
python3 regression_test.py --env cloud-local --categories U

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
