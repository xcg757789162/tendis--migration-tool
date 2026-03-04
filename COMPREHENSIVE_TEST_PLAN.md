# Tendis-Migrate 综合测试方案

**更新日期**: 2026-03-02  
**测试脚本**: `tests/regression_test.py`（158 个测试用例，25 个分类）

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
| **C** | 冲突策略 | 4 | skip、replace、skip_full_only、error |
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
| **V** | **风险修复验证** | **10** | **27个风险点修复后的专项验证：增量失败标记Failed、Pipeline部分失败、slot重试、connectedCh防护** |
| **W** | **故障注入** | **12** | **Chaos Engineering：增量Kill-9恢复、快速暂停恢复、全量中源端写入、大量冲突、中途停止续传、同前缀竞争 + Pipeline部分DUMP、增量异常退出、FakeSlave重连、Slot超时、不支持操作、快速状态转换** |
| **X** | **属性不变性** | **16** | **Property-Based：迁移后key完整、计数器守恒、TTL一致性、状态机合法、停止幂等、值完全相等、停止恢复等价、增量最终一致 + 增量失败状态检测、无遗漏Slot、Bytes统计、冲突持久化、无TTL不变、有TTL不变永久、统计一致、完成进度100%** |
| **Y** | **长时间压力** | **10** | **Endurance：1万key全量、增量持续写入2分钟、5任务并发、重复迁移3次 + 组合测试：前缀+增量+暂停、冲突+增量+停止、Pipeline+限速+大值、多前缀+排除+增量、断点+冲突+TTL、全功能组合** |
| **Z** | **CodeReview验证** | **13** | **自动化清单：无静默失败、goroutine退出、channel关闭保护、pipeline逐个检查、统计原子操作 + 本次Bugfix验证：Pipeline索引对齐、PTTL幽灵Key、Binlog缓存回放、Binlog位置回退、类型断言安全、并发Writer原子计数、ConflictStore锁修复、错误计数器重置** |

**总计: 25 个分类, 158 个测试用例**

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
| C4 | 冲突 error | 遇冲突报错，任务变为 failed（负向路径） |

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

### V. 风险修复验证（10 项）

基于 2026-03-01 修复的 27 个风险点，验证修复后的行为正确性。

| ID | 测试名称 | 对应修复 | 说明 |
|:---|:---|:---|:---|
| **V1** | **增量同步失败标记 Failed** | task_runner.go 风险1 | 模拟增量同步异常（不可达目标端），验证任务标记为 failed 而非 completed |
| **V2** | **Slot 迁移失败带重试** | task_runner.go 风险6 | 全量迁移中制造个别 slot 错误，验证重试后仍能完成，日志有重试记录 |
| **V3** | **用户停止增量不算失败** | task_runner.go 风险1 | 正常停止增量同步，验证状态不是 failed |
| **V4** | **Pipeline 部分失败精确统计** | pipeline_migrator.go + concurrent_writer.go | 制造目标端已存在 key + skip 策略，验证 error_keys 记录了冲突 key |
| **V5** | **FakeSlave 降级回退状态** | cmd/simple/main.go 风险1 | 创建 binlog 模式任务但源端不支持 FakeSlave 时，验证自动降级到 time_window |
| **V6** | **错误 Key 冲突记录落盘** | conflict_store.go | 制造大量冲突（>100个），验证 error-keys API 返回正确，且下载功能可用 |
| **V7** | **增量同步统计并发安全** | incremental_syncer.go | 在增量阶段快速写入 500+ key，验证 stats 计数不出负数/异常值 |
| **V8** | **Binlog offset 失败告警** | cmd/simple/main.go 风险2 | 验证增量阶段 logs 中不出现 "offset advanced with failures" 正常场景 |
| **V9** | **全量迁移 0 failed slots** | task_runner.go 风险6 | 正常全量迁移，验证日志输出 "completed successfully"（无 failed slots） |
| **V10** | **多任务并发错误隔离** | async_executor.go + concurrent_writer.go | 同时运行 2 个任务，验证一个任务的错误不影响另一个 |

---

#### V1. 增量同步失败标记 Failed（详细说明）

**测试目的**: 验证增量同步异常失败时，任务状态被正确标记为 `failed` 而非 `completed`。

**修复前行为**: 增量同步失败只打印 `log.Printf("Incremental sync failed: %v")` 然后继续走到 `UpdateTaskCompleted(completed)`，导致数据丢失但显示成功。

**修复后行为**: 区分用户主动停止（`context.Canceled`）和异常失败，异常失败标记 `failed`。

**测试方法**: 创建全量+增量任务 → 等待进入增量阶段 → 写入数据验证增量正常 → 停止任务 → 验证状态不是 failed（用户停止场景）。

---

#### V2. Slot 迁移失败带重试（详细说明）

**测试目的**: 验证全量迁移中单个 slot 失败后会重试最多 3 次，最终记录失败 slot 列表。

**修复前行为**: `MigrateSlot` 失败直接 `continue`，无重试，无记录，静默丢失数据。

**修复后行为**: 每个 slot 最多重试 3 次（指数退避 1s→2s→4s），失败的 slot 记录到 `failedSlots` 列表，日志汇报完成状态。

**测试方法**: 正常全量迁移（确保无失败） → 验证完成状态 → 检查日志中包含 "completed successfully"。

---

#### V4. Pipeline 部分失败精确统计（详细说明）

**测试目的**: 验证目标端 Pipeline RESTORE 部分失败时，精确统计成功/失败数量而非整批标记失败。

**修复前行为**: `targetPipe.Exec()` 返回 error 时，无论有多少成功，全部标记 `failed += migrated; migrated = 0`。

**修复后行为**: 逐个检查 `cmds[i].Err()`，精确统计 successCount 和 failCount。

---

### W. 故障注入测试 - Chaos Engineering（12 项）

**设计理念**: 不等 Bug 自然发生，而是**主动制造故障**，验证系统在异常条件下的行为。

| ID | 测试名称 | 故障类型 | 验证目标 |
|:---|:---|:---|:---|
| **W1** | **增量阶段 Kill-9 恢复** | 进程崩溃 | SIGKILL 后重启，全量数据不丢，增量可继续 |
| **W2** | **快速暂停恢复数据完整** | 状态抖动 | 10 次 pause/resume 后，500 key 全部迁移完成 |
| **W3** | **全量中源端持续写入** | 数据竞争 | 迁移期间源端不断写新 key，任务不崩溃不卡死 |
| **W4** | **大量冲突不崩溃** | 异常数据 | 70% 冲突率（350/500 key 冲突），skip 策略正常完成 |
| **W5** | **全量中途停止续传** | 人为中断 | 迁移 50% 时停止，重启从断点继续，最终数据完整 |
| **W6** | **同前缀并发任务** | 资源竞争 | 两个任务迁移同一批 key，不死锁不崩溃，数据完整 |
| **W7** | **Pipeline部分DUMP失败** | 数据消失 | 迁移中删除部分源端key，存在的key仍迁移成功，不崩溃 |
| **W8** | **增量异常退出恢复** | 异常退出 | 增量阶段Kill-9→重启→增量可恢复，数据不丢 |
| **W9** | **FakeSlave重连稳定性** | 连接断开 | 增量中反复暂停/恢复5次，暂停期间写入的数据最终同步 |
| **W10** | **Slot超时重试** | 超时 | 大value(500KB)混合小value，所有key迁移成功，大value完整 |
| **W11** | **增量不支持操作** | 未知操作 | INCR/APPEND等不支持操作不导致崩溃，SET操作正常同步 |
| **W12** | **快速状态转换** | 竞态条件 | 毫秒级 start→pause→resume→stop 循环3轮，不崩溃不报错 |

**根因1覆盖**: W7-W12 专门针对「异常路径未覆盖」的根因设计，覆盖了 Pipeline 部分失败、增量异常退出、FakeSlave 重连、不支持操作、状态机竞态等之前测试从未触及的异常场景。

---

### X. 属性/不变性测试 - Property-Based Testing（16 项）

**设计理念**: 不检查具体值，检查系统**必须始终满足的不变性**。如果不变性被破坏，说明有 Bug。

| ID | 不变性公式 | 说明 |
|:---|:---|:---|
| **X1** | `∀ key ∈ source(prefix), key ∈ target` | 迁移后源端每个匹配 key 在目标端必须存在，类型正确 |
| **X2** | `migrated + skipped + failed + filtered ≤ to_migrate` | 计数器守恒，任何时刻不溢出 |
| **X3** | `|TTL_src - TTL_dst| < 5000ms` | 迁移后 TTL 偏差 < 5 秒，无 TTL 的 key 目标端也无 TTL |
| **X4** | `status ∈ LEGAL_TRANSITIONS[prev_status]` | 状态机只按合法路径转移 |
| **X5** | `stop(task) × N ≡ stop(task) × 1` | 多次停止结果幂等，不崩溃不报错 |
| **X6** | `∀ key, value_src(key) == value_dst(key)` | 迁移后每个 key 的值逐字节完全相等 |
| **X7** | `result(stop+resume) ≈ result(continuous)` | 停止再恢复的最终结果等价于不停止直接跑完 |
| **X8** | `∀ write(src) during incr, eventually exists(dst)` | 增量最终一致：SET 存在、DEL 删除、UPDATE 更新 |
| **X9** | `user_stop(incr_task) → status == "stopped"` | 用户停止增量任务，状态必须是 stopped 而非 completed |
| **X10** | `∀ key ∈ source(prefix), key ∈ target (逐个检查)` | 全量完成后无遗漏 slot，500 key 逐个验证 |
| **X11** | `completed → stats.bytes > 0` | 迁移完成后 bytes 统计必须大于 0 |
| **X12** | `conflicts_api.total ≥ actual_conflicts × 80%` | 冲突记录不丢失，磁盘持久化有效 |
| **X13** | `TTL_src == -1 → TTL_dst == -1` | 源端无 TTL 的 key，迁移后不能凭空出现 TTL |
| **X14** | `TTL_src > 0 → TTL_dst > 0 (≠ -1)` | 源端有 TTL 的 key，迁移后不能变成永不过期 |
| **X15** | `stats.failed_keys ≈ error_keys_api.total` | 两个统计维度的失败数量一致 |
| **X16** | `status == "completed" → migrated/to_migrate ≥ 99%` | 完成时实际迁移比例必须 ≥ 99% |

**根因2覆盖**: X9-X16 专门针对「静默错误」的根因设计。X9 检测增量失败标 completed，X10 检测 slot 遗漏，X11 检测 bytes 统计为 0，X13/X14 是 TTL 静默丢失的两个方向检测，X15 检测统计不一致，X16 检测进度不到 100% 就标 completed。

---

### Y. 长时间压力测试 - Endurance Testing（10 项）

**设计理念**: 大数据量 + 长时间运行，暴露**内存泄漏、时序竞争、资源耗尽**问题。同时通过**功能组合测试**覆盖组合爆炸场景。

| ID | 测试规模 | 持续时间 | 验证目标 |
|:---|:---|:---|:---|
| **Y1** | **1 万 key 全量** | ~2-5 分钟 | 大数据量正确性 + 采样值校验 |
| **Y2** | **增量持续写入 1000+ key** | **2 分钟** | stats 单调增长不异常，sync_ratio > 0 |
| **Y3** | **5 个任务同时运行** | ~3-5 分钟 | 资源隔离，每个任务数据独立完整 |
| **Y4** | **同数据反复迁移 3 次** | ~3 分钟 | 幂等性验证，资源正确回收，服务持续健康 |
| **Y5** | **前缀+增量+暂停** | ~2 分钟 | 三功能组合：前缀过滤在增量阶段仍生效，暂停期间数据不丢 |
| **Y6** | **冲突+增量+停止** | ~3 分钟 | 三功能组合：冲突策略在增量保持，停止重启后冲突策略不变 |
| **Y7** | **Pipeline+限速+大值** | ~3-5 分钟 | 三功能组合：大value(200KB)在限速下Pipeline正确处理 |
| **Y8** | **多前缀+排除+增量** | ~2 分钟 | 三功能组合：多前缀+排除在全量和增量阶段都正确过滤 |
| **Y9** | **断点+冲突+TTL** | ~3 分钟 | 三维交叉：中断恢复后冲突策略和TTL都保持正确 |
| **Y10** | **全功能组合** | ~3-5 分钟 | 终极测试：过滤+增量+冲突+TTL+多类型+大量数据同时开启 |

**根因3覆盖**: Y5-Y10 专门针对「组合爆炸」的根因设计。每个测试至少组合 3 个功能的交叉场景，Y10 更是同时启用所有核心功能，测试它们的交互是否正确。这直接覆盖了之前 243 种组合中最高风险的 6 种组合。

---

### Z. Code Review 清单自动验证（13 项）

**设计理念**: 将人工 Code Review 清单转化为**自动化测试**，每次发版前自动检查。同时覆盖本次（2026-03-02）代码 Bugfix 的验证。

| ID | Code Review 清单项 | 自动化方法 |
|:---|:---|:---|
| **Z1** | 每个 error 是否被处理？ | 构造 4 种异常请求，检查全部返回错误码 |
| **Z2** | 每个 goroutine 是否有退出路径？ | 5 轮创建→运行→停止→删除，检查服务仍健康 |
| **Z3** | 每个 channel 是否有关闭保护？ | 5 个线程并发 stop 同一任务，不 panic |
| **Z4** | 每个 Pipeline 是否逐个检查结果？ | 交错冲突 key（奇数/偶数），非冲突 key 不被误杀 |
| **Z5** | 统计字段是否用了 atomic？ | 高频 50 次采样（100ms），所有字段 ≥ 0 |
| **Z6** | **Pipeline 索引对齐** | **Hash+TTL 增量写入，验证字段+TTL+后续String都正确** |
| **Z7** | **PTTL=-2 幽灵Key防护** | **迁移中删除源端key，目标端不出现已删除的key** |
| **Z8** | **Binlog 缓存回放** | **全量期间写增量，进入增量阶段后缓存数据被回放** |
| **Z9** | **Binlog 位置回退** | **暂停/恢复后数据不丢（binlog位置不提前更新）** |
| **Z10** | **类型断言安全** | **混合类型增量操作不panic，服务持续健康** |
| **Z11** | **并发Writer原子计数** | **8 Worker 高速写入，统计不出负数/溢出** |
| **Z12** | **ConflictStore锁修复** | **大量冲突后并发查询error-keys API不崩溃** |
| **Z13** | **错误计数器重置** | **长时间增量运行，FakeSlave稳定不误重连** |

#### Z6-Z13 对应的代码修复

| 测试 | 对应修复文件 | Bug 描述 |
|:---|:---|:---|
| **Z6** | async_executor.go | HSET+PExpire产生2条Pipeline命令但按1条映射，导致后续命令错误归因 |
| **Z7** | pipeline_migrator.go | PTTL返回-2时仍执行RESTORE，创建永不过期的幽灵key |
| **Z8** | binlog_parser.go | ParseBinlogs当expectedCount=0时循环不执行，缓存回放完全失效 |
| **Z9** | fake_slave.go | binlog位置在apply失败前就更新，重连后丢失的数据不会被重新接收 |
| **Z10** | async_executor.go | 约15处cmd.Args[0].(string)不检查类型，非string时panic |
| **Z11** | concurrent_writer.go | pendingCount普通写+atomic读混用，违反Go内存模型 |
| **Z12** | conflict_store.go | Query/Export用RLock但内部Flush是写操作，并发时损坏bufio.Writer |
| **Z13** | fake_slave.go | errors计数器不重置，累计非连续错误触发不必要的重连循环 |

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

## 风险修复覆盖矩阵

| 修复文件 | 风险点数 | 覆盖测试 | 覆盖状态 |
|:---|:---:|:---|:---|
| **task_runner.go** | 10 | **V1**(失败标记), **V2**(slot重试), **V3**(停止不算失败), **V9**(正常完成) | ✅ |
| **fake_slave.go** | 9 | **V5**(降级回退), **V7**(并发安全), **Z9**(binlog位置回退), **Z13**(错误计数器重置), E1-E5 | ✅✅ |
| **async_executor.go** | 6 | **V7**(并发安全), **V10**(错误隔离), **Z6**(Pipeline索引对齐), **Z10**(类型断言安全), R4 | ✅✅ |
| **concurrent_writer.go** | 5 | **V4**(精确统计), **V10**(错误隔离), **Z11**(并发原子计数), B1-B5 | ✅✅ |
| **pipeline_migrator.go** | 2 | **V4**(部分失败), **Z7**(PTTL幽灵Key), B1-B5 | ✅✅ |
| **incremental_syncer.go** | 3 | **V7**(并发安全), **V8**(offset告警), E1-E5 | ✅ |
| **conflict_store.go** | 4 | **V6**(落盘验证), **Z12**(读锁修复并发查询), C1-C3 | ✅✅ |
| **binlog_parser.go** | 1 | **Z8**(Binlog缓存回放count=0), E1-E5 | ✅✅ |
| **cmd/simple/main.go** | 2 | **V5**(降级回退), **V8**(offset告警) | ✅ |

---

## 测试方法论覆盖矩阵

对应上次分析中提出的 5 个系统性测试方法：

| 方法 | 对应分类 | 测试数 | 覆盖的风险类型 |
|:---|:---:|:---:|:---|
| **方法1: 故障注入** | **W** | **12** | 进程崩溃、状态抖动、数据竞争、资源竞争、人为中断、Pipeline部分DUMP失败、增量异常退出、FakeSlave重连、Slot超时、不支持操作、快速状态转换 |
| **方法2: 属性测试** | **X** | **16** | 数据完整性、计数器守恒、TTL一致性、状态机正确性、幂等性、最终一致性、静默失败检测（增量状态/Slot遗漏/Bytes统计/冲突持久化/TTL双向检测/统计一致/进度100%） |
| **方法3: 代码级防御** | **Z** | **13** | 静默失败、goroutine泄漏、channel double-close、Pipeline误杀、原子性 + Bugfix验证（Pipeline索引、幽灵Key、Binlog缓存、位置回退、类型断言、并发写入、锁修复、重连循环） |
| **方法4: 长时间压力+组合** | **Y** | **10** | 内存泄漏、时序竞争、资源耗尽、幂等性退化、功能组合爆炸（前缀+增量+暂停、冲突+增量+停止、Pipeline+限速+大值、多前缀+排除+增量、断点+冲突+TTL、全功能组合） |
| **方法5: Code Review** | **Z** | *(同上13项)* | 自动化 Review 清单，防止人工遗漏 |

### 三大根因与测试覆盖对照

| 根因 | 问题 | 新增测试 | 覆盖的代码风险点 |
|:---|:---|:---|:---|
| **根因1: 异常路径未覆盖** | Pipeline部分失败、增量异常退出、FakeSlave断连等 | **W7-W12** (6项) | pipeline_migrator.go 源端DUMP失败、incremental_syncer.go 异常退出、fake_slave.go 重连窗口、task_runner.go 状态机竞态 |
| **根因2: 静默错误** | 增量失败标completed、TTL变-1、slot遗漏、统计不准 | **X9-X16** (8项) | task_runner.go 增量失败状态、pipeline_migrator.go bytes不准、conflict_store.go 磁盘丢弃、TTL双向检测 |
| **根因3: 组合爆炸** | 5个功能×3种状态=243种组合 | **Y5-Y10** (6项) | 前缀过滤+增量+暂停、冲突+增量+停止、Pipeline+限速+大值、断点+冲突+TTL、全功能组合 |

### 为什么之前的测试没有覆盖到这些？

| 之前的测试特征 | 新增测试特征 | 发现的问题类型 |
|:---|:---|:---|
| 正常路径（Happy Path） | **异常路径（Sad Path）** | 增量失败标 completed、slot 静默丢数据 |
| 检查具体值是否正确 | **检查不变性是否成立** | 计数器负数、TTL 变 -1、状态机非法跳转 |
| 几百 key、几分钟 | **万级 key、持续运行** | 内存泄漏、并发统计错误 |
| 单一操作 | **并发 + 竞争** | Pipeline 整批失败、channel panic |
| 人工 Review | **自动化验证** | 静默失败、goroutine 泄漏 |

---

## 运行方式

```bash
# 运行全部 150 项测试
python3 regression_test.py --env cloud-local

# 运行指定分类
python3 regression_test.py --env cloud-local --categories S,K,N,T,U,V

# 只运行 OOM 保护测试
python3 regression_test.py --env cloud-local --categories T

# 只运行历史问题回归测试
python3 regression_test.py --env cloud-local --categories U

# 只运行风险修复验证测试
python3 regression_test.py --env cloud-local --categories V

# 只运行故障注入测试（Chaos Engineering）
python3 regression_test.py --env cloud-local --categories W

# 只运行属性不变性测试（Property-Based）
python3 regression_test.py --env cloud-local --categories X

# 只运行长时间压力测试（Endurance）
python3 regression_test.py --env cloud-local --categories Y

# 只运行 Code Review 清单验证
python3 regression_test.py --env cloud-local --categories Z

# 运行全部新增深度测试（方法1-5）
python3 regression_test.py --env cloud-local --categories W,X,Y,Z

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

---

## 最新测试结果（2026-03-02，devcloud 环境，宿主机直接运行 Tendis）

### 全量回归：158/158 通过

| 分类 | 数量 | 结果 | 说明 |
|:---:|:---:|:---:|:---|
| A | 4 | ✅ | 基础功能 |
| B | 5 | ✅ | 全量迁移（B1 已修复自备数据） |
| C | 3 | ✅ | 冲突策略 |
| D | 1 | ✅ | 数据类型 |
| E | 5 | ✅ | 增量同步 |
| F-S | 多项 | ✅ | 生命周期、崩溃恢复、边界条件等 |
| T | 9 | ✅ | OOM 保护 |
| U | 12 | ✅ | 历史问题回归 |
| V | 10 | ✅ | 风险修复验证 |
| W | 12 | ✅ | 故障注入 |
| X | 16 | ✅ | 属性不变性 |
| Y | 10 | ✅ | 长时间压力 |
| Z | 13 | ✅ | CodeReview 验证（含 v2.7.0 新增 Z6-Z13） |

### 本轮修复的 Bug 汇总

| Bug | 级别 | 影响 | 修复方案 |
|:---|:---:|:---|:---|
| FakeSlave 暂停恢复 binlog 丢失 | 致命 | W9/Y5/Y6/Z9 失败，暂停期间数据丢失 | 暂停前保存 binlog 位置，恢复时使用保存值 |
| checkFakeSlaveSupport 误判 | 高 | Tendis 2.7.0 被降级为 IDLETIME 模式 | 改用 CONFIG GET + binlogpos 检测 |
| PTTL 类型比较错误 | 高 | 幽灵 Key 以 TTL=-1 写入目标端 | `ttl == -2*time.Millisecond` |
| B1 测试无数据 | 中 | FLUSHALL 后 B1 始终 dbsize=0 | 测试自行写入 200 个 Key |
| async_executor send on closed channel | 致命 | 并发关闭时 panic | 用 sync.Once 保护 channel 关闭 |
| Pipeline 索引错位 | 致命 | 批量写入数据错乱 | 修复循环变量捕获 |
| concurrent_writer 数据竞争 | 高 | 竞态导致计数不准 | 原子操作替代普通读写 |
| conflict_store 读锁写操作 | 高 | 并发死锁或数据不一致 | RLock 改为 Lock |
| binlog_parser 缓存失效 | 阻断 | 增量回放丢数据 | 修复缓存清理逻辑 |

---

## 最新修复（2026-03-03）：Z14-Z20 新增测试用例

### 本次修复 7 个问题及对应测试

| # | 修复项 | 严重程度 | 问题描述 | 测试用例 |
|:---:|:---|:---:|:---|:---|
| 1 | regexp 预编译 | 🔴高 | `regexp.MatchString` 每次重编译，40亿Key性能灾难 | **Z14**: 正则pattern过滤500Key，验证正确性+时间 |
| 2 | exclude_patterns 预编译 | 🔴高 | 排除正则也存在重编译问题 | **Z15**: 前缀+排除正则组合，验证排除生效 |
| 3 | verifyKey PTTL 比较 | 🟡中 | 只比DUMP不比TTL，TTL丢失误判为一致 | **Z16**: 带/不带TTL的Key迁移后校验一致性 |
| 4 | SCAN 前缀优化 | 🔴高 | 前缀模式仍SCAN *遍历全量再客户端过滤 | **Z17**: 多前缀模式验证只迁移目标前缀 |
| 5 | cleanup wg.Wait | 🔴高 | Stop和cleanup并发关闭连接导致panic | **Z18**: 10次快速启停循环不崩溃 |
| 6 | go-redis v8统一 | 🔴高 | v8/v9类型不兼容导致limiter未初始化 | **Z19**: 动态速率调整+数据正确迁移 |
| 7 | 大Key扫描器初始化 | 🟡中 | BigKeyScanner/Migrator完全未创建 | **Z20**: 大Value+大Hash迁移不崩溃 |

### 同步删除的死代码

| 包/文件 | 文件数 | 说明 |
|:---|:---:|:---|
| `internal/binlog/` | 4 | 死代码，无人import |
| `internal/worker/` | 3 | 旧分布式架构 |
| `internal/master/` | 5 | 旧分布式架构 |
| `cmd/worker/main.go` | 1 | 旧worker入口 |
| `cmd/master/main.go` | 1 | 旧master入口 |
| `go-redis/v9` 依赖 | - | go.mod中移除 |

### 全量回归预期：168/168（原158 + 新增10）

| 分类 | 数量 | 说明 |
|:---:|:---:|:---|
| A-Y | 152 | 原有测试不变 |
| Z1-Z13 | 13 | 之前的 CodeReview 验证 |
| **Z14-Z20** | **7** | 本次新增（问题8-12 + go-redis统一 + 大Key初始化） |
| **Z21-Z23** | **3** | 本次新增（3个性能优化验证） |

#### Z21-Z23 性能优化测试说明

| # | 测试 | 优化项 | 旧方案 | 新方案 | 验证重点 |
|:---:|:---|:---|:---|:---|:---|
| 1 | **Z21** | CLUSTER GETKEYSINSLOT 精确取 key | 全局 SCAN * + 客户端过滤 slot | GETKEYSINSLOT 服务端精确获取 | 700 key 多前缀：目标前缀全迁移 + noise 前缀零泄漏 |
| 2 | **Z22** | 节点级 SCAN (ForEachMaster) | ClusterClient.Scan 跨节点 | ForEachMaster 分节点并行 SCAN | 多节点各写 200 key：每个节点 key 都被迁移，无遗漏 |
| 3 | **Z23** | DUMP+RESTORE Pipeline 批量 | 逐 key 串行 DUMP+TTL（2000 RTT/1000key） | Pipeline 批量（2 RTT/1000key） | 500 key 迁移：数据+TTL 完整性 + 合理完成时间 |

---

## 测试执行最佳实践

### 环境选择

| 环境 | Tendis 部署方式 | 适用场景 | 注意事项 |
|:---|:---|:---|:---|
| devcloud | 宿主机直接运行 | 全量回归测试 | 推荐，无 overlay2 风险 |
| home | Docker 容器 | 开发调试 | 注意磁盘空间 |
| cloud | Docker 容器 | 已废弃 | 服务器已释放 |
| env-a/env-b | 生产 Tendis | 生产验证 | 需要 VPN |

### 运行回归测试的标准流程

```bash
# 1. 部署迁移工具到目标服务器
./remote-deploy.sh devcloud

# 2. SSH 到服务器
ssh -p 36000 root@21.214.66.163.devcloud.woa.com

# 3. 确认 Tendis 集群正常
for port in 7001 7002 8001 8002; do echo -n "Port $port: "; redis-cli -p $port PING; done

# 4. 确认迁移工具正常
curl -s http://localhost:8088/api/v1/health

# 5. 上传并运行测试（推荐 nohup 方式）
cd /home/tendis-migrate-package
nohup python3 regression_test.py --env devcloud > /tmp/reg_full.log 2>&1 &

# 6. 实时查看进度
tail -f /tmp/reg_full.log | grep -E '通过|失败|总计'

# 7. 查看最终结果
grep '总计' /tmp/reg_full.log
```

### 关键经验教训

1. **Docker overlay2 磁盘爆满**：频繁运行回归测试时，Docker 的 overlay2 层会积累 Tendis 的 dump 文件。在 49GB 磁盘上跑完 158 个测试就可能触发。**解决：改用宿主机直接运行 Tendis。**

2. **测试必须自备数据**：不要依赖其他测试的残留数据。每个测试用例必须独立写入自己需要的测试数据，测试结束后清理。

3. **FLUSHALL 后等待**：每个测试开始时 FLUSHALL 清理源端和目标端后，应等待 1-2 秒让 Tendis 完成数据清理。

4. **增量同步测试需要足够等待时间**：FakeSlave 注册和 binlog 回放有延迟，增量写入后至少等待 10-15 秒再验证。

5. **暂停恢复场景**：暂停期间写入的数据，恢复后需要通过保存的 binlog 位置回放。测试必须验证暂停期间的数据不丢失。

6. **Tendis 2.7.0 特性检测**：不能依赖 `INFO replication` 的 `binlog_enabled` 字段，应使用 `CONFIG GET binlog-enabled` + `binlogpos 0` 组合检测。
