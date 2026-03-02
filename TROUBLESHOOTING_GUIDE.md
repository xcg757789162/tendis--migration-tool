# Tendis-Migrate 问题排查与经验总结

**文档目的**: 记录开发和测试过程中遇到的问题及解决方案，避免重复踩坑，提高问题排查效率。

**使用方法**: 遇到问题时，先在本文档中搜索关键词，查看是否有类似问题的解决方案。

---

## 目录

1. [编译和构建问题](#1-编译和构建问题)
2. [API 和数据结构问题](#2-api-和数据结构问题)
3. [增量同步问题](#3-增量同步问题)
4. [测试脚本问题](#4-测试脚本问题)
5. [连接和网络问题](#5-连接和网络问题)
6. [性能问题](#6-性能问题)
7. [代码设计教训](#7-代码设计教训)

---

## 1. 编译和构建问题

### 1.1 CGO 导致进程卡死 (macOS)

**问题描述**: 
- 使用 CGO 编译的二进制在 macOS 上多次启动后进程卡死
- 进程状态显示为 `UNE` (Uninterruptible)
- 无法绑定端口，无法正常退出

**根本原因**: 
- CGO 依赖的 SQLite C 代码在多进程场景下资源管理存在问题
- macOS 对进程资源的限制与 Linux 不同

**解决方案**:
```bash
# 使用无 CGO 编译
CGO_ENABLED=0 go build -o tendis-migrate-nocgo ./cmd/simple
```

**预防措施**:
- 在 macOS 上始终使用 `CGO_ENABLED=0` 编译
- 如果需要 CGO 功能（如 SQLite），考虑使用纯 Go 替代方案

**相关关键词**: `CGO`, `UNE`, `进程卡死`, `macOS`, `端口绑定失败`

---

### 1.2 交叉编译

**场景**: 在 macOS (arm64) 上编译 Linux (amd64) 版本

**正确命令**:
```bash
GOOS=linux GOARCH=amd64 CGO_ENABLED=0 go build -o tendis-migrate-linux ./cmd/simple
```

**注意**: 交叉编译时必须禁用 CGO，否则会链接到本地的 C 库。

---

## 2. API 和数据结构问题

### 2.1 创建任务时字段名错误

**问题描述**: 
- 创建任务 API 返回成功，但连接的是 `127.0.0.1:6379` 而不是指定地址

**根本原因**: 使用了错误的字段名

**错误示例**:
```json
{
  "source": {"addresses": "10.10.10.1:8901"},
  "target": {"addresses": "10.10.10.2:8901"}
}
```

**正确示例**:
```json
{
  "source_cluster": {
    "addrs": ["10.10.10.1:8901", "10.10.10.1:8902"]
  },
  "target_cluster": {
    "addrs": ["10.10.10.2:8901", "10.10.10.2:8902"]
  }
}
```

**检查清单**:
- [x] 字段名是 `source_cluster` / `target_cluster`（不是 `source`/`target`）
- [x] `addrs` 是 JSON 数组（不是字符串）
- [x] 字段名是 `addrs`（不是 `addresses`）

**相关关键词**: `127.0.0.1:6379`, `默认地址`, `字段名`, `JSON 格式`

---

### 2.2 API 响应结构不一致

**问题描述**: 
- 测试脚本读取 `task.stats.incr_keys_synced` 返回 0
- 但实际数据已经同步成功

**根本原因**: 
- `incr_keys_synced` 只在响应的顶层返回，不在 `stats` 对象中

**解决方案**: 
1. 修改 API 在 `stats` 对象中也返回增量指标
2. 或者测试脚本同时检查两个位置

**代码修复** (main.go):
```go
"stats": map[string]interface{}{
    // ... 其他字段
    "incr_keys_synced":   task.IncrKeysSynced,
    "incr_keys_skipped":  task.IncrKeysSkipped,
    "incr_keys_failed":   task.IncrKeysFailed,
    "incr_keys_filtered": task.IncrKeysFiltered,
}
```

**测试脚本兼容写法**:
```python
incr = task.get("incr_keys_synced", 0) or task.get("stats", {}).get("incr_keys_synced", 0)
```

**相关关键词**: `incr_keys_synced`, `stats`, `API 响应`, `字段位置`

---

## 3. 增量同步问题

### 3.1 Pattern 模式匹配失败

**问题描述**: 
- 使用 pattern 过滤模式（如 `incr_pattern_*`）时，Key 无法匹配
- 增量同步计数为 0，但数据实际上同步成功了

**根本原因**: 
- `matchKeyFilter` 函数使用 `strings.Contains` 进行匹配
- `strings.Contains("incr_pattern_0", "incr_pattern_*")` 返回 false
- 因为实际的 key 不包含 `*` 字符

**错误代码**:
```go
case "pattern":
    for _, pattern := range filter.Patterns {
        if strings.Contains(key, pattern) {  // ❌ 错误
            return true
        }
    }
```

**修复方案**:
```go
case "pattern":
    for _, pattern := range filter.Patterns {
        if matchSimplePattern(key, pattern) {  // ✅ 正确
            return true
        }
    }
```

**最佳实践**: 
- 统一所有过滤函数，避免多个实现不一致
- 已将 `matchKeyFilter` 重构为调用 `matchKeyFilterV2`

**相关关键词**: `pattern`, `通配符`, `matchKeyFilter`, `strings.Contains`

---

### 3.2 FakeSlave 连接检测

**问题描述**: 
- 测试脚本检查 `stats.fake_slave_count` 判断连接状态，但总是返回 0

**根本原因**: 
- API 中没有 `fake_slave_count` 字段

**解决方案**: 
- 使用 `incr_heartbeats` 字段判断 FakeSlave 是否已连接

**正确写法**:
```python
# 检查 FakeSlave 是否已连接
if task.get('incr_heartbeats', 0) > 0:
    print("FakeSlave 已连接")
```

**相关关键词**: `FakeSlave`, `连接检测`, `fake_slave_count`, `incr_heartbeats`

---

### 3.3 增量同步计数不更新

**问题描述**: 
- 数据已同步到目标端，但 `incr_keys_synced` 始终为 0

**排查步骤**:
1. 检查 FakeSlave 是否已连接（`incr_heartbeats > 0`）
2. 检查任务状态是否为 `running`
3. 检查 API 响应中 `incr_keys_synced` 的位置
4. 检查目标端是否有数据（`redis-cli KEYS '*'`）

**常见原因**:
- API 响应结构问题（见 2.2）
- Key 过滤未通过（见 3.1）
- FakeSlave 未正确连接

---

## 4. 测试脚本问题

### 4.1 Shell 命令在 expect 中失败

**问题描述**: 
- 在 expect 脚本中使用 shell 条件判断 `[ -d dir ]` 报错
- 错误信息: `invalid command name "-d"`

**根本原因**: 
- Tcl（expect 使用的语言）将 `[ ]` 解释为命令替换

**错误示例**:
```bash
/usr/bin/expect -c 'send "if [ -d dir ]; then ...\r"'
```

**解决方案**: 
- 将复杂的 shell 语句拆分为多个简单命令

**正确示例**:
```bash
/usr/bin/expect -c '
spawn ssh user@host
expect "*password:*"
send "password\r"
expect "*#*"
send "./stop.sh\r"
expect "*#*"
send "rm -rf old-dir\r"
expect "*#*"
send "./run.sh\r"
'
```

**相关关键词**: `expect`, `Tcl`, `方括号`, `命令替换`

---

### 4.2 SSH 命令执行 heredoc 失败

**问题描述**: 
- 测试脚本使用 `ssh_cmd(f"redis-cli <<< '{cmds}'")` 写入数据失败

**根本原因**: 
- Docker exec 中使用 heredoc 语法不可靠

**解决方案**: 
- 使用多个单独的 SET 命令

**正确写法**:
```python
def write_incremental_data(prefix="incr_test:", count=500):
    for i in range(count):
        ssh_cmd(f"exec tendis-src redis-cli -p 7001 SET {prefix}{i} value_{i}")
```

**相关关键词**: `heredoc`, `Docker exec`, `redis-cli`, `批量写入`

---

## 5. 连接和网络问题

### 5.1 端口被占用

**问题描述**: 
- 启动服务时报 `address already in use`

**排查命令**:
```bash
# 查看占用端口的进程
lsof -i :8088

# 查找所有 tendis-migrate 进程
ps aux | grep tendis-migrate

# 强制结束所有相关进程
pkill -9 -f tendis-migrate
```

**预防措施**: 
- 使用不同的端口号进行测试（如 9099）
- 在启动前先停止旧进程

---

### 5.2 Tendis 连接失败

**排查步骤**:
1. 检查 Tendis 容器是否运行: `docker ps | grep tendis`
2. 检查端口是否可达: `redis-cli -h <ip> -p <port> PING`
3. 检查防火墙设置
4. 检查地址是否正确（特别是内网 IP vs 公网 IP）

---

## 6. 性能问题

### 6.1 40 亿 Key 场景下的内存问题

**核心原则**:
- ❌ **绝对不能**使用 `map`/`sync.Map` 存储全量 Key
- ✅ 全量同步：流式 SCAN，边扫描边迁移
- ✅ 增量同步：必须使用 Binlog，不能用定时轮询 SCAN

**内存估算**:
- 40 亿 Key × 20 字节/Key = 80GB 内存
- 加上 Go map 开销可达 150GB

**相关关键词**: `40亿Key`, `内存`, `OOM`, `流式处理`

---

## 7. 代码设计教训

### 7.1 避免重复实现

**问题**: 存在两个功能相同但实现不一致的函数
- `matchKeyFilter` - 使用 `strings.Contains`
- `matchKeyFilterV2` - 使用 `matchSimplePattern`

**后果**: Pattern 模式在增量同步中失效

**解决方案**: 统一入口，避免重复代码
```go
func matchKeyFilter(key string, options *TaskOptions) bool {
    if options == nil || options.KeyFilter == nil {
        return true
    }
    return matchKeyFilterV2(key, options.KeyFilter)
}
```

**教训**: 
- 相同功能只保留一个实现
- 新功能时先检查是否已有类似实现

---

### 7.2 API 契约先行

**问题**: 测试脚本依赖的字段在 API 中不存在或位置错误

**解决方案**: 
1. 在修改 API 前先确定响应结构
2. 添加 API 版本标识
3. 使用 OpenAPI/Swagger 文档化

**最佳实践**:
```go
"stats": map[string]interface{}{
    // ...
    "api_version": "v2.3.1-bugfix",
}
```

---

### 7.3 测试脚本也需要版本控制

**问题**: 测试脚本与 API 版本不匹配导致测试失败

**解决方案**:
- 测试脚本应该与代码一起提交
- 修改 API 时同步更新测试脚本
- 测试脚本应该兼容多种响应格式

---

### 7.4 TTL 一致性：迁移前后必须完全一致

**问题**: 增量同步阶段，EXPIRE/PERSIST 等 TTL 变更命令未被正确回放，导致目标端 TTL 与源端不一致（如 TTL 变为 -1）。

**根因链条**:
1. Tendis binlog entry 的 TTL 字段在 `parseBinlogs` 中被硬编码为 0
2. PERSIST/EXPIRE 命令在 binlog 中产生 `op=SET` 的 entry（RocksDB 层面是重写 RecordValue）
3. `processBinlogEntries` 的 `case "SET":` 使用 `entry.TTL`（=0）→ `syncKeyByType` → 目标端 TTL=0（永不过期）
4. 独立的 `TTL`/`TTLDEL` OpType 没有对应的 case 分支处理，直接落入 default 被忽略

**修复（2026-02-27）**:
1. `processBinlogEntries` 添加 `case "TTL":` — 从源端获取 PTTL 并 PExpire 到目标端
2. `processBinlogEntries` 添加 `case "TTLDEL":` — 在目标端执行 Persist
3. `case "SET":` 中不再使用 `entry.TTL`，改为从源端实时查询 PTTL
4. `syncKeyByType` 中 hash/list/set/zset 的 `Expire` 改为 `PExpire`（毫秒精度）+ 检查返回值
5. `ConcurrentWriter` 和 `AsyncCommandExecutor` 中为 HSET/LPUSH/RPUSH/SADD/ZADD 添加 TTL 设置
6. 全量迁移 `MigrateBatchWithPipeline` 中 `TTL` 改为 `PTTL`（毫秒精度）

**教训**:
- **迁移一致性是第一原则**：Key 值、TTL、数据类型迁移后必须与源端完全一致
- 任何写入目标端的操作都必须检查是否正确保留了 TTL
- `Expire`（秒精度）应统一替换为 `PExpire`（毫秒精度）
- TTL 设置的返回值必须检查，不能静默忽略

---

## 8. 快速诊断检查清单

遇到问题时，按以下顺序检查：

### 8.1 服务启动问题
- [ ] 进程是否正常运行？ `ps aux | grep tendis-migrate`
- [ ] 端口是否被占用？ `lsof -i :8088`
- [ ] 是否使用了正确的编译方式？ (CGO_ENABLED=0)
- [ ] 日志中是否有错误？ `tail -100 logs/*.log`

### 8.2 任务创建问题
- [ ] JSON 格式是否正确？ (source_cluster, target_cluster, addrs)
- [ ] 地址是否可达？ `redis-cli -h <ip> -p <port> PING`
- [ ] API 返回的任务状态是什么？

### 8.3 增量同步问题
- [ ] FakeSlave 是否已连接？ (`incr_heartbeats > 0`)
- [ ] 任务是否在运行？ (`status == "running"`)
- [ ] Key 过滤是否正确？ (检查 filter.mode 和 filter.patterns)
- [ ] 目标端是否有数据？ `redis-cli KEYS '*'`

---

## 更新日志

| 日期 | 更新内容 |
|------|----------|
| 2026-02-10 | 创建初始文档，记录前期遇到的问题和解决方案 |
| 2026-02-10 | 添加 UI 问题修复记录（ETA 不刷新、全量模式、校验状态、停止按钮、日志优化） |
| 2026-02-11 | 添加功能缺失修复（运行时参数、增量Key Filter、incremental模式、FakeSlave修复、集群拓扑缓存、DBSIZE超时） |
| 2026-02-12 | 添加环境适配代码污染回滚记录、v2.4.0新增功能问题记录（Preflight Check、拓扑刷新、IP探测、Error Keys） |
| 2026-02-16 | v2.5.0: 限速修复（BUG-4/5/6）、TTL一致性、系统key过滤、FakeSlave panic、崩溃恢复、生产故障（P0/P1） |
| 2026-03-01 | v2.6.0: 流式处理优化、数据校验增强、回归测试 97/97 全部通过 |
| 2026-03-02 | v2.7.0: 代码深度审查修复16个Bug（async_executor/concurrent_writer/conflict_store/fake_slave/binlog_parser/pipeline_migrator/task_runner），测试脚本修复10个Bug，新增8个Z分类回归测试 |
| 2026-03-02 | v2.7.1: 修复FakeSlave暂停恢复binlog丢失、checkFakeSlaveSupport误判、PTTL类型比较、Docker overlay2磁盘爆满；改为宿主机直接运行Tendis；B1测试自备数据；全量回归158/158通过 |

---

## 9. UI 和前端问题

### 9.1 WebSocket 指标字段缺失

**问题描述**: 
- 预计剩余时间、总数据量不刷新
- 只有刷新按钮点击后才更新

**根本原因**: 
- `sendTaskMetrics` 函数没有发送 `estimated_eta`、`total_bytes`、`keys_to_migrate` 等字段
- 前端 `handleMetricsUpdate` 没有处理这些字段

**解决方案**:
1. 后端 `sendTaskMetrics` 添加缺失字段：
```go
"keys_to_migrate": task.KeysToMigrate,
"total_bytes":     task.BytesTotal,
"estimated_eta":   calculateETA(task),
"elapsed_time":    calculateElapsedTime(task),
"filtered_keys":   task.KeysFiltered,
"migration_mode":  task.MigrationMode,
// 增量同步相关指标
"incr_keys_synced":   task.IncrKeysSynced,
"incr_keys_skipped":  task.IncrKeysSkipped,
"incr_keys_failed":   task.IncrKeysFailed,
"incr_keys_filtered": task.IncrKeysFiltered,
"incr_lag_ms":        task.IncrLagMs,
```

2. 前端 `handleMetricsUpdate` 处理新字段：
```javascript
if (payload.keys_to_migrate !== undefined) {
  progress.value.keys_to_migrate = payload.keys_to_migrate
}
if (payload.total_bytes !== undefined) {
  progress.value.total_bytes = payload.total_bytes
}
if (payload.estimated_eta !== undefined) {
  progress.value.estimated_eta = payload.estimated_eta
}
```

**相关关键词**: `WebSocket`, `sendTaskMetrics`, `estimated_eta`, `total_bytes`, `实时更新`

---

### 9.2 全量模式完成后显示增量面板

**问题描述**: 
- `full_only` 模式全量迁移完成后，仍显示增量同步统计面板
- 任务应该直接标记为完成

**根本原因**: 
1. 前端 `showIncrementalStats` 计算属性没有排除 `full_only` 模式
2. 后端全量完成时没有设置 `CompletedAt` 和广播状态

**解决方案**:
1. 前端修复：
```javascript
const showIncrementalStats = computed(() => {
  // 如果是 full_only 模式，不显示增量同步面板
  const migrationMode = task.value?.options?.migration_mode || task.value?.migration_mode
  if (migrationMode === 'full_only') {
    return false
  }
  // ... 其他逻辑
})
```

2. 后端修复：
```go
if task.MigrationMode == "full_only" {
    task.Status = "completed"
    task.Progress = 100
    task.Phase = "completed"
    task.CompletedAt = time.Now().Format(time.RFC3339)
}
// 广播状态更新
broadcastTaskUpdate(task.ID)
broadcastTaskStatus(task.ID, "completed")
```

**相关关键词**: `full_only`, `showIncrementalStats`, `增量面板`, `任务完成`

---

### 9.3 校验结果显示全 0 困惑

**问题描述**: 
- 点击校验按钮后，校验结果区域显示全 0
- 用户不知道是校验进行中还是校验失败

**解决方案**:
添加校验进行中状态显示：

```vue
<!-- 校验进行中状态 -->
<div v-if="verifying" class="verify-loading">
  <el-icon class="is-loading"><Loading /></el-icon>
  <span>正在进行数据校验，请稍候...</span>
</div>
```

```javascript
const verifying = ref(false)

const triggerVerify = async () => {
  try {
    verifying.value = true
    await api.triggerVerify(taskId.value)
    // 定时检查校验结果
    verifyCheckTimer = setInterval(async () => {
      const result = await api.getVerifyResults(taskId.value)
      if (result?.length > 0 && result[result.length - 1].sampled_keys > 0) {
        verifyResults.value = result
        verifying.value = false
        clearInterval(verifyCheckTimer)
      }
    }, 2000)
  } catch (err) {
    verifying.value = false
  }
}
```

**相关关键词**: `校验`, `loading`, `进行中状态`, `全0`

---

### 9.4 任务列表缺少停止按钮

**问题描述**: 
- 任务列表只有暂停和删除按钮
- 用户无法直接停止任务

**解决方案**:
1. API 添加 `stopTask` 方法：
```javascript
stopTask(id) {
  return api.post(`/tasks/${id}/stop`)
}
```

2. 前端添加停止按钮（带确认弹窗）：
```vue
<el-popconfirm 
  title="确定要停止该任务吗？停止后任务将标记为失败状态。"
  @confirm="stopTask(row.id)"
>
  <template #reference>
    <el-button size="small" type="warning">停止</el-button>
  </template>
</el-popconfirm>
```

**相关关键词**: `停止按钮`, `任务列表`, `el-popconfirm`

---

### 9.5 日志过大问题

**问题描述**: 
- 日志级别设置为 DEBUG，输出大量无用信息
- 日志文件快速增长

**解决方案**:
1. 将默认日志级别改为 INFO：
```go
logger.Init(*flagLogDir, logger.INFO)
```

2. 将关键的 Debug 日志升级为 Info（如 FakeSlave 启动）

**最佳实践**:
- DEBUG: 只用于开发调试，生产环境不输出
- INFO: 关键事件（启动、完成、阶段切换）
- WARN: 可恢复的异常情况
- ERROR: 需要关注的错误

**相关关键词**: `日志级别`, `DEBUG`, `INFO`, `日志优化`

---

### 9.6 待迁移Key数显示为0

**问题描述**: 
- 无过滤规则的情况下，"待迁移Key"显示为 0
- 只有全量迁移完成后才显示正确的数值

**根本原因**: 
- `KeysToMigrate` 原本计算方式是 `已迁移 + 失败 + 跳过`，这不是"待迁移"的正确含义
- **正确理解**：
  - **待迁移 Key 数** (`KeysToMigrate`)：SCAN 扫描到的、符合过滤条件的 Key 总数
  - **已迁移 Key 数** (`KeysMigrated`)：实际已经成功迁移到目标端的 Key 数

**正确的逻辑**：
1. 每次 SCAN 获取一批 Key
2. 对这批 Key 进行过滤判断，符合条件的累加到 `KeysToMigrate`
3. 然后再执行迁移，成功的累加到 `KeysMigrated`

**解决方案**:
1. 声明新的计数器：
```go
var keysToMigrateCount int64  // 符合过滤条件的待迁移 Key 数
```

2. 在 SCAN 阶段统计符合条件的 Key：
```go
// SCAN 后统计符合过滤条件的待迁移 Key 数
var matchedInBatch int64
for _, key := range keys {
    // 检查是否符合过滤条件（本地二次过滤）
    if matchKeyFilterV2(key, keyFilter) {
        matchedInBatch++
    }
    keyChan <- key
}
// 累加待迁移 Key 数
atomic.AddInt64(&keysToMigrateCount, matchedInBatch)
```

3. 进度更新协程使用 SCAN 阶段的统计：
```go
toMigrate := atomic.LoadInt64(&keysToMigrateCount)
task.KeysToMigrate = toMigrate

// 进度 = 已处理数 / 待迁移数
if toMigrate > 0 {
    task.Progress = float64(mc + sc) / float64(toMigrate) * 100
}
```

**关键区别**：
- 旧逻辑：`KeysToMigrate = 已迁移 + 失败 + 跳过`（实际是"已处理数"）
- 新逻辑：`KeysToMigrate = SCAN 阶段符合过滤条件的 Key 总数`（真正的"待迁移数"）

**相关关键词**: `KeysToMigrate`, `待迁移Key`, `SCAN`, `过滤统计`

---

### 9.7 macOS 上 go build 编译的二进制启动后变僵尸进程（UNE 状态）

**问题描述**: 
- `go build` 编译出的二进制文件在 macOS 上启动后，进程立即进入 `UNE`（Uninterruptible）状态
- 进程不执行任何代码，不监听端口，不输出日志
- `kill -9` 都无法杀死这些进程，形成僵尸进程
- 而 `go run` 启动则完全正常

**根本原因**: 
- macOS 的安全策略（syspolicyd + com.apple.provenance）对未签名的二进制文件会进行安全评估
- `go build` 产出的二进制签名是 **adhoc**（自签名，Identifier=a.out），macOS 不信任
- macOS 在首次执行时会阻塞进程进行评估，导致进入 `UNE` 内核态阻塞
- `go run` 正常是因为 Go 编译器本身有 **Apple 认证的开发者签名**（Google LLC, EQHXZ8M8AV），子进程继承了信任上下文

**诊断方法**:
```bash
# 检查进程状态（UNE = 被内核阻塞）
ps aux | grep tendis-migrate | awk '{print $2, $8, $11}'

# 检查二进制的扩展属性
xattr ./tendis-migrate
# 输出: com.apple.provenance  ← 这就是罪魁祸首

# 检查签名信息
codesign -dvvv ./tendis-migrate 2>&1 | grep -E "Signature|Identifier"
# 输出: Identifier=a.out, Signature=adhoc  ← 不受信任的自签名
```

**解决方案**:
编译后执行 `codesign --force --sign -` 重签名：
```bash
go build -o tendis-migrate ./cmd/simple
codesign --force --sign - ./tendis-migrate   # 关键！修复 macOS 安全阻塞
```

已在 `run.sh` 中添加自动签名逻辑，启动前会自动修复。

**重要提醒**:
- 已产生的 UNE 僵尸进程 **无法被 kill**，只能通过**重启系统**清理
- 每次 `go build` 后都必须重签名
- Linux 不受此问题影响

**相关关键词**: `UNE`, `僵尸进程`, `codesign`, `provenance`, `adhoc`, `go build`, `macOS`, `syspolicyd`

---

## 8. 功能缺失问题（已修复）

### 8.1 运行时参数修改不支持 (已修复 2026-02-11)

**问题描述**: 
- 任务创建后无法动态修改 Worker 数量和 SCAN 批次大小
- `PUT /api/v1/tasks/{id}/config` 只更新配置值，但不实际调用 `SetWorkerCount`
- 测试用例 1.5-1.8, 2.5-2.8, 3.5-3.8 全部标记为"不支持"

**根本原因**: 
- `updateTaskConfigHandler` 函数更新了 `task.Options.WorkerCount`，但没有调用 `workerPool.SetWorkerCount()`

**解决方案**:
1. 在 `updateTaskConfigHandler` 中添加实际调整逻辑：
```go
if task.workerPool != nil {
    if oldWorkerCount != task.Options.WorkerCount {
        // 【BUG-FIX】实际调用 SetWorkerCount 使变更生效
        task.workerPool.SetWorkerCount(task.Options.WorkerCount)
    }
    // 动态更新限速器
    if req.RateLimit != nil {
        task.workerPool.UpdateRateLimiter(req.RateLimit.SourceQPS)
        task.workerPool.UpdateTargetRateLimiter(req.RateLimit.TargetQPS)
    }
}
```

2. 添加 `PATCH` 方法支持（与 `PUT` 相同逻辑）：
```go
case action == "config" && r.Method == "PATCH":
    updateTaskConfigHandler(w, r, id, log, taskLog)
```

3. 更新 CORS 配置：
```go
w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, PATCH, DELETE, OPTIONS")
```

**API 使用示例**:
```bash
curl -X PATCH http://localhost:8088/api/v1/tasks/{task_id}/config \
  -H "Content-Type: application/json" \
  -d '{"worker_count": 16, "scan_batch_size": 2000}'
```

**相关关键词**: `Worker`, `动态调整`, `运行时`, `SetWorkerCount`, `PATCH`

---

### 8.2 增量阶段 Key Filter 不生效 (已修复 2026-02-11)

**问题描述**: 
- 全量迁移阶段 prefix/pattern 过滤正常工作
- 增量同步阶段过滤不生效，所有 Key 都被同步
- 测试用例 2.2, 2.3, 3.2, 3.3 失败

**根本原因**: 
- `processBinlogEntries` 函数没有对 binlog entry 进行 Key Filter 检查
- 虽然 `FakeSlave` 有过滤逻辑，但 `processBinlogEntries` 是最终写入目标的函数

**解决方案**:
在 `processBinlogEntries` 函数中添加 Key Filter 检查：
```go
// 【BUG-FIX】获取 Key Filter 配置用于增量阶段过滤
var keyFilter *KeyFilter
if task.Options != nil {
    keyFilter = task.Options.KeyFilter
}

var synced, skipped, failed, filtered int64

for _, entry := range entries {
    // 确定需要检查的 Key
    keyToCheck := entry.Key
    if entry.OpType == "CMD" && keyToCheck == "" {
        args := parseRESPCommand(string(entry.Value))
        if len(args) >= 2 {
            keyToCheck = args[1] // 大多数命令的第二个参数是 Key
        }
    }
    
    // 应用 Key Filter
    if keyToCheck != "" && keyFilter != nil && !matchKeyFilterV2(keyToCheck, keyFilter) {
        filtered++
        continue
    }
    // ... 继续处理
}

// 更新统计
task.IncrKeysFiltered += filtered
```

**相关关键词**: `Key Filter`, `增量同步`, `processBinlogEntries`, `prefix`, `pattern`

---

### 8.3 migration_mode: "incremental" 不启动 FakeSlave (已修复 2026-02-11)

**问题描述**: 
- 创建任务时指定 `migration_mode: "incremental"` 纯增量模式
- FakeSlave 不会启动，增量同步不工作
- 测试计划中标记为 BUG

**根本原因**: 
- `runMigrationTask` 中 `needIncremental` 的条件只检查 `MigrationMode == "full_and_incremental"`
- 不支持 `"incremental"` 值

**解决方案**:
```go
// 【BUG-FIX】支持 migration_mode: "incremental" 纯增量模式
isIncrementalOnly := task.MigrationMode == "incremental"
if isIncrementalOnly {
    actualSkipFullSync = true  // 纯增量模式跳过全量
    taskLog.Info("【纯增量模式】migration_mode=incremental, skipping full migration")
}
needIncremental := !skipIncremental && (task.MigrationMode == "full_and_incremental" || isIncrementalOnly)
```

**支持的 migration_mode 值**:
- `"full_only"`: 只做全量迁移
- `"full_and_incremental"`: 全量+增量迁移（默认）
- `"incremental"`: 纯增量迁移（跳过全量，直接启动 FakeSlave）

**API 使用示例**:
```bash
curl -X POST http://localhost:8088/api/v1/tasks \
  -H "Content-Type: application/json" \
  -d '{
    "name": "pure-incremental-task",
    "migration_mode": "incremental",
    "source_cluster": {"addrs": ["192.168.1.19:7001"]},
    "target_cluster": {"addrs": ["192.168.1.19:8001"]}
  }'
```

**相关关键词**: `migration_mode`, `incremental`, `纯增量`, `FakeSlave`

---

### 8.4 纯增量模式 FakeSlave 立即被停止 (已修复 2026-02-11)

**问题描述**:
- 创建 `migration_mode: "incremental"` 任务
- FakeSlave 连接成功后立即断开："use of closed network connection"
- 增量阶段没有接收任何 binlog 数据
- Key Filter 无法工作，因为 FakeSlave 根本没有运行

**根本原因**:
在 `runMigrationTask` 函数的增量阶段入口条件中：
```go
// 问题代码（第 4216 行）
if status == "running" && mode == "full_and_incremental" {
    // 只有 full_and_incremental 模式才会进入增量阶段
    // 导致 incremental 模式下 FakeSlave 被立即停止
}
```

由于纯增量模式的 `mode == "incremental"`，不满足条件，代码直接跳到 else 分支（4299-4311 行），立即停止了所有 FakeSlave。

**代码流程分析**:
1. 第 4113 行：`startFakeSlaves()` 启动 FakeSlave 并等待连接成功 ✅
2. 第 4138-4155 行：由于 `actualSkipFullSync = true`，跳过全量迁移 ✅
3. 第 4216 行：检查 `mode == "full_and_incremental"`，条件不满足 ❌
4. 第 4299-4311 行：else 分支调用 `binlogCancel()` 和 `fs.Stop()`，立即停止 FakeSlave ❌

**解决方案**:
```go
// 【BUG-FIX】支持纯增量模式：mode 可以是 "full_and_incremental" 或 "incremental"
if status == "running" && (mode == "full_and_incremental" || mode == "incremental") {
    taskLog.Info("Starting incremental sync phase")
    tasksMu.Lock()
    task.Phase = "incremental"
    // ... 进入增量同步阶段
}
```

**测试验证**:
```bash
# 创建纯增量任务（过滤 app1: 和 app2: 前缀）
curl -X POST http://localhost:8088/api/v1/tasks -d '{
  "name": "test-key-filter",
  "migration_mode": "incremental",
  "source_cluster": {"addrs": ["192.168.1.19:7001"]},
  "target_cluster": {"addrs": ["192.168.1.19:8001"]},
  "options": {
    "key_filter": {"mode": "prefix", "prefixes": ["app1:", "app2:"]}
  }
}'

# 写入测试数据
redis-cli -h 192.168.1.19 -p 7001 SET app1:key1 value1
redis-cli -h 192.168.1.19 -p 7001 SET app2:key1 value2
redis-cli -h 192.168.1.19 -p 7001 SET app3:key1 value3  # 应被过滤

# 检查目标端：只有 app1: 和 app2: 前缀的 Key ✅
redis-cli -h 192.168.1.19 -p 8001 KEYS "*"
# 输出: app1:key1, app2:key1 （app3:key1 被过滤）
```

**影响范围**:
- 8.2 增量阶段 Key Filter 不生效的问题也因此得到解决
- 因为 FakeSlave 没有运行，所以 Key Filter 根本没有机会工作

**关键教训**:
1. 新增功能时，要检查所有相关的条件分支是否都支持新功能
2. 纯增量模式不仅要在启动时处理，还要在增量阶段入口处理
3. 测试时要检查日志中的 "Connection error" 和 "closed network connection"

**相关关键词**: `incremental`, `FakeSlave`, `closed network connection`, `mode`, `增量阶段`

---

### 8.5 代码审查：Migration Mode 判断完整性检查 (2026-02-11)

**审查目的**: 
- 在修复 8.4 问题后，系统排查代码中是否还有类似的 bug（模式判断不完整）
- 检查 `migration_mode` 的三种模式在所有关键决策点是否都得到正确处理

**审查范围**: 
- `cmd/simple/main.go` 中所有涉及 `MigrationMode` 判断的代码
- 包括：模式定义、默认值、FakeSlave 启动条件、全量完成处理、增量阶段入口、资源清理等

**审查结果**: 
✅ **未发现新的 Bug**

经过系统排查，三种模式（`full_only`, `full_and_incremental`, `incremental`）在所有关键决策点都得到了正确处理：

1. **模式定义** (第 55 行)：文档正确定义三种模式
2. **默认值** (第 2040-2043 行)：默认使用 `full_and_incremental` ✅
3. **纯增量检测** (第 4060-4065 行)：正确设置 `actualSkipFullSync` ✅
4. **FakeSlave 启动条件** (第 4079 行)：`needIncremental` 正确判断三种模式 ✅
5. **全量完成处理** (第 5974-5986 行)：`full_only` 直接完成，其他模式进入增量 ✅
6. **增量阶段入口** (第 4217 行)：已修复，支持 `incremental` 模式 ✅
7. **资源清理** (第 4300-4312 行)：逻辑正确，只在不需要增量时执行 ✅

**边界情况验证**:

```
场景 1: full_only
  → needIncremental = false，不启动 FakeSlave ✅
  → 全量完成后 task.Status = "completed" ✅
  → 不进入增量阶段 ✅

场景 2: full_and_incremental
  → needIncremental = true，启动 FakeSlave（缓存模式）✅
  → 全量完成后 task.Phase = "incremental" ✅
  → 进入增量阶段，回放缓存 ✅

场景 3: incremental（纯增量）
  → isIncrementalOnly = true，跳过全量 ✅
  → needIncremental = true，启动 FakeSlave（实时模式）✅
  → 进入增量阶段（已修复）✅
```

**代码质量评估**:
- ✅ 三种模式的处理逻辑清晰
- ✅ 边界情况都得到了正确处理
- ✅ FakeSlave 的启动和清理逻辑正确
- ✅ 任务状态转换合理

**建议**: 
- 添加单元测试覆盖三种模式的完整流程
- 在集成测试中验证模式切换的边界情况

**相关文档**: 
- 详细审查报告：`CODE_REVIEW_MIGRATION_MODE.md`

**相关关键词**: `migration_mode`, `full_only`, `full_and_incremental`, `incremental`, `代码审查`, `边界情况`

---

### 8.6 集群拓扑缓存导致 `:0` 连接失败 (已修复 2026-02-11)

**问题描述**:
- 全量同步阶段出现大量失败 Key（132,159 / 16,472,624 ≈ 0.8%）
- 错误信息：`restore failed: dial tcp :0: connect: connection refused (batch pipeline)`
- 前提条件：目标集群之前存在断开的节点（地址为 `:0`），后来修复了集群（重新分配 slots）

**根本原因**:
1. **集群修复前**：目标集群有一个断开的节点（地址为 `:0`），持有 slots 0-5460
2. **go-redis 缓存机制**：
   - `redis.ClusterClient` 初始化时通过 `CLUSTER SLOTS` 获取集群拓扑并**缓存**
   - 缓存包括了 `:0` 节点的信息
   - 即使后来修复了集群，**旧的连接池仍然保留错误信息**
3. **Pipeline 路由失败**：
   - 批量写入时，计算 Key 的 slot（`slot = CRC16(key) % 16384`）
   - 如果 slot 在 0-5460 范围，go-redis 路由到缓存中的 `:0` 节点
   - 连接 `:0:0` 失败 → `dial tcp :0: connect: connection refused`

**失败比例分析**:
- 理论上 slots 0-5460 占比 = 5461 / 16384 ≈ 33.3%
- 实际失败比例 ≈ 0.8%（远低于理论值）
- 说明部分 Key 通过重试或自动刷新成功了

**解决方案**:

1. **在任务启动时验证和刷新集群拓扑**：
```go
// 【BUG-FIX】连接集群后立即验证拓扑
if targetIsCluster {
    if clusterClient, ok := targetClient.(*redis.ClusterClient); ok {
        // 1. 强制刷新拓扑（避免使用缓存的错误信息）
        if err := clusterClient.ReloadState(ctx); err != nil {
            taskLog.Warn("Failed to reload target cluster state", map[string]interface{}{
                "error": err.Error(),
            })
        }
        
        // 2. 验证拓扑是否有无效节点（如 `:0`）
        if err := validateClusterTopology(ctx, clusterClient, taskLog); err != nil {
            taskLog.Error("⚠️ Target cluster topology validation failed", map[string]interface{}{
                "error": err.Error(),
            })
        }
    }
}
```

2. **添加拓扑验证函数**：
```go
// validateClusterTopology 检测无效节点（如 `:0`）
func validateClusterTopology(ctx context.Context, client *redis.ClusterClient, taskLog *logger.TaskLogger) error {
    slots, err := client.ClusterSlots(ctx).Result()
    if err != nil {
        return fmt.Errorf("CLUSTER SLOTS failed: %w", err)
    }
    
    invalidNodes := []string{}
    for _, slot := range slots {
        for _, node := range slot.Nodes {
            if node.Addr == ":0" || node.Addr == "" || strings.HasPrefix(node.Addr, ":") {
                invalidNodes = append(invalidNodes, fmt.Sprintf("slots %d-%d -> %s", 
                    slot.Start, slot.End, node.Addr))
            }
        }
    }
    
    if len(invalidNodes) > 0 {
        return fmt.Errorf("found %d invalid node(s): %v", len(invalidNodes), invalidNodes)
    }
    
    return nil
}
```

3. **启动定期刷新机制**：
```go
// 每 30 秒自动刷新集群拓扑
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    
    for {
        select {
        case <-ticker.C:
            if err := clusterClient.ReloadState(ctx); err != nil {
                taskLog.Debug("Periodic cluster reload failed", map[string]interface{}{
                    "error": err.Error(),
                })
            }
        case <-ctx.Done():
            return
        }
    }
}()
```

**预防措施**:
1. 任务创建时先验证集群健康状态
2. 如果检测到无效节点，拒绝创建任务或显示警告
3. 添加集群健康检查 API：`GET /api/v1/cluster/health`

**临时解决方案**（对已失败的 Key）:
1. 停止任务
2. 重启 tendis-migrate 服务（刷新连接池）
3. 使用"重试失败 Key"功能

或者：
1. 停止并删除任务
2. 重新创建任务（使用新的连接和拓扑信息）

**关键教训**:
1. go-redis `ClusterClient` 的拓扑缓存机制可能导致连接失败
2. 集群修复后，必须刷新客户端连接
3. 定期刷新拓扑可以避免类似问题

**相关关键词**: `dial tcp :0`, `connection refused`, `CLUSTER SLOTS`, `ClusterClient`, `拓扑缓存`, `ReloadState`

---

### 8.7 getDBSize 超时导致 totalKeys=10000 (已修复 2026-02-11)

**问题描述**:
- 集群有 50 亿 Key，但全量同步显示 `total_keys: 10000`
- 迁移了 1647 万 Key 后进入增量阶段，进度显示异常
- 日志中出现：`Failed to get source DB size, using estimate {"error": "read tcp ... i/o timeout"}`

**根本原因**:
1. **DBSIZE 命令超时**：
   - Tendis 集群节点多，`getDBSize()` 需要遍历所有主节点
   - 默认超时时间太短（5 秒），大集群容易超时
   - 超时后使用硬编码的默认值 `10000`，严重低估

2. **Tendis DBSIZE 不准确**：
   - Tendis 的 `DBSIZE` 命令可能返回 `0`（不准确）
   - 但代码没有处理这种情况，直接使用 `0`

3. **低估的影响**：
   - `totalKeys = 10000`，但实际有 50 亿 Key
   - 全量同步不知道实际要迁移多少 Key
   - 进度计算错误（使用 `KeysToMigrate` 还好，不受影响）

**代码分析**:
```go
// 第 4085 行（修复前）
totalKeys, err := getDBSize(ctx, sourceClient, sourceIsCluster)
if err != nil {
    taskLog.Warn("Failed to get source DB size, using estimate", ...)
    totalKeys = 10000 // ❌ 硬编码的默认值太小了
}
```

**解决方案**:

1. **增加超时时间并为每个节点单独设置超时**：
```go
// 【BUG-FIX】增加超时时间，避免大集群 DBSIZE 超时
dbSizeCtx, dbSizeCancel := context.WithTimeout(ctx, 30*time.Second)  // 总超时 30 秒
totalKeys, err := getDBSize(dbSizeCtx, sourceClient, sourceIsCluster)
dbSizeCancel()

// getDBSize 内部为每个节点设置 10 秒超时
nodeCtx, nodeCancel := context.WithTimeout(ctx, 10*time.Second)
defer nodeCancel()
size, err := node.DBSize(nodeCtx).Result()
```

2. **使用 -1 表示"未知"，而不是低估值**：
```go
if err != nil {
    taskLog.Warn("Failed to get source DB size, will not display progress percentage", ...)
    // 【BUG-FIX】使用 -1 表示"未知"，而不是低估值 10000
    // - 全量同步会正常运行，只是不显示进度百分比
    // - 迁移完成后会显示实际迁移的 Key 数量
    totalKeys = -1
} else if totalKeys == 0 {
    // 【BUG-FIX】Tendis DBSIZE 可能返回 0（不准确），使用 -1 表示未知
    taskLog.Warn("DBSIZE returned 0 (may be inaccurate for Tendis), treating as unknown", ...)
    totalKeys = -1
}
```

3. **修复 BytesTotal 计算**：
```go
// 【BUG-FIX】totalKeys 可能是 -1（未知），不计算 BytesTotal
if totalKeys > 0 {
    task.BytesTotal = totalKeys * 256
} else {
    task.BytesTotal = 0  // 未知大小
}
```

4. **getDBSize 容错处理**：
```go
func getDBSize(ctx context.Context, client redis.UniversalClient, isCluster bool) (int64, error) {
    // ...
    var firstErr error
    err := clusterClient.ForEachMaster(ctx, func(ctx context.Context, node *redis.Client) error {
        size, err := node.DBSize(nodeCtx).Result()
        if err != nil {
            // 记录第一个错误，但继续尝试其他节点
            if firstErr == nil {
                firstErr = err
            }
            log.Printf("[getDBSize] Node %s DBSIZE failed: %v", node.Options().Addr, err)
            return nil  // 继续尝试其他节点
        }
        // ...
    })
    
    // 如果所有节点都失败，返回错误
    if total == 0 && firstErr != nil {
        return 0, firstErr
    }
    
    return total, nil
}
```

**测试验证**:
```bash
# 日志输出（修复后）
[18:49:24] Failed to get source DB size, will not display progress percentage {"error": "i/o timeout"}
[18:49:24] Note: Full migration will continue without total key count

# totalKeys = -1 时的表现
- KeysTotal = -1（不显示总数）
- BytesTotal = 0（不显示总字节）
- Progress 基于 KeysToMigrate 计算（SCAN 阶段统计的实际值）
```

**关键优势**:
1. ✅ 不再因为超时而使用错误的默认值
2. ✅ 全量同步正常运行，只是不显示"总 Key 数"
3. ✅ 进度计算使用实际的 `KeysToMigrate`（SCAN 阶段统计），不受影响
4. ✅ 最终完成时显示实际迁移的 Key 数量

**误区澄清**:
- ❌ **错误理解**：totalKeys 必须准确，否则任务无法运行
- ✅ **正确理解**：totalKeys 只是用于显示，不影响实际迁移逻辑
- ✅ **关键指标**：`KeysToMigrate`（SCAN 阶段统计）才是进度计算的依据

**相关关键词**: `getDBSize`, `totalKeys`, `DBSIZE超时`, `10000`, `i/o timeout`, `-1`, `未知总数`

---

## 10. 部署和环境问题

### 10.1 环境适配代码污染工具代码 (2026-02-12)

**问题描述**: 
- 为解决家里 macOS Docker bridge 网络问题，在 `cmd/simple/main.go` 中添加了 `buildAddrMapping` 函数
- 该函数为 Docker 容器 IP 和宿主机 IP 建立映射
- 虽然在正常环境下不会触发（mapping 为空），但**违反了工具普适性原则**

**核心原则**: 
> **环境是环境，工具是工具。** 绝对不要修改工具代码去适配某个特定的部署环境。

**错误做法**:
```go
// ❌ 在工具代码中添加环境特定的 workaround
func buildAddrMapping(ctx context.Context, addrs []string, password string) map[string]string {
    // Docker bridge 网络地址映射...
}
clusterOpts.NewClient = func(opt *redis.Options) *redis.Client {
    if mappedAddr, ok := addrMapping[opt.Addr]; ok {
        opt.Addr = mappedAddr
    }
    return redis.NewClient(opt)
}
```

**正确做法**:
- 环境兼容性问题应在**网络层/部署层**解决
- 方案 A：使用 macvlan 网络让容器直接获得宿主机同网段 IP
- 方案 B：使用 iptables/socat 端口转发

**处理结果**: 
- 已回滚 `buildAddrMapping` 相关代码
- 已清理检查所有 `.go` 文件，确认无其他环境特定代码污染

**相关关键词**: `buildAddrMapping`, `Docker`, `bridge`, `环境适配`, `普适性`

---

### 10.2 部署时误删 data 目录导致任务数据丢失

**问题描述**: 
- 使用 `rm -rf tendis-migrate-package` 后重新部署
- 删除了 `data/` 目录中的所有任务数据（tasks-state.json、checkpoints、error-keys 等）

**正确的部署方式**（只替换二进制和前端，保留 data/ 和 logs/）：
```bash
# 安全替换：不动 data/ 和 logs/
ssh -p 8822 root@8.137.20.144 "cd /home/tendis-migrate-package && \
  bash stop.sh && \
  cp /tmp/tendis-migrate-new ./tendis-migrate && chmod +x ./tendis-migrate && \
  rm -rf ./web/dist && cp -r /tmp/web-dist-new ./web/dist && \
  bash run.sh"
```

**绝对禁止**: `rm -rf tendis-migrate-package`、`rm -rf tendis-migrate-package/data`

**相关关键词**: `部署`, `data目录`, `任务丢失`, `rm -rf`

---

## 11. v2.4.0 新增功能相关问题

### 11.1 Preflight Check 校验相关

**功能描述**: 
- v2.4.0 新增迁移前校验功能（Preflight Check）
- 自动校验：源端/目标端连通性、集群状态、Binlog 配置、版本兼容性
- API: `POST /api/v1/tasks/:id/preflight-check`

**常见问题**:
1. 校验超时：大集群节点多时，校验可能超时
   - 解决：校验为每个节点设置独立超时
2. Binlog 未启用：增量同步需要 binlog-enabled=yes
   - 解决：校验结果会明确提示

**相关关键词**: `preflight-check`, `校验`, `连通性`, `Binlog`

---

### 11.2 集群拓扑自动刷新

**功能描述**: 
- v2.4.0 新增定期刷新集群拓扑机制
- 解决集群拓扑变更后连接失效的问题（如 8.6 节描述的 `:0` 问题）

**实现方式**:
```go
// 每 30 秒刷新一次集群拓扑
go func() {
    ticker := time.NewTicker(30 * time.Second)
    defer ticker.Stop()
    for {
        select {
        case <-ticker.C:
            clusterClient.ReloadState(ctx)
        case <-ctx.Done():
            return
        }
    }
}()
```

**相关关键词**: `拓扑刷新`, `ReloadState`, `ClusterClient`

---

### 11.3 FakeSlave 本地 IP 自动探测

**功能描述**: 
- v2.4.0 新增 `getOutboundIP()` 自动探测本机到源端的出口 IP
- 不再需要手动配置 FakeSlave 绑定地址

**实现原理**: 使用 UDP 连接到目标地址，获取本地出口 IP（不实际发送数据）

**相关关键词**: `getOutboundIP`, `FakeSlave`, `IP探测`, `出口IP`

---

### 11.4 Error Keys 查询接口

**功能描述**: 
- v2.4.0 新增查看迁移失败 Key 列表的接口
- API: `GET /api/v1/tasks/:id/error-keys`
- 便于排查和重试

**相关关键词**: `error-keys`, `失败Key`, `排查`

---

## 更新日志

遇到新问题并解决后，请按以下格式添加记录：

```markdown
### X.Y 问题简述

**问题描述**: 
详细描述问题现象

**根本原因**: 
分析问题的根本原因

**解决方案**:
给出具体的解决步骤或代码

---

## 2026-02-16 新发现和修复

### BUG-1: 系统 key (stat:total/daily/hourly) 被迁移到目标端

**现象**: 全量迁移完成后，目标端存在 `stat:total:*`、`stat:daily:*`、`stat:hourly:*` 等 Tendis 内部统计 key。

**根因**: `isSystemInternalKey()` 过滤函数只在 `internal/engine/task_runner.go` 中定义和使用，但实际的全量迁移走的是 `cmd/simple/main.go` 中的 `matchKeyFilterV2()` 函数，后者没有系统 key 过滤逻辑。

**修复**: 在 `cmd/simple/main.go` 中的 `matchKeyFilterV2()` 函数开头加入系统 key 检查：
```go
func matchKeyFilterV2(key string, filter *KeyFilter) bool {
    // 内置排除：系统内部 key 始终跳过（无论过滤配置如何）
    if isSystemInternalKey(key) {
        return false
    }
    // ...
}
```

### BUG-2: FakeSlave atomic.Value panic (sync/atomic: store of inconsistently typed value)

**现象**: 多个 FakeSlave 并发重连时，程序 panic 崩溃。

**根因**: `atomic.Value` 要求每次 `Store()` 的值类型完全一致。`lastError` 字段存储的是 `error` 接口，但不同错误的底层类型不同（`*net.OpError`、`*fmt.wrapError` 等），导致 panic。

**修复**: 定义 `errorWrapper` 结构体包装 error，确保 `atomic.Value` 始终存储同一类型：
```go
type errorWrapper struct { err error }
fs.lastError.Store(&errorWrapper{err: err})
```

### BUG-3: 升级重启后无法区分"手动暂停"和"升级自动暂停"的任务

**现象**: 升级重启后，所有 paused 任务都需手动恢复，无法自动恢复升级前正在运行的任务。

**修复**: Task 结构体新增 `ShutdownPaused bool` 字段，SIGTERM 时标记 running 任务为 `ShutdownPaused=true`，重启后 `autoResumeShutdownPausedTasks()` 只恢复带标记的任务。

**相关关键词**: `关键词1`, `关键词2`
```

---

### BUG-4: 全量迁移限速完全不生效（2026-02-16 修复）

**现象**: 设置 `source_qps=500, target_qps=500`，但实际迁移速度 33000+ keys/s，3 秒迁移完 10 万 key，限速形同虚设。

**根因**: `cmd/simple/main.go` 中 `processBatchKeys()` 方法的限速调用有 BUG：
- 源端限速 `rl.Wait()` 只消耗 1 个令牌/批次（而一批可能有 100-1000 个 key）
- 目标端限速 `tl.Wait()` 也只消耗 1 个令牌/批次
- 注释甚至明确写了"仅等待一次令牌"，是有意为之但逻辑错误
- 实际 QPS = 设定值 × 批次大小（100-1000倍放大）

**修复**:
1. `RateLimiter` 新增 `WaitN(n int)` 方法，一次消耗 N 个令牌
2. `processBatchKeys()` 中源端调用 `rl.WaitN(len(keys))`，按实际 key 数消耗
3. 目标端同理调用 `tl.WaitN(len(keys))`
4. `internal/limiter/pid_controller.go` 中也新增了 `AcquireSourceN/AcquireTargetN`

**修改文件**:
- `cmd/simple/main.go`: 新增 `WaitN` 方法，修改 `processBatchKeys` 限速调用
- `internal/limiter/pid_controller.go`: 新增 `AcquireSourceN/AcquireTargetN` 方法
- `internal/engine/task_runner.go`: `migrateKeys` 改用 N 令牌消耗

**验证结果**:

| 场景 | QPS设置 | 耗时 | 实际速度 | 结论 |
|------|---------|------|---------|------|
| 修复前 500 QPS | 500 | 3s | 33000+ keys/s | ❌ 限速无效 |
| 修复后 500 QPS | 500 | 4m30s | ~370 keys/s | ✅ 限速生效 |
| 修复后 50000 QPS | 50000 | 11s | ~9000 keys/s | ✅ 高速模式正常 |
| 无限速 (QPS=0) | 0 | 3s | 36000 keys/s | ✅ 不限速全速 |

### BUG-5: 运行中动态调整限速导致迁移卡死

**日期**: 2026-02-16

**现象**: 任务运行中通过 `PUT /api/v1/tasks/{id}/config` 修改 QPS 限速后，迁移完全卡住（migrated_keys 不再增长，realtime_speed=0，active_workers 仍为 8）。

**根因**: `RateLimiter.Stop()` 只关闭了 `stopChan` 停止令牌填充 goroutine，但正在 `Wait()`/`WaitN()` 中阻塞等待 `<-rl.tokens` 的 worker goroutine 无法感知到限速器已停止：
- Worker 调用 `GetRateLimiter()` 获取限速器指针 `rl`
- Worker 在 `rl.WaitN(1000)` 中循环执行 `<-rl.tokens`
- `UpdateRateLimiter()` 调用 `rl.Stop()` → 后台填充 goroutine 退出 → tokens channel 不再有新令牌
- 但 `tokens` channel 未关闭，`<-rl.tokens` **永远阻塞**
- Worker 手中持有旧限速器指针，无法获取新限速器

**修复**: `Wait()`/`WaitN()` 方法使用 `select` 同时监听 `tokens` 和 `stopChan`：
```go
// 修复前（卡死）：
func (rl *RateLimiter) Wait() {
    <-rl.tokens  // Stop后永远阻塞
}

// 修复后（Stop时立即返回）：
func (rl *RateLimiter) Wait() {
    select {
    case <-rl.tokens:
    case <-rl.stopChan:  // 限速器被Stop时立即唤醒
    }
}
```

**修改文件**: `cmd/simple/main.go` — `RateLimiter.Wait()` 和 `WaitN()` 方法

**验证结果**（60万key，服务器 1.95.147.159）：

| 时间 | 操作 | Migrated | 速度变化 | 结论 |
|------|------|----------|----------|------|
| 23:25:17 | 初始 5000 QPS | 0 → 388000 | ~1400/s | 基准 |
| 23:26:47 | 5000→100 QPS | 388000 → 396000 | 大幅降低（45s出1批）| ✅ 降速生效 |
| 23:29:12 | 100→0 QPS (无限) | 396000 → 600000 | 30600/s | ✅ 取消限速生效 |

另外验证（10万key）：

| 时间 | 操作 | 速度变化 | 结论 |
|------|------|----------|------|
| 初始 500 QPS | ~400/s | 基准 |
| 500→5000 QPS | ~1400/s | ✅ 提速生效 |

### BUG-6: 多 Worker 下限速器 QPS 严重下降（详见上文 BUG-6 节）

---

## 12. 生产故障记录（50 亿 Key 迁移）

### 12.1 P0：增量阶段恢复后错误执行全量迁移（已修复 2026-02-08）

**故障场景**：测试环境 A，58.75 亿 Key 迁移任务

**现象**：
- 任务在增量阶段被手动暂停（`phase=incremental`, `progress=100`）
- 恢复后没有跳过全量，而是重新执行全量迁移
- 日志：`Task auto-resumed {phase: "incremental"}` → `Starting full migration`
- 导致 5.5 小时浪费扫描已存在的 58.87 亿 Key

**根因**：`runMigration()` 恢复逻辑没有检查 `task.Phase == "incremental"` 的情况，无条件执行全量迁移。

**修复**：在全量迁移入口检查 phase 状态：
```go
if task.Phase == "incremental" || task.Phase == "completed" || task.FullSyncCompleted {
    taskLog.Info("Full migration already completed, skipping to incremental")
    // 跳过全量，直接进入增量
} else {
    startFullMigration(task)
}
```

**关键教训**：恢复任务时必须根据保存的 phase 状态决定从哪个阶段开始，不能无条件重新执行。

---

### 12.2 P0：增量阶段并发启动新全量迁移（已修复 2026-02-08）

**现象**：
- 第 1 次全量完成后进入增量阶段
- 日志中同时出现两套进度统计：`{elapsed: 864s}` 和 `{elapsed: 61164s}`
- 增量阶段同时运行了一个新的全量迁移，浪费 5 小时

**根因**：并发控制缺失，某处 goroutine 在不应该的时候触发了新的全量迁移。

**修复**：添加全量迁移互斥锁：
```go
var fullMigrationMu sync.Mutex
var fullMigrationRunning map[string]bool

func startFullMigration(task *Task) error {
    fullMigrationMu.Lock()
    if fullMigrationRunning[task.ID] {
        fullMigrationMu.Unlock()
        logger.Warn("Full migration already running, skip duplicate start")
        return nil
    }
    fullMigrationRunning[task.ID] = true
    fullMigrationMu.Unlock()
    defer func() {
        fullMigrationMu.Lock()
        delete(fullMigrationRunning, task.ID)
        fullMigrationMu.Unlock()
    }()
    return doFullMigration(task)
}
```

**关键教训**：
1. 全量和增量阶段必须互斥，不能并行运行
2. 在进入增量前确保所有全量 Worker 已退出
3. 使用互斥锁防止重复启动

---

### 12.3 P1：断点恢复后 Cursor 始终为 0（已修复 2026-02-08）

**现象**：
- 任务自动暂停后恢复，检测到 checkpoint（`node_cursors: 3`）
- 但所有节点 cursor 都是 0：`Resuming node scan from cursor {cursor: 0}`
- 导致所有节点从头扫描

**根因**：断点保存时 cursor 值没有被正确赋值或序列化丢失。

**修复**：
1. 每次 SCAN 后立即更新 cursor 到 checkpoint 结构
2. 定期保存 checkpoint（每 10000 Key 或 30 秒）
3. 保存时记录日志验证 cursor 值

---

### 12.4 P2：时间字段被重复覆盖

**现象**：每次执行全量都会更新 `FullSyncStartTime`，无法追溯首次启动时间。

**修复**：添加空值检查：
```go
if task.FullStartAt == "" {
    task.FullStartAt = time.Now().Format(time.RFC3339)
}
```

---

### 12.5 P2：443 万失败 Key 原因不可追溯

**现象**：失败 Key 日志只记录 `reason: "failed"`，没有具体错误信息。

**修复**：增强 `ErrorKey` 结构体，添加详细字段：
```go
type ErrorKey struct {
    Key        string `json:"key"`
    Type       string `json:"type"`
    Reason     string `json:"reason"`
    Detail     string `json:"detail"`       // 具体错误信息
    SourceNode string `json:"source_node"`
    TargetNode string `json:"target_node"`
    Operation  string `json:"operation"`    // DUMP/RESTORE/HSET等
    Phase      string `json:"phase"`        // full/incremental
    RetryCount int    `json:"retry_count"`
    Timestamp  string `json:"timestamp"`
}
```

**关键教训**：错误日志必须记录足够的上下文信息，否则大规模迁移出问题时无法排查。

**相关关键词**：`生产故障`, `50亿Key`, `增量恢复`, `并发全量`, `cursor=0`, `P0`, `P1`

---

## 13. 崩溃恢复机制

### 13.1 安全 Cursor 回退机制（getSafeCheckpointCursor）

**问题**：旧版本断点保存的是最新 SCAN cursor，但 `keyChan` 中还有未被 worker 消费的 key。SIGKILL 后这些 key 丢失。

**机制原理**：
1. 维护 cursor 历史栈，记录每批 SCAN 的 `{prevCursor, keyCount}`
2. 保存断点时，根据 `len(keyChan) + workerCount * batchSize` 估算未消费 key 数量
3. 回退到覆盖所有未消费 key 的安全 cursor
4. 恢复时重复迁移一小批 key（迁移是幂等的：skip/replace）

**实测结果**（10 万 Key 场景）：
```
崩溃前断点: safe_cursor=60501, total_scanned=62000 (cursor 回退了约 1500)
恢复后: 从 safe_cursor=60501 继续 SCAN
恢复迁移: migrated_keys=26723, skipped_keys=13277 (重复的被 skip)
源端总数: 100,000 keys → 目标端总数: 100,000 keys
数据丢失: 0 keys ✅ 零丢失
```

**关键点**：
- SIGTERM（优雅关闭）：等待 keyChan 排空后保存精确 cursor
- SIGKILL（强制杀死）：依赖定期保存的安全 cursor，最多丢失 30 秒进度
- 重复迁移不会导致数据错误（幂等性保证）

**相关关键词**：`getSafeCheckpointCursor`, `安全cursor`, `SIGKILL`, `零丢失`, `崩溃恢复`, `幂等`

---

## 14. API 边界条件问题（已修复）

### 14.1 Stop API 404（已修复 2026-02-05 发现）

**问题**：`/api/v1/tasks/:id/stop` 返回 404，路由未注册，只有 `/stop-incremental`。

**修复**：在 `taskHandler` 路由中注册 `stop` action。

### 14.2 空参数可以创建任务（已修复）

**问题**：POST `/api/v1/tasks` 传 `{}` 或只传 `name` 都能创建成功，没有验证必填字段。

**修复**：添加必填字段验证：
```go
if req.SourceCluster == nil || len(req.SourceCluster.Addrs) == 0 {
    return error("source_cluster is required")
}
if req.TargetCluster == nil || len(req.TargetCluster.Addrs) == 0 {
    return error("target_cluster is required")
}
```

### 14.3 启动不存在任务返回 success（已修复）

**问题**：POST `/api/v1/tasks/non-existent-id/start` 返回 `{"code":0, "message":"success"}`，应返回 404。

**修复**：在 `startTaskHandler` 中先检查任务是否存在。

**相关关键词**：`API`, `404`, `空参数`, `参数验证`, `边界条件`

---

### BUG-6: 多 Worker 下限速器 QPS 严重下降

**发现时间**: 2026-02-17

**现象**: QPS=500 限速, Worker 从 2 增到 8 后，速度从 ~526/s 暴降到 ~100/s，远低于预期的 ~500/s

**根因**: 自制 token-channel 限速器的 `WaitN` 逐个从 channel 取 token，多个 Worker 并发调用时串行争抢同一个 channel，导致有效吞吐严重退化。

旧实现核心缺陷：
```go
// 8 个 Worker 各攒 100 个 key，同时调用 WaitN(100)
// 总共需要 800 个 source token + 800 个 target token
// 但 token channel 每秒只填充 500 个，且逐个争抢
func (rl *RateLimiter) WaitN(n int) {
    for i := 0; i < n; i++ {
        <-rl.tokens  // 逐个等待，多 goroutine 串行争抢
    }
}
```

**修复**: 替换为 `golang.org/x/time/rate.Limiter`，基于精确时间计算的标准 token bucket 算法，`WaitN` 一次性预约 N 个 token，多 goroutine 并发不退化。

**验证结果** (260万 key, 服务器 1.95.147.159):

| 操作 | 旧限速器 | 新限速器 |
|------|---------|---------|
| W=2, QPS=500 基准 | 526/s | 526/s |
| W=2→8, QPS=500 | **105/s** ❌ | **421-526/s** ✅ |
| W=8→16, QPS=500 | - | **421-526/s** ✅ |
| W=16, QPS=500→5000 | - | **~5000/s** ✅ |
| W=16, 无限速 | - | **~38000/s** ✅ |
| 无限速→QPS=1000 | - | **收敛到 ~1052/s** ✅ |

---

## 15. 回归测试覆盖映射（2026-03-01 更新）

本文档中记录的每个问题是否有对应的自动化回归测试，确保问题不会回归。

**测试脚本**: `tests/regression_test.py` — **U 分类（历史问题回归）**

| 编号 | 问题 | 回归测试 | 覆盖方式 |
|:---|:---|:---|:---|
| 2.1 | 创建任务字段名错误 | **U1** | 使用错误字段名创建任务，验证被拒绝 |
| 2.2 | incr_keys_synced 字段位置 | **U2** | 检查 stats/progress/top-level 三个位置 |
| 3.1 | Pattern 增量匹配失败 | **U3** | 增量阶段 pattern 通配符过滤验证 |
| 3.2 | FakeSlave heartbeats 检测 | **U2** | 验证 incr_heartbeats > 0 |
| 7.4 | TTL 一致性/PTTL 精度 | **U11** | EXPIRE/PEXPIRE/PERSIST 增量同步精度 |
| 9.6 | 待迁移Key数显示为0 | **U9** | 采样检查 keys_to_migrate > 0 |
| 12.1 | 增量恢复后重新全量 | **U5** | 增量阶段暂停恢复后验证 phase=incremental |
| 14.1 | Stop API 404 | **U6** | POST /tasks/{id}/stop 可用 |
| 14.2 | 空参数创建任务 | **U7** | 空 body / 只有 name 应被拒绝 |
| 14.3 | 启动不存在任务 | **U8** | 返回错误而非 success |
| BUG-1 | 系统 key 被迁移 | **U4** | stat:total 等不出现在目标端 |
| BUG-3 | ShutdownPaused 自动恢复 | **U12** | SIGTERM→重启→任务自动恢复 |
| BUG-5 | 动态调整限速卡死 | **U10** | 运行中修改 QPS 后任务正常完成 |

**运行回归测试**:
```bash
# 只运行历史问题回归
python3 tests/regression_test.py --env home --categories U

# 运行全部（包含 158 个测试）
python3 tests/regression_test.py --env home
```

---

## 16. 深度代码审查修复（2026-03-02）

本次基于 3 个根因分析（异常路径未覆盖、静默错误、组合爆炸）对 7 个核心 Go 文件进行了全面排查，共发现和修复 16 个 Bug。

### 16.1 致命级 Bug

#### BUG-A1: async_executor.go — Stop 后 send on closed channel panic

**现象**: 增量同步停止后程序 panic 崩溃。

**根因**: `Stop()` 关闭 buffer channel 后，worker 排空时失败命令重试写入已关闭的 channel。

**修复**: 先 cancel context 再 close buffer；重试前检查 running 状态；Submit 添加 `defer recover()`。

#### BUG-A2: async_executor.go — Pipeline 索引错位

**现象**: HSET 命令的错误被归因到后续无关命令，错误统计混乱。

**根因**: HSET+PExpire 产生 2 条 Pipeline 命令，但 results 按 1:1 映射到 cmds，导致索引错位。

**修复**: `addToPipelineWithCount` 返回每个命令实际产生的 Pipeline 条目数，用累加索引正确映射。

**测试**: Z6（Pipeline 索引对齐验证）

#### BUG-A3: async_executor.go — 约 15 处非安全类型断言

**现象**: 非 string 类型参数导致 panic。

**修复**: 全部改为安全模式 `key, ok := cmd.Args[0].(string); if !ok { return 0 }`。

**测试**: Z10（类型断言安全）

### 16.2 高级 Bug

#### BUG-B1: concurrent_writer.go — pendingCount 数据竞争

**根因**: `w.pendingCount[idx]++` 普通写与 `atomic.LoadInt64` 读混用，违反 Go 内存模型。

**修复**: 统一使用 `atomic.AddInt64` / `atomic.StoreInt64`。

**测试**: Z11（并发 Writer 原子计数）

#### BUG-B2: conflict_store.go — 读锁下执行写操作

**根因**: Query/Export 持有 RLock 调用 readFromDisk→Flush()（写操作），并发 Query 损坏 bufio.Writer。

**修复**: Query 和 Export 改用写锁（`s.mu.Lock()`）。

**测试**: Z12（ConflictStore 锁修复）

#### BUG-B3: conflict_store.go — Close 数据丢失窗口

**根因**: Flush() 和 Close() 分两次加锁，之间可能有新写入丢失。

**修复**: 合并到同一个锁作用域。

### 16.3 阻断级 Bug

#### BUG-C1: binlog_parser.go — ParseBinlogs count=0 不解析

**现象**: `ReplayCachedBinlogs` 完全失效，全量期间写入的增量数据丢失。

**根因**: 循环条件 `i < expectedCount` 当 expectedCount=0 时永远不执行。

**修复**: 添加 `expectedCount == 0` 时解析所有可用数据的逻辑。

**测试**: Z8（Binlog 缓存回放）

### 16.4 严重级 Bug

#### BUG-D1: fake_slave.go — binlog 位置提前更新

**现象**: apply 失败后重连使用新位置，丢失的 binlog 不会被重新接收。

**修复**: 只在 apply 成功后更新位置。

**测试**: Z9（Binlog 位置回退）

#### BUG-D2: fake_slave.go — errors 计数器不重置

**现象**: 累计非连续错误触发不必要的重连循环。

**修复**: 成功处理命令时 `fs.stats.errors.Store(0)` 重置计数器。

**测试**: Z13（错误计数器重置）

#### BUG-D3: fake_slave.go — conn 无锁访问

**根因**: receiveLoop 中 `fs.conn.SetReadDeadline` 与 Stop() 并发设 nil 导致 panic。

**修复**: 先加锁获取 conn 副本再使用。

### 16.5 中等级 Bug

#### BUG-E1: pipeline_migrator.go — PTTL=-2 幽灵 key

**根因**: Key 在 DUMP 和 PTTL 之间被删除，PTTL 返回 -2 但仍执行 RESTORE（ttl=0=永不过期）。

**修复**: 检测 PTTL=-2 时跳过此 key。

**测试**: Z7（幽灵 Key 防护）

#### BUG-E2: task_runner.go — json.Unmarshal 错误被忽略

**修复**: 添加错误检查，失败时使用默认配置。

#### BUG-E3: task_runner.go — GetOrCreateStats 返回 nil

**修复**: 添加 nil 检查，失败时跳过统计更新。

#### BUG-E4: concurrent_writer.go — SET 命令 Args 越界 panic

**修复**: 添加 `if len(cmd.Args) < 1` 检查。

---

### 16.6 测试脚本 Bug 修复（同期）

本次还修复了 regression_test.py 中发现的 10 个问题：

| # | 位置 | 问题 | 修复 |
|:---|:---|:---|:---|
| 1 | W11 第6150行 | `and`/`or` 运算符优先级错误 | 添加括号 `service_ok and (... or ...)` |
| 2 | W8 第5948行 | 双重 API 调用导致不一致 | 先存结果再判断 |
| 3 | W9 第6021行 | FakeSlave 容忍度过高（70%） | 提高到 85% |
| 4 | W12 | API 返回值未检查 | 添加返回值检查和 stop 等待时间 |
| 5 | V1 第4929行 | 等待时间不足（TCP超时>30s） | 改为轮询方式等待 |
| 6 | V4 第5082行 | skipped 字段名双重计数 | 改为 fallback 取值 |
| 7 | V6 第5221行 | `or` 条件使检查失效 | 改为检查 API code |
| 8 | X9 | docstring 与实现不一致 | 修正 docstring 描述 |
| 9 | X11 第6884行 | bytes 验证过于宽松 | 添加合理范围检查 |
| 10 | X16 第7127行 | progress 嵌套重名字段 | 修正字段访问路径 |

**相关关键词**: `深度代码审查`, `Pipeline索引`, `Binlog缓存`, `类型断言`, `原子操作`, `读锁`, `幽灵Key`, `计数器重置`

---

## 17. Docker Overlay2 磁盘爆满与 Tendis 容器问题（2026-03-02）

### 17.1 Docker overlay2 积累 Tendis dump 文件导致磁盘 100%

**现象**: 回归测试运行到 Z 分类时，所有写入操作返回 `ERR:3,msg:db stopped!`，目标端数据只写入一半（精确到一半 slot 范围），磁盘 `/data` 使用 100%。

**根因**:
1. Tendis 的 `dumpdir` 默认为 `./dump`（相对路径），在 Docker 容器中写入到 overlay2 diff 层
2. RocksDB 的 SST dump 文件不受宿主机 `-v /data/tendis/xxx:/data` 挂载管控
3. 大量回归测试持续读写，dump 文件在 overlay2 层累积到 46-97GB
4. `/data` 分区只有 49GB，一旦满了 Tendis 进入 `db stopped` 只读模式

**典型症状**:
- 回归测试恰好一半数据写入成功（50/100, 5/10, 750/1500, 15/30）
- `df -h /data` 显示 100% 使用
- `du -sh /data/docker/lib/overlay2/` 显示数十 GB
- Tendis 错误: `ERR:3,msg:db stopped!`

**解决方案**（彻底方案：不用 Docker，直接宿主机运行 Tendis）:
```bash
# 1. 停止并删除所有 Docker 容器
docker rm -f tendis-7001 tendis-7002 tendis-8001 tendis-8002

# 2. 清理 Docker 存储
docker system prune -a -f
systemctl restart docker  # 释放被删除文件占用的空间

# 3. 直接在宿主机运行 Tendis
TENDIS_BIN=/home/Tendis-2.7.0-rocksdb-v8.5.3/build/bin/tendisplus
for port in 7001 7002 8001 8002; do
    mkdir -p /data/tendis/$port/log /data/tendis/$port/dump
    $TENDIS_BIN /data/tendis/$port/tendis.conf
done
```

**配置要点**（tendis.conf）:
```
dir /data/tendis/$port        # 数据目录指向宿主机路径
logdir /data/tendis/$port/log
dumpdir /data/tendis/$port/dump  # 关键！dump 文件也在可控路径下
daemon yes                     # 宿主机运行用 daemon 模式
dump-file-keep-num 1           # 只保留 1 个 dump 文件
dump-file-keep-hour 1          # dump 文件只保留 1 小时
```

**预防措施**:
- 优先宿主机直接运行 Tendis，避免 Docker overlay2 积累
- 如必须用 Docker，配置文件中 `dumpdir` 必须指向挂载卷内路径
- 定期监控 `df -h /data` 和 `du -sh /data/docker/lib/overlay2/`

### 17.2 Docker 容器中 Tendis daemon 模式导致容器立即退出

**现象**: Docker 容器创建后立即退出（exit code 0），不断重启，但 Tendis 进程实际没有运行。

**根因**: Tendis 默认 `daemon:yes`，fork 出子进程后父进程退出。Docker 监控的是父进程（PID 1），父进程退出后 Docker 认为容器已停止。

**解决方案**:
- Docker 容器中必须配置 `daemon no`（前台模式）
- 宿主机直接运行时用 `daemon yes`（后台模式）

### 17.3 B1 测试依赖前置数据

**现象**: 全量回归测试中 B1 始终失败，`src_dbsize=0, migrated=0`。

**根因**: B1（全量无过滤）测试不自己准备数据，依赖源端已有残留数据。在 FLUSHALL 后源端为空。

**修复**: B1 测试自行写入 200 个测试 key：
```python
src = SRC_PORTS[0]
for i in range(200):
    redis_set(src, f"b1_data:{i:04d}", f"value_{i}")
```

### 17.4 FakeSlave 暂停恢复 binlog 位置丢失

**现象**: W9/Y5/Y6/Z9 测试失败 — 暂停恢复后增量同步数据为 0。

**根因**: 暂停时 `task.Cleanup()` 关闭 `stopCh` 导致 FakeSlave 断开。恢复时 `startFakeSlaves()` 获取**当前** binlogpos（已前进），暂停期间的 binlog 条目永久丢失。

**修复**: 
1. FakeSlave 新增 `GetCurrentBinlogPos()` / `GetSourceAddr()` / `GetStoreID()` 方法
2. Task 结构体新增 `savedBinlogPositions map[string]uint64` 字段
3. `waitForFakeSlaves()` 在停止 FakeSlave 前调用 `saveFakeSlaveBinlogPositions()` 保存位置
4. `startFakeSlaves()` 恢复时优先使用保存的位置

### 17.5 checkFakeSlaveSupport() 误判 Tendis 不支持 binlog

**现象**: Tendis 2.7.0 已启用 binlog，但迁移工具降级使用 IDLETIME 模式。

**根因**: `checkFakeSlaveSupport()` 检查 `INFO replication` 中的 `binlog_enabled` 字段，但 Tendis 2.7.0 不暴露此字段。

**修复**: 改用 `CONFIG GET binlog-enabled` 和 `binlogpos 0` 命令检测。

### 17.6 PTTL 类型比较错误

**现象**: 幽灵 key（DUMP 后被删除的 key）被以 TTL=0（永不过期）写入目标端。

**根因**: `if ttl == -2` 比较 `time.Duration`（纳秒）和整数 -2（即 -2 纳秒），但 PTTL 返回 -2 毫秒。

**修复**: `if ttl == -2*time.Millisecond`

**相关关键词**: `overlay2`, `磁盘爆满`, `db stopped`, `daemon`, `binlog位置`, `FakeSlave`, `checkFakeSlaveSupport`, `PTTL`
