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

## 贡献指南

遇到新问题并解决后，请按以下格式添加记录：

```markdown
### X.Y 问题简述

**问题描述**: 
详细描述问题现象

**根本原因**: 
分析问题的根本原因

**解决方案**:
给出具体的解决步骤或代码

**相关关键词**: `关键词1`, `关键词2`
```
