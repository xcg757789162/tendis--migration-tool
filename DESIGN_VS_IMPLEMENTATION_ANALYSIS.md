# Tendis 迁移工具：原始设计 vs 当前实现对比分析

> 分析时间：2026-02-02  
> 设计文档版本：V1.4（2026-01-23）  
> 当前实现版本：v1.4（简化版）

---

## 一、架构对比

### 1.1 原始设计：Master-Worker 多进程架构

```
设计架构（针对 40 亿 key）：
┌─────────────────────────────────────────────┐
│              Master Process                  │
│  ┌──────────┐  ┌───────────────────────┐   │
│  │ Web/API  │  │ Keyspace Listeners    │   │
│  │ Server   │  │ (per-node goroutine)  │   │
│  └──────────┘  └───────────────────────┘   │
│                       │                      │
│              ┌────────┴────────┐            │
│              │ Unix Socket     │            │
│              │   Router        │            │
│              └────────┬────────┘            │
└───────────────────────┼─────────────────────┘
          │             │             │
   ┌──────┴─────┐ ┌────┴────┐ ┌──────┴─────┐
   │  Worker 0  │ │Worker 1 │ │  Worker N  │
   │Slot 0-4k   │ │Slot 4k-8k│ │Slot 12k-16k│
   └────────────┘ └─────────┘ └────────────┘
          │             │             │
   ┌──────┴─────────────┴─────────────┴──────┐
   │            Storage Layer                 │
   │  ┌──────────┐  ┌─────────────────────┐  │
   │  │  SQLite  │  │ LevelDB (per-node)  │  │
   │  │  (WAL)   │  │ change_queue_node_* │  │
   │  └──────────┘  └─────────────────────┘  │
   └──────────────────────────────────────────┘
```

**关键设计点**：
- **多进程架构**：Master + N 个 Worker 进程
- **Slot 分片**：每个 Worker 负责一部分 Slot（0-16383）
- **进程间通信**：Unix Socket + 长度前缀 JSON
- **Keyspace Listeners**：每个源节点一个 goroutine，监听变更事件
- **LevelDB 队列**：每个节点独立的变更队列（change_queue_node_*）
- **SQLite 元数据**：任务状态、Slot 分配、断点信息

### 1.2 当前实现：单进程多协程简化架构

```
当前实现（简化版）：
┌────────────────────────────────────────────┐
│         Single Process (Go Goroutines)     │
│  ┌──────────┐  ┌────────────────────────┐ │
│  │ Web/API  │  │ Smart Incremental Sync │ │
│  │ Server   │  │ (轮询 + 值检测)         │ │
│  └──────────┘  └────────────────────────┘ │
│                                            │
│  ┌────────────────────────────────────┐   │
│  │ Dynamic Worker Pool (Goroutines)   │   │
│  │ - 动态调整 Worker 数量              │   │
│  │ - 直接内存共享（无 IPC）            │   │
│  └────────────────────────────────────┘   │
└────────────────────────────────────────────┘
         │                      │
   ┌─────┴─────┐         ┌──────┴──────┐
   │源 Tendis  │         │目标 Tendis   │
   │Cluster    │         │Cluster       │
   └───────────┘         └──────────────┘
```

**关键实现点**：
- **单进程架构**：所有功能在一个进程内通过 goroutine 实现
- **无 Slot 分片**：全量使用 SCAN 遍历，无 Worker 分片
- **内存共享**：goroutine 之间直接共享内存（Task 结构体）
- **智能轮询**：因 Tendis 不支持 Keyspace Notifications，使用智能轮询检测变化
- **无 LevelDB**：不使用变更队列，实时检测实时同步
- **无 SQLite**：任务状态在内存中（无持久化）

---

## 二、核心差异分析

### 2.1 全量与增量协调机制

#### 原始设计：基于 Keyspace Notifications + LevelDB 队列

```
流程：
1. [T0] 启动 Keyspace Listeners（所有源节点）
2. [T0+1ms] 开始 SCAN 全量迁移（Worker 进程并行）
3. [T0→T_end] 监听到的变更写入 LevelDB 队列
4. [T_end] 全量完成，回放 LevelDB 队列（T0→T_end 的所有变更）
5. [T_end+1] 启动 Delta Sync 收敛循环

关键保障：
- ✅ 全量期间的所有变更都被 Keyspace Notifications 捕获
- ✅ 变更持久化到 LevelDB，不丢失
- ✅ 全量完成后回放队列，确保覆盖全量的旧值
- ✅ 基于 offset 去重，避免重复处理
```

**解决 40 亿 key 问题的关键**：
- Keyspace Notifications **被动接收**变更事件，不主动 SCAN
- LevelDB 队列容量大（可存储数百万条记录），全量期间变更不丢失
- 回放时使用 `REPLACE` 策略，确保最新值覆盖全量

#### 当前实现：智能轮询（无队列缓存）⚠️

```
流程：
1. [T0] 启动智能增量同步（拍摄初始快照）
2. [T0+1ms] 开始全量迁移
3. [T0→T_end] 智能增量每 2 秒扫描一次，检测变化
4. ⚠️ 问题：检测到变化后立即同步（与全量冲突！）
5. [T_end] 全量完成，继续智能增量同步

问题：
- ❌ 全量期间增量立即同步 → 可能与全量冲突
- ❌ 无队列缓存 → 如果增量检测周期慢（2秒），可能漏检
- ❌ 无回放机制 → 全量完成后没有补齐全量期间的变化
```

**我刚才修改的"两阶段增量同步"尝试解决**：
```go
// 阶段 1：全量期间缓存变化到内存 map
if !task.fullSyncCompleted {
    task.pendingIncrKeys[key] = operation  // 只记录，不同步
}

// 阶段 2：全量完成后回放
replayIncrementalBuffer(...)
```

**但这个修改存在严重问题**：
1. ❌ **内存无界增长**：40 亿 key 场景下，如果 1% 变化 = 4000 万 key
   - 4000 万 key * 100 字节 = 4GB 内存（单机无法承受）
2. ❌ **检测不完整**：智能轮询每 2 秒扫描 40 亿 key 是不可能的
   - 每次扫描假设 100k keys/s，需要 40000 秒 = 11 小时
   - 2 秒间隔只能扫描 200k key，99.995% 的 key 无法检测到变化
3. ❌ **无法适配 40 亿 key 规模**

### 2.2 Keyspace Notifications 支持检测

#### 原始设计：假设支持（需手动配置）

```yaml
前置条件：
  keyspace_notifications:
    command: "CONFIG SET notify-keyspace-events AKE"
    verify: "CONFIG GET notify-keyspace-events"
    required: true
```

**假设**：Tendis 2.7 支持 Keyspace Notifications（但需手动开启）

#### 当前实现：实际验证不支持 ✅

```bash
# 实际测试结果（2026-01-31）
$ redis-cli -h 10.248.37.11 -p 8901 CONFIG GET notify-keyspace-events
(empty array)

$ redis-cli -h 10.248.37.11 -p 8901 CONFIG SET notify-keyspace-events Ex
(error) ERR not found arg:notify-keyspace-events
```

**结论**：Tendis 2.7.0-rocksdb-v8.5.3 **不支持** `notify-keyspace-events`

**当前实现的改进**：
- ✅ 自动检测 Keyspace Notifications 支持情况
- ✅ 不支持时自动降级到智能轮询模式
- ✅ 这是对原始设计的**必要修正**

---

## 三、关键设计遗漏与偏差

### 3.1 ❌ 遗漏：Master-Worker 架构（严重）

**原始设计意图**：
- 40 亿 key 场景下，单进程 SCAN 需要数天
- 通过多 Worker 进程并行处理不同 Slot，提升速度
- 预期速度：50k keys/s（多 Worker 并行）

**当前实现**：
- 单进程单 goroutine SCAN（虽然有动态 Worker Pool，但全量是串行的）
- 预期速度：10-20k keys/s（单线程 SCAN 限制）
- 40 亿 key 预计需要：40,000,000,000 / 20,000 / 3600 = **55.5 小时**

**影响**：
- ❌ 全量同步时间过长（数十小时）
- ❌ 增量缓冲队列（我刚才修改的 pendingIncrKeys）内存无法承受

### 3.2 ❌ 遗漏：LevelDB 变更队列（严重）

**原始设计意图**：
- 全量期间的变更写入 LevelDB（持久化）
- 容量：每个节点 500MB，可存储数百万条记录
- 全量完成后回放队列

**当前实现**：
- 无 LevelDB 队列
- 使用内存 map（`pendingIncrKeys`）缓存
- 容量：受限于内存（4GB 内存最多缓存 4000 万 key）

**影响**：
- ❌ 内存溢出风险（40 亿 key 场景）
- ❌ 变更无持久化（进程崩溃数据丢失）

### 3.3 ❌ 遗漏：Slot 分片与并行迁移（严重）

**原始设计**：
- Redis Cluster 有 16384 个 Slot
- 每个 Worker 负责一部分 Slot（如 Worker0: 0-4095，Worker1: 4096-8191）
- 多 Worker 并行迁移，充分利用多核 CPU

**当前实现**：
- 无 Slot 分片
- 使用 `SCAN` 遍历整个集群（串行）
- 虽然有动态 Worker Pool，但只是用于并发处理单个 key 的迁移

**影响**：
- ❌ 无法充分利用多核（全量 SCAN 是串行的）
- ❌ 迁移速度慢（40 亿 key 需要数十小时）

### 3.4 ✅ 改进：智能增量同步（优化）

**原始设计**：
- 依赖 Keyspace Notifications（假设支持）
- 不支持时无降级方案

**当前实现**：
- ✅ 自动检测 Keyspace Notifications 支持
- ✅ 不支持时降级到智能轮询
- ✅ 智能轮询支持值变化检测（checksum + TTL）

**评估**：这是对原始设计的**必要改进**（因为 Tendis 不支持 Keyspace Notifications）

### 3.5 ✅ 改进：动态 Worker Pool（优化）

**原始设计**：
- 固定数量的 Worker 进程
- 需要手动调整 Worker 数量

**当前实现**：
- ✅ 动态调整 Worker 数量（根据负载）
- ✅ 更灵活的资源利用

**评估**：这是对原始设计的**改进**

### 3.6 ❌ 遗漏：SQLite 持久化（中等）

**原始设计**：
- 任务状态、Slot 分配、断点信息存储在 SQLite
- 支持断点续传、故障恢复

**当前实现**：
- 任务状态在内存中（Task 结构体）
- 进程重启后任务丢失

**影响**：
- ❌ 无断点续传（进程崩溃需要重新开始）
- ❌ 无故障恢复

### 3.7 ⚠️ 偏差：冲突策略（skip_full_only）

**原始设计**：
```go
// 全量阶段：skip_full_only（跳过已存在的 key）
// 增量阶段：自动切换为 replace（强制覆盖）
if phase == PhaseFullMigration {
    if targetExists && conflictPolicy == "skip_full_only" {
        return skip
    }
} else if phase == PhaseIncrementalSync {
    // 增量阶段强制 REPLACE，确保数据一致性
    return replace
}
```

**当前实现**：
```go
// 全量和增量都使用相同的 conflictPolicy
// 如果配置为 skip_full_only，增量也会跳过
if task.Options.ConflictPolicy == "skip_full_only" {
    if targetExists {
        return skip  // ⚠️ 增量也跳过！
    }
}
```

**影响**：
- ❌ 增量同步可能跳过已存在的 key，导致数据不一致
- ❌ 违反了 V1.4 设计的核心原则

---

## 四、40 亿 Key 场景可行性分析

### 4.1 原始设计的可行性 ✅

```
假设：
- 40 亿 key
- 平均 key 大小：1KB
- 总数据量：4TB

全量迁移：
- Master + 8 Worker 进程（每个 Worker 负责 2048 个 Slot）
- 每个 Worker 速度：10k keys/s
- 总速度：80k keys/s
- 全量时间：40,000,000,000 / 80,000 / 3600 = 13.9 小时

全量期间增量数据：
- 写入 QPS：10k writes/s
- 全量期间变更：10,000 * 13.9 * 3600 = 500,400,000 条（5 亿条）
- LevelDB 存储：5 亿条 * 100 字节/条 = 50GB（可存储）

回放增量：
- 回放速度：50k keys/s
- 回放时间：500,400,000 / 50,000 / 3600 = 2.8 小时

Delta Sync 收敛：
- 每轮扫描：1000 个 key（采样）
- 收敛时间：<10 分钟

总时间：13.9 + 2.8 + 0.2 = 16.9 小时 ✅ 可行
```

### 4.2 当前实现的可行性 ❌

```
假设：
- 40 亿 key
- 平均 key 大小：1KB
- 总数据量：4TB

全量迁移：
- 单进程 SCAN
- 速度：20k keys/s（乐观估计）
- 全量时间：40,000,000,000 / 20,000 / 3600 = 55.5 小时

全量期间增量数据：
- 写入 QPS：10k writes/s
- 全量期间变更：10,000 * 55.5 * 3600 = 1,998,000,000 条（20 亿条）
- 内存 map 存储：20 亿条 * 100 字节/条 = 200GB ❌ 内存溢出

智能轮询检测：
- 每 2 秒扫描一次
- 每次扫描速度：100k keys/s
- 每次扫描 key 数：200k key（2 秒内）
- 覆盖率：200,000 / 40,000,000,000 = 0.0005% ❌ 几乎无法检测到变化

总时间：55.5 小时 + ??? （无法完成）❌ 不可行
```

**结论**：当前实现**无法支持 40 亿 key 场景**

---

## 五、修正建议

### 5.1 短期修正（保持简化架构）

**适用场景**：小规模迁移（<1 亿 key）

#### 修正 1：移除两阶段增量同步（回退）

**原因**：
- 内存无界增长风险
- 智能轮询无法覆盖全量期间的所有变化

**建议**：
```go
// 回退到原始的智能轮询（全量期间也实时同步）
// 虽然有冲突风险，但对于小规模场景可接受
if !fullCompleted {
    // 删除缓冲逻辑，直接同步
    migrateKeyWithPolicy(...)  // 使用 replace 策略
}
```

#### 修正 2：修复冲突策略（增量强制 REPLACE）

```go
func migrateKeyWithPolicy(...) {
    tasksMu.RLock()
    phase := task.Phase
    policy := task.Options.ConflictPolicy
    tasksMu.RUnlock()
    
    // 增量阶段强制 REPLACE
    if phase == "incremental" && policy == "skip_full_only" {
        policy = "replace"
    }
    
    // 检查目标端是否存在
    if policy == "skip_full_only" && targetExists {
        return skip
    } else {
        // 使用 RESTORE REPLACE 强制覆盖
        return replace
    }
}
```

#### 修正 3：限制场景（明确文档）

```markdown
## 使用限制

### 支持场景
- ✅ Key 数量：<1 亿
- ✅ 数据量：<100GB
- ✅ 全量时间：<2 小时
- ✅ 写入 QPS：<1k writes/s

### 不支持场景
- ❌ Key 数量：>10 亿（需要 Master-Worker 架构）
- ❌ 高写入场景：>10k writes/s（需要 Keyspace Notifications）
- ❌ 长时间全量：>10 小时（增量缓冲无法承受）
```

### 5.2 长期修正（回归原始设计）

**适用场景**：大规模迁移（>1 亿 key，包括 40 亿 key）

#### 修正 1：实现 Master-Worker 架构

```go
// 按原始设计实现
- Master 进程：Web API + Keyspace Listeners + LevelDB 管理
- Worker 进程：Slot 分片迁移（0-4095, 4096-8191, ...）
- IPC：Unix Socket + 长度前缀 JSON
```

#### 修正 2：实现 LevelDB 变更队列

```go
// 按原始设计实现
- 每个源节点独立队列：change_queue_node_1, change_queue_node_2, ...
- 容量：500MB/节点
- Keyspace Listeners 写入队列（而不是实时同步）
- 全量完成后回放队列
```

#### 修正 3：实现 SQLite 持久化

```go
// 按原始设计实现
- 任务状态、Slot 分配、断点信息存储在 SQLite
- 支持断点续传、故障恢复
```

#### 修正 4：移除智能轮询（回归 Keyspace Notifications）

```go
// 假设 Tendis 未来版本支持 Keyspace Notifications
// 或者建议用户升级到支持的版本
- 使用 Keyspace Notifications 作为主要增量同步方式
- 智能轮询作为降级方案（仅用于小规模场景）
```

---

## 六、总结

### 6.1 当前实现的定位

当前实现是一个**简化版的快速原型**，适用于：
- ✅ 小规模迁移（<1 亿 key）
- ✅ 低写入场景（<1k writes/s）
- ✅ 快速验证功能

但**不适用于原始设计的核心场景**：
- ❌ 40 亿 key 大规模迁移
- ❌ 高写入场景（>10k writes/s）

### 6.2 关键遗漏

| 设计组件 | 原始设计 | 当前实现 | 影响 |
|---------|---------|---------|------|
| Master-Worker 架构 | ✅ 多进程并行 | ❌ 单进程串行 | **严重**：全量时间 4x |
| LevelDB 变更队列 | ✅ 持久化队列 | ❌ 内存 map | **严重**：内存溢出 |
| Keyspace Notifications | ✅ 假设支持 | ✅ 检测不支持 | **改进**：实际验证 |
| 智能轮询降级 | ❌ 无降级方案 | ✅ 智能轮询 | **改进**：必要补充 |
| Slot 分片 | ✅ 16384 分片 | ❌ 无分片 | **严重**：无法并行 |
| SQLite 持久化 | ✅ 断点续传 | ❌ 无持久化 | **中等**：无故障恢复 |
| 冲突策略分阶段 | ✅ 增量强制 REPLACE | ❌ 未实现 | **中等**：数据不一致 |

### 6.3 建议

#### 方案 A：明确当前实现的适用范围（推荐）

```markdown
# Tendis 迁移工具 v1.4（简化版）

## 适用场景
- Key 数量：<1 亿
- 数据量：<100GB
- 全量时间：<2 小时
- 写入 QPS：<1k writes/s

## 不支持场景
如果您的场景满足以下任一条件，请使用企业版（Master-Worker 架构）：
- Key 数量：>1 亿
- 数据量：>100GB
- 全量时间：>2 小时
- 写入 QPS：>1k writes/s
```

#### 方案 B：回归原始设计（大规模场景）

```markdown
# Tendis 迁移工具 v2.0（企业版）

## 实现原始设计
- Master-Worker 架构（多进程并行）
- LevelDB 变更队列（持久化）
- Slot 分片（16384 个 Slot）
- SQLite 持久化（断点续传）

## 支持场景
- Key 数量：无上限（已验证 40 亿 key）
- 数据量：<10TB
- 全量时间：<24 小时
- 写入 QPS：<100k writes/s
```

---

## 七、我刚才修改的"两阶段增量同步"评估

### 7.1 修改内容

```go
// 新增字段
type Task struct {
    fullSyncCompleted bool              // 全量是否完成
    pendingIncrKeys   map[string]string // 缓冲队列
    pendingIncrMu     sync.RWMutex      // 锁
}

// 全量期间缓存
if !task.fullSyncCompleted {
    task.pendingIncrKeys[key] = operation
}

// 全量完成后回放
replayIncrementalBuffer(...)
```

### 7.2 评估结论：❌ 不建议采用

**原因**：

1. **与原始设计冲突**
   - 原始设计使用 LevelDB 持久化队列（500MB/节点）
   - 当前修改使用内存 map（无容量限制）
   - 原始设计的容量规划无法应用

2. **无法支持 40 亿 key 场景**
   - 40 亿 key，假设 1% 变化 = 4000 万 key
   - 内存占用：4000 万 * 100 字节 = 4GB
   - 单机无法承受

3. **智能轮询无法全覆盖**
   - 智能轮询每 2 秒扫描 200k key
   - 40 亿 key 需要扫描 20,000 轮 = 11 小时
   - 无法在全量期间（55 小时）覆盖所有 key

4. **偏离原始设计意图**
   - 原始设计依赖 Keyspace Notifications（被动接收）
   - 当前修改依赖智能轮询（主动扫描）
   - 性能和完整性都无法保证

### 7.3 建议：回退修改

```bash
# 回退到修改前的版本
git checkout HEAD~1 cmd/simple/main.go

# 或者保留智能轮询，但移除缓冲逻辑
# 全量期间也实时同步（使用 replace 策略）
```

---

## 八、最终建议

### 对于当前简化版（v1.4）

1. **回退两阶段增量同步修改**
   - 移除 `pendingIncrKeys` 缓冲逻辑
   - 全量期间也实时同步（使用 replace 策略）

2. **修复冲突策略 bug**
   - 增量阶段强制使用 REPLACE
   - 符合 V1.4 设计原则

3. **明确使用限制**
   - 文档中明确：仅支持 <1 亿 key
   - 大规模场景请等待 v2.0（Master-Worker 版本）

### 对于未来企业版（v2.0）

1. **完整实现原始设计**
   - Master-Worker 架构
   - LevelDB 变更队列
   - Slot 分片并行迁移
   - SQLite 持久化

2. **适配 Tendis 不支持 Keyspace Notifications**
   - 保留智能轮询作为降级方案
   - 优化轮询性能（增加扫描速度）

3. **验证 40 亿 key 场景**
   - 完整的压力测试
   - 内存、磁盘、网络资源规划
