# 增量同步设计：解决全量期间增量数据丢失问题

## 问题背景

在大规模数据迁移场景（如 40 亿 key）中，全量同步可能需要**数小时甚至数天**。如果增量同步在全量期间立即同步变化的 key，会导致以下问题：

### 问题 1：数据冲突
- **问题**：全量和增量可能同时处理同一个 key
- **后果**：数据覆盖冲突，可能导致数据不一致

### 问题 2：增量覆盖未迁移的全量数据
- **问题**：增量检测到 key 的变化并同步，但全量还没有迁移该 key
- **后果**：如果冲突策略是 `skip`，增量会写入目标端，但全量后续会跳过（因为目标端已存在）
- **结果**：目标端的数据可能是中间版本，而不是最新版本

### 问题 3：增量数据丢失
- **问题**：如果增量同步在全量完成后才启动，全量期间的变化会丢失
- **后果**：数据不一致

## 解决方案：两阶段增量同步

### 核心思想

```
阶段 1（全量同步期间）：缓冲模式
- 增量同步检测到变化的 key
- 不立即同步，而是记录到缓冲队列 (pendingIncrKeys)
- 避免与全量同步冲突

阶段 2（全量完成后）：回放+实时同步
- 回放缓冲队列中的所有变化
- 使用 replace 策略确保覆盖全量数据
- 然后启动实时增量同步
```

### 时间线图示

```
时间线：
|←————— 全量同步（数小时）—————→|←———— 增量同步 ————→|

阶段 1: 全量期间
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
全量：SCAN → key1, key2, key3...
增量：检测到 key100 变化 → 记录到缓冲队列
增量：检测到 key200 新增 → 记录到缓冲队列
增量：检测到 key1 变化 → 记录到缓冲队列 ⚠️ key1 全量还没迁移

阶段 2: 全量完成后
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
回放：key100 → 同步最新值（覆盖全量）
回放：key200 → 同步最新值
回放：key1 → 同步最新值（覆盖全量刚迁移的旧值）✅ 正确！

阶段 3: 实时增量
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
增量：检测到 key300 变化 → 立即同步
增量：检测到 key400 新增 → 立即同步
```

## 实现细节

### 1. Task 结构体增强

```go
type Task struct {
    // ... 原有字段 ...
    
    // 增量同步控制
    fullSyncCompleted bool              `json:"-"` // 全量同步是否完成
    pendingIncrKeys   map[string]string `json:"-"` // 缓冲队列：key -> operation
    pendingIncrMu     sync.RWMutex      `json:"-"` // 保护缓冲队列的锁
}
```

### 2. 智能增量同步修改

#### 检测新 key 时
```go
if !task.fullSyncCompleted {
    // 全量期间：只记录到缓冲队列
    task.pendingIncrKeys[key] = "new"
    taskLog.Debug("New key buffered")
} else {
    // 全量完成后：立即同步
    migrateKeyWithPolicy(...)
    taskLog.Info("New key synced")
}
```

#### 检测值变化时
```go
if currentChecksum != oldChecksum {
    if !task.fullSyncCompleted {
        // 全量期间：只记录到缓冲队列
        task.pendingIncrKeys[key] = "updated"
        taskLog.Debug("Key update buffered")
    } else {
        // 全量完成后：立即同步
        migrateKeyWithPolicy(...)
        taskLog.Debug("Key updated")
    }
}
```

### 3. 缓冲队列回放

在全量完成后、实时增量开始前，回放缓冲队列：

```go
func replayIncrementalBuffer(ctx, task, sourceClient, targetClient, taskLog) {
    for key, operation := range task.pendingIncrKeys {
        // 检查 key 是否仍存在（可能已被删除）
        if sourceClient.Exists(key) == 0 {
            continue
        }
        
        // 同步（使用 replace 策略确保覆盖全量）
        migrateKeyWithPolicy(ctx, sourceClient, targetClient, key, "replace")
    }
    
    // 清空缓冲队列
    task.pendingIncrKeys = make(map[string]string)
}
```

### 4. 主流程控制

```go
func runTask() {
    // 1. 全量同步
    doFullMigration(...)
    
    // 2. 标记全量完成
    task.fullSyncCompleted = true
    
    // 3. 回放缓冲队列
    replayIncrementalBuffer(...)
    
    // 4. 启动实时增量同步
    doIncrementalSync(...)
}
```

## 关键优势

### ✅ 数据一致性保证
- 全量期间不会与增量冲突
- 回放时使用 `replace` 策略确保最新数据覆盖全量
- 保证目标端数据是最新的

### ✅ 零数据丢失
- 全量期间的所有变化都被缓存
- 缓冲队列在全量完成后立即回放
- 回放后立即启动实时增量同步

### ✅ 内存效率
- 缓冲队列只记录 key 名称（string）和操作类型（string）
- 不缓存 key 的值（value）
- 40 亿 key，假设平均 key 长度 50 字节：
  - 最坏情况（所有 key 都变化）：40亿 * 100字节 = 400GB（不现实）
  - 实际情况（1% 变化）：4000万 * 100字节 = 4GB（可接受）
  - 典型场景（0.1% 变化）：400万 * 100字节 = 400MB（很小）

### ✅ 性能优化
- 全量期间增量只做检测和记录（极快）
- 回放时批量处理，一次性清空缓冲队列
- 实时增量没有额外开销

## 风险与缓解

### 风险 1：缓冲队列过大导致内存溢出

**场景**：全量同步时间过长（如数天），且变化率很高（如 10% key 变化）

**缓解方案**：
1. **监控缓冲队列大小**：添加日志记录 `len(pendingIncrKeys)`
2. **设置阈值告警**：当缓冲队列超过 1000 万 key 时告警
3. **分批回放**：回放时分批处理，避免一次性加载所有 key

### 风险 2：回放时 key 已被删除

**场景**：全量期间检测到 key 变化并缓存，但回放时该 key 已被删除

**缓解方案**：
- 回放前检查 key 是否存在：`sourceClient.Exists(key)`
- 如果不存在，跳过该 key（记录到 `skipped` 计数）

### 风险 3：回放时间过长

**场景**：缓冲队列有 1000 万 key，回放需要很长时间

**缓解方案**：
1. **并行回放**：使用 Worker Pool 并行处理缓冲队列
2. **进度监控**：记录回放进度，显示在 UI 上
3. **增量回放**：边回放边启动实时增量（更复杂，暂不实现）

## 监控与日志

### 关键日志

```
[INFO] Smart incremental sync initialized (mode: two-phase, known_keys: 40000000000)
[DEBUG] New key buffered (full sync in progress) key=testkey:001
[DEBUG] Key update buffered (full sync in progress) key=testkey:002
[INFO] Full sync completed, preparing incremental sync
[INFO] Replaying buffered incremental changes (buffered_keys: 4000000)
[DEBUG] Buffered key replayed (key=testkey:001, operation=new, bytes=1024)
[INFO] Incremental buffer replay completed (replayed: 3999000, skipped: 1000, failed: 0)
[INFO] Starting real-time incremental sync
```

### 监控指标

- `pending_incr_keys_count`：缓冲队列大小（实时）
- `replay_duration_seconds`：回放耗时
- `replay_keys_total`：回放总数
- `replay_keys_skipped`：回放跳过数（key 已删除）
- `replay_keys_failed`：回放失败数

## 测试验证

### 测试场景 1：基本功能
1. 启动全量+增量任务
2. 全量期间写入 1000 个新 key
3. 全量期间修改 1000 个已有 key
4. 验证回放后目标端数据正确

### 测试场景 2：key 删除
1. 全量期间写入 key1
2. 全量完成前删除 key1
3. 验证回放时跳过 key1

### 测试场景 3：大规模缓冲
1. 全量期间持续写入 100 万 key
2. 验证内存使用在可接受范围
3. 验证回放成功

### 测试场景 4：冲突场景
1. 全量正在迁移 key1（旧值）
2. 增量检测到 key1 变化（新值）并缓存
3. 全量完成后回放 key1（新值覆盖旧值）
4. 验证目标端 key1 是最新值

## 总结

通过**两阶段增量同步**机制，我们成功解决了大规模数据迁移中的增量数据丢失问题：

1. ✅ **数据一致性**：全量和增量不冲突
2. ✅ **零数据丢失**：全量期间变化全部缓存
3. ✅ **内存高效**：只缓存 key 名称，不缓存值
4. ✅ **性能优化**：全量期间只检测不同步
5. ✅ **可监控**：完善的日志和指标

这个方案适用于**任何大规模数据迁移场景**，特别是当全量同步时间远大于增量检测周期时。
