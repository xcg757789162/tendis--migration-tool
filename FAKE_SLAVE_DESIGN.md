# 伪 Slave 模式设计文档

## 概述

伪装成 Tendis 从节点（FakeSlave）接收 binlog 是最高效的增量同步方式，特别适用于 40 亿 Key 的大规模迁移场景。

## 设计原理

### Tendis 官方增量同步协议

基于官方 Tendis 2.7.0 源码分析（`src/tendisplus/replication/`）：

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                    Tendis 增量同步协议（INCRSYNC）                            │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  ┌──────────────────┐                        ┌──────────────────┐          │
│  │   迁移工具        │                        │   源 Tendis       │          │
│  │  (伪装成 Slave)   │                        │   (Master)        │          │
│  └────────┬─────────┘                        └────────┬─────────┘          │
│           │                                           │                     │
│           │  1. INCRSYNC storeId dstStoreId binlogPos ip port              │
│           │────────────────────────────────────────────>│                   │
│           │                                           │                     │
│           │  2. +OK (注册成功)                         │                     │
│           │<────────────────────────────────────────────│                   │
│           │                                           │                     │
│           │  3. +PONG (确认)                          │                     │
│           │────────────────────────────────────────────>│                   │
│           │                                           │                     │
│           │  4. applybinlogsv2 storeId binlogs cnt flag (持续推送)          │
│           │<────────────────────────────────────────────│                   │
│           │                                           │                     │
│           │  4a. binlog_heartbeat storeId timestamp (心跳)                  │
│           │<────────────────────────────────────────────│                   │
│           │                                           │                     │
└─────────────────────────────────────────────────────────────────────────────┘
```

### 关键协议细节

1. **INCRSYNC 命令格式**：
   ```
   INCRSYNC <storeId> <dstStoreId> <binlogPos> <myIp> <myPort>
   ```
   - `storeId`: 源端存储 ID（Tendis 集群中的 store 编号）
   - `dstStoreId`: 目标存储 ID（通常与 storeId 相同）
   - `binlogPos`: 已应用的最大 binlog ID（0 表示从头开始）
   - `myIp/myPort`: Slave 监听地址（可以是虚假值，Master 不会真正连接）

2. **握手流程**：
   - Slave 发送 `INCRSYNC`
   - Master 返回 `+OK`
   - Slave 发送 `+PONG`
   - 之后 Master 持续推送 binlog

3. **binlog 推送命令**：
   - `applybinlogsv2 storeId binlogs cnt flag` - 批量 binlog 数据
   - `binlog_heartbeat storeId timestamp` - 心跳保活

## 实现架构

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                         FakeSlave 模块架构                                   │
├─────────────────────────────────────────────────────────────────────────────┤
│                                                                             │
│  internal/replication/                                                       │
│  ├── fake_slave.go      # 伪 Slave 主逻辑                                   │
│  │   ├── FakeSlave 结构体                                                   │
│  │   ├── Start() - 启动连接和接收循环                                        │
│  │   ├── sendIncrSync() - 发送 INCRSYNC 注册                                │
│  │   ├── receiveLoop() - 接收和处理 binlog                                   │
│  │   └── handleApplyBinlogsV2() - 处理 applybinlogsv2 命令                  │
│  │                                                                          │
│  └── binlog_parser.go   # Binlog 解析器                                     │
│      ├── BinlogParser 结构体                                                │
│      ├── ParseBinlogs() - 解析 binlog 数据                                  │
│      ├── decodeReplLogKey() - 解析 binlog key                               │
│      └── decodeReplLogValue() - 解析 binlog value                           │
│                                                                             │
└─────────────────────────────────────────────────────────────────────────────┘
```

## 与其他模式对比

| 模式 | 适用场景 | 40亿 Key | 实时性 | 复杂度 |
|------|---------|----------|--------|--------|
| **FakeSlave** | Tendis | ✅ 推荐 | 毫秒级 | 高 |
| PSYNC | Redis/Tendis | ⚠️ 需测试 | 秒级 | 中 |
| IDLETIME | 通用 | ❌ 不推荐 | 分钟级 | 低 |

### FakeSlave 模式优势

1. **实时性高**：Master 主动推送 binlog，延迟毫秒级
2. **不需要 SCAN**：无需遍历全量 Key
3. **内存友好**：流式处理，不存储 Key 列表
4. **40 亿 Key 适用**：基于 binlog 增量，与 Key 数量无关

### FakeSlave 模式限制

1. **仅支持 Tendis**：依赖 INCRSYNC 协议
2. **需要 binlog 启用**：源端必须开启 binlog
3. **需要网络连接**：与源端保持持久 TCP 连接

## Binlog 格式

### ReplLogKeyV2（binlog key）
```
┌────────────────┐
│ binlogId (8B)  │  big-endian uint64
└────────────────┘
```

### ReplLogValueV2（binlog value header）
```
┌────────────────┬──────────┬────────────┬──────────────┬────────────┬────────────┬─────────┐
│ chunkId (4B)   │ flag (2B)│ txnId (8B) │ timestamp(8B)│ versionEp  │ cmdStrLen  │ cmdStr  │
│ little-endian  │ uint16   │ uint64     │ uint64(ms)   │ (8B)       │ (4B)       │ (var)   │
└────────────────┴──────────┴────────────┴──────────────┴────────────┴────────────┴─────────┘
│                                                                                           │
│                               后续是 ReplLogValueEntryV2 列表                               │
└───────────────────────────────────────────────────────────────────────────────────────────┘
```

### ReplLogValueEntryV2（binlog entry）
```
┌─────────┬──────────────┬────────────┬─────────┬─────────────┬───────────┐
│ op (1B) │ timestamp(8B)│ keyLen (4B)│ key     │ valueLen(4B)│ value     │
│ ReplOp  │ uint64 (ms)  │ uint32     │ (var)   │ uint32      │ (var)     │
└─────────┴──────────────┴────────────┴─────────┴─────────────┴───────────┘
```

## 使用方法

### 自动检测和使用

增量同步会自动检测并选择最优模式：

```go
// task_runner.go 中的逻辑
func (r *TaskRunner) runIncrementalSync() error {
    // 优先尝试 FakeSlave 模式
    if r.checkFakeSlaveSupport() {
        return r.runFakeSlaveIncrementalSync()
    }
    
    // 其次 PSYNC 模式
    if r.checkPsyncSupport() {
        return r.runPsyncIncrementalSync()
    }
    
    // 最后 IDLETIME 模式
    return r.runIdletimeIncrementalSync()
}
```

### 手动使用 FakeSlave

```go
import "tendis-migrate/internal/replication"

config := replication.FakeSlaveConfig{
    SourceAddr:     "10.248.37.11:8901",
    SourcePassword: "",
    StoreID:        0,
    StartBinlogPos: 0,
    KeyFilter: func(key string) bool {
        return strings.HasPrefix(key, "myprefix:")
    },
}

fakeSlave := replication.NewFakeSlave(config, targetClient)
if err := fakeSlave.Start(ctx); err != nil {
    log.Fatal(err)
}
```

## 参考源码

官方 Tendis 2.7.0 源码路径：
- `src/tendisplus/replication/spov.cpp` - Slave 端逻辑
- `src/tendisplus/replication/mpov.cpp` - Master 端逻辑
- `src/tendisplus/replication/repl_util.cpp` - 复制工具函数
- `src/tendisplus/storage/record.h` - binlog 格式定义
- `src/tendisplus/commands/repl.cpp` - 复制命令处理
