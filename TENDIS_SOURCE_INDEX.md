# Tendis 2.7.0 源码索引

**源码位置**: `/Users/chenguoxie/study_doc/迁移/redis&tendis/Tendis-2.7.0-rocksdb-v8.5.3`

**说明**: 本文档是 Tendis 源码的快速索引，用于在开发 tendis-migrate 工具时快速查找相关实现。

---

## 1. 目录结构概览

```
Tendis-2.7.0-rocksdb-v8.5.3/
├── src/tendisplus/              # 主要源码目录
│   ├── cluster/                 # 集群管理 (15 files)
│   ├── commands/                # Redis 命令实现 (25 files)
│   ├── lock/                    # 锁相关
│   ├── network/                 # 网络层
│   ├── replication/             # 复制相关 ⭐ 关键
│   ├── script/                  # Lua 脚本
│   ├── server/                  # 服务器核心
│   ├── storage/                 # 存储层（RocksDB封装）⭐ 关键
│   ├── tools/                   # 工具
│   └── utils/                   # 工具函数
├── tests/                       # Tcl 测试脚本
└── tendisplus.conf              # 默认配置文件
```

---

## 2. 核心文件索引

### 2.1 复制相关 (replication/)

| 文件 | 功能 | 关键类/函数 |
|------|------|-------------|
| `repl_manager.h` | 复制管理器头文件 | `ReplManager`, `SPovStatus`, `MPovStatus`, `ReplState` |
| `repl_manager.cpp` | 复制管理主逻辑 | `supplyFullSync`, `registerIncrSync`, `masterPushRoutine` |
| `mpov.cpp` | Master 视角的复制实现 | `supplyFullSyncRoutine`, `registerIncrSyncStatus` |
| `spov.cpp` | Slave 视角的复制实现 | `slaveStartFullsync`, `slaveChkSyncStatus` |
| `repl_util.cpp` | 复制工具函数 | `masterSendBinlogV2`, `masterSendAof`, `applySingleTxnV2` |
| `binlog_tool.cpp` | Binlog 工具 | Binlog 解析和处理 |

### 2.2 命令实现 (commands/)

| 文件 | 功能 | 关键命令 |
|------|------|----------|
| `repl.cpp` | 复制相关命令 | `INCRSYNC`, `FULLSYNC`, `APPLYBINLOGSV2`, `BINLOG_HEARTBEAT` |
| `scan.cpp` | SCAN 命令 | `SCAN`, `SSCAN`, `HSCAN`, `ZSCAN` |
| `kv.cpp` | 基本 KV 命令 | `GET`, `SET`, `DEL`, `MGET`, `MSET` |
| `debug.cpp` | 调试命令 | `BINLOGPOS`, `BINLOGSTART`, `BINLOGTIME` |
| `dump.cpp` | 导出命令 | `DUMP`, `RESTORE` |

### 2.3 存储层 (storage/)

| 文件 | 功能 | 关键内容 |
|------|------|----------|
| `kvstore.h` | KVStore 接口定义 | `KVStore`, `Cursor`, `BinlogCursor`, `BinlogVersion` |
| `kvstore.cpp` | KVStore 实现 | 存储操作核心逻辑 |
| `record.h/cpp` | 记录格式 | `RecordKey`, `RecordValue`, `ReplLogKeyV2`, `ReplLogValueV2` |
| `repllog.cpp` | 复制日志 | Binlog 格式和解析 |
| `catalog.h/cpp` | 元数据目录 | 版本信息、配置存储 |
| `rocks/rocks_kvstore.cpp` | RocksDB 实现 | RocksDB 适配层 |

### 2.4 服务器核心 (server/)

| 文件 | 功能 |
|------|------|
| `server_entry.cpp` | 服务器入口 |
| `session.cpp` | 会话管理 |
| `repl_test.cpp` | 复制相关测试 |

### 2.5 集群管理 (cluster/)

| 文件 | 功能 |
|------|------|
| `cluster_manager.cpp` | 集群管理器 |
| `migrate_manager.cpp` | 数据迁移管理 |
| `migrate_sender.cpp` | 迁移发送端 |

---

## 3. 关键协议和命令

### 3.1 增量同步协议 (INCRSYNC)

**命令格式**: `INCRSYNC storeId dstStoreId binlogPos ip port`

**源码位置**: `src/tendisplus/commands/repl.cpp` (IncrSyncCommand, 第 410-442 行)

**注册逻辑**: `src/tendisplus/replication/mpov.cpp` (registerIncrSync, 第 179-289 行)

**工作流程**:
1. Slave 发送 `INCRSYNC storeId dstStoreId binlogPos ip port` 注册
2. Master 返回 `+OK`
3. Slave 发送 `+PONG` 确认
4. Master 持续推送 `applybinlogsv2` 命令

### 3.2 Binlog 相关命令

| 命令 | 格式 | 说明 | 源码位置 |
|------|------|------|----------|
| `BINLOGPOS` | `binlogpos <storeId>` | 获取当前 binlog 位置 | `debug.cpp` |
| `BINLOGSTART` | `binlogstart <storeId>` | 获取最早 binlog 位置 | `debug.cpp` |
| `BINLOGTIME` | `binlogtime <storeId> <pos>` | 获取位置对应的时间戳 | `debug.cpp` |
| `APPLYBINLOGSV2` | `applybinlogsv2 storeId binlogs cnt flag` | 应用 binlog | `repl.cpp` |
| `BINLOG_HEARTBEAT` | `binlog_heartbeat storeId [binlogts]` | 心跳 | `repl.cpp` |

### 3.3 全量同步命令

| 命令 | 格式 | 说明 |
|------|------|------|
| `FULLSYNC` | `fullsync storeId slaveIp slavePort` | 全量同步请求 |
| `BACKUP` | `backup dir [mode]` | 备份数据 |
| `RESTOREBACKUP` | `restorebackup storeId dir [force]` | 恢复备份 |

---

## 4. 关键数据结构

### 4.1 ReplState (复制状态)

```cpp
enum class ReplState : std::uint8_t {
  REPL_NONE = 0,      // 无复制
  REPL_CONNECT = 1,   // 尝试连接
  REPL_TRANSFER = 2,  // 全量传输中
  REPL_CONNECTED = 3, // 增量同步中
  REPL_ERR = 4        // 错误
};
```

### 4.2 MPovStatus (Master 视角的 Slave 状态)

```cpp
struct MPovStatus {
  bool isRunning = false;
  uint32_t dstStoreId = 0;
  uint64_t binlogPos = 0;    // 已应用的最大 binlogId
  uint64_t binlogTs = 0;     // binlog 时间戳（毫秒）
  SCLOCK::time_point nextSchedTime;
  SCLOCK::time_point lastSendBinlogTime;
  std::shared_ptr<BlockingTcpClient> client;
  uint64_t clientId = 0;
  std::string slave_listen_ip;
  uint16_t slave_listen_port = 0;
  MPovClientType clientType;
};
```

### 4.3 BinlogVersion (Binlog 版本)

```cpp
enum class BinlogVersion : uint8_t {
  BINLOG_VERSION_1 = 1,  // 数据和 binlog 在同一 CF
  BINLOG_VERSION_2,      // 数据和 binlog 在不同 CF
};
```

---

## 5. 关键常量

```cpp
const uint32_t gBinlogHeartbeatSecs = 1;     // 心跳间隔（秒）
const uint32_t gBinlogHeartbeatTimeout = 10; // 心跳超时（秒）
#define CLUSTER_SLOTS 16384                   // 集群槽数量
```

---

## 6. 配置参数

关键配置项 (tendisplus.conf):

| 参数 | 默认值 | 说明 |
|------|--------|------|
| `kvstorecount` | 10 | 每个节点的 store 数量 |
| `binlog-enabled` | yes | 是否启用 binlog |
| `binlog-save-path` | binlog | binlog 保存路径 |
| `aof-enabled` | no | 是否启用 AOF |
| `psync-enabled` | no | 是否启用 PSYNC |

---

## 7. 常用查找命令

### 搜索特定命令实现
```bash
grep -rn "命令名" src/tendisplus/commands/
```

### 搜索复制相关代码
```bash
grep -rn "incrsync\|INCRSYNC\|binlogpos" src/tendisplus/
```

### 搜索 binlog 处理
```bash
grep -rn "applybinlog\|ReplLog" src/tendisplus/
```

---

## 8. 与 tendis-migrate 的关系

| tendis-migrate 功能 | Tendis 源码位置 |
|---------------------|-----------------|
| FakeSlave 增量同步 | `replication/mpov.cpp` - `registerIncrSync` |
| Binlog 解析 | `storage/record.cpp` - `ReplLogKeyV2`, `ReplLogValueV2` |
| 心跳机制 | `commands/repl.cpp` - `BinlogHeartbeatCommand` |
| Store 数量获取 | `CONFIG GET kvstorecount` |
| Binlog 位置获取 | `commands/debug.cpp` - `BinlogPosCommand` |

---

## 9. 更新日志

| 日期 | 更新内容 |
|------|----------|
| 2026-02-10 | 创建初始索引 |

