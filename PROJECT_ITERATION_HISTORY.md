# Tendis-Migrate 项目迭代演化过程

> 本文档从技术实现视角记录项目的完整迭代历程，包括架构演进、BUG 修复、性能优化等。
> 整合自：ITERATION_HISTORY.md、COMPLETE_EVOLUTION_HISTORY.md、IMPLEMENTATION_CHANGELOG_P0_P3.md

---

## 一、项目概述

Tendis-Migrate 是一个基于 Go + Vue3 + ElementPlus 的 Tendis/Redis 集群数据迁移管理工具，支持全量迁移、增量同步、断点续传、数据校验等功能，设计目标是支持 40 亿+ Key 的大规模数据迁移。

---

## 二、版本迭代时间线

### V1.0 - 初始版本（2026-01-23）

**Git Commit**: `906086b`

**核心功能**：
- ✅ 单任务全量迁移（支持 Cluster 模式，DUMP/RESTORE）
- ✅ Key 过滤器（prefix/pattern/keys 三种模式）
- ✅ 冲突策略（skip/replace/error/skip_full_only）
- ✅ Web UI（Vue3 前端，实时监控）
- ✅ REST API（完整任务管理接口）
- ✅ 智能配置推荐
- ✅ 并行迁移（Worker Pool）

**支持规模**：~1 亿 Key

**架构**：
```
┌─────────────────────────────────────────────────────────┐
│                    tendis-migrate v1.0                   │
├─────────────────────────────────────────────────────────┤
│  HTTP API (Gin) │ 任务调度器 │ Web UI (Vue 3)          │
│                 ↓                                       │
│  Worker Pool (goroutine): W1 W2 W3 W4 W5 W6            │
│         ↓                                               │
│  源 Redis/Tendis  ──DUMP/RESTORE──>  目标 Redis/Tendis  │
└─────────────────────────────────────────────────────────┘
```

---

### V1.1 - 动态配置调整（2026-01-25）

**Git Commit**: `69c27c2`

**新增功能**：
- ✅ 动态 Worker 调整：运行时增减并发数量
- ✅ 动态 QPS 调整：实时调整限速
- ✅ 动态批次大小：运行时调整 SCAN 批次
- ✅ 智能 Worker 管理：Worker 减少时优雅停止

**API 新增**：`PUT /api/v1/tasks/{id}/config`

---

### V2.0 Phase 1 - 基础架构（2026-01-28）

**Git Commit**: `b8d4481`

**架构升级**：
- ✅ Master-Worker 多进程架构：支持 8+ Worker 并行
- ✅ IPC 通信框架：Unix Socket + 长度前缀 JSON
- ✅ SQLite 元数据层：5 张表
- ✅ LevelDB 变更队列：每个源节点独立队列

**新增模块**：
```
internal/ipc/       - IPC 通信
internal/storage/   - 存储层（sqlite.go, leveldb.go）
internal/master/    - Master 模块骨架
internal/worker/    - Worker 模块骨架
```

---

### V2.0 Phase 2 - Slot 分片迁移（2026-01-29）

**Git Commit**: `74c68ca`

**核心实现**：
- ✅ 16384 Slot 并行处理：每个 Slot 独立迁移
- ✅ Slot 分配管理器：动态分配算法
- ✅ Worker 进程池管理：os/exec fork Worker 进程
- ✅ Slot 迁移器：CRC16 Hash Slot 计算
- ✅ Slot 级别断点恢复

---

### V2.0 Phase 3-5 - 增量同步与性能优化（2026-01-30）

**Git Commit**: `8c9302d`

**Phase 3 - 增量同步**：
- ✅ Keyspace Notifications 监听
- ✅ LevelDB 队列消费
- ✅ 收敛检测器（30s 稳定窗口）

**Phase 4 - 性能优化**：
- ✅ Pipeline 批量迁移（100 key/batch）
- ✅ 批量 DUMP/RESTORE
- ✅ LevelDB WriteBatch 优化

**Phase 5 - 测试文档**：
- ✅ 端到端测试脚本、使用指南、实施计划

---

### P0-P3 核心改进（2026-02-02）

针对评审反馈和大规模测试暴露的问题进行核心改进。

#### P0：ErrorKeys 上限提升 + 落盘机制

| 改进前 | 改进后 |
|-------|-------|
| 上限 1 万 | 上限 100 万 |
| 纯内存 | 10 万内存 + 自动落盘 |

```go
type ErrorKeysFileTracker struct {
    TaskID        string   `json:"task_id"`
    FileCount     int      `json:"file_count"`
    TotalInFiles  int64    `json:"total_in_files"`
    Files         []string `json:"files"`
    LastFlushTime string   `json:"last_flush_time"`
}
```

落盘目录：
```
./data/error-keys/
├── {taskID}_tracker.json
├── {taskID}_batch_1234.json
└── ...
```

#### P1：时间窗口增量同步（解决 40 亿 Key OOM）

| 指标 | 改进前 | 改进后 | 提升 |
|-----|-------|-------|-----|
| 内存占用 | 456 GB (OOM) | < 100 MB | **99.98%** |
| 支持 40 亿 Key | ❌ | ✅ | - |
| 断点续传 | ❌ | ✅ | - |

核心改进：从"存储全量 Key 对比"改为"OBJECT IDLETIME 时间窗口检测"。

```go
// 改进前：存储全量 Key（OOM）
knownKeys := scanAllKeys()  // 40亿 Key = 456 GB

// 改进后：流式时间窗口，不存储 Key
scanWithCallback(func(key string) {
    idleTime := redis.ObjectIdleTime(key)
    if idleTime < 30*time.Second {
        migrateKey(key)
    }
})
```

#### P2：Pipeline 批量优化 + 详细进度指标

- Pipeline 批量 DUMP/RESTORE
- `detailed_progress` API 字段（含内存、轮次、延迟等）

#### P3：Tendis Binlog 支持（可选）

- `CheckTendisBinlogSupport()` 检测
- `ReadBinlog()` 读取
- 优先级回退：Binlog → 时间窗口 V2

---

### 崩溃恢复完善（2026-02-02）

| 功能 | 实现方式 |
|------|----------|
| 全量 SCAN cursor 持久化 | 每 10000 Key 或 30 秒保存 |
| 增量断点 V2 | 每 30 秒保存 |
| 任务状态持久化 | 每 30 秒保存 |
| 优雅关闭 | SIGINT/SIGTERM 处理 |
| 错误 Key 落盘 | 10 万条自动落盘 |

---

### 前后端功能完善（2026-02-04）

**第一轮分析**：后端 43 API，前端已调用 37 (86%)，界面已展示 34 (79%)

**新增后端 Handler**：stopIncremental、completeTask、taskMetrics、systemWorkers

**新增前端功能**：
- Key 清单上传（TXT/CSV/JSON）
- 配置导入/导出
- 自动恢复设置
- 日志导出

**最终覆盖率**：45 API / 45 调用 / 45 展示 = **100%**

---

### Binlog 解析深度分析与 Bug 修复（2026-02-05）

通过深入分析 Tendis 源码，发现并修复 3 个隐藏 Bug：

**Bug 1：RESP 解析使用 strings.Split 导致二进制数据出错**

```go
// ❌ 原代码
parts := strings.Split(cmdStr, "\r\n")  // value含\r\n时错误分割

// ✅ 修复：基于长度读取
argLen, _ := strconv.Atoi(string(data[offset:lenEnd]))
args = append(args, string(data[offset+2:offset+2+argLen]))
```

**Bug 2：ReplOp 枚举值与 Tendis 源码不一致**

```go
// ❌ iota 生成值与 Tendis record.h 不符
ReplOpGenericCmd  // 3  ← Tendis 里是 STMT

// ✅ 显式声明值
ReplOpStmt ReplOp = 3  // REPL_OP_STMT
ReplOpSpec ReplOp = 4  // REPL_OP_SPEC
```

**Bug 3：未区分 RESP 格式和非 RESP 格式的 cmdStr**

```go
// ✅ 修复：检查首字节判断格式
if binlog.CmdStr != "" && binlog.CmdStr[0] == '*' {
    // RESP 格式，可以执行
} else if binlog.CmdStr != "" {
    log.Printf("Warning: cmdStr is not RESP format")
}
```

---

### FakeSlave 增量同步完善（2026-02-05）

- 心跳间隔从 10 秒调整为 5 秒
- 实现自动重连机制
- 完善多 store 支持
- 增量同步详细指标展示

---

### 性能优化 - 学习 Redis-Shake（2026-02-05）

| 模块 | 功能 | 性能提升 |
|------|------|----------|
| ConcurrentWriter | 并发 Pipeline 写入 | 写入 QPS 10k→40k+ (4x) |
| BigKeySyncer | 大 Key 分批同步 | 避免超时 |
| AsyncCommandExecutor | 异步命令执行 | 延迟 <50ms |

---

### v2.3.0 - BUG 修复版（2026-02-09）

1. **key_filter.prefixes 不生效**：`NormalizeKeyFilter()` 自动设置 mode
2. **RocksDB Key 解析不正确**：修复 `extractRedisKey()` varint 长度前缀处理
3. **增量失败 Key 记录不全**：所有失败路径调用 `addErrorKeyWithDetails()`

**增量 Key 过滤验证通过**：testkey:* 同步 ✅，otherkey:* 过滤 ✅（String/Hash/List 全部正确）

---

### v2.3.1 - 大规模 BUG 修复版（2026-02-10 ~ 2026-02-11）

在 50 亿 Key 测试环境中发现并修复 8 个 BUG：

| BUG | 描述 | 严重程度 | 根因 |
|-----|------|----------|------|
| 1 | WebSocket 指标字段缺失 | 中 | sendTaskMetrics 遗漏字段 |
| 2 | full_only 模式显示增量面板 | 低 | 前端未排除 full_only |
| 3 | 校验结果全 0 | 低 | 缺少 loading 状态 |
| 4 | 运行时参数修改不生效 | 高 | 未调用 workerPool.SetWorkerCount() |
| 5 | 增量阶段 Key Filter 不生效 | 高 | processBinlogEntries 无 Filter 检查 |
| 6 | 纯增量模式 FakeSlave 不启动/立即停止 | 高 | needIncremental 条件不全 |
| 7 | 集群拓扑缓存 `:0` 地址 | 致命 | go-redis 缓存旧拓扑 |
| 8 | DBSIZE 超时回退为 10000 | 中 | 超时太短 + fallback 不合理 |

---

### v2.4.0 - 新功能版（2026-02-11 ~ 2026-02-12）

**Git Commit**: `0b7df2a` | **文件变更**: 37 files, +3822/-734

| 功能 | 说明 |
|------|------|
| Preflight Check | 迁移前校验（连通性/集群/Binlog/版本） |
| 集群拓扑自动刷新 | 30 秒周期刷新 + 启动时验证 |
| FakeSlave IP 探测 | getOutboundIP 自动探测出口 IP |
| Error Keys 查询 | GET /api/v1/tasks/:id/error-keys |

**Web UI 升级**：

| 页面 | 改进 |
|------|------|
| TaskDetail.vue | 拓扑告警 + Preflight 面板 + 进度展示（+924 行）|
| VerifyTasks.vue | 校验结果展示升级（+478 行）|
| CreateTask.vue | 表单校验优化（+337 行）|
| Tasks.vue | 状态展示改进（+40 行）|

**核心原则确立**：
> "环境是环境，工具是工具。绝对不要修改工具代码去适配特定部署环境。"

---

### v2.5.0 - 限速修复与崩溃恢复（2026-02-16 ~ 2026-02-28）

**Git Commit**: `e345a73` | **文件变更**: 112 files, +12519/-33560

#### 核心修复：限速系统三连修

| BUG | 现象 | 根因 | 修复方案 |
|-----|------|------|----------|
| 限速不生效 | QPS=500 实际 33000/s | `WaitN` 每批只消耗 1 令牌 | `WaitN(len(keys))` 按实际 key 数消耗 |
| 动态调速卡死 | 改 QPS 后迁移停止 | 旧限速器 Stop 后 channel 永久阻塞 | `select` 监听 tokens + stopChan |
| 多 Worker QPS 退化 | W=8 时 526→105/s | 自制 token-channel 串行争抢 | 替换为 `golang.org/x/time/rate.Limiter` |

**验证结果**：

| 场景 | 修复前 | 修复后 |
|------|--------|--------|
| QPS=500, W=2 | 33000/s（不生效） | ~370/s ✅ |
| QPS=500, W=2→8 | 105/s（退化） | 421-526/s ✅ |
| 运行中 500→100→0 QPS | 卡死 | 平滑切换 ✅ |

#### TTL 一致性修复

**问题**：迁移后目标端 Key 的 TTL 变成 -1（永不过期）

**修复**：
1. 增量阶段添加 `case "TTL":` — 从源端获取 PTTL 并 PExpire 到目标端
2. 增量阶段添加 `case "TTLDEL":` — 在目标端执行 Persist
3. 全量迁移 `TTL` → `PTTL`（毫秒精度）
4. `Expire` 统一替换为 `PExpire`

#### 崩溃恢复零丢失机制

实现 `getSafeCheckpointCursor` 安全 Cursor 回退：

```
正常运行:  keyChan: [key_60501..key_62000]  (约 1500 个未消费)
保存断点:  safe_cursor = 60501 (回退到覆盖所有未消费 key 的位置)
SIGKILL:   进程被杀
恢复:      从 safe_cursor=60501 继续 SCAN
结果:      源端 100,000 = 目标端 100,000, 零丢失 ✅
```

#### 生产故障修复（50 亿 Key 场景）

| 故障 | 严重度 | 问题 | 修复 |
|------|--------|------|------|
| P0 | 致命 | 增量恢复后错误重新全量（浪费 5.5 小时） | 检查 task.Phase |
| P0 | 致命 | 增量阶段并发启动新全量 | 互斥锁保护 |
| P1 | 高 | 断点 Cursor 始终为 0 | 修复序列化 |

#### 其他修复

- 系统 key（`stat:total/*`）被迁移 → `matchKeyFilterV2` 内置排除
- FakeSlave `atomic.Value` panic → `errorWrapper` 包装
- 升级重启自动恢复 → `ShutdownPaused` 标记机制
- ErrorKey 增强：记录操作类型、源/目标节点、重试次数

#### 文档整理

- 合并 6 个演化历史文档为 3 个（EVOLUTION_SUMMARY / PROJECT_ITERATION_HISTORY / REQUIREMENT_EVOLUTION）
- 删除 16 个冗余测试报告，关键信息补充到 TROUBLESHOOTING_GUIDE
- 移除 git 追踪的二进制文件和日志

---

### v2.6.0 - 流式处理与回归测试（2026-03-01）

**Git Commit**: `1585b55` | **文件变更**: 7 files, +2712/-824

#### 流式处理优化

| 模块 | 改进 | 目的 |
|------|------|------|
| StreamKeyListFromFile | 大文件流式处理 + 采样预览 | 避免加载全量到内存 |
| 错误 Key 导出 | 流式 ZIP 生成 | 避免大文件内存问题 |
| 数据校验 | 流式 SCAN + sample/full 模式 | 灵活性 + 内存可控 |

#### 数据校验增强

`triggerVerify` API 新增参数：
- `mode: "sample"` — 随机采样校验（快速）
- `mode: "full"` — 全量校验（精确）
- `sample_size` — 采样数量

#### 回归测试体系（97/97 全部通过）

| 分类 | 覆盖范围 | 测试数 |
|------|----------|--------|
| B | 基础 CRUD / 生命周期 | 12 |
| C | 集群连通性 | 5 |
| D | 数据类型迁移 | 6 |
| F | Key 过滤 | 6 |
| G | 全量+增量模式 | 4 |
| H | 崩溃恢复 | 3 |
| I | 进度与限速 | 4 |
| K | 冲突策略 | 4 |
| T | TTL 一致性 | 10 |
| P | 性能 | 3 |
| U | 历史问题回归 | 12 |

**关键测试修复**：
- `create_task` 字段名不匹配（`workers`→`worker_count`，`scan_count`→`scan_batch_size`）
- H1 Kill-9 恢复测试逻辑优化
- I2 进度百分比测试数据量和采样间隔调整

#### Bug 修复

- `binlogCancel` context 泄漏（go vet 检测）

---

## 三、核心需求满足情况

| 序号 | 核心需求 | 状态 | 实现方式 |
|-----|---------|------|---------|
| 1 | **按前缀迁移或跳过** | ✅ | KeyFilter（prefixes/exclude_prefixes/patterns）|
| 2 | **40亿Key高效迁移** | ✅ | 流式 SCAN + Binlog 增量（内存 <100MB）|
| 3 | **崩溃恢复不丢数据** | ✅ | 安全 Cursor 回退 + 优雅关闭 + SIGKILL 零丢失 |
| 4 | **冲突Key记录审查** | ✅ | 100万上限 + 自动落盘 + 查询接口 |
| 5 | **迁移前校验** | ✅ | Preflight Check（连通性/集群/Binlog/版本）|
| 6 | **集群拓扑刷新** | ✅ | 30 秒自动刷新 + 无效节点检测 |
| 7 | **限速控制** | ✅ | golang.org/x/time/rate + 动态调速 + 多 Worker 不退化 |
| 8 | **TTL 一致性** | ✅ | PTTL 毫秒精度 + TTL/TTLDEL OpType 处理 |
| 9 | **数据校验** | ✅ | 流式 SCAN + sample/full 模式 |

---

## 四、架构演进对比

| 指标 | V1.0 简化版 | V2.0 企业版 | 提升 |
|------|------------|------------|------|
| 支持规模 | ~1 亿 key | 40 亿+ key | **40x** |
| 并行度 | 单进程多 goroutine | 8-64 Worker 进程 | **8-64x** |
| 迁移速度 | ~10k keys/s | ≥50k keys/s | **5x** |
| 断点数量 | 1 个（任务级别） | 16384 个（Slot 级别） | **16384x** |
| 增量延迟 | ~60s（智能轮询） | <5s（FakeSlave Binlog） | **12x** |
| 内存占用 | ~500MB | ~150MB | **优化 3.3x** |
| 变更队列 | 内存 map | LevelDB 持久化 | 可靠性提升 |
| 元数据存储 | 内存 | SQLite (WAL) | 可靠性提升 |

---

## 五、项目结构

```
tendis-migrate/
├── cmd/
│   ├── master/main.go          # V2.0 Master 主程序
│   ├── worker/main.go          # V2.0 Worker 主程序
│   └── simple/main.go          # V1.x 简化版主程序
├── internal/
│   ├── master/                 # Master 模块
│   ├── worker/                 # Worker 模块
│   ├── storage/                # 存储层（SQLite + LevelDB）
│   ├── ipc/                    # IPC 通信
│   ├── engine/                 # 迁移引擎
│   ├── replication/            # FakeSlave 增量同步
│   ├── limiter/                # 限流控制
│   └── api/                    # HTTP API 服务
├── web/                        # Vue3 前端
├── data/                       # 数据目录（断点/错误Key/队列）
├── logs/                       # 日志目录
├── run.sh / stop.sh            # 启动/停止脚本
└── INSTALL.txt                 # 安装说明
```

---

## 六、Git 提交历史

```
5bded5c docs: 更新文档至v2.6.0（需求演化、问题排查指南）
5791d7b chore: 移除构建产物的git追踪(二进制+前端dist)
1585b55 v2.6.0: 流式处理优化、数据校验增强、回归测试全通过(97/97)
e345a73 v2.5.0: 限速修复、TTL一致性、崩溃恢复零丢失、文档整理
0b7df2a v2.4.0: 迁移前校验、集群拓扑刷新、UI全面升级
35be7bc test: 添加综合测试方案和测试脚本
7a8e329 feat: 实现零丢失断点恢复机制
ca0e17c docs: 更新演化历程文档至 v2.3.0
6c2e6eb v2.3.0: BUG修复 - Key过滤、RocksDB解析、增量同步
940d01d fix: 添加创建任务必填字段验证
dedfcfb v2.2: UI优化 + 文档更新 + 300轮对话迭代完成
3e8e7ed docs(v2): 添加完整测试脚本和使用文档
8c9302d feat(v2): Phase 3-5 完整实现
74c68ca feat(v2): Phase 2 完成 - Slot分片迁移
b8d4481 feat(v2): Phase 1 - 基础架构实现
69c27c2 v1.1: 新增动态配置调整功能
6ce3407 docs: 添加完整的 README 文档
906086b tendis-migrate v1.0 - Redis迁移工具
```

---

## 七、测试环境配置

### 家里测试环境（192.168.1.19）

- Docker 服务器：Mac
- 源端集群：192.168.1.19:7001/7002
- 目标端集群：192.168.1.19:8001/8002

### 公司测试环境 A（3 主节点，50 亿 Key）

- 源端：10.31.36.8:8902, 10.31.36.10:8903, 10.31.36.12:8901
- 目标端：10.31.36.3:8902, 10.31.36.15:8901, 10.31.36.13:8903
- 部署服务器：8.137.20.144:8088

### 公司测试环境 B（单机多端口）

- 源端：10.31.36.5:8901/8902/8903
- 目标端：10.31.36.16:8901/8902/8903

---

## 八、部署命令速查

```bash
# 编译 Linux 版本
GOOS=linux GOARCH=amd64 go build -o tendis-migrate ./cmd/simple

# 编译前端
cd web && npm run build && cd ..

# 打包
TIMESTAMP=$(date +%Y%m%d%H%M%S)
PACKAGE_DIR="tendis-migrate-package"
mkdir -p "$PACKAGE_DIR/logs" "$PACKAGE_DIR/data" "$PACKAGE_DIR/web"
cp tendis-migrate run.sh stop.sh INSTALL.txt "$PACKAGE_DIR/"
cp -r web/dist "$PACKAGE_DIR/web/"
COPYFILE_DISABLE=1 tar --no-xattrs -czvf "tendis-migrate-linux-${TIMESTAMP}.tar.gz" "$PACKAGE_DIR"

# 安全部署（不删除 data/ 和 logs/）
scp -P 8822 tendis-migrate-linux root@8.137.20.144:/tmp/tendis-migrate-new
ssh -p 8822 root@8.137.20.144 "cd /home/tendis-migrate-package && bash stop.sh && cp /tmp/tendis-migrate-new ./tendis-migrate && chmod +x ./tendis-migrate && bash run.sh"
```

---

## 九、总结

Tendis-Migrate 项目从 2026-01-23 创建至今，经历了以下重大迭代：

1. **V1.0** (01-23): 初始版本，单任务全量迁移
2. **V1.1** (01-25): 动态配置调整
3. **V2.0 Phase 1-5** (01-28~30): Master-Worker 多进程架构
4. **P0-P3 改进** (02-02): 解决 40 亿 Key OOM 问题
5. **前后端完善** (02-04): API 覆盖率 100%
6. **Binlog 深度分析** (02-05): 修复 3 个隐藏 Bug + 性能优化
7. **v2.3.0** (02-09): Key 过滤 + RocksDB 解析修复
8. **v2.3.1** (02-10~11): 50 亿 Key 大规模测试，修复 8 个 BUG
9. **v2.4.0** (02-12): Preflight Check + 拓扑刷新 + IP 探测 + UI 升级
10. **v2.5.0** (02-16~28): 限速三连修 + TTL 一致性 + 崩溃恢复零丢失 + 生产故障修复
11. **v2.6.0** (03-01): 流式处理优化 + 数据校验增强 + 回归测试 97/97 全部通过

当前版本已满足全部核心需求，可支持 40 亿+ Key 的大规模数据迁移，并建立了完整的自动化回归测试体系。

---

**文档维护者**: AI Coding Assistant
**最后更新**: 2026-03-01
