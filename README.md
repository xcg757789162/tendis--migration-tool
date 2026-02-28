# Tendis Migration Tool

[![Go Version](https://img.shields.io/badge/Go-1.20+-blue.svg)](https://golang.org/)
[![Vue Version](https://img.shields.io/badge/Vue-3.x-green.svg)](https://vuejs.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-2.5.0-brightgreen.svg)](https://github.com/xcg757789162/tendis--migration-tool/releases)

基于 Go + Vue3 + ElementPlus 的 **Tendis/Redis 集群数据迁移管理工具**。

支持 **40 亿级 Key** 的大规模迁移，具备 **流式处理**、**Slot 级断点续传**、**FakeSlave 增量同步** 等企业级特性。

## 核心特性

### 大规模数据迁移
- **流式处理**：边扫描边迁移，内存占用 < 100MB
- **40 亿 Key 支持**：专为超大规模数据设计，绝不存储全量 Key
- **高性能**：并发 Pipeline 写入，速度可达 **50,000+ keys/s**
- **精确限速**：基于 `golang.org/x/time/rate` 标准令牌桶，多 Worker 并发不退化

### 增量同步
- **FakeSlave 模式**：伪装为从节点，实时接收 Binlog
- **低延迟**：增量同步延迟 < 5 秒
- **多 Store 支持**：正确处理 Tendis 的多 Store 架构（每个 Store 独立 INCRSYNC）
- **TTL 一致性**：毫秒精度 PTTL 同步，支持 EXPIRE/PERSIST 命令回放

### 断点续传与崩溃恢复
- **安全 Cursor 回退**：`getSafeCheckpointCursor` 机制，SIGKILL 后零数据丢失
- **定期保存**：每 10000 Key 或 30 秒保存断点
- **优雅关闭**：SIGINT/SIGTERM 时等待 keyChan 排空后保存精确 cursor
- **自动恢复**：重启后从断点继续，升级重启自动恢复运行中任务

### 冲突处理
- **多种策略**：skip（跳过）、replace（覆盖）、error（报错）、skip_full_only
- **详细记录**：记录冲突 Key 的操作类型、源/目标节点、错误详情
- **100 万上限**：内存存储 10 万 + 自动落盘，防止 OOM

### Key 过滤
- **前缀过滤**：`prefixes` / `exclude_prefixes` 正反向过滤
- **Pattern 通配符**：支持 `*` 通配符匹配
- **Keylist 模式**：指定精确 Key 列表迁移
- **系统 Key 自动排除**：`stat:total/daily/hourly` 等 Tendis 内部 Key 不迁移

## 技术指标

| 指标 | 数值 |
|------|------|
| 迁移速度 | ≥ 50,000 keys/s |
| 内存占用 | < 100 MB |
| 增量延迟 | < 5 秒 |
| 断点粒度 | Slot 级 |
| 冲突记录 | 100 万条 |
| 崩溃恢复 | 零数据丢失 |

## 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                      Web UI (Vue3)                          │
├─────────────────────────────────────────────────────────────┤
│                    REST API + WebSocket                      │
├─────────────────────────────────────────────────────────────┤
│                     核心引擎                                 │
│  ┌───────────┬───────────┬───────────┬────────────────────┐ │
│  │  任务调度  │  状态管理  │  限速控制  │  FakeSlave 增量   │ │
│  └───────────┴───────────┴───────────┴────────────────────┘ │
│  ┌───────────┬───────────┬───────────┬────────────────────┐ │
│  │ Worker 池  │ Pipeline  │ 断点恢复  │  冲突 Key 管理     │ │
│  └───────────┴───────────┴───────────┴────────────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                     存储层                                   │
│  ┌──────────────────────┐ ┌──────────────────────┐          │
│  │  SQLite (任务/断点)   │ │  文件 (Error Keys)   │          │
│  └──────────────────────┘ └──────────────────────┘          │
└─────────────────────────────────────────────────────────────┘
```

## 快速开始

### 环境要求
- Go 1.20+
- Node.js 18+（仅前端开发需要）
- Linux / macOS

### 编译部署

```bash
# 1. 克隆项目
git clone https://github.com/xcg757789162/tendis--migration-tool.git
cd tendis--migration-tool

# 2. 编译后端（Linux 部署）
GOOS=linux GOARCH=amd64 go build -o tendis-migrate ./cmd/simple

# 3. 编译前端（可选，已包含预编译的 dist）
cd web && npm install && npm run build && cd ..

# 4. 打包
./package-linux.sh

# 5. 部署到服务器
scp tendis-migrate-linux-*.tar.gz user@server:/path/to/deploy/
ssh user@server "cd /path/to/deploy && tar -xzvf tendis-migrate-linux-*.tar.gz"

# 6. 启动 / 停止
./tendis-migrate-package/run.sh
./tendis-migrate-package/stop.sh
```

**macOS 本地开发**：
```bash
go build -o tendis-migrate ./cmd/simple
codesign --force --sign - ./tendis-migrate   # macOS 必须重签名
./run.sh
```

### 访问地址
- **Web UI**: http://服务器IP:8088

## 使用说明

### 创建迁移任务

1. 打开 Web UI，点击"创建任务"
2. 配置源端和目标端集群地址
3. 选择迁移模式（全量 / 增量 / 全量+增量）
4. 配置 Key 过滤规则（可选）
5. 设置高级选项（Worker 数量、冲突策略、限速等）
6. 点击"创建"

### 迁移模式

| 模式 | 说明 |
|------|------|
| `full_only` | 仅全量迁移 |
| `incremental` | 仅增量同步（FakeSlave） |
| `full_and_incremental` | 先全量后自动切换增量（默认） |

### Key 过滤

```json
{
  "key_filter": {
    "mode": "prefix",
    "prefixes": ["user:", "order:"],
    "exclude_prefixes": ["temp:", "cache:"]
  }
}
```

### 冲突策略

| 策略 | 说明 |
|------|------|
| `skip` | 跳过已存在的 Key，记录到冲突列表 |
| `replace` | 覆盖目标端已存在的 Key |
| `error` | 遇到冲突立即报错停止 |
| `skip_full_only` | 全量阶段跳过，增量阶段覆盖 |

## 配置说明

### 任务配置项

| 配置项 | 说明 | 默认值 | 动态调整 |
|--------|------|--------|---------|
| `worker_count` | 并发 Worker 数量 | 8 | ✅ |
| `scan_batch_size` | SCAN 批次大小 | 1000 | ✅ |
| `conflict_policy` | 冲突策略 | skip | ❌ |
| `large_key_threshold` | 大 Key 阈值 | 10MB | ❌ |

### 限速配置

| 配置项 | 说明 | 默认值 | 动态调整 |
|--------|------|--------|---------|
| `source_qps` | 源端 QPS 限制 | 0（不限制） | ✅ |
| `target_qps` | 目标端 QPS 限制 | 0（不限制） | ✅ |

## API 端点

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/tasks | 获取任务列表 |
| POST | /api/v1/tasks | 创建任务 |
| GET | /api/v1/tasks/:id | 获取任务详情 |
| DELETE | /api/v1/tasks/:id | 删除任务 |
| POST | /api/v1/tasks/:id/start | 启动任务 |
| POST | /api/v1/tasks/:id/pause | 暂停任务 |
| POST | /api/v1/tasks/:id/resume | 恢复任务 |
| POST | /api/v1/tasks/:id/stop | 停止任务 |
| POST | /api/v1/tasks/:id/complete | 完成任务 |
| POST | /api/v1/tasks/:id/preflight-check | 迁移前校验 |
| GET | /api/v1/tasks/:id/progress | 获取迁移进度 |
| GET | /api/v1/tasks/:id/error-keys | 查看失败 Key |
| PUT/PATCH | /api/v1/tasks/:id/config | 动态更新配置 |
| POST | /api/v1/tasks/:id/verify | 触发数据校验 |
| GET | /api/v1/tasks/:id/verify | 获取校验结果 |

### 动态配置调整示例

```bash
curl -X PATCH "http://localhost:8088/api/v1/tasks/{task_id}/config" \
  -H "Content-Type: application/json" \
  -d '{
    "worker_count": 16,
    "rate_limit": {
      "source_qps": 10000,
      "target_qps": 8000
    }
  }'
```

### WebSocket 实时监控

```javascript
const ws = new WebSocket('ws://localhost:8088/api/v1/ws/tasks/{task_id}')
ws.onmessage = (event) => {
  const data = JSON.parse(event.data)
  console.log('Progress:', data.progress)
}
```

## 项目结构

```
tendis-migrate/
├── cmd/simple/main.go             # 主程序入口（单进程架构）
├── internal/
│   ├── api/                       # REST API 服务
│   ├── engine/                    # 迁移引擎（task_runner、pipeline、writer）
│   ├── limiter/                   # 限速控制（PID 控制器）
│   ├── master/                    # Keyspace 监听
│   ├── replication/               # FakeSlave 增量同步
│   └── storage/                   # SQLite 存储层
├── pkg/logger/                    # 日志模块
├── web/                           # Vue3 前端
│   ├── src/
│   │   ├── api/                   # API 接口封装
│   │   ├── views/                 # 页面组件
│   │   └── router/                # 路由配置
│   └── dist/                      # 构建产物
├── run.sh                         # 启动脚本
├── stop.sh                        # 停止脚本
├── package-linux.sh               # Linux 打包脚本
├── TROUBLESHOOTING_GUIDE.md       # 问题排查手册
├── COMPREHENSIVE_TEST_PLAN.md     # 测试计划
└── CORE_REQUIREMENTS_CHECKLIST.md # 核心需求检查清单
```

## 版本历史

| 版本 | 日期 | 内容 |
|------|------|------|
| V1.0 | 2026-01 | 基础全量迁移、Web UI |
| V2.0 | 2026-01 | Master-Worker 架构、Slot 分片 |
| V2.1 | 2026-02 | FakeSlave 增量、Binlog 解析 |
| V2.3 | 2026-02-09 | BUG 修复：Key 过滤、RocksDB Key 解析 |
| V2.4 | 2026-02-12 | 迁移前校验、集群拓扑刷新、UI 升级 |
| **V2.5** | **2026-02-28** | **限速修复、TTL 一致性、崩溃恢复零丢失、文档整理** |

### V2.5.0 更新内容（2026-02-28）

**BUG 修复**：
- 修复全量迁移限速完全不生效（1 token/批次 → N tokens/批次）
- 修复运行中动态调整限速导致迁移卡死（`WaitN` 监听 `stopChan`）
- 修复多 Worker 下限速器 QPS 严重退化（替换为 `golang.org/x/time/rate`）
- 修复增量同步 TTL 不一致（`Expire` → `PExpire` 毫秒精度，支持 TTL/TTLDEL OpType）
- 修复系统 key（`stat:total/daily/hourly`）被迁移到目标端
- 修复 FakeSlave `atomic.Value` panic（并发重连时类型不一致）
- 修复增量阶段恢复后错误重新执行全量迁移（P0 生产故障）
- 修复增量阶段并发启动新全量（P0 并发 BUG）
- 修复断点 Cursor 始终为 0（P1 生产故障）

**新特性**：
- 安全 Cursor 回退机制（`getSafeCheckpointCursor`），SIGKILL 后零数据丢失
- 升级重启自动恢复（`ShutdownPaused` 标记机制）
- Error Key 增强：记录操作类型、源/目标节点、重试次数等详细信息

## 注意事项

1. **内存控制**：绝不使用 map/sync.Map 存储全量 Key
2. **流式处理**：全量同步边扫描边迁移，不缓存 Key 列表
3. **TTL 精度**：统一使用 PTTL/PExpire（毫秒级），不用 TTL/Expire（秒级）
4. **macOS 签名**：`go build` 后必须执行 `codesign --force --sign -`，否则进程变僵尸
5. **部署安全**：升级时只替换二进制和前端，绝不删除 `data/` 目录

## License

MIT License

---

*Version: 2.5.0 | 最后更新：2026-02-28*
