# Tendis Migration Tool

[![Go Version](https://img.shields.io/badge/Go-1.20+-blue.svg)](https://golang.org/)
[![Vue Version](https://img.shields.io/badge/Vue-3.x-green.svg)](https://vuejs.org/)
[![License](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Version](https://img.shields.io/badge/Version-2.3.0-brightgreen.svg)](https://github.com/xcg757789162/tendis--migration-tool/releases)

基于 Go + Vue3 + ElementPlus 的 **Tendis/Redis 集群数据迁移管理工具**。

支持 **40 亿级 Key** 的大规模迁移，具备 **流式处理**、**Slot 级断点续传**、**FakeSlave 增量同步** 等企业级特性。

## 🌟 核心特性

### 大规模数据迁移
- **流式处理**：边扫描边迁移，内存占用 < 100MB
- **40 亿 Key 支持**：专为超大规模数据设计，绝不存储全量 Key
- **高性能**：并发 Pipeline 写入，速度可达 **50,000+ keys/s**

### 增量同步
- **FakeSlave 模式**：伪装为从节点，实时接收 Binlog
- **低延迟**：增量同步延迟 < 5 秒
- **多 Store 支持**：正确处理 Tendis 的多 Store 架构

### 断点续传
- **Slot 级断点**：16384 个独立断点，崩溃只重做未完成的 Slot
- **优雅关闭**：SIGINT/SIGTERM 时自动保存所有状态
- **自动恢复**：重启后从断点继续，不丢失进度

### 冲突处理
- **多种策略**：skip（跳过）、replace（覆盖）、error（报错）
- **详细记录**：记录所有冲突 Key，支持导出审查
- **100 万上限**：内存存储 10 万 + 自动落盘，防止 OOM

## 📊 技术指标

| 指标 | 数值 |
|------|------|
| 迁移速度 | ≥ 50,000 keys/s |
| 内存占用 | < 100 MB |
| 增量延迟 | < 5 秒 |
| 断点粒度 | 16384 个 Slot |
| 冲突记录 | 100 万条 |

## 🏗️ 架构设计

```
┌─────────────────────────────────────────────────────────────┐
│                        Web UI (Vue3)                         │
├─────────────────────────────────────────────────────────────┤
│                      REST API (Gin)                          │
├─────────────────────────────────────────────────────────────┤
│                     Master Process                           │
│  ┌─────────────┬─────────────┬─────────────┬──────────────┐ │
│  │   任务调度   │   状态管理   │   IPC 通信   │  WebSocket   │ │
│  └─────────────┴─────────────┴─────────────┴──────────────┘ │
├─────────────────────────────────────────────────────────────┤
│                    Worker Processes                          │
│  ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐       │
│  │ Worker 1 │ │ Worker 2 │ │ Worker 3 │ │ Worker N │       │
│  │ Slot 0-x │ │ Slot x-y │ │ Slot y-z │ │ Slot ... │       │
│  └──────────┘ └──────────┘ └──────────┘ └──────────┘       │
├─────────────────────────────────────────────────────────────┤
│                     Storage Layer                            │
│  ┌─────────────────────┐ ┌─────────────────────┐            │
│  │   SQLite (任务状态)  │ │  LevelDB (Slot断点) │            │
│  └─────────────────────┘ └─────────────────────┘            │
└─────────────────────────────────────────────────────────────┘
```

## 🚀 快速开始

### 环境要求
- Go 1.20+
- Node.js 18+
- Linux/macOS

### 编译部署

```bash
# 1. 克隆项目
git clone https://github.com/xcg757789162/tendis--migration-tool.git
cd tendis--migration-tool

# 2. 编译后端（Linux）
GOOS=linux GOARCH=amd64 go build -o tendis-migrate ./cmd/simple

# 3. 编译前端
cd web
npm install
npm run build
cd ..

# 4. 打包
./package-linux.sh

# 5. 部署到服务器
scp tendis-migrate-linux-*.tar.gz user@server:/path/to/deploy/
ssh user@server "cd /path/to/deploy && tar -xzvf tendis-migrate-linux-*.tar.gz"

# 6. 启动服务
./tendis-migrate-package/run.sh

# 7. 停止服务
./tendis-migrate-package/stop.sh
```

### 访问地址
- **Web UI**: http://服务器IP:8088

## 📖 使用说明

### 创建迁移任务

1. 打开 Web UI，点击"创建任务"
2. 配置源端和目标端集群地址
3. 选择迁移模式（全量/增量/全量+增量）
4. 配置 Key 过滤规则（可选）
5. 设置高级选项（Worker 数量、冲突策略等）
6. 点击"创建"

### 迁移模式

| 模式 | 说明 |
|------|------|
| `full` | 仅全量迁移 |
| `incremental` | 仅增量同步 |
| `full_and_incremental` | 先全量后自动切换增量 |

### Key 过滤

```json
{
  "key_filter": {
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

## 🔧 配置说明

### 任务配置项

| 配置项 | 说明 | 默认值 | 支持动态调整 |
|--------|------|--------|-------------|
| `worker_count` | 并发 Worker 数量 | 8 | ✅ |
| `scan_batch_size` | SCAN 批次大小 | 1000 | ✅ |
| `conflict_policy` | 冲突策略 | skip | ❌ |
| `large_key_threshold` | 大 Key 阈值（字节） | 10MB | ❌ |

### 限速配置

| 配置项 | 说明 | 默认值 | 支持动态调整 |
|--------|------|--------|-------------|
| `source_qps` | 源端 QPS 限制 | 0（不限制） | ✅ |
| `target_qps` | 目标端 QPS 限制 | 0（不限制） | ✅ |
| `source_connections` | 源端连接数 | 50 | ❌ |
| `target_connections` | 目标端连接数 | 50 | ❌ |

## 📡 API 端点

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
| GET | /api/v1/tasks/:id/progress | 获取迁移进度 |
| PUT | /api/v1/tasks/:id/config | 动态更新配置 |

### 动态配置调整

```bash
curl -X PUT "http://localhost:8088/api/v1/tasks/{task_id}/config" \
  -H "Content-Type: application/json" \
  -d '{
    "worker_count": 16,
    "rate_limit": {
      "source_qps": 10000,
      "target_qps": 8000
    }
  }'
```

### WebSocket 实时日志

```javascript
const ws = new WebSocket('ws://localhost:8088/api/v1/ws/tasks/{task_id}')
ws.onmessage = (event) => {
  const data = JSON.parse(event.data)
  console.log('Progress:', data.progress)
  console.log('Logs:', data.logs)
}
```

## 📁 项目结构

```
tendis-migrate/
├── cmd/
│   └── simple/main.go          # 主程序入口
├── internal/
│   ├── api/                    # REST API
│   ├── engine/                 # 迁移引擎
│   ├── ipc/                    # 进程间通信
│   ├── master/                 # Master 进程
│   ├── storage/                # 存储层
│   └── worker/                 # Worker 进程
├── pkg/
│   └── logger/                 # 日志模块
├── web/                        # Vue3 前端
│   ├── src/
│   │   ├── api/                # API 接口
│   │   ├── views/              # 页面组件
│   │   └── router/             # 路由配置
│   └── dist/                   # 构建产物
├── run.sh                      # 启动脚本
├── stop.sh                     # 停止脚本
└── INSTALL.txt                 # 安装说明
```

## 📈 项目演化历程

本项目经过 **300+ 轮对话** 的持续迭代开发：

| 版本 | 内容 |
|------|------|
| V1.0 | 基础全量迁移、Web UI |
| V1.1 | 动态配置调整、增量同步 |
| V2.0 | Master-Worker 架构、Slot 分片 |
| V2.1 | FakeSlave 增量、Binlog 解析 |
| V2.2 | 性能优化、UI 完善 |
| **V2.3** | **BUG 修复：Key 过滤、RocksDB Key 解析、错误记录** |

### V2.3.0 更新内容（2026-02-09）

#### 🐛 BUG 修复

1. **BUG-1: key_filter.prefixes 参数不生效**
   - 修复：当配置 `prefixes` 时自动设置 `mode` 为 `"prefix"`
   - 修复：API 返回完整的 `options.key_filter` 配置

2. **BUG-2: 增量同步 RocksDB Key 解析**
   - 改进 `extractRedisKey()` 函数，正确解析 RocksDB RecordKey 格式
   - 支持从 9 字节头部 + varint 长度前缀中提取真正的 Redis Key

3. **BUG-3: 增量失败 Key 记录**
   - 确保所有增量同步失败路径都调用 `addErrorKeyWithDetails()`

#### ✅ 测试验证

- 增量 Key 过滤：String、Hash、List 类型全部验证通过
- `incr_keys_synced` / `incr_keys_filtered` 统计正确
- FakeSlave INCRSYNC 连接稳定，心跳正常

详细演化历程见 [EVOLUTION_ONE_LINE_SUMMARY.md](./EVOLUTION_ONE_LINE_SUMMARY.md)

## ⚠️ 注意事项

1. **内存控制**：绝不使用 map/sync.Map 存储全量 Key
2. **流式处理**：全量同步边扫描边迁移，不缓存 Key 列表
3. **断点保存**：每 10000 Key 或 30 秒保存一次断点
4. **优雅关闭**：捕获 SIGINT/SIGTERM 保存状态
5. **冲突上限**：内存最多 10 万条，超过自动落盘

## 🧪 测试验证

### 数据完整性验证
- ✅ String 类型：100 keys 校验通过
- ✅ Hash 类型：50 keys 校验通过
- ✅ List 类型：50 keys 校验通过
- ✅ Set 类型：50 keys 校验通过
- ✅ ZSet 类型：50 keys 校验通过

### 大规模测试
- ✅ 200 万 Key 全量迁移：10 分钟完成
- ✅ 内存占用稳定：< 100 MB
- ✅ 增量同步延迟：< 5 秒

## 📄 License

MIT License

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

---

*Version: 2.3.0 | 最后更新：2026-02-09*
