# Tendis Migration Tool 使用手册

**版本**: v2.3.0  
**更新日期**: 2026-02-27

---

## 目录

- [1. 工具简介](#1-工具简介)
- [2. 系统要求](#2-系统要求)
- [3. 安装部署](#3-安装部署)
- [4. 快速开始](#4-快速开始)
- [5. 迁移任务管理](#5-迁移任务管理)
- [6. 独立校验任务](#6-独立校验任务)
- [7. Key 过滤规则](#7-key-过滤规则)
- [8. 增量同步（Binlog）](#8-增量同步binlog)
- [9. 限速与自适应流控](#9-限速与自适应流控)
- [10. 冲突策略与异常 Key](#10-冲突策略与异常-key)
- [11. 大 Key 处理](#11-大-key-处理)
- [12. 断点续传](#12-断点续传)
- [13. API 接口参考](#13-api-接口参考)
- [14. 运维与故障排查](#14-运维与故障排查)
- [15. 常见问题 FAQ](#15-常见问题-faq)

---

## 1. 工具简介

Tendis Migration Tool 是一款专为 **Tendis / Redis 集群**设计的数据迁移工具，支持全量迁移、增量同步（Binlog）、数据校验等核心功能。

### 核心特性

| 特性 | 说明 |
|:---|:---|
| **全量迁移** | 基于 SCAN + DUMP/RESTORE 流式迁移，支持 40 亿级别 Key |
| **增量同步** | 基于 Tendis Binlog 的 FakeSlave 实时同步 |
| **断点续传** | SCAN cursor 持久化，崩溃后从断点恢复 |
| **Key 过滤** | 按前缀/模式/排除前缀灵活过滤 |
| **冲突管理** | 支持 skip/replace/error 策略，异常 Key 记录审查 |
| **大 Key 处理** | 自动识别大 Key，分片迁移避免阻塞 |
| **自适应限流** | PID 控制器动态调整迁移速度，保护源端 |
| **数据校验** | 支持全量/抽样校验，多轮比较，智能比较模式 |
| **Web UI** | Vue 3 + Element Plus 可视化管理界面 |
| **实时监控** | WebSocket 推送实时进度、速度、延迟等指标 |

### 技术架构

```
┌──────────────────────────────────────────────────┐
│                   Web UI (Vue 3)                 │
│              Element Plus + ECharts              │
└──────────────────┬───────────────────────────────┘
                   │ REST API + WebSocket
┌──────────────────▼───────────────────────────────┐
│              Go 后端 (Gin Framework)             │
│  ┌─────────────┬─────────────┬─────────────────┐ │
│  │  Task Runner │  FakeSlave  │   Verifier      │ │
│  │  (全量迁移)  │  (增量同步)  │   (数据校验)    │ │
│  └──────┬──────┴──────┬──────┴────────┬────────┘ │
│  ┌──────▼─────────────▼───────────────▼────────┐ │
│  │  SQLite (任务状态) + LevelDB (冲突Key存储)   │ │
│  └─────────────────────────────────────────────┘ │
└──────────┬────────────────────────┬──────────────┘
           │                        │
    ┌──────▼──────┐          ┌──────▼──────┐
    │  源端集群    │          │  目标端集群  │
    │ Tendis/Redis │          │ Tendis/Redis │
    └─────────────┘          └─────────────┘
```

---

## 2. 系统要求

### 运行环境

| 项目 | 要求 |
|:---|:---|
| **操作系统** | Linux (CentOS 7+, Ubuntu 18.04+), macOS 10.15+ |
| **CPU 架构** | x86_64 (amd64), ARM64 |
| **内存** | 最低 512MB，推荐 2GB+ |
| **磁盘** | 至少 1GB 可用空间（日志 + 断点数据 + binlog 缓存） |
| **网络** | 工具需能同时访问源端和目标端集群 |

### 源端/目标端要求

| 项目 | 要求 |
|:---|:---|
| **支持类型** | Tendis 2.x 集群、Redis 3.x/4.x/5.x/6.x/7.x 集群 |
| **集群模式** | Cluster 模式（自动发现所有 Master 节点） |
| **Tendis 增量同步** | 需开启 `binlog-enabled=yes` |

---

## 3. 安装部署

### 3.1 二进制部署（推荐）

**适用场景**: 快速部署到 Linux 服务器。

```bash
# 1. 上传压缩包到目标服务器
scp tendis-migrate-linux-*.tar.gz root@your-server:/home/

# 2. 登录服务器解压
ssh root@your-server
cd /home
tar -zxvf tendis-migrate-linux-*.tar.gz

# 3. 启动服务
cd tendis-migrate-package
./run.sh

# 4. 访问 Web UI
# 浏览器打开 http://your-server:8088
```

### 3.2 自定义端口启动

```bash
# 通过环境变量指定端口
PORT=9090 ./run.sh

# 或者指定数据目录和 Worker 数
PORT=8088 DATA_DIR=/data/tendis-migrate WORKERS=8 ./run.sh
```

### 3.3 停止服务

```bash
./stop.sh
```

> 工具会先发送 SIGTERM 信号触发优雅关闭（保存所有任务状态和断点），默认等待 30 秒。  
> 可通过 `GRACEFUL_TIMEOUT=60 ./stop.sh` 调整等待时间。

### 3.4 目录结构说明

```
tendis-migrate-package/
├── tendis-migrate          # 主程序二进制
├── run.sh                  # 启动脚本
├── stop.sh                 # 停止脚本
├── USER_GUIDE.md           # 使用手册（本文档）
├── INSTALL.txt             # 快速安装说明
├── web/
│   └── dist/               # 前端静态文件
├── data/                   # 数据目录（自动创建，请勿删除！）
│   ├── tendis-migrate.db   # SQLite 数据库（任务状态）
│   ├── conflict_keys/      # 冲突 Key 存储
│   └── binlog_cache/       # Binlog 缓存
└── logs/                   # 日志目录
    └── tendis-migrate.log  # 运行日志
```

> **⚠️ 重要**: `data/` 目录包含所有任务状态和断点信息，升级时**绝对不能删除**！

---

## 4. 快速开始

### 4.1 创建第一个迁移任务

1. 打开浏览器访问 `http://your-server:8088`
2. 点击左侧菜单「创建任务」
3. 填写基本信息：

| 配置项 | 示例值 | 说明 |
|:---|:---|:---|
| 任务名称 | `prod-migration-01` | 便于识别 |
| 源端集群地址 | `10.0.1.1:7001,10.0.1.2:7002` | 逗号分隔多个节点 |
| 源端密码 | (可选) | 如果集群设了 requirepass |
| 目标端集群地址 | `10.0.2.1:8001,10.0.2.2:8002` | 逗号分隔多个节点 |
| 目标端密码 | (可选) | |
| 迁移模式 | `全量+增量` | 见下方说明 |

4. 点击「创建」
5. 在任务列表中点击「启动」

### 4.2 迁移模式

| 模式 | 值 | 说明 |
|:---|:---|:---|
| **仅全量** | `full_only` | 只做全量迁移（SCAN 所有 Key），适合一次性迁移 |
| **全量+增量** | `full_and_incremental` | 全量完成后自动进入增量同步，适合在线迁移 |

- **仅全量**: 全量 SCAN 完成后任务自动结束
- **全量+增量**: 全量完成后，通过 FakeSlave 注册为源端 Binlog 从节点，实时同步增量写入。需手动停止增量同步

### 4.3 迁移流程图

```
创建任务 → 启动任务 → 全量迁移(SCAN) → [增量同步(Binlog)] → 数据校验 → 完成
                         │                    │
                         ├── 断点续传支持 ──────┤
                         │                    │
                         └── 可随时暂停/恢复 ──┘
```

---

## 5. 迁移任务管理

### 5.1 任务生命周期

```
pending(待启动) → running(运行中) → completed(已完成)
                     │    ▲
                     ▼    │
                  paused(已暂停)
                     │
                     ▼
                  failed(失败)
```

### 5.2 任务操作

| 操作 | 说明 |
|:---|:---|
| **启动** | 开始执行迁移任务 |
| **暂停** | 暂停迁移，保存当前 SCAN cursor 和进度 |
| **恢复** | 从断点处继续迁移 |
| **停止** | 停止增量同步（全量+增量模式下） |
| **完成** | 停止增量同步 + 标记任务完成 |
| **删除** | 删除任务及其所有数据 |

### 5.3 实时监控指标

任务详情页提供以下实时指标：

| 指标 | 说明 |
|:---|:---|
| **迁移进度** | 已迁移 Key 数 / 总 Key 数 |
| **迁移速度** | Keys/秒、Bytes/秒 |
| **当前阶段** | 全量(full) / 增量(incremental) |
| **活跃 Worker 数** | 并行执行迁移的 Worker 数量 |
| **增量延迟** | Binlog 同步延迟（毫秒） |
| **失败 Key 数** | 迁移失败的 Key 数量 |
| **跳过 Key 数** | 因冲突策略跳过的 Key 数量 |

---

## 6. 独立校验任务

独立于迁移任务的数据一致性校验功能，用于验证源端和目标端的数据是否一致。

### 6.1 校验模式

| 模式 | 值 | 说明 |
|:---|:---|:---|
| **全量校验** | `full` | 遍历源端所有 Key 进行比较 |
| **抽样校验** | `sample` | 按采样率随机抽取 Key 比较 |

### 6.2 比较模式

| 模式 | 值 | 说明 |
|:---|:---|:---|
| **全量比较** | `full_value` | 比较完整的 Key 值 |
| **长度比较** | `length_only` | 只比较值的长度（更快） |
| **快速比较** | `quick` | 比较序列化后的字节 |

### 6.3 校验参数

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| `verify_mode` | `full` | 校验模式 |
| `sample_rate` | `0.01` | 抽样率（1%），仅 sample 模式生效 |
| `max_keys` | `100000` | 最大抽样 Key 数，仅 sample 模式生效 |
| `compare_mode` | `full_value` | 比较模式 |
| `compare_value` | `true` | 是否比较值内容 |
| `compare_ttl` | `true` | 是否比较 TTL |
| `ttl_tolerance` | `5` | TTL 容差（秒），差异在此范围内视为一致 |
| `smart_compare` | `false` | 智能比较（自动选择最优比较策略） |
| `big_key_threshold` | `5000` | 大 Key 判定阈值（元素数量） |
| `compare_rounds` | `2` | 比较轮次（多轮可消除因数据同步延迟导致的误判） |
| `round_interval` | `3` | 轮次间隔（秒） |
| `concurrency` | `4` | 校验并发数 |

### 6.4 校验结果解读

| 指标 | 说明 |
|:---|:---|
| `scanned_keys` | 扫描的 Key 总数 |
| `matched_keys` | 一致的 Key 数 |
| `missing_keys` | 目标端缺失的 Key 数 |
| `value_mismatch` | 值不一致的 Key 数 |
| `ttl_mismatch` | TTL 不一致的 Key 数 |
| `consistency_rate` | 一致性比率（%） |

> **注意**: Tendis 的 `DBSIZE` 包含已过期未清理的 Key，会远大于 SCAN 实际扫描到的存活 Key 数。这是正常现象。

---

## 7. Key 过滤规则

### 7.1 按前缀迁移

只迁移指定前缀的 Key：

```json
{
  "key_filter": {
    "prefixes": ["user:", "order:", "product:"]
  }
}
```

### 7.2 按前缀排除

迁移所有 Key，但跳过指定前缀：

```json
{
  "key_filter": {
    "exclude_prefixes": ["tmp:", "cache:", "session:"]
  }
}
```

### 7.3 同时使用

优先匹配 `prefixes`（白名单），然后排除 `exclude_prefixes`（黑名单）：

```json
{
  "key_filter": {
    "prefixes": ["user:"],
    "exclude_prefixes": ["user:tmp:", "user:cache:"]
  }
}
```

### 7.4 前端配置

在「创建任务」页面的「Key 过滤」区域：
- **过滤前缀**: 填写要迁移的前缀，多个前缀换行分隔
- **排除前缀**: 填写要排除的前缀，多个前缀换行分隔

> **40 亿 Key 场景提示**: `SCAN MATCH prefix*` 是**服务端过滤**，不会将不匹配的 Key 传输到工具端，可大幅减少网络开销。

---

## 8. 增量同步（Binlog）

### 8.1 工作原理

增量同步基于 Tendis 的 **FakeSlave** 机制：

1. 工具向源端每个 Master 节点注册为从节点（`INCRSYNC` 命令）
2. 源端持续推送 Binlog 数据（`applybinlogsv2`）
3. 工具解析 Binlog 并回放到目标端

```
源端 Master ──INCRSYNC──> FakeSlave(工具) ──回放──> 目标端
```

### 8.2 前提条件

- 源端为 **Tendis 2.x**，且开启了 `binlog-enabled=yes`
- 迁移模式选择 `full_and_incremental`

### 8.3 关键特性

| 特性 | 说明 |
|:---|:---|
| **多 Store 支持** | 自动检测 `kvstorecount`，为每个 Store 注册 INCRSYNC |
| **全量阶段 Binlog 缓存** | 全量迁移期间，Binlog 缓存到本地磁盘，全量完成后回放 |
| **断点续传** | 保存 Binlog offset，重启后从断点继续 |
| **自动重连** | 连接断开后自动重连并从上次 offset 继续 |

### 8.4 监控增量同步

任务详情页增量同步面板显示：

| 指标 | 说明 |
|:---|:---|
| `IncrKeysSynced` | 增量已同步 Key 数 |
| `IncrLagMs` | 同步延迟（毫秒），越小表示同步越及时 |
| `IncrBinlogPos` | 当前 Binlog 位置 |
| `IncrHeartbeats` | 心跳次数 |
| `IncrReconnects` | 重连次数 |

### 8.5 停止增量同步

当源端和目标端数据基本一致后，可以：
1. 在 Web UI 任务详情页点击「停止增量同步」
2. 或点击「完成任务」（会自动停止增量并标记完成）

---

## 9. 限速与自适应流控

### 9.1 基础限速

在创建任务时可配置限速参数：

| 参数 | 默认值 | 说明 |
|:---|:---|:---|
| `source_qps` | `10000` | 源端 QPS 限制 |
| `target_qps` | `10000` | 目标端 QPS 限制 |
| `source_connections` | `50` | 源端连接数 |
| `target_connections` | `50` | 目标端连接数 |
| `pipeline_size` | `100` | Pipeline 批量大小 |
| `max_bandwidth_mbps` | `0` | 带宽限制（MB/s），0 表示不限 |

### 9.2 自适应流控（PID 控制器）

工具内置 PID 自适应限流控制器，根据以下指标自动调节迁移速度：

- **源端 CPU 使用率**: 避免迁移导致源端过载
- **源端响应延迟**: 延迟上升时自动降速
- **目标端写入压力**: 目标端负载高时降速

> 无需手动干预，工具会自动在迁移速度和集群稳定性之间取得平衡。

---

## 10. 冲突策略与异常 Key

### 10.1 冲突策略

当目标端已存在同名 Key 时的处理方式：

| 策略 | 值 | 说明 |
|:---|:---|:---|
| **全量跳过+增量覆盖** | `skip_full_only` | 全量阶段跳过已存在的 Key，增量阶段用新值覆盖（**默认，推荐**） |
| **跳过** | `skip` | 始终跳过，记录到异常 Key 列表 |
| **覆盖** | `replace` | 始终用源端值覆盖目标端 |
| **报错** | `error` | 遇到冲突时记录错误，不跳过也不覆盖 |

### 10.2 异常 Key 查看

在任务详情页的「异常 Key」标签可以：

- **查看列表**: 分页浏览所有异常 Key
- **按前缀过滤**: 搜索特定前缀的异常 Key
- **导出**: 下载为 JSONL/JSON/CSV 格式
- **重试**: 对失败的 Key 进行重新迁移

### 10.3 冲突 Key 存储

- 内存中保留最近 10 万条冲突 Key
- 超过 10 万条后自动落盘（LevelDB 存储）
- 最大上限 100 万条

---

## 11. 大 Key 处理

### 11.1 大 Key 判定

默认阈值为 **10MB**（可在创建任务时配置 `large_key_threshold`）。

超过阈值的 Key 会被识别为大 Key，采用特殊的分片迁移策略。

### 11.2 大 Key 迁移策略

| 数据类型 | 迁移方式 |
|:---|:---|
| **Hash** | HSCAN 分片读取 + HSET 逐批写入 |
| **Set** | SSCAN 分片读取 + SADD 逐批写入 |
| **ZSet** | ZSCAN 分片读取 + ZADD 逐批写入 |
| **List** | LRANGE 分段读取 + RPUSH 逐批写入 |
| **String** | 直接 DUMP/RESTORE |

> 大 Key 迁移会单独计算进度，不影响其他 Key 的迁移。

---

## 12. 断点续传

### 12.1 全量阶段

- 每迁移 10,000 个 Key 或每 30 秒保存一次 SCAN cursor
- 保存到 SQLite 数据库的 `checkpoints` 表
- 任务暂停/崩溃后恢复时，从最后保存的 cursor 继续

### 12.2 增量阶段

- 保存每个节点、每个 Store 的 Binlog offset
- 重启后从保存的 offset 继续接收 Binlog

### 12.3 优雅关闭

收到 SIGINT (Ctrl+C) 或 SIGTERM 信号时：
1. 停止接收新数据
2. 保存所有 SCAN cursor 和 Binlog offset
3. 等待正在处理的数据写入完成
4. 安全退出

> **⚠️ 强制 kill -9 不会保存断点信息**，请始终使用 `./stop.sh` 或 Ctrl+C 停止。

---

## 13. API 接口参考

所有 API 以 `/api/v1` 为前缀，返回 JSON 格式。

### 13.1 任务管理

#### 创建迁移任务

```
POST /api/v1/tasks
```

**请求体示例**:

```json
{
  "name": "my-migration",
  "migration_mode": "full_and_incremental",
  "source_cluster": {
    "addrs": ["10.0.1.1:7001", "10.0.1.2:7002"],
    "password": ""
  },
  "target_cluster": {
    "addrs": ["10.0.2.1:8001", "10.0.2.2:8002"],
    "password": ""
  },
  "key_filter": {
    "prefixes": ["user:", "order:"],
    "exclude_prefixes": ["user:tmp:"]
  },
  "options": {
    "workers": 4,
    "conflict_policy": "skip_full_only",
    "rate_limit": {
      "source_qps": 10000,
      "target_qps": 10000,
      "pipeline_size": 100
    }
  }
}
```

#### 启动任务

```
POST /api/v1/tasks/:id/start
```

#### 暂停/恢复任务

```
POST /api/v1/tasks/:id/pause
POST /api/v1/tasks/:id/resume
```

#### 获取任务详情

```
GET /api/v1/tasks/:id
```

#### 获取迁移进度

```
GET /api/v1/tasks/:id/progress
```

### 13.2 独立校验任务

#### 创建校验任务

```
POST /api/v1/verify-tasks
```

**请求体示例**:

```json
{
  "name": "verify-after-migration",
  "source_cluster": "10.0.1.1:7001,10.0.1.2:7002",
  "target_cluster": "10.0.2.1:8001,10.0.2.2:8002",
  "verify_mode": "full",
  "compare_mode": "full_value",
  "compare_value": true,
  "compare_ttl": true,
  "ttl_tolerance": 5,
  "smart_compare": true,
  "big_key_threshold": 5000,
  "compare_rounds": 2,
  "round_interval": 3,
  "concurrency": 4,
  "key_filter": {
    "prefixes": ["user:"]
  }
}
```

#### 启动/停止校验

```
POST /api/v1/verify-tasks/:id/start
POST /api/v1/verify-tasks/:id/stop
```

#### 获取校验结果

```
GET /api/v1/verify-tasks/:id
```

### 13.3 系统接口

#### 测试连接

```
POST /api/v1/test-connection

{
  "addrs": ["10.0.1.1:7001"],
  "password": ""
}
```

#### 健康检查

```
GET /api/v1/health
```

#### 系统状态

```
GET /api/v1/system/status
```

---

## 14. 运维与故障排查

### 14.1 日志位置

```bash
# 主日志
tail -f logs/tendis-migrate.log

# 按日期分割的日志
ls logs/tendis-migrate-*.log
```

### 14.2 常用运维操作

```bash
# 查看进程状态
ps aux | grep tendis-migrate

# 查看端口占用
netstat -tlnp | grep 8088

# 修改端口重启
./stop.sh
PORT=9090 ./run.sh
```

### 14.3 数据备份

```bash
# 备份任务数据（推荐在停止服务后操作）
cp -r data/ data-backup-$(date +%Y%m%d)/
```

### 14.4 升级部署

```bash
# 1. 停止旧版本
./stop.sh

# 2. 备份（仅替换二进制和前端，保留 data/ 和 logs/）
cp tendis-migrate tendis-migrate.bak

# 3. 替换二进制和前端
cp /path/to/new/tendis-migrate ./tendis-migrate
rm -rf web/dist && cp -r /path/to/new/web/dist ./web/dist

# 4. 启动新版本
./run.sh
```

> **⚠️ 升级时绝对不要删除 `data/` 目录**，否则所有任务状态和历史数据会丢失！

---

## 15. 常见问题 FAQ

### Q: 源端 Key 数量和实际扫描数量差距很大？

**A**: Tendis 基于 RocksDB，`DBSIZE` 返回的数量包含了大量已过期但未被 compaction 清理的 Key。`SCAN` 会自动跳过过期 Key，所以实际扫描到的存活 Key 数远小于 DBSIZE。这是正常现象。可执行 `compactall` 命令触发 RocksDB compaction 来清理过期 Key。

### Q: 增量同步延迟持续增大怎么办？

**A**: 检查以下方面：
1. 目标端写入是否存在瓶颈（检查目标端 CPU/内存/磁盘）
2. 网络是否存在丢包或延迟
3. 尝试增加 `workers` 数量或 `pipeline_size`

### Q: 迁移过程中源端有新写入怎么办？

**A**: 使用 `full_and_incremental` 模式。全量迁移期间的新写入会通过 Binlog 缓存，全量完成后自动回放并进入实时增量同步。

### Q: 如何验证迁移数据的正确性？

**A**: 
1. 迁移完成后，创建独立校验任务
2. 选择「全量校验」+「全量比较」模式
3. 开启「比较 TTL」
4. 设置 2-3 轮比较（消除同步延迟导致的误判）
5. 查看一致性比率

### Q: 工具崩溃后重启，任务会从头开始吗？

**A**: 不会。工具内置断点续传机制，会从最后保存的 SCAN cursor 或 Binlog offset 继续。前提是 `data/` 目录完好。

### Q: 如何迁移特定前缀的 Key？

**A**: 在创建任务时配置 Key 过滤规则，填写 `prefixes` 字段。例如只迁移 `user:` 和 `order:` 前缀的 Key。

### Q: 目标端已有数据，迁移会覆盖吗？

**A**: 取决于冲突策略配置：
- `skip_full_only`（默认）：全量阶段不覆盖，增量阶段覆盖
- `skip`：永远不覆盖
- `replace`：总是覆盖
- `error`：遇到冲突记录错误

---

> 如有更多问题，请查看日志或联系工具维护人员。
