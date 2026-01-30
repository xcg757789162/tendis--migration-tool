# Tendis Migration Tool

基于 Go + Vue3 + ElementPlus 的 Tendis/Redis 集群数据迁移管理工具。

## 功能特性

### 核心功能
- **全量迁移**：支持大规模数据全量同步
- **增量同步**：基于 Keyspace Notification 的实时增量同步
- **全量+增量**：先全量后自动切换增量模式
- **断点续传**：支持任务暂停/恢复，迁移进度持久化
- **数据校验**：两级抽样校验，确保数据一致性

### 高级配置
- **并发控制**：可配置 Worker 数量和扫描批次大小
- **限速配置**：支持源端/目标端 QPS 限制和连接数控制
- **重试配置**：可配置最大重试次数、全量/增量重试间隔
- **Key 过滤**：支持前缀匹配、正则匹配、排除前缀等过滤模式
- **冲突策略**：skip（跳过）、replace（覆盖）、error（报错）、skip_full_only（仅全量跳过）
- **大Key处理**：可配置大 Key 阈值，自动分片处理

### 🆕 动态配置调整（v1.1 新增）
- **动态 Worker 调整**：运行时增加或减少并发 Worker 数量
- **动态 QPS 调整**：实时调整源端/目标端 QPS 限速
- **动态批次大小**：运行时调整 SCAN 批次大小
- **智能 Worker 管理**：Worker 减少时优雅停止，确保数据完整性

### 模板管理
- 支持任务模板的创建、编辑、删除
- 预置默认模板，快速创建迁移任务
- 模板参数一键加载

## 快速开始

### 环境要求
- Go 1.20+
- Node.js 18+

### 编译部署

```bash
# 1. 编译后端（Linux）
GOOS=linux GOARCH=amd64 go build -o tendis-migrate ./cmd/simple

# 2. 编译前端
cd web
npm install
npm run build
cd ..

# 3. 打包
TIMESTAMP=$(date +%Y%m%d%H%M%S)
mkdir -p tendis-migrate-package/{logs,data,web}
cp tendis-migrate run.sh stop.sh INSTALL.txt tendis-migrate-package/
cp -r web/dist tendis-migrate-package/web/
tar -czvf "tendis-migrate-linux-${TIMESTAMP}.tar.gz" tendis-migrate-package

# 4. 部署到服务器
scp tendis-migrate-linux-*.tar.gz user@server:/path/to/deploy/
ssh user@server "cd /path/to/deploy && tar -xzvf tendis-migrate-linux-*.tar.gz"

# 5. 启动服务
./tendis-migrate-package/run.sh

# 6. 停止服务
./tendis-migrate-package/stop.sh
```

### 访问地址
- Web UI: http://服务器IP:8088

## 配置说明

### 任务配置项

| 配置项 | 说明 | 默认值 | 支持动态调整 |
|--------|------|--------|-------------|
| `worker_count` | 并发 Worker 数量 | 8 | ✅ |
| `scan_batch_size` | 扫描批次大小 | 1000 | ✅ |
| `conflict_policy` | 冲突策略 | skip | ❌ |
| `large_key_threshold` | 大 Key 阈值（字节） | 10485760 (10MB) | ❌ |
| `enable_compression` | 启用压缩 | false | ❌ |
| `skip_full_sync` | 跳过全量同步 | false | ❌ |
| `skip_incremental` | 跳过增量同步 | false | ❌ |

### 限速配置 (rate_limit)

| 配置项 | 说明 | 默认值 | 支持动态调整 |
|--------|------|--------|-------------|
| `source_qps` | 源端 QPS 限制（0=不限制） | 0 | ✅ |
| `target_qps` | 目标端 QPS 限制（0=不限制） | 0 | ✅ |
| `source_connections` | 源端连接数 | 50 | ❌ |
| `target_connections` | 目标端连接数 | 50 | ❌ |

> **注意**：连接数（source_connections/target_connections）在任务启动时设置，不支持动态调整。如需修改连接数，需要停止任务后重新创建。

### 重试配置 (retry_config)

| 配置项 | 说明 | 默认值 |
|--------|------|--------|
| `max_retries` | 最大重试次数 | 3 |
| `full_retry_interval_ms` | 全量迁移重试间隔基数（毫秒） | 100 |
| `incr_retry_interval_ms` | 增量同步重试间隔基数（毫秒） | 1000 |

### Key 过滤配置 (key_filter)

| 配置项 | 说明 |
|--------|------|
| `mode` | 过滤模式：prefix（前缀）、pattern（正则） |
| `prefixes` | 包含的前缀列表 |
| `exclude_prefixes` | 排除的前缀列表 |
| `patterns` | 正则表达式列表 |

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
| GET | /api/v1/tasks/:id/progress | 获取迁移进度 |
| POST | /api/v1/tasks/:id/verify | 触发数据校验 |
| PUT | /api/v1/tasks/:id/config | 动态更新任务配置（Worker/QPS/BatchSize）|

### 动态配置调整 API

动态更新运行中任务的配置参数：

```bash
# 动态调整 Worker 数量和 QPS
curl -X PUT "http://localhost:8088/api/v1/tasks/{task_id}/config" \
  -H "Content-Type: application/json" \
  -d '{
    "worker_count": 4,
    "scan_batch_size": 200,
    "rate_limit": {
      "source_qps": 100,
      "target_qps": 80
    }
  }'
```

**支持的动态调整参数：**
| 参数 | 增加 | 减少 | 说明 |
|------|------|------|------|
| `worker_count` | ✅ | ✅ | 智能增减，减少时优雅停止 |
| `scan_batch_size` | ✅ | ✅ | 下次 SCAN 生效 |
| `rate_limit.source_qps` | ✅ | ✅ | 500ms 内生效 |
| `rate_limit.target_qps` | ✅ | ✅ | 500ms 内生效 |

### 模板管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/templates | 获取模板列表 |
| POST | /api/v1/templates | 创建模板 |
| GET | /api/v1/templates/:id | 获取模板详情 |
| PUT | /api/v1/templates/:id | 更新模板 |
| DELETE | /api/v1/templates/:id | 删除模板 |

### 集群分析

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/v1/analyze-cluster | 分析集群信息 |
| POST | /api/v1/recommend-config | 获取推荐配置 |

### 系统管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/logs | 获取系统日志 |
| GET | /api/v1/system/status | 获取系统状态 |

## 项目结构

```
tendis-migrate/
├── cmd/
│   └── simple/main.go          # 主程序入口（简化版）
├── web/                         # Vue3 前端
│   ├── src/
│   │   ├── api/                 # API 接口
│   │   ├── views/               # 页面组件
│   │   │   ├── Dashboard.vue    # 仪表盘
│   │   │   ├── Tasks.vue        # 任务列表
│   │   │   ├── TaskDetail.vue   # 任务详情
│   │   │   ├── CreateTask.vue   # 创建任务
│   │   │   └── Logs.vue         # 日志查看
│   │   └── router/              # 路由配置
│   └── dist/                    # 前端构建产物
├── run.sh                       # 启动脚本
├── stop.sh                      # 停止脚本
└── INSTALL.txt                  # 安装说明
```

## 迁移流程

1. **创建任务**：配置源端/目标端集群地址、迁移模式、高级选项
2. **启动任务**：系统自动执行全量扫描和数据迁移
3. **增量同步**：全量完成后自动切换到增量模式，实时同步变更
4. **数据校验**：支持手动触发数据一致性校验
5. **任务管理**：支持暂停、恢复、停止、删除任务

## 注意事项

- 确保源端集群开启 Keyspace Notification（增量同步需要）
- 目标端集群需要有足够的内存和带宽
- 大规模迁移建议先进行小规模测试
- 网络异常时系统会自动重试，可通过重试配置调整策略
- 连接数在任务启动时固定，如需修改需重新创建任务

## 测试验证

本工具已通过完整的功能测试，包括：

### 数据完整性验证
- ✅ String 类型：100 keys 值校验通过
- ✅ Hash 类型：50 keys 值校验通过
- ✅ List 类型：50 keys 值校验通过
- ✅ Set 类型：50 keys 值校验通过
- ✅ ZSet 类型：50 keys 值校验通过

### 动态配置调整验证
- ✅ Worker 增加/减少
- ✅ QPS 增加/减少
- ✅ ScanBatchSize 增加/减少

## License

MIT License
