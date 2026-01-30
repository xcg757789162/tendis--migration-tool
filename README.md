# Tendis Migration Tool

<p align="center">
  <img src="https://img.shields.io/badge/Go-1.18+-00ADD8?style=flat&logo=go" alt="Go Version">
  <img src="https://img.shields.io/badge/Vue-3.x-4FC08D?style=flat&logo=vue.js" alt="Vue Version">
  <img src="https://img.shields.io/badge/License-MIT-blue.svg" alt="License">
</p>

一款高性能的 Redis/Tendis 数据迁移工具，支持集群模式、智能配置推荐、并行迁移，提供友好的 Web 界面。

## ✨ 特性

- 🚀 **高性能并行迁移** - 支持多 Worker 并行迁移，实测速度可达 300+ keys/s
- 🧠 **智能配置推荐** - 自动分析源端和目标端集群，推荐最优迁移配置
- 🔄 **多种迁移模式** - 支持全量迁移、全量+增量迁移
- 📊 **实时进度监控** - Web 界面实时展示迁移进度、速度、剩余时间
- 🎯 **灵活的 Key 过滤** - 支持前缀过滤、排除前缀、正则匹配
- ⚡ **限速控制** - 可配置源端/目标端 QPS 限制，避免影响生产环境
- 🛡️ **冲突处理策略** - 支持跳过、覆盖、报错等多种策略
- 📝 **详细日志记录** - 完整的操作日志和错误追踪

## 📸 界面预览

### 任务列表
- 查看所有迁移任务状态
- 实时显示迁移进度和速度

### 创建任务
- 配置源端/目标端集群
- 测试连接并获取智能推荐配置
- 灵活的高级选项配置

### 任务详情
- 实时进度监控
- 迁移速度图表
- 错误 Key 追踪

## 🚀 快速开始

### 环境要求

- Go 1.18+
- Node.js 16+
- Redis 4.0+ / Tendis

### 编译安装

```bash
# 克隆项目
git clone https://github.com/xcg757789162/tendis--migration-tool.git
cd tendis--migration-tool

# 编译后端
go build -o tendis-migrate ./cmd/simple

# 编译前端
cd web
npm install
npm run build
cd ..

# 启动服务
./run.sh
```

### Docker 部署

```bash
# 使用 docker-compose
docker-compose up -d

# 或直接 docker
docker build -t tendis-migrate .
docker run -d -p 8088:8088 tendis-migrate
```

### 访问 Web 界面

启动后访问: http://localhost:8088

## 📖 使用指南

### 1. 创建迁移任务

1. 点击「创建任务」
2. 填写任务名称，选择迁移模式
3. 配置源端集群地址和密码
4. 配置目标端集群地址和密码
5. 点击「测试连接」验证连通性
6. （推荐）点击「智能推荐配置」获取最优参数
7. 根据需要调整高级选项
8. 点击「创建任务」

### 2. 智能配置推荐

测试连接成功后，点击「智能推荐配置」按钮，系统会：

- 分析源端集群的 Key 数量、内存使用、当前 QPS
- 分析目标端集群的资源状况
- 综合评估后推荐最优的 Worker 数量、批次大小、QPS 限制等

推荐配置会显示：
- 预计迁移速度 (keys/s)
- 预计完成时间
- 推荐理由

### 3. 高级配置选项

| 参数 | 说明 | 默认值 |
|------|------|--------|
| Worker 数量 | 并行迁移的 Worker 数 | 8 |
| 扫描批次大小 | 每次 SCAN 获取的 Key 数量 | 1000 |
| 冲突处理策略 | skip/replace/error | skip |
| 大 Key 阈值 | 大于此值的 Key 使用特殊处理 | 10MB |
| 源端 QPS 限制 | 限制对源端的读取速率 | 0 (不限) |
| 目标端 QPS 限制 | 限制对目标端的写入速率 | 0 (不限) |

### 4. Key 过滤

支持三种过滤模式：

- **全部迁移**: 迁移所有 Key
- **前缀过滤**: 只迁移指定前缀的 Key，可设置排除前缀
- **正则匹配**: 使用正则表达式匹配 Key

## 🏗️ 项目结构

```
tendis-migrate/
├── cmd/
│   └── simple/
│       └── main.go          # 主程序入口
├── pkg/
│   └── logger/
│       └── logger.go        # 日志模块
├── web/                     # 前端项目 (Vue 3 + Element Plus)
│   ├── src/
│   │   ├── views/           # 页面组件
│   │   ├── api/             # API 接口
│   │   └── router/          # 路由配置
│   └── dist/                # 编译后的静态文件
├── run.sh                   # 启动脚本
├── stop.sh                  # 停止脚本
├── Dockerfile               # Docker 构建文件
└── docker-compose.yml       # Docker Compose 配置
```

## 🔧 API 接口

### 任务管理

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/tasks | 获取任务列表 |
| POST | /api/v1/tasks | 创建任务 |
| GET | /api/v1/tasks/:id | 获取任务详情 |
| POST | /api/v1/tasks/:id/start | 启动任务 |
| POST | /api/v1/tasks/:id/stop | 停止任务 |
| DELETE | /api/v1/tasks/:id | 删除任务 |

### 连接测试

| 方法 | 路径 | 说明 |
|------|------|------|
| POST | /api/v1/test-connection | 测试 Redis 连接 |
| POST | /api/v1/analyze-cluster | 分析集群信息 |
| POST | /api/v1/recommend-config | 获取推荐配置 |

## 📊 性能优化

### 优化前后对比

| 指标 | 优化前 | 优化后 |
|------|--------|--------|
| 迁移速度 | ~8 keys/s | 300+ keys/s |
| 52K keys 耗时 | ~1.8 小时 | ~3 分钟 |

### 性能优化要点

1. **并行 Worker 模式**: 使用 goroutine worker pool 并行处理
2. **批量操作**: 使用 Pipeline 批量读写
3. **异步进度更新**: 独立 goroutine 定时更新进度，避免锁竞争
4. **原子计数器**: 使用 sync/atomic 替代互斥锁
5. **集群并行 SCAN**: 对集群模式下的多个 Master 并行扫描

## 🤝 贡献

欢迎提交 Issue 和 Pull Request！

## 📄 License

MIT License
