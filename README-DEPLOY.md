# Tendis 迁移工具

基于 Go + Vue3 + ElementPlus 的 Tendis 数据迁移管理工具。

## 快速启动

### 方式一：Docker（推荐，适合移植）

**前提**：安装 Docker

```bash
# 一键启动（生产模式）
./docker-start.sh

# 开发模式（前后端分离，支持热重载）
./docker-start.sh dev

# 停止服务
./docker-start.sh stop

# 查看日志
./docker-start.sh logs
```

访问地址：http://localhost:8080

### 方式二：本地运行

**前提**：安装 Go 1.20+ 和 Node.js 18+

```bash
# 后端
go mod tidy
go run cmd/server/main.go

# 前端（另开终端）
cd web
npm install
npm run dev
```

后端：http://localhost:8080  
前端（开发）：http://localhost:3000

## 项目结构

```
tendis-migrate/
├── cmd/server/main.go         # 主程序入口
├── internal/
│   ├── api/server.go          # RESTful API
│   ├── engine/
│   │   ├── master.go          # Master进程管理
│   │   └── task_runner.go     # 任务执行引擎
│   ├── ipc/protocol.go        # IPC通信协议
│   ├── model/types.go         # 数据模型
│   └── storage/sqlite.go      # SQLite存储层
├── web/                       # Vue3前端
├── Dockerfile                 # Docker镜像定义
├── docker-compose.yml         # 编排配置
└── docker-start.sh            # 一键启动脚本
```

## 核心功能

- Master-Worker 架构，支持多 Worker 并行迁移
- `skip_full_only` 冲突策略，保证增量数据一致性
- 按 Slot 分组批量 EXISTS，避免 CROSSSLOT 错误
- SQLite 断点续传
- 两级抽样数据校验

## API 端点

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | /api/v1/tasks | 任务列表 |
| POST | /api/v1/tasks | 创建任务 |
| GET | /api/v1/tasks/:id | 任务详情 |
| POST | /api/v1/tasks/:id/start | 启动任务 |
| POST | /api/v1/tasks/:id/pause | 暂停任务 |
| GET | /api/v1/tasks/:id/progress | 迁移进度 |
| POST | /api/v1/tasks/:id/verify | 触发校验 |
| GET | /api/v1/system/status | 系统状态 |
