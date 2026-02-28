#!/bin/bash
# 家里环境 Tendis 2主节点集群搭建脚本
# 服务器: 192.168.1.19 (xiechenguo)
# 
# 源集群: 7001 + 7002 (2 master)
# 目标集群: 8001 + 8002 (2 master)
#
# 关键: 使用 --network host 模式，避免 bridge 网络下集群节点返回容器内部 IP 不可达的问题

set -e

REMOTE="xiechenguo@192.168.1.19"
DOCKER="/usr/local/bin/docker"
IMAGE="registry.cn-zhangjiakou.aliyuncs.com/xiaoduoai/devops:tendisplus-v2.7.0"
HOST_IP="192.168.1.19"

echo "=========================================="
echo "  家里 Tendis 2主节点集群搭建"
echo "=========================================="

# ========================
# Step 1: 停止并删除旧容器
# ========================
echo ""
echo "[Step 1] 停止并删除旧容器..."
ssh $REMOTE "$DOCKER stop tendis-src tendis-dst tendis-src-7001 tendis-src-7002 tendis-dst-8001 tendis-dst-8002 2>/dev/null; $DOCKER rm tendis-src tendis-dst tendis-src-7001 tendis-src-7002 tendis-dst-8001 tendis-dst-8002 2>/dev/null; echo 'Old containers cleaned'"

# ========================
# Step 2: 创建配置文件和数据目录
# ========================
echo ""
echo "[Step 2] 创建配置文件和数据目录..."

# 源集群节点 1: 7001
ssh $REMOTE "mkdir -p /tmp/tendis-cluster/src-7001/data/log /tmp/tendis-cluster/src-7001/data/dump"
ssh $REMOTE "cat > /tmp/tendis-cluster/src-7001/tendisplus.conf << 'EOF'
bind 0.0.0.0
port 7001
loglevel notice
cluster-enabled on
storage rocks
dir /data
rocks.blockcachemb 128
kvstorecount 10
binlog-enabled yes
maxBinlogKeepNum 10000000
minBinlogKeepSec 86400
logdir /data/log
dumpdir /data/dump
truncateBinlogIntervalMs 1000
truncateBinlogNum 10000
maxclients 10000
EOF"

# 源集群节点 2: 7002
ssh $REMOTE "mkdir -p /tmp/tendis-cluster/src-7002/data/log /tmp/tendis-cluster/src-7002/data/dump"
ssh $REMOTE "cat > /tmp/tendis-cluster/src-7002/tendisplus.conf << 'EOF'
bind 0.0.0.0
port 7002
loglevel notice
cluster-enabled on
storage rocks
dir /data
rocks.blockcachemb 128
kvstorecount 10
binlog-enabled yes
maxBinlogKeepNum 10000000
minBinlogKeepSec 86400
logdir /data/log
dumpdir /data/dump
truncateBinlogIntervalMs 1000
truncateBinlogNum 10000
maxclients 10000
EOF"

# 目标集群节点 1: 8001
ssh $REMOTE "mkdir -p /tmp/tendis-cluster/dst-8001/data/log /tmp/tendis-cluster/dst-8001/data/dump"
ssh $REMOTE "cat > /tmp/tendis-cluster/dst-8001/tendisplus.conf << 'EOF'
bind 0.0.0.0
port 8001
loglevel notice
cluster-enabled on
storage rocks
dir /data
rocks.blockcachemb 128
kvstorecount 10
binlog-enabled yes
maxBinlogKeepNum 10000000
minBinlogKeepSec 86400
logdir /data/log
dumpdir /data/dump
truncateBinlogIntervalMs 1000
truncateBinlogNum 10000
maxclients 10000
EOF"

# 目标集群节点 2: 8002
ssh $REMOTE "mkdir -p /tmp/tendis-cluster/dst-8002/data/log /tmp/tendis-cluster/dst-8002/data/dump"
ssh $REMOTE "cat > /tmp/tendis-cluster/dst-8002/tendisplus.conf << 'EOF'
bind 0.0.0.0
port 8002
loglevel notice
cluster-enabled on
storage rocks
dir /data
rocks.blockcachemb 128
kvstorecount 10
binlog-enabled yes
maxBinlogKeepNum 10000000
minBinlogKeepSec 86400
logdir /data/log
dumpdir /data/dump
truncateBinlogIntervalMs 1000
truncateBinlogNum 10000
maxclients 10000
EOF"

echo "配置文件创建完成"

# ========================
# Step 3: 启动 4 个容器 (host 网络模式)
# ========================
echo ""
echo "[Step 3] 启动容器 (host 网络模式)..."

# 源集群
ssh $REMOTE "$DOCKER run -d --name tendis-src-7001 --network host \
  -v /tmp/tendis-cluster/src-7001/tendisplus.conf:/opt/midd/tendisplus/conf/tendisplus.conf \
  -v /tmp/tendis-cluster/src-7001/data:/data \
  $IMAGE"

ssh $REMOTE "$DOCKER run -d --name tendis-src-7002 --network host \
  -v /tmp/tendis-cluster/src-7002/tendisplus.conf:/opt/midd/tendisplus/conf/tendisplus.conf \
  -v /tmp/tendis-cluster/src-7002/data:/data \
  $IMAGE"

# 目标集群
ssh $REMOTE "$DOCKER run -d --name tendis-dst-8001 --network host \
  -v /tmp/tendis-cluster/dst-8001/tendisplus.conf:/opt/midd/tendisplus/conf/tendisplus.conf \
  -v /tmp/tendis-cluster/dst-8001/data:/data \
  $IMAGE"

ssh $REMOTE "$DOCKER run -d --name tendis-dst-8002 --network host \
  -v /tmp/tendis-cluster/dst-8002/tendisplus.conf:/opt/midd/tendisplus/conf/tendisplus.conf \
  -v /tmp/tendis-cluster/dst-8002/data:/data \
  $IMAGE"

echo "容器启动完成，等待 Tendis 就绪..."
sleep 5

# ========================
# Step 4: 验证节点启动
# ========================
echo ""
echo "[Step 4] 验证节点启动..."
for port in 7001 7002 8001 8002; do
  result=$(ssh $REMOTE "$DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p $port ping 2>/dev/null" || echo "FAIL")
  echo "  端口 $port: $result"
done

# ========================
# Step 5: 组建源集群 (CLUSTER MEET + ADDSLOTS)
# ========================
echo ""
echo "[Step 5] 组建源集群 (7001 + 7002)..."

# 获取宿主机 IP（用于 CLUSTER MEET）
# host 网络模式下，节点间通过宿主机 IP 通信
ssh $REMOTE "$DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p 7001 CLUSTER MEET 127.0.0.1 7002"
sleep 2

# 分配 slot: 7001 -> 0-8191, 7002 -> 8192-16383
echo "  分配 slot 给 7001 (0-8191)..."
ssh $REMOTE "for i in \$(seq 0 8191); do $DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p 7001 CLUSTER ADDSLOTS \$i; done"

echo "  分配 slot 给 7002 (8192-16383)..."
ssh $REMOTE "for i in \$(seq 8192 16383); do $DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p 7002 CLUSTER ADDSLOTS \$i; done"

echo "源集群组建完成"

# ========================
# Step 6: 组建目标集群 (CLUSTER MEET + ADDSLOTS)
# ========================
echo ""
echo "[Step 6] 组建目标集群 (8001 + 8002)..."

ssh $REMOTE "$DOCKER exec tendis-dst-8001 redis-cli -h 127.0.0.1 -p 8001 CLUSTER MEET 127.0.0.1 8002"
sleep 2

# 分配 slot: 8001 -> 0-8191, 8002 -> 8192-16383
echo "  分配 slot 给 8001 (0-8191)..."
ssh $REMOTE "for i in \$(seq 0 8191); do $DOCKER exec tendis-dst-8001 redis-cli -h 127.0.0.1 -p 8001 CLUSTER ADDSLOTS \$i; done"

echo "  分配 slot 给 8002 (8192-16383)..."
ssh $REMOTE "for i in \$(seq 8192 16383); do $DOCKER exec tendis-dst-8001 redis-cli -h 127.0.0.1 -p 8002 CLUSTER ADDSLOTS \$i; done"

echo "目标集群组建完成"

# ========================
# Step 7: 验证集群状态
# ========================
echo ""
echo "[Step 7] 验证集群状态..."
echo ""
echo "--- 源集群 ---"
ssh $REMOTE "$DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p 7001 CLUSTER INFO | head -4"
echo ""
ssh $REMOTE "$DOCKER exec tendis-src-7001 redis-cli -h 127.0.0.1 -p 7001 CLUSTER NODES"

echo ""
echo "--- 目标集群 ---"
ssh $REMOTE "$DOCKER exec tendis-dst-8001 redis-cli -h 127.0.0.1 -p 8001 CLUSTER INFO | head -4"
echo ""
ssh $REMOTE "$DOCKER exec tendis-dst-8001 redis-cli -h 127.0.0.1 -p 8001 CLUSTER NODES"

# ========================
# Step 8: 从 Mac 验证可达性
# ========================
echo ""
echo "[Step 8] 从本地 Mac 验证可达性..."
for port in 7001 7002 8001 8002; do
  result=$(redis-cli -h $HOST_IP -p $port ping 2>/dev/null || echo "FAIL")
  echo "  $HOST_IP:$port -> $result"
done

echo ""
echo "=========================================="
echo "  部署完成！"
echo "=========================================="
echo ""
echo "  源集群: $HOST_IP:7001, $HOST_IP:7002"
echo "  目标集群: $HOST_IP:8001, $HOST_IP:8002"
echo ""
echo "  从 Mac 连接: redis-cli -h $HOST_IP -p 7001"
echo ""
