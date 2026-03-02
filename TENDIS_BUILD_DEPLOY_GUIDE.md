# Tendis 2.7.0 编译部署最佳实践

> 基于在 CentOS 8 x86_64 上从源码编译 Tendis 2.7.0-rocksdb-v8.5.3、构建 Docker 镜像、部署双集群的完整实战经验总结。

## 目录

- [一、环境准备](#一环境准备)
- [二、源码准备与子模块克隆](#二源码准备与子模块克隆)
- [三、打补丁与修复编译问题](#三打补丁与修复编译问题)
- [四、编译 Tendis](#四编译-tendis)
- [五、构建 Docker 镜像](#五构建-docker-镜像)
- [六、部署 Tendis 集群](#六部署-tendis-集群)
- [七、推送镜像到仓库](#七推送镜像到仓库)
- [八、踩坑总结](#八踩坑总结)

---

## 一、环境准备

### 1.1 系统要求

- **OS**: CentOS 8 / CentOS Stream 8+ (x86_64)
- **内存**: >= 4GB（编译 RocksDB 较耗内存）
- **磁盘**: >= 10GB 可用空间
- **Docker**: >= 20.x

### 1.2 安装编译依赖

```bash
# CentOS 8 需要启用 PowerTools 仓库
dnf install -y epel-release
dnf config-manager --set-enabled powertools  # CentOS 8
# dnf config-manager --set-enabled crb       # CentOS Stream 9+

dnf install -y \
  gcc gcc-c++ make cmake git \
  snappy-devel lz4-devel libzstd-devel \
  gflags-devel bzip2-devel \
  autoconf automake libtool \
  which tar
```

---

## 二、源码准备与子模块克隆

### 2.1 解压源码

```bash
cd /home
tar -xzf Tendis-2.7.0-rocksdb-v8.5.3.tar.gz
cd Tendis-2.7.0-rocksdb-v8.5.3
```

### 2.2 克隆 9 个 git 子模块

**关键**：tar 包不包含 git 子模块内容，必须手动克隆！

```bash
SRC=/home/Tendis-2.7.0-rocksdb-v8.5.3

# 1. glog v0.4.0
rm -rf $SRC/src/thirdparty/glog && \
git clone --depth 1 --branch v0.4.0 https://github.com/google/glog.git $SRC/src/thirdparty/glog

# 2. rapidjson (master)
rm -rf $SRC/src/thirdparty/rapidjson && \
git clone --depth 1 https://github.com/Tencent/rapidjson.git $SRC/src/thirdparty/rapidjson

# 3. googletest release-1.10.0
rm -rf $SRC/src/thirdparty/googletest && \
git clone --depth 1 --branch release-1.10.0 https://github.com/google/googletest.git $SRC/src/thirdparty/googletest

# 4. snappy 1.1.8
rm -rf $SRC/src/thirdparty/snappy && \
git clone --depth 1 --branch 1.1.8 https://github.com/google/snappy.git $SRC/src/thirdparty/snappy

# 5. lz4 v1.9.2
rm -rf $SRC/src/thirdparty/lz4 && \
git clone --depth 1 --branch v1.9.2 https://github.com/lz4/lz4.git $SRC/src/thirdparty/lz4

# 6. jemalloc 5.2.1
rm -rf $SRC/src/thirdparty/jemalloc && \
git clone --depth 1 --branch 5.2.1 https://github.com/jemalloc/jemalloc.git $SRC/src/thirdparty/jemalloc

# 7. asio (asio-1-12-0)
rm -rf $SRC/src/thirdparty/asio && \
git clone --depth 1 --branch asio-1-12-0 https://github.com/chriskohlhoff/asio.git $SRC/src/thirdparty/asio

# 8. gflags v2.2.2
rm -rf $SRC/src/thirdparty/gflags && \
git clone --depth 1 --branch v2.2.2 https://github.com/gflags/gflags.git $SRC/src/thirdparty/gflags

# 9. RocksDB v8.5.3
rm -rf $SRC/src/thirdparty/rocksdb && \
git clone --depth 1 --branch v8.5.3 https://github.com/facebook/rocksdb.git $SRC/src/thirdparty/rocksdb
```

---

## 三、打补丁与修复编译问题

### 3.1 RocksDB 补丁（必须）

Tendis 对 RocksDB 有两个自定义补丁，位于 `src/thirdparty/patches/` 目录：

```bash
cd $SRC/src/thirdparty/rocksdb

# 补丁1: SST 文件信息统计
git am ../patches/0001-add-statistic-for-sst-file-info.patch

# 补丁2: 延迟统计日志
git am ../patches/0002-add-latency-statistic-log.patch
```

### 3.2 修复 glog CMake 冲突（必须）

glog 的 CMakeLists.txt 中有 export/install 命令会与 Tendis 的构建系统冲突，需要注释掉：

```bash
cd $SRC/src/thirdparty/glog
```

编辑 `CMakeLists.txt`，找到约第 670-690 行，注释掉以下三行：

```cmake
# 注释掉这三行（约第675、676、684行）：
# export (TARGETS glog NAMESPACE glog:: FILE ${CMAKE_BINARY_DIR}/glog-targets.cmake)
# export (PACKAGE glog)
# install (EXPORT glog-targets NAMESPACE glog:: DESTINATION ${_glog_CMake_INSTALLDIR})
```

可用 sed 命令一键处理：

```bash
sed -i \
  -e '/^export (TARGETS glog/s/^/#/' \
  -e '/^export (PACKAGE glog)/s/^/#/' \
  -e '/^install (EXPORT glog-targets/s/^/#/' \
  $SRC/src/thirdparty/glog/CMakeLists.txt
```

---

## 四、编译 Tendis

### 4.1 CMake 配置

```bash
cd $SRC
mkdir -p build && cd build

cmake .. \
  -DCMAKE_BUILD_TYPE=Release \
  -DNEW_ROCKSDB=ON \
  -DCOM_OPT_LTO=OFF \
  -DUSE_RTTI=ON
```

**关键参数说明**：

| 参数 | 值 | 说明 |
|------|-----|------|
| `CMAKE_BUILD_TYPE` | Release | 生产模式，开启优化 |
| `NEW_ROCKSDB` | ON | 使用 RocksDB v8.5.3 |
| `COM_OPT_LTO` | **OFF** | **必须关闭**，否则 LTO 链接器找不到标准库符号 |
| `USE_RTTI` | **ON** | **必须开启**，否则 RocksDB Release 模式禁用 RTTI 导致 typeinfo 缺失 |

### 4.2 编译

```bash
make -j$(nproc)
```

编译时间约 10-20 分钟（取决于 CPU 核数）。

### 4.3 验证

```bash
# 二进制位于 build/bin/tendisplus
./bin/tendisplus --version

# strip 减小体积（约 200MB → 15MB）
strip bin/tendisplus
ls -lh bin/tendisplus
```

---

## 五、构建 Docker 镜像

### 5.1 准备构建目录

```bash
mkdir -p /home/docker-build
cp $SRC/build/bin/tendisplus /home/docker-build/
strip /home/docker-build/tendisplus
```

### 5.2 Dockerfile

```bash
cat > /home/docker-build/Dockerfile << 'EOF'
FROM centos:8

# 替换为阿里云镜像源（CentOS 8 已 EOL）
RUN cd /etc/yum.repos.d/ && \
    sed -i 's/mirrorlist/#mirrorlist/g' /etc/yum.repos.d/CentOS-*.repo && \
    sed -i 's|#baseurl=http://mirror.centos.org|baseurl=http://vault.centos.org|g' /etc/yum.repos.d/CentOS-*.repo && \
    dnf install -y snappy lz4-libs libzstd gflags && \
    dnf clean all

COPY tendisplus /usr/local/bin/tendisplus
RUN chmod +x /usr/local/bin/tendisplus

EXPOSE 6379
CMD ["tendisplus", "--help"]
EOF
```

### 5.3 构建镜像

```bash
cd /home/docker-build
docker build -t tendisplus:v2.7.0 .
```

可选：打上仓库 tag

```bash
# 华为云 SWR
docker tag tendisplus:v2.7.0 swr.cn-southwest-2.myhuaweicloud.com/tendis/tendisplus:v2.7.0

# 阿里云 ACR
docker tag tendisplus:v2.7.0 registry.cn-zhangjiakou.aliyuncs.com/xiaoduoai/devops:tendisplus-v2.7.0
```

---

## 六、部署 Tendis 集群

以下示例部署一个**源集群（2 主节点）+ 目标集群（2 主节点）**的测试环境。

### 6.1 网络模式选择

**推荐使用 `--network host` 模式**，原因：
- Tendis 2.7.0 **不支持** `cluster-announce-ip` 参数
- bridge 模式下容器间 cluster meet 会使用容器内部 IP，导致集群通信失败
- host 模式直接使用宿主机 IP，集群通信无障碍

### 6.2 创建数据目录

```bash
HOST_IP="192.168.0.142"  # 替换为你的宿主机内网 IP
IMAGE="swr.cn-southwest-2.myhuaweicloud.com/tendis/tendisplus:v2.7.0"

mkdir -p /data/tendis/{7001,7002,8001,8002}
```

### 6.3 启动 4 个 Tendis 容器

```bash
for PORT in 7001 7002 8001 8002; do
  docker run -d \
    --name tendis-${PORT} \
    --network host \
    --restart always \
    -v /data/tendis/${PORT}:/data \
    ${IMAGE} \
    tendisplus \
      --bind 0.0.0.0 \
      --port ${PORT} \
      --dir /data \
      --logdir /data \
      --loglevel notice \
      --cluster-enabled true \
      --kvstorecount 2 \
      --binlog-enabled yes \
      --daemon no
done
```

### 6.4 组建源集群（7001 + 7002）

```bash
# 节点握手
redis-cli -h ${HOST_IP} -p 7001 cluster meet ${HOST_IP} 7002

# 等待几秒让集群发现完成
sleep 3

# 分配 slots
# 7001: slots 0-8191
for i in $(seq 0 8191); do
  redis-cli -h ${HOST_IP} -p 7001 cluster addslots $i
done

# 7002: slots 8192-16383
for i in $(seq 8192 16383); do
  redis-cli -h ${HOST_IP} -p 7002 cluster addslots $i
done

# 验证
redis-cli -h ${HOST_IP} -p 7001 cluster info
redis-cli -h ${HOST_IP} -p 7001 cluster nodes
```

### 6.5 组建目标集群（8001 + 8002）

```bash
redis-cli -h ${HOST_IP} -p 8001 cluster meet ${HOST_IP} 8002
sleep 3

for i in $(seq 0 8191); do
  redis-cli -h ${HOST_IP} -p 8001 cluster addslots $i
done

for i in $(seq 8192 16383); do
  redis-cli -h ${HOST_IP} -p 8002 cluster addslots $i
done

redis-cli -h ${HOST_IP} -p 8001 cluster info
redis-cli -h ${HOST_IP} -p 8001 cluster nodes
```

### 6.6 验证集群状态

```bash
# 两个集群都应该显示 cluster_state:ok
redis-cli -h ${HOST_IP} -p 7001 cluster info | grep cluster_state
redis-cli -h ${HOST_IP} -p 8001 cluster info | grep cluster_state

# 写入测试数据
redis-cli -h ${HOST_IP} -p 7001 -c set test:hello world
redis-cli -h ${HOST_IP} -p 7001 -c get test:hello
```

---

## 七、推送镜像到仓库

### 7.1 华为云 SWR

```bash
# 登录（替换为你的 AK/SK）
docker login -u cn-southwest-2@<AK> -p <SK> swr.cn-southwest-2.myhuaweicloud.com

# 打 tag 并推送
docker tag tendisplus:v2.7.0 swr.cn-southwest-2.myhuaweicloud.com/<组织名>/tendisplus:v2.7.0
docker push swr.cn-southwest-2.myhuaweicloud.com/<组织名>/tendisplus:v2.7.0
```

> **注意**：华为云 SWR 必须先在控制台创建组织（namespace），否则推送会报 `name unknown` 错误。

### 7.2 阿里云 ACR

```bash
docker login --username=<用户名> registry.cn-zhangjiakou.aliyuncs.com
docker tag tendisplus:v2.7.0 registry.cn-zhangjiakou.aliyuncs.com/<命名空间>/tendisplus:v2.7.0
docker push registry.cn-zhangjiakou.aliyuncs.com/<命名空间>/tendisplus:v2.7.0
```

---

## 八、踩坑总结

### 问题 1：tar 包不含 git 子模块

**现象**：`cmake` 报找不到 glog、rocksdb 等目录下的 CMakeLists.txt

**原因**：tar 打包时不会包含 `.git` 子模块的实际文件

**解决**：手动 `git clone` 全部 9 个子模块，版本号必须匹配（见第二节）

---

### 问题 2：LTO 链接失败

**现象**：

```
lto1: fatal error: bytecode stream in file '/usr/lib/gcc/x86_64-redhat-linux/8/libstdc++.a' 
generated with LTO version 8.0 instead of the expected 9.4
```

**原因**：LTO（Link Time Optimization）要求所有目标文件使用相同 LTO 版本编译，系统预编译的 libstdc++.a 版本不匹配

**解决**：关闭 LTO → `-DCOM_OPT_LTO=OFF`

---

### 问题 3：RTTI typeinfo 缺失

**现象**：

```
undefined reference to `typeinfo for rocksdb::Logger'
undefined reference to `typeinfo for rocksdb::CompactionFilter'
```

**原因**：RocksDB 在 Release 模式默认禁用 RTTI（`-fno-rtti`），但 Tendis 代码使用了 `dynamic_cast` 等需要 RTTI 的特性

**解决**：强制开启 RTTI → `-DUSE_RTTI=ON`

---

### 问题 4：glog CMake export 冲突

**现象**：

```
CMake Error: install(EXPORT "glog-targets" ...) includes target "glog" which requires target 
"gflags" that is not in any export set.
```

**原因**：glog 的 CMakeLists.txt 尝试 export 自身的 target，但依赖的 gflags 不在同一个 export set 中

**解决**：注释掉 glog/CMakeLists.txt 中的 3 行 export/install 命令

---

### 问题 5：Tendis 不支持 cluster-announce-ip

**现象**：启动报 `unrecognized option 'cluster-announce-ip'`

**原因**：Tendis 2.7.0 未实现此 Redis 5.0+ 特性

**解决**：使用 `--network host` 模式部署 Docker 容器，避免需要 announce IP

---

### 问题 6：华为云 SWR 推送报 name unknown

**现象**：`docker push` 报错 `name unknown: The specified repository or organization does not exist`

**原因**：华为云 SWR 要求提前在控制台创建组织（namespace）

**解决**：在华为云 SWR 控制台 → 组织管理 → 创建组织，然后使用创建好的组织名作为镜像路径的 namespace

---

### 问题 7：不同 CPU 导致 SIGILL (exit code 132)

**现象**：Docker 容器启动后立即退出，exit code 132，`docker logs` 无输出

**原因**：编译时 RocksDB 使用 `-march=native`，如果编译机 CPU 支持 AVX-512（如 Intel Xeon），但运行机 CPU 只支持 AVX2（如 AMD EPYC 7K62），就会出现非法指令（SIGILL）

**诊断方法**：
```bash
# 检查 CPU 指令集
cat /proc/cpuinfo | grep flags | head -1 | tr ' ' '\n' | grep -E 'avx|sse4|bmi'

# 直接在宿主机运行测试
/path/to/tendisplus --help
# 如果报 "Illegal instruction" 就是 CPU 不兼容
```

**解决**：在目标机器上重新从源码编译 Tendis，关键参数：
```bash
cmake .. -DCMAKE_BUILD_TYPE=Release -DNEW_ROCKSDB=ON -DCOM_OPT_LTO=OFF -DUSE_RTTI=ON
```

---

### 问题 8：容器内 GLIBC 版本不兼容

**现象**：`tendisplus: /lib64/libm.so.6: version 'GLIBC_2.38' not found`

**原因**：宿主机是新系统（如 TencentOS 4, glibc 2.38），但 Docker 基础镜像是 CentOS 8（glibc 2.28）。编译出的二进制链接了宿主机的高版本 glibc

**解决方案**：
1. 使用与宿主机 glibc 兼容的基础镜像（如 `opencloudos/opencloudos:9.0`）
2. 或者在 CentOS 8 容器内编译（保证 glibc 一致）
3. 或者静态链接编译

---

### 问题 9：链接时找不到 -lstdc++

**现象**：`/usr/bin/ld: cannot find -lstdc++: No such file or directory`

**原因**：系统只有 libstdc++ 动态库，没有静态库

**解决**：安装 libstdc++ 静态库
```bash
dnf install -y libstdc++-static
```

---

### 问题 10：RocksDB 子目录结构

**现象**：cmake 报 `add_subdirectory given source "xxx/rocksdb/rocksdb" which is not an existing directory`

**原因**：Tendis CMakeLists.txt 中 `ROCKSDB_DIR` 设为 `src/thirdparty/rocksdb`，然后 `add_subdirectory(${ROCKSDB_DIR}/rocksdb ...)`，即期望 RocksDB 源码在 `src/thirdparty/rocksdb/rocksdb/` 嵌套目录下

**解决**：克隆 RocksDB 到正确的嵌套路径：
```bash
# 错误：git clone ... $SRC/src/thirdparty/rocksdb
# 正确：
mkdir -p $SRC/src/thirdparty/rocksdb
git clone --depth 1 --branch v8.5.3 https://github.com/facebook/rocksdb.git $SRC/src/thirdparty/rocksdb/rocksdb
```

---

### 问题 11：Tendis 镜像必须用配置文件启动

**现象**：Docker CMD 使用命令行参数 `tendisplus --bind 0.0.0.0 --port 7001 ...` 启动失败

**原因**：Tendis 2.7.0 的参数解析是读配置文件，不支持命令行 `--key value` 格式

**解决**：创建配置文件挂载到容器内：
```bash
docker run -d --name tendis-7001 --network host \
  -v /data/tendis/7001:/data \
  ${IMAGE} tendisplus /data/tendisplus.conf
```

---

### 问题 12：Docker overlay2 积累 Tendis dump 文件导致磁盘 100%（重要！）

**现象**：回归测试运行到后半段时，所有写入操作返回 `ERR:3,msg:db stopped!`，目标端数据只写入一半，`df -h /data` 显示 100%

**根因**：
1. Tendis 的 `dumpdir` 默认为 `./dump`（相对路径），在 Docker 容器中写入到 overlay2 diff 层
2. RocksDB 的 SST dump 文件不受宿主机 `-v /data/tendis/xxx:/data` 挂载管控
3. 大量回归测试持续读写，dump 文件在 overlay2 层累积到 46-97GB
4. 分区满后 Tendis 进入 `db stopped` 只读模式

**典型症状**：
- 回归测试恰好一半数据写入成功（50/100, 5/10, 750/1500）
- `du -sh /data/docker/lib/overlay2/` 显示数十 GB

**解决**：**直接在宿主机运行 Tendis，不用 Docker**（见下方"附录 B"）

---

### 问题 13：Docker 容器中 daemon 模式导致容器立即退出

**现象**：Docker 容器创建后立即退出（exit code 0），不断重启

**根因**：Tendis 默认 `daemon:yes`，fork 出子进程后父进程退出。Docker 监控 PID 1，父进程退出后容器停止

**解决**：
- **Docker 容器**内必须配置 `daemon no`（前台模式）
- **宿主机直接运行**用 `daemon yes`（后台模式）

---

## 附录 A：Docker 快速一键部署脚本

如果已有镜像，可以用以下脚本快速部署集群。

**重要**：必须使用配置文件方式启动，不能用命令行参数。

**注意**：Docker 部署存在 overlay2 磁盘爆满风险（见问题 12），如果频繁运行回归测试，**强烈建议使用附录 B 的宿主机直接部署方式**。

```bash
#!/bin/bash
# deploy-tendis-cluster.sh
# 用法: bash deploy-tendis-cluster.sh <宿主机IP> <镜像地址> [kvstorecount]

HOST_IP=${1:?"用法: $0 <宿主机IP> <镜像地址> [kvstorecount]"}
IMAGE=${2:?"用法: $0 <宿主机IP> <镜像地址> [kvstorecount]"}
KVSTORECOUNT=${3:-2}

echo "=== 创建数据目录和配置文件 ==="
mkdir -p /data/tendis/{7001,7002,8001,8002}

for PORT in 7001 7002 8001 8002; do
cat > /data/tendis/${PORT}/tendisplus.conf << EOF
bind 0.0.0.0
port ${PORT}
dir /data
logdir /data
dumpdir /data/dump
loglevel notice
cluster-enabled true
kvstorecount ${KVSTORECOUNT}
binlog-enabled yes
daemon no
dump-file-keep-num 1
dump-file-keep-hour 1
rocks.blockcachemb 128
rocks.write-buffer-size 8388608
rocks.max-write-buffer-number 2
EOF
done

echo "=== 启动容器 ==="
for PORT in 7001 7002 8001 8002; do
  docker rm -f tendis-${PORT} 2>/dev/null
  docker run -d \
    --name tendis-${PORT} \
    --network host \
    --restart always \
    -v /data/tendis/${PORT}:/data \
    ${IMAGE} \
    tendisplus /data/tendisplus.conf
done

sleep 5

echo "=== 组建源集群 (7001+7002) ==="
redis-cli -h ${HOST_IP} -p 7001 cluster meet ${HOST_IP} 7002
sleep 3
for i in $(seq 0 8191); do redis-cli -h ${HOST_IP} -p 7001 cluster addslots $i > /dev/null; done
for i in $(seq 8192 16383); do redis-cli -h ${HOST_IP} -p 7002 cluster addslots $i > /dev/null; done

echo "=== 组建目标集群 (8001+8002) ==="
redis-cli -h ${HOST_IP} -p 8001 cluster meet ${HOST_IP} 8002
sleep 3
for i in $(seq 0 8191); do redis-cli -h ${HOST_IP} -p 8001 cluster addslots $i > /dev/null; done
for i in $(seq 8192 16383); do redis-cli -h ${HOST_IP} -p 8002 cluster addslots $i > /dev/null; done

sleep 3

echo "=== 集群状态 ==="
echo "--- 源集群 ---"
redis-cli -h ${HOST_IP} -p 7001 cluster info | grep -E "cluster_state|cluster_known_nodes|cluster_slots"
echo "--- 目标集群 ---"
redis-cli -h ${HOST_IP} -p 8001 cluster info | grep -E "cluster_state|cluster_known_nodes|cluster_slots"

echo "=== 完成! ==="
echo "源集群: ${HOST_IP}:7001, ${HOST_IP}:7002"
echo "目标集群: ${HOST_IP}:8001, ${HOST_IP}:8002"
```

使用方式：

```bash
bash deploy-tendis-cluster.sh 21.214.66.163 tendisplus:v2.7.0-local 2
```

---

## 附录 B：宿主机直接部署 Tendis（推荐）

**推荐理由**：
- 避免 Docker overlay2 磁盘爆满问题
- dump 文件、数据文件、日志文件完全可控
- 更易调试和监控
- 适合频繁运行回归测试的开发环境

### 前置条件

已在宿主机编译好 Tendis，二进制路径如 `/home/Tendis-2.7.0-rocksdb-v8.5.3/build/bin/tendisplus`。

### 一键部署脚本

```bash
#!/bin/bash
# deploy-tendis-native.sh - 宿主机直接部署 Tendis 集群
# 用法: bash deploy-tendis-native.sh <宿主机IP> <tendisplus二进制路径> [kvstorecount]
#
# 示例:
#   bash deploy-tendis-native.sh 21.214.66.163 /home/Tendis-2.7.0-rocksdb-v8.5.3/build/bin/tendisplus 2

HOST_IP=${1:?"用法: $0 <宿主机IP> <tendisplus路径> [kvstorecount]"}
TENDIS_BIN=${2:?"用法: $0 <宿主机IP> <tendisplus路径> [kvstorecount]"}
KVSTORECOUNT=${3:-2}

if [ ! -f "$TENDIS_BIN" ]; then
    echo "错误: 找不到 tendisplus 二进制: $TENDIS_BIN"
    exit 1
fi

echo "=== 停止已有的 Tendis 进程 ==="
for PORT in 7001 7002 8001 8002; do
    redis-cli -h ${HOST_IP} -p ${PORT} SHUTDOWN NOSAVE 2>/dev/null || true
done
sleep 2

echo "=== 清理旧数据（可选，回归测试建议清理）==="
for PORT in 7001 7002 8001 8002; do
    rm -rf /data/tendis/${PORT}
done

echo "=== 创建数据目录和配置文件 ==="
for PORT in 7001 7002 8001 8002; do
    mkdir -p /data/tendis/${PORT}/log /data/tendis/${PORT}/dump
    cat > /data/tendis/${PORT}/tendis.conf << EOF
bind 0.0.0.0
port ${PORT}
dir /data/tendis/${PORT}
logdir /data/tendis/${PORT}/log
dumpdir /data/tendis/${PORT}/dump
loglevel notice
cluster-enabled true
kvstorecount ${KVSTORECOUNT}
binlog-enabled yes
daemon yes
dump-file-keep-num 1
dump-file-keep-hour 1
rocks.blockcachemb 128
rocks.write-buffer-size 8388608
rocks.max-write-buffer-number 2
EOF
done

echo "=== 启动 Tendis ==="
for PORT in 7001 7002 8001 8002; do
    ${TENDIS_BIN} /data/tendis/${PORT}/tendis.conf
    echo "Started port ${PORT}"
done

sleep 3

echo "=== 验证启动 ==="
ALL_OK=true
for PORT in 7001 7002 8001 8002; do
    PONG=$(redis-cli -h ${HOST_IP} -p ${PORT} PING 2>/dev/null)
    if [ "$PONG" = "PONG" ]; then
        echo "  ✓ ${HOST_IP}:${PORT} PONG"
    else
        echo "  ✗ ${HOST_IP}:${PORT} 启动失败!"
        ALL_OK=false
    fi
done

if [ "$ALL_OK" = "false" ]; then
    echo "有节点启动失败，检查日志: /data/tendis/*/log/"
    exit 1
fi

echo "=== 组建源集群 (7001+7002) ==="
redis-cli -h ${HOST_IP} -p 7001 cluster meet ${HOST_IP} 7002
sleep 3
for i in $(seq 0 8191); do redis-cli -h ${HOST_IP} -p 7001 cluster addslots $i > /dev/null; done
for i in $(seq 8192 16383); do redis-cli -h ${HOST_IP} -p 7002 cluster addslots $i > /dev/null; done

echo "=== 组建目标集群 (8001+8002) ==="
redis-cli -h ${HOST_IP} -p 8001 cluster meet ${HOST_IP} 8002
sleep 3
for i in $(seq 0 8191); do redis-cli -h ${HOST_IP} -p 8001 cluster addslots $i > /dev/null; done
for i in $(seq 8192 16383); do redis-cli -h ${HOST_IP} -p 8002 cluster addslots $i > /dev/null; done

sleep 3

echo "=== 集群状态 ==="
echo "--- 源集群 ---"
redis-cli -h ${HOST_IP} -p 7001 cluster info | grep -E "cluster_state|cluster_known_nodes|cluster_slots"
echo "--- 目标集群 ---"
redis-cli -h ${HOST_IP} -p 8001 cluster info | grep -E "cluster_state|cluster_known_nodes|cluster_slots"

echo ""
echo "=== 部署完成 ==="
echo "源集群: ${HOST_IP}:7001, ${HOST_IP}:7002"
echo "目标集群: ${HOST_IP}:8001, ${HOST_IP}:8002"
echo ""
echo "配置关键参数:"
echo "  kvstorecount=${KVSTORECOUNT}, binlog-enabled=yes"
echo "  dumpdir 在各节点 /data/tendis/PORT/dump/"
echo "  dump-file-keep-num=1, dump-file-keep-hour=1（防止磁盘占满）"
```

### 停止 Tendis

```bash
for PORT in 7001 7002 8001 8002; do
    redis-cli -h <HOST_IP> -p ${PORT} SHUTDOWN NOSAVE
done
```

### Docker 方式 vs 宿主机方式对比

| 方面 | Docker 部署 | 宿主机直接部署 |
|------|-------------|----------------|
| 磁盘风险 | overlay2 可能积累 dump 文件，导致磁盘爆满 | 所有文件都在 /data/tendis/ 下，完全可控 |
| daemon 模式 | 必须 `daemon no`（前台） | 用 `daemon yes`（后台） |
| 调试便利 | 需要 docker logs/exec | 直接查看日志文件 |
| 环境隔离 | 隔离好，但增加复杂度 | 依赖宿主机环境 |
| 适用场景 | 生产环境、需要环境隔离时 | 开发测试环境、频繁运行回归测试 |

### 配置要点

| 配置项 | Docker | 宿主机 | 说明 |
|--------|--------|--------|------|
| `daemon` | no | yes | Docker 必须前台，宿主机用后台 |
| `dumpdir` | /data/dump（挂载卷内） | /data/tendis/PORT/dump | 必须指向可控路径 |
| `dump-file-keep-num` | 1 | 1 | 只保留 1 个 dump，防止积累 |
| `dump-file-keep-hour` | 1 | 1 | dump 文件只保留 1 小时 |
