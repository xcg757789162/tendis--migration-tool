#!/bin/bash

# 打包脚本 - 用于将项目打包并移植到其他机器

set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$SCRIPT_DIR"

PACKAGE_NAME="tendis-migrate-$(date +%Y%m%d).tar.gz"

echo "正在打包项目..."

# 排除不需要的文件
tar -czvf "../$PACKAGE_NAME" \
    --exclude='node_modules' \
    --exclude='dist' \
    --exclude='data' \
    --exclude='*.db' \
    --exclude='*.sqlite' \
    --exclude='.DS_Store' \
    --exclude='*.log' \
    -C .. tendis-migrate

echo ""
echo "打包完成: ../$PACKAGE_NAME"
echo ""
echo "移植步骤:"
echo "1. 将 $PACKAGE_NAME 复制到目标机器"
echo "2. 解压: tar -xzvf $PACKAGE_NAME"
echo "3. 进入目录: cd tendis-migrate"
echo "4. 启动: ./docker-start.sh"
