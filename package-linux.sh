#!/bin/bash
# 打包 Linux 版本

cd "$(dirname "$0")"

VERSION=$(date +%Y%m%d%H%M%S)
RELEASE_DIR="release-linux-$VERSION"

mkdir -p "$RELEASE_DIR/web/dist" "$RELEASE_DIR/logs"

# 复制文件
cp simple-server-linux "$RELEASE_DIR/simple-server"
cp -r web/dist/* "$RELEASE_DIR/web/dist/"

# 创建启动脚本
cat > "$RELEASE_DIR/start.sh" << 'STARTEOF'
#!/bin/bash
cd "$(dirname "$0")"
mkdir -p logs
pkill -f simple-server 2>/dev/null || true
sleep 1
nohup ./simple-server > logs/stdout.log 2>&1 &
sleep 2
if pgrep -f simple-server > /dev/null; then
    IP=$(hostname -I 2>/dev/null | awk '{print $1}' || echo "localhost")
    echo "=========================================="
    echo "服务已启动!"
    echo "=========================================="
    echo "访问地址: http://${IP}:8088"
    echo "日志页面: http://${IP}:8088/logs"
    echo ""
    echo "查看日志: tail -f logs/stdout.log"
    echo "停止服务: ./stop.sh"
    echo "=========================================="
else
    echo "启动失败，查看日志: cat logs/stdout.log"
fi
STARTEOF

# 创建停止脚本
cat > "$RELEASE_DIR/stop.sh" << 'STOPEOF'
#!/bin/bash
pkill -f simple-server && echo "服务已停止" || echo "服务未运行"
STOPEOF

chmod +x "$RELEASE_DIR/start.sh" "$RELEASE_DIR/stop.sh" "$RELEASE_DIR/simple-server"

# 打包
tar -czvf "tendis-migrate-linux-$VERSION.tar.gz" "$RELEASE_DIR"
rm -rf "$RELEASE_DIR"

echo ""
echo "=========================================="
echo "Linux 版本打包完成!"
echo "=========================================="
echo "文件: tendis-migrate-linux-$VERSION.tar.gz"
echo ""
echo "部署步骤:"
echo "  1. scp tendis-migrate-linux-$VERSION.tar.gz user@服务器:/tmp/"
echo "  2. ssh user@服务器"
echo "  3. cd /tmp && tar -xzf tendis-migrate-linux-$VERSION.tar.gz"
echo "  4. cd release-linux-$VERSION && ./start.sh"
echo "=========================================="
