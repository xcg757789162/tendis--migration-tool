#!/usr/bin/env python3
"""
测试环境配置
============
通过环境变量或命令行参数切换不同的测试环境。

环境变量优先级最高，其次是 --env 参数选择预设环境。

预设环境:
  home     - 家里环境（本地工具 + 192.168.1.19 Docker 容器）
  cloud    - 默认测试服务器（1.95.147.159 工具 + 192.168.0.142 Tendis）
  env-a    - 测试环境 A（8.137.20.144 工具 + 10.31.36.x Tendis）
  env-b    - 测试环境 B（8.137.20.144 工具 + 10.31.36.5/16 Tendis）
  custom   - 完全自定义（通过环境变量指定）

用法:
  # 使用预设环境
  python tests/regression_test.py --env cloud

  # 使用环境变量覆盖
  TM_API=http://localhost:8088/api/v1 TM_SRC_HOST=192.168.1.19 python tests/regression_test.py

  # 只运行指定分类
  python tests/regression_test.py --env cloud --categories A,B,D

  # 列出所有测试
  python tests/regression_test.py --list
"""

import os
import argparse


# ============================================================
# 预设环境配置
# ============================================================
ENVIRONMENTS = {
    "home": {
        "api":       "http://localhost:8088/api/v1",
        "ssh_cmd":   "",  # 本地环境不需要 SSH
        "src_host":  "192.168.1.19",
        "dst_host":  "192.168.1.19",  # 【BUG-FIX】显式指定目标端 IP，避免源/目标不同 IP 时连错地址
        "src_ports": [7001, 7002],
        "dst_ports": [8001, 8002],
        "redis_via_ssh": False,  # 本地直接 redis-cli
    },
    "cloud": {
        "api":       "http://1.95.147.159:8088/api/v1",
        "ssh_cmd":   "ssh root@1.95.147.159",
        "src_host":  "192.168.0.142",
        "dst_host":  "192.168.0.142",  # 【BUG-FIX】显式指定目标端 IP
        "src_ports": [7001, 7002],
        "dst_ports": [8001, 8002],
        "redis_via_ssh": True,
    },
    # cloud-local: 测试脚本在 1.95.147.159 上本地运行时使用
    # 不需要 SSH 包装（脚本已在服务器上，可直接访问 Tendis 和本地进程）
    "cloud-local": {
        "api":       "http://localhost:8088/api/v1",
        "ssh_cmd":   "",
        "src_host":  "192.168.0.142",
        "dst_host":  "192.168.0.142",  # 【BUG-FIX】显式指定目标端 IP
        "src_ports": [7001, 7002],
        "dst_ports": [8001, 8002],
        "redis_via_ssh": False,
    },
    "env-a": {
        "api":       "http://8.137.20.144:8088/api/v1",
        "ssh_cmd":   "ssh -p 8822 root@8.137.20.144",
        "src_host":  "10.31.36.8",
        "dst_host":  "10.31.36.3",  # 【BUG-FIX】显式指定目标端 IP（env-a 源/目标不同 IP！）
        "src_ports": [8902],
        "src_addrs": ["10.31.36.8:8902", "10.31.36.10:8903", "10.31.36.12:8901"],
        "dst_addrs": ["10.31.36.3:8902", "10.31.36.15:8901", "10.31.36.13:8903"],
        "dst_ports": [8902],
        "redis_via_ssh": True,
    },
    "env-b": {
        "api":       "http://8.137.20.144:8088/api/v1",
        "ssh_cmd":   "ssh -p 8822 root@8.137.20.144",
        "src_host":  "10.31.36.5",
        "src_ports": [8901, 8902, 8903],
        "dst_host":  "10.31.36.16",
        "dst_ports": [8901, 8902, 8903],
        "redis_via_ssh": True,
    },
    "env-b-deploy": {
        "api":       "http://140.143.218.100:8088/api/v1",
        "ssh_cmd":   "ssh -p 5542 root@140.143.218.100",
        "src_host":  "10.31.36.5",
        "src_ports": [8901, 8902, 8903],
        "dst_host":  "10.31.36.16",
        "dst_ports": [8901, 8902, 8903],
        "redis_via_ssh": True,
    },
}


class TestConfig:
    """测试配置类，支持环境变量覆盖"""

    def __init__(self, env_name="cloud"):
        env = ENVIRONMENTS.get(env_name, ENVIRONMENTS["cloud"]).copy()

        # 环境变量覆盖
        self.api = os.environ.get("TM_API", env["api"])
        self.ssh_cmd = os.environ.get("TM_SSH_CMD", env.get("ssh_cmd", ""))
        self.src_host = os.environ.get("TM_SRC_HOST", env.get("src_host", ""))
        self.dst_host = os.environ.get("TM_DST_HOST", env.get("dst_host", self.src_host))
        self.redis_via_ssh = env.get("redis_via_ssh", True)

        # 端口
        src_ports_str = os.environ.get("TM_SRC_PORTS", "")
        if src_ports_str:
            self.src_ports = [int(p) for p in src_ports_str.split(",")]
        else:
            self.src_ports = env.get("src_ports", [7001, 7002])

        dst_ports_str = os.environ.get("TM_DST_PORTS", "")
        if dst_ports_str:
            self.dst_ports = [int(p) for p in dst_ports_str.split(",")]
        else:
            self.dst_ports = env.get("dst_ports", [8001, 8002])

        # 地址列表（集群节点可能分布在不同 IP）
        if "src_addrs" in env:
            self.src_nodes = env["src_addrs"]
        else:
            self.src_nodes = [f"{self.src_host}:{p}" for p in self.src_ports]

        if "dst_addrs" in env:
            self.dst_nodes = env["dst_addrs"]
        else:
            self.dst_nodes = [f"{self.dst_host}:{p}" for p in self.dst_ports]

    def describe(self):
        print(f"  API:     {self.api}")
        print(f"  SSH:     {self.ssh_cmd or '(本地)'}")
        print(f"  源端:    {', '.join(self.src_nodes)}")
        print(f"  目标端:  {', '.join(self.dst_nodes)}")
        print(f"  Redis:   {'SSH 远程执行' if self.redis_via_ssh else '本地直连'}")


def get_config_from_args(args=None):
    """从命令行参数解析配置"""
    parser = argparse.ArgumentParser(description='tendis-migrate 回归测试')
    parser.add_argument('--env', default='cloud', choices=list(ENVIRONMENTS.keys()) + ['custom'],
                        help='选择预设测试环境 (default: cloud)')
    parser.add_argument('--categories', default='', help='只运行指定分类 (逗号分隔, 如 A,B,D)')
    parser.add_argument('--list', action='store_true', help='列出所有测试用例')
    parser.add_argument('--api', help='覆盖 API 地址')
    parser.add_argument('--src-host', help='覆盖源端主机')
    parser.add_argument('--dst-host', help='覆盖目标端主机')

    parsed = parser.parse_args(args)

    # 命令行覆盖环境变量
    if parsed.api:
        os.environ["TM_API"] = parsed.api
    if parsed.src_host:
        os.environ["TM_SRC_HOST"] = parsed.src_host
    if parsed.dst_host:
        os.environ["TM_DST_HOST"] = parsed.dst_host

    config = TestConfig(parsed.env)
    return config, parsed
