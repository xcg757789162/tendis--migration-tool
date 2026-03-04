#!/usr/bin/env python3
"""最小复现测试：验证 devcloud 环境迁移+验证是否正常"""
import subprocess, time, requests

A = "http://localhost:8088/api/v1"
H = "21.214.66.163"
P = "P@ssw0rd!!"

def sh(c):
    return subprocess.run(c, shell=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE, timeout=30).stdout.decode().strip()

def rset(port, k, v):
    sh("redis-cli -c -h %s -p %d -a '%s' --no-auth-warning SET %s \"%s\"" % (H, port, P, k, v))

def dget(port, k):
    return sh("redis-cli -c -h %s -p %d -a '%s' --no-auth-warning GET %s" % (H, port, P, k))

# Flush all
for p in [7001, 7002, 8001, 8002]:
    sh("redis-cli -h %s -p %d -a '%s' --no-auth-warning FLUSHALL" % (H, p, P))

# Write 50 keys
for i in range(50):
    rset(7001, "mini:%04d" % i, "v%d" % i)
time.sleep(1)
print("Written 50 keys")

# Create task
j = {
    "name": "minitest",
    "migration_mode": "full_only",
    "source_cluster": {"addrs": [H + ":7001", H + ":7002"], "password": P},
    "target_cluster": {"addrs": [H + ":8001", H + ":8002"], "password": P},
    "options": {"worker_count": 2, "key_filter": {"mode": "prefix", "prefixes": ["mini:"]}}
}
r = requests.post(A + "/tasks", json=j).json()
tid = r["data"]["task_id"]
print("Task: %s" % tid)

# Start and wait
requests.post(A + "/tasks/" + tid + "/start")
for _ in range(30):
    t = requests.get(A + "/tasks/" + tid).json()["data"]
    if t["status"] in ("completed", "failed", "error"):
        break
    time.sleep(1)

m = t.get("progress", {}).get("migrated_keys", 0)
print("status=%s migrated=%d" % (t["status"], m))

# Verify
ok = fail = 0
for i in range(50):
    v = dget(8001, "mini:%04d" % i)
    expected = "v%d" % i
    if v == expected:
        ok += 1
    else:
        fail += 1
        if fail <= 5:
            print("  FAIL mini:%04d: got=%r want=%r" % (i, v, expected))
print("ok=%d/50 fail=%d/50" % (ok, fail))
