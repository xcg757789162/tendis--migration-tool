#!/usr/bin/env python3
"""Debug B2 test - run on server 1.95.147.159"""
import requests, json, time, subprocess, os

API = 'http://localhost:8088/api/v1'
SRC = '192.168.0.142'

def run(cmd):
    r = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    out, _ = r.communicate()
    return out.decode().strip()

# Flush dst
for p in [8001, 8002]:
    run(['redis-cli', '-c', '-h', SRC, '-p', str(p), 'FLUSHALL'])
time.sleep(1)

# Write test data
for i in range(30):
    run(['redis-cli', '-c', '-h', SRC, '-p', '7001', 'SET', 'b2_yes:%04d' % i, 'yes_%d' % i])
    run(['redis-cli', '-c', '-h', SRC, '-p', '7001', 'SET', 'b2_no:%04d' % i, 'no_%d' % i])
time.sleep(1)

v = run(['redis-cli', '-c', '-h', SRC, '-p', '7001', 'GET', 'b2_yes:0001'])
print('src b2_yes:0001=%s' % v)

# Create task with CORRECT field names
data = {
    'name': 'dbg-B2',
    'migration_mode': 'full_only',
    'source_cluster': {'addrs': ['%s:7001' % SRC, '%s:7002' % SRC]},
    'target_cluster': {'addrs': ['%s:8001' % SRC, '%s:8002' % SRC]},
    'options': {
        'worker_count': 2,
        'scan_batch_size': 100,
        'conflict_policy': 'replace',
        'key_filter': {'mode': 'prefix', 'prefixes': ['b2_yes:']}
    }
}
r = requests.post('%s/tasks' % API, json=data)
print('create resp: %s' % r.json())
tid = r.json().get('data', {}).get('task_id', '')
print('tid=%s' % tid)

# Start
requests.post('%s/tasks/%s/start' % (API, tid))
for _ in range(30):
    time.sleep(2)
    r = requests.get('%s/tasks/%s' % (API, tid))
    d = r.json().get('data', {})
    st = d.get('status', '')
    p = d.get('progress', {})
    s = d.get('stats', {})
    print('  st=%s migrated=%s filtered=%s phase=%s' % (st, p.get('migrated_keys'), s.get('filtered_keys'), p.get('phase')))
    if st in ('completed', 'failed', 'stopped'):
        break

# Verify
for i in range(3):
    v = run(['redis-cli', '-c', '-h', SRC, '-p', '8001', 'GET', 'b2_yes:%04d' % i])
    print('DST b2_yes:%04d=%s' % (i, v))
    v = run(['redis-cli', '-c', '-h', SRC, '-p', '8001', 'GET', 'b2_no:%04d' % i])
    print('DST b2_no:%04d=%s' % (i, v))

# Cleanup
requests.delete('%s/tasks/%s' % (API, tid))
for i in range(30):
    run(['redis-cli', '-c', '-h', SRC, '-p', '7001', 'DEL', 'b2_yes:%04d' % i])
    run(['redis-cli', '-c', '-h', SRC, '-p', '7001', 'DEL', 'b2_no:%04d' % i])
