#!/usr/bin/env python3
import sys
from pathlib import Path
BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.device_auth import hash_device_key

print('🔍 Checking all devices in database...')
with get_cursor() as cur:
    cur.execute('SELECT id, name, code, farm_id, is_active, created_at FROM edge_device ORDER BY created_at DESC')
    devices = cur.fetchall()

    if not devices:
        print('❌ No devices found in database')
    else:
        print(f'✅ Found {len(devices)} device(s):')
        for device in devices:
            print(f'  - ID: {device["id"]}')
            print(f'    Name: {device["name"]}')
            print(f'    Code: {device["code"]}')
            print(f'    Farm ID: {device["farm_id"]}')
            print(f'    Active: {device["is_active"]}')
            print(f'    Created: {device["created_at"]}')
            print()

print('🔑 Checking hash of wf_test_device_key_001...')
test_key = 'wf_test_device_key_001'
test_hash = hash_device_key(test_key)
print(f'Hash: {test_hash}')

with get_cursor() as cur:
    cur.execute('SELECT id, name FROM edge_device WHERE api_key_hash = %s', (test_hash,))
    result = cur.fetchone()
    if result:
        print(f'✅ Found device with this hash: {result["name"]} (ID: {result["id"]})')
    else:
        print('❌ No device found with this hash')