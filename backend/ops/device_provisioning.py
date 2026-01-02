"""
Device Provisioning  (One-Time)
Registers a Jetson edge device in the system.

# Purpose:
- One-time registration of a Jetson device into the system.

# This script:
- Creates a logical device identity
- Generates a device API key
- Binds device → farm
- Marks device as ACTIVE
No inference, no streaming, no heartbeats

# What this does:
1. Generates cryptographically safe device_api_key
2	Inserts row into edge_device
3	Links device to farm
4	Prints API key once (must be stored on Jetson)

# Usage:
python ops/device_provisioning.py \
    --farm-id <uuid> \
    --device-name jetson-orin-01 \
    --device-type JETSON_ORIN

# How Jetson uses this later:
- Jetson sends X-DEVICE-KEY header
- Backend maps key → edge_device.id
- Used for ingestion auth, heartbeat, telemetry
"""

import argparse
import secrets
import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def provision_device(farm_id: str, device_name: str, device_type: str):
    device_api_key = secrets.token_hex(32)

    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO edge_device (
                farm_id,
                device_name,
                device_type,
                device_api_key,
                status,
                created_at
            )
            VALUES (%s, %s, %s, %s, 'ACTIVE', %s)
            RETURNING id
            """,
            (
                farm_id,
                device_name,
                device_type,
                device_api_key,
                utc_now(),
            ),
        )
        device_id = cur.fetchone()[0]

    print("✅ Device provisioned successfully")
    print(f"Device ID      : {device_id}")
    print(f"Device Name    : {device_name}")
    print(f"Device Type    : {device_type}")
    print(f"API Key (SAVE) : {device_api_key}")


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--farm-id", required=True)
    parser.add_argument("--device-name", required=True)
    parser.add_argument("--device-type", required=True)

    args = parser.parse_args()

    provision_device(
        farm_id=args.farm_id,
        device_name=args.device_name,
        device_type=args.device_type,
    )
