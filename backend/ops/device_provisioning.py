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
import hashlib
import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def provision_device(farm_id: str, device_name: str, device_code: str):
    """
    Provision a new edge device.
    
    Args:
        farm_id: UUID of the farm
        device_name: Human-readable device name
        device_code: Unique device code/identifier
    """
    # Generate cryptographically secure API key (plain text - shown once)
    device_api_key = secrets.token_hex(32)  # 64 character hex string
    
    # Hash the API key for storage (security best practice)
    api_key_hash = hashlib.sha256(device_api_key.encode()).hexdigest()
    
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO edge_device (
                farm_id,
                name,
                code,
                api_key_hash,
                is_active,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            RETURNING id
            """,
            (
                farm_id,
                device_name,
                device_code,
                api_key_hash,  # Store hash, not plain text
                True,  # is_active
                utc_now(),
            ),
        )
        device_id = cur.fetchone()[0]

    print("✅ Device provisioned successfully")
    print(f"Device ID      : {device_id}")
    print(f"Device Name    : {device_name}")
    print(f"Device Code    : {device_code}")
    print(f"API Key (SAVE) : {device_api_key}")
    print()
    print("⚠️  WARNING: Save this API key now! It will not be shown again.")
    print("   Store it securely on the Jetson device.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Provision a new Jetson edge device"
    )
    parser.add_argument("--farm-id", required=True, help="UUID of the farm")
    parser.add_argument("--device-name", required=True, help="Human-readable device name (e.g., 'jetson-orin-01')")
    parser.add_argument("--device-code", required=True, help="Unique device code/identifier")

    args = parser.parse_args()

    provision_device(
        farm_id=args.farm_id,
        device_name=args.device_name,
        device_code=args.device_code,
    )
