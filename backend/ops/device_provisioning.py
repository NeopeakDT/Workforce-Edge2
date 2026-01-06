#!/usr/bin/env python3
"""
Device Provisioning (One-Time)
Registers a Jetson edge device in the system.

Purpose:
- One-time registration of a Jetson device
- Generates a secure device API key
- Stores only the hashed key in DB
- Links device to farm
- Marks device as ACTIVE

IMPORTANT:
- Run ONLY from admin/backend machine
- Do NOT run on Jetson
"""

import argparse
import secrets
import hashlib
import sys
from pathlib import Path

# ------------------------------------------------------------------
# Setup path for imports (allows script to run from any directory)
# ------------------------------------------------------------------
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
        device_code: Unique device code (UPPERCASE recommended)
    """

    # Normalize device_code (recommended)
    device_code = device_code.strip().upper()

    # Generate cryptographically secure API key (shown once)
    device_api_key = secrets.token_hex(32)  # 256-bit key

    # Hash API key for DB storage
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
                api_key_hash,
                True,
                utc_now(),
            ),
        )

        # IMPORTANT: dict cursor → access by column name
        row = cur.fetchone()
        device_id = row["id"]

    print("✅ Device provisioned successfully")
    print(f"Device ID      : {device_id}")
    print(f"Device Name    : {device_name}")
    print(f"Device Code    : {device_code}")
    print(f"API Key (SAVE) : {device_api_key}")
    print()
    print("⚠️  WARNING: Save this API key now!")
    print("   It will NOT be shown again.")
    print("   Store it securely on the Jetson device.")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description="Provision a new Jetson edge device"
    )
    parser.add_argument("--farm-id", required=True, help="UUID of the farm")
    parser.add_argument("--device-name", required=True, help="Human-readable device name")
    parser.add_argument("--device-code", required=True, help="Unique device code")

    args = parser.parse_args()

    provision_device(
        farm_id=args.farm_id,
        device_name=args.device_name,
        device_code=args.device_code,
    )
