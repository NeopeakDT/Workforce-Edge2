"""
Device Authentication Resolver

Purpose:
- Authenticate Jetson edge devices using X-DEVICE-KEY header
- Resolve device identity (device_id, farm_id, device_name)
- Update last_seen_at on every successful request

Security model:
- Plain API key is NEVER stored in DB
- SHA-256 hash (api_key_hash) is stored in edge_device
- Device can be revoked via is_active = false

This is MACHINE authentication (not user auth).
"""

import hashlib
from typing import Dict

from common.db import get_cursor
from common.time_utils import utc_now


class DeviceAuthError(Exception):
    """Raised when device authentication fails"""
    pass


def _hash_api_key(api_key: str) -> str:
    """Hash API key using SHA-256"""
    return hashlib.sha256(api_key.encode()).hexdigest()


def resolve_device_from_headers(headers: Dict[str, str]) -> dict:
    """
    Resolve device identity from request headers.

    Expected header:
        X-DEVICE-KEY: <plain api key>

    Database fields used (edge_device):
        - api_key_hash (TEXT)
        - is_active (BOOLEAN)
        - id (UUID)
        - farm_id (UUID)
        - name (TEXT)
        - last_seen_at (TIMESTAMPTZ)

    Returns:
        {
            "device_id": UUID,
            "farm_id": UUID,
            "device_name": str
        }

    Raises:
        DeviceAuthError
    """

    api_key = headers.get("X-DEVICE-KEY")

    if not api_key:
        raise DeviceAuthError("Missing X-DEVICE-KEY header")

    api_key_hash = _hash_api_key(api_key)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                farm_id,
                name,
                is_active
            FROM edge_device
            WHERE api_key_hash = %s
            """,
            (api_key_hash,),
        )

        # IMPORTANT: get_cursor() uses dict cursor
        row = cur.fetchone()

        if not row:
            raise DeviceAuthError("Invalid device API key")

        device_id = row["id"]
        farm_id = row["farm_id"]
        device_name = row["name"]
        is_active = row["is_active"]

        if not is_active:
            raise DeviceAuthError("Device is inactive")

        # Update heartbeat timestamp
        cur.execute(
            """
            UPDATE edge_device
            SET last_seen_at = %s
            WHERE id = %s
            """,
            (utc_now(), device_id),
        )

    return {
        "device_id": device_id,
        "farm_id": farm_id,
        "device_name": device_name,
    }
