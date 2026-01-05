"""
Device Authentication Resolver

Purpose:
    Authenticates Jetson edge devices using X-DEVICE-KEY header.
    Resolves device identity and provides device_id + farm_id to downstream handlers.
    This is machine-to-machine authentication, not user authentication.

Architecture:
    - No JWT tokens
    - No Supabase Auth
    - Uses SHA-256 hashed API keys stored in database
    - Plain text key sent in header, hashed for comparison

Security Properties:
    - Plain key leakage: ❌ Not stored in database (only hash)
    - DB breach: Hash only (cannot reverse to plain key)
    - Device revocation: is_active = false blocks access
    - Replay attacks: Key never transmitted except over TLS
    - Per-device isolation: Strong (each device has unique key)
    
    Database:
        edge_device.api_key_hash → stored SHA-256 hash
        edge_device.is_active → must be true for access
        edge_device.last_seen_at → updated on each request

Usage Example:
    from fastapi import Request, HTTPException
    from common.device_auth import resolve_device_from_headers, DeviceAuthError
    
    @app.post("/api/v1/events/detection")
    async def ingest_detection(request: Request):
        try:
            device_ctx = resolve_device_from_headers(request.headers)
        except DeviceAuthError as e:
            raise HTTPException(status_code=401, detail=str(e))
        
        device_id = device_ctx["device_id"]
        farm_id = device_ctx["farm_id"]
        # Use device_id and farm_id in your logic
"""

import hashlib
from typing import Dict

from .db import get_cursor
from .time_utils import utc_now


class DeviceAuthError(Exception):
    """Raised when device authentication fails"""
    pass


def _hash_api_key(api_key: str) -> str:
    """Hash API key using SHA-256"""
    return hashlib.sha256(api_key.encode()).hexdigest()


def resolve_device_from_headers(headers: Dict[str, str]) -> dict:
    """
    Resolve device identity from request headers.
    
    Contract:
    - Request header: X-DEVICE-KEY: <plain api key>
    - DB: edge_device.api_key_hash → stored SHA-256 hash
    - DB: edge_device.is_active → must be true
    
    Returns:
        {
            "device_id": uuid,
            "farm_id": uuid,
            "device_name": str
        }
    
    Raises:
        DeviceAuthError: If device key is missing, invalid, or device is inactive
    
    Security properties:
    - Plain key leakage: ❌ Not stored (only hash in DB)
    - DB breach: Hash only (cannot reverse to plain key)
    - Device revocation: is_active = false
    - Replay attacks: Key never transmitted except TLS
    - Per-device isolation: Strong
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
        row = cur.fetchone()
        
        if not row:
            raise DeviceAuthError("Invalid device API key")
        
        device_id, farm_id, device_name, is_active = row
        
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

