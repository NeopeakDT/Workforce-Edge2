"""
Heartbeat Ingest API

Accepts device heartbeat signals and writes health telemetry to edge_device_heartbeat table.

Responsibilities:
    - Device authentication via X-DEVICE-KEY header
    - Insert heartbeat record with status and metrics
    - Update edge_device.last_seen_at timestamp
    - Lightweight and cheap (high-frequency safe)

Architecture:
    - Edge devices send periodic heartbeat signals (typically every 120 seconds)
    - Backend stores heartbeat history for health monitoring
    - Updates last_seen_at for device liveness tracking
    - Designed for high-frequency calls without performance impact
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from typing import Optional, Dict, Any

from common.db import get_cursor
from common.time_utils import utc_now

router = APIRouter(prefix="/ingest", tags=["heartbeat"])


# -------------------- SCHEMA --------------------

class HeartbeatIn(BaseModel):
    status: str = "OK"                     # OK / DEGRADED / ERROR
    metrics: Optional[Dict[str, Any]] = None   # CPU, RAM, FPS, temp, disk, etc.


# -------------------- HELPERS --------------------

def resolve_device(device_key: str):
    """
    Resolve device ID from device API key.
    
    Args:
        device_key: Plain text device API key from X-DEVICE-KEY header
        
    Returns:
        UUID: Device ID
        
    Raises:
        HTTPException: 401 if device key is invalid or device is inactive
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM edge_device
            WHERE api_key = %s
              AND is_active = true
            """,
            (device_key,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid device key")
        return row["id"]


# -------------------- ENDPOINT --------------------

@router.post("/heartbeat")
def ingest_heartbeat(
    payload: HeartbeatIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    """
    Ingest heartbeat signal from edge device.
    
    Process:
    1. Authenticate device via X-DEVICE-KEY header
    2. Insert heartbeat record into edge_device_heartbeat table
    3. Update edge_device.last_seen_at timestamp
    4. Return success
    
    This endpoint is designed to be lightweight and safe for high-frequency calls.
    Edge devices typically call this every 120 seconds.
    
    Args:
        payload: Heartbeat payload with status and optional metrics
        x_device_key: Device API key from X-DEVICE-KEY header
        
    Returns:
        dict: {"status": "alive"} on success
    """
    device_id = resolve_device(x_device_key)
    now = utc_now()

    with get_cursor() as cur:
        # Insert heartbeat
        cur.execute(
            """
            INSERT INTO edge_device_heartbeat (
                device_id,
                status,
                metrics,
                created_at
            )
            VALUES (%s,%s,%s,%s)
            """,
            (
                device_id,
                payload.status,
                payload.metrics,
                now,
            ),
        )

        # Update last_seen
        cur.execute(
            """
            UPDATE edge_device
            SET last_seen_at = %s
            WHERE id = %s
            """,
            (now, device_id),
        )

    return {"status": "alive"}
