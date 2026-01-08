"""
Heartbeat Ingest API

Accepts device heartbeat signals and writes health telemetry to
edge_device_heartbeat table.

PHASE 4 — TRUST BOUNDARY

Rules enforced:
- Authenticate device via hashed API key
- Store timestamps in UTC
- Insert heartbeat history
- Update edge_device.last_seen_at
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel
from typing import Optional
from decimal import Decimal

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import hash_device_key

router = APIRouter(prefix="/ingest", tags=["heartbeat"])


# -------------------------------------------------
# Request Schema
# -------------------------------------------------

class HeartbeatIn(BaseModel):
    cpu_temp_c: Optional[Decimal] = None
    gpu_temp_c: Optional[Decimal] = None
    disk_usage_pct: Optional[Decimal] = None
    memory_usage_pct: Optional[Decimal] = None
    notes: Optional[str] = None


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def resolve_device(device_key: str):
    key_hash = hash_device_key(device_key)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM edge_device
            WHERE api_key_hash = %s
              AND is_active = true
            """,
            (key_hash,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(status_code=401, detail="Invalid device key")
        return row[0]


# -------------------------------------------------
# Endpoint
# -------------------------------------------------

@router.post("/heartbeat")
def ingest_heartbeat(
    payload: HeartbeatIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    device_id = resolve_device(x_device_key)
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO edge_device_heartbeat (
                device_id,
                heartbeat_time,
                cpu_temp_c,
                gpu_temp_c,
                disk_usage_pct,
                memory_usage_pct,
                notes,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                device_id,
                now,
                payload.cpu_temp_c,
                payload.gpu_temp_c,
                payload.disk_usage_pct,
                payload.memory_usage_pct,
                payload.notes,
                now,
            ),
        )

        cur.execute(
            """
            UPDATE edge_device
            SET last_seen_at = %s
            WHERE id = %s
            """,
            (now, device_id),
        )

    return {"status": "alive"}
