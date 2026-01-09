"""
Heartbeat Ingest API (Phase 4)

Accepts periodic health pings from Jetson devices.

Responsibilities:
- Authenticate device via X-DEVICE-KEY
- Insert heartbeat telemetry (append-only)
- Update edge_device.last_seen_at
- Enforce UTC timestamps
- Lightweight & high-frequency safe

NO aggregation
NO alerting
NO scheduling

# Run this below in terminal
curl.exe -X POST http://127.0.0.1:8000/api/v1/ingest/heartbeat `
  -H "X-DEVICE-KEY: wf_test_device_key_001" `
  -H "Content-Type: application/json" `
  --data-binary "@heartbeat.json"

"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["heartbeat"])


# -------------------- SCHEMA --------------------

class HeartbeatIn(BaseModel):
    cpu_temp_c: Optional[float] = Field(default=None, ge=0)
    gpu_temp_c: Optional[float] = Field(default=None, ge=0)
    disk_usage_pct: Optional[float] = Field(default=None, ge=0, le=100)
    memory_usage_pct: Optional[float] = Field(default=None, ge=0, le=100)
    notes: Optional[str] = None


# -------------------- ENDPOINT --------------------

@router.post("/heartbeat")
def ingest_heartbeat(
    payload: HeartbeatIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    """
    Ingest heartbeat from Jetson device.

    Flow:
    1. Authenticate device
    2. Insert heartbeat record
    3. Update last_seen_at
    """

    try:
        device_ctx = resolve_device_from_headers(
            {"X-DEVICE-KEY": x_device_key}
        )
    except DeviceAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))

    device_id = device_ctx["device_id"]
    now = utc_now()

    with get_cursor() as cur:
        # Insert heartbeat
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
                str(device_id),
                now,
                payload.cpu_temp_c,
                payload.gpu_temp_c,
                payload.disk_usage_pct,
                payload.memory_usage_pct,
                payload.notes,
                now,
            ),
        )

        # Update device liveness
        cur.execute(
            """
            UPDATE edge_device
            SET last_seen_at = %s
            WHERE id = %s
            """,
            (now, str(device_id)),
        )

    return {"status": "alive"}
