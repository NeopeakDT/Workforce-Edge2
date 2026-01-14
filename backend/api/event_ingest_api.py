"""
Event Ingest API

Accepts detection events from Jetson devices and writes to
activity_detection_event table.

PHASE 4 — TRUST BOUNDARY

Rules enforced:
- Authenticate device via hashed API key
- Accept ONLY UTC timestamps
- Ignore any farm_id from edge
- Resolve farm via device_id
- Insert-only (no aggregation, no schedules)

Test:
curl.exe -X POST http://127.0.0.1:8000/api/v1/ingest/event `
  -H "X-DEVICE-KEY: wf_test_device_key_001" `
  -H "Content-Type: application/json" `
  --data-binary "@event_ok.json"
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timezone

from psycopg2.extras import Json

from common.db import get_cursor
from common.idempotency import is_duplicate, mark_processed
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["ingestion"])


# -------------------------------------------------
# Request Schema
# -------------------------------------------------

class DetectionEventIn(BaseModel):
    camera_id: UUID
    activity_type: str
    event_type: str              # detection_event_type enum
    confidence: float = Field(ge=0.0, le=1.0)
    frame_ts: datetime

    objects: Dict[str, Any]
    zones: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None


# -------------------------------------------------
# Helpers
# -------------------------------------------------

def validate_utc_timestamp(ts: datetime):
    if ts.tzinfo is None:
        raise HTTPException(
            status_code=400,
            detail="frame_ts must include timezone (UTC)"
        )
    if ts.utcoffset() != timezone.utc.utcoffset(ts):
        raise HTTPException(
            status_code=400,
            detail="frame_ts must be in UTC (Z)"
        )


def resolve_activity_type_id(activity_type: str) -> int:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM activity_type
            WHERE code = %s
            """,
            (activity_type,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=400,
                detail=f"Unknown activity_type: {activity_type}"
            )
        return row["id"]


def validate_detection_event_type(event_type: str) -> str:
    """
    Validate against DB enum detection_event_type.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT enumlabel
            FROM pg_enum
            WHERE enumtypid = 'detection_event_type'::regtype
              AND enumlabel = %s
            """,
            (event_type,),
        )
        row = cur.fetchone()
        if not row:
            raise HTTPException(
                status_code=400,
                detail=f"Invalid detection event_type: {event_type}"
            )
    return event_type


# -------------------------------------------------
# Endpoint
# -------------------------------------------------

@router.post("/event")
def ingest_event(
    payload: DetectionEventIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    # ---------------- Device authentication ----------------
    try:
        device_ctx = resolve_device_from_headers(
            {"X-DEVICE-KEY": x_device_key}
        )
    except DeviceAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))

    device_id = device_ctx["device_id"]
    farm_id = device_ctx["farm_id"]

    # ---------------- UTC enforcement ----------------
    validate_utc_timestamp(payload.frame_ts)

    # ---------------- Idempotency ----------------
    if payload.idempotency_key:
        if is_duplicate(payload.idempotency_key):
            return {"status": "duplicate_ignored"}
        mark_processed(payload.idempotency_key)

    # ---------------- Reference resolution ----------------
    activity_type_id = resolve_activity_type_id(payload.activity_type)
    validated_event_type = validate_detection_event_type(payload.event_type)

    # ---------------- Payload ----------------
    event_payload = Json({
        "objects": payload.objects,
        "zones": payload.zones,
        "metadata": payload.metadata,
    })

    # ---------------- Insert-only write ----------------
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO activity_detection_event (
                farm_id,
                device_id,
                camera_id,
                activity_type_id,
                event_type,
                event_time,
                ai_confidence,
                payload,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                str(farm_id),
                str(device_id),
                str(payload.camera_id),
                activity_type_id,
                validated_event_type,
                payload.frame_ts,
                payload.confidence,
                event_payload,
                utc_now(),
            ),
        )

    return {"status": "ok"}
