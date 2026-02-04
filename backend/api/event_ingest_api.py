"""
Event Ingest API

Accepts detection events from Jetson devices and writes to
activity_detection_event table.

PHASE 4 — TRUST BOUNDARY
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timezone, timedelta

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
    event_time: datetime  # Changed from frame_ts to event_time for consistency

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
            detail="event_time must include timezone (UTC)"
        )
    if ts.utcoffset() != timedelta(0):
        raise HTTPException(
            status_code=400,
            detail="event_time must be in UTC (Z)"
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
    validate_utc_timestamp(payload.event_time)

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

    # -------------------------------------------------
    # 🔑 CRITICAL FIX: Attach END_CANDIDATE to open instance
    # -------------------------------------------------
    activity_instance_id = None

    if validated_event_type == "END_CANDIDATE":
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id
                FROM activity_instance
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND status = 'IN_PROGRESS'
                  AND actual_start_at <= %s
                ORDER BY actual_start_at DESC
                LIMIT 1
                """,
                (farm_id, activity_type_id, payload.event_time),
            )
            row = cur.fetchone()
            if row:
                activity_instance_id = row["id"]

    # ---------------- Insert-only write ----------------
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO activity_detection_event (
                farm_id,
                device_id,
                camera_id,
                activity_type_id,
                activity_instance_id,
                event_type,
                event_time,
                ai_confidence,
                payload,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                str(farm_id),
                str(device_id),
                str(payload.camera_id),
                activity_type_id,
                activity_instance_id,
                validated_event_type,
                payload.event_time,
                payload.confidence,
                event_payload,
                utc_now(),
            ),
        )

    return {"status": "ok"}
