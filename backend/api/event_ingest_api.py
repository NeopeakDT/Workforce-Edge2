"""
backend/api/event_ingest_api.py
Event Ingest API — STEP-2 (APPEND ONLY)

Responsibilities:
- Authenticate device
- Validate payload
- Insert activity_detection_event
- Enforce DB idempotency

NO instance creation here.
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timedelta

from psycopg2.extras import Json
from psycopg2 import IntegrityError

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["ingestion"])


class DetectionEventIn(BaseModel):
    event_id: UUID
    session_id: UUID
    camera_id: UUID
    activity_type: str
    event_type: str
    confidence: float = Field(ge=0.0, le=1.0)
    event_time: datetime
    objects: Dict[str, Any]
    zones: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


def validate_utc(ts: datetime):
    if ts.tzinfo is None or ts.utcoffset() != timedelta(0):
        raise HTTPException(400, "event_time must be UTC")


def resolve_activity_type_id(code: str) -> int:
    with get_cursor() as cur:
        cur.execute("SELECT id FROM activity_type WHERE code=%s", (code,))
        row = cur.fetchone()
        if not row:
            raise HTTPException(400, f"Unknown activity_type {code}")
        return row["id"]


@router.post("/event")
def ingest_event(
    payload: DetectionEventIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    try:
        device_ctx = resolve_device_from_headers({"X-DEVICE-KEY": x_device_key})
    except DeviceAuthError as e:
        raise HTTPException(401, str(e))

    validate_utc(payload.event_time)
    activity_type_id = resolve_activity_type_id(payload.activity_type)

    # Extract zone_id from payload.zones.primary
    zone_id = None
    if payload.zones and "primary" in payload.zones:
        zone_id = payload.zones["primary"]

    with get_cursor() as cur:
        try:
            cur.execute(
                """
                INSERT INTO activity_detection_event (
                    event_id,
                    session_id,
                    farm_id,
                    device_id,
                    camera_id,
                    activity_type_id,
                    event_type,
                    event_time,
                    ai_confidence,
                    zone_id,
                    payload,
                    created_at
                )
                VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
                """,
                (
                    str(payload.event_id),
                    str(payload.session_id),
                    device_ctx["farm_id"],
                    device_ctx["device_id"],
                    str(payload.camera_id),
                    activity_type_id,
                    payload.event_type,
                    payload.event_time,
                    payload.confidence,
                    zone_id,
                    Json({
                        "objects": payload.objects,
                        "zones": payload.zones,
                        "metadata": payload.metadata,
                    }),
                    utc_now(),
                ),
            )
        except IntegrityError:
            return {"status": "duplicate_ignored"}

    return {"status": "ok"}
