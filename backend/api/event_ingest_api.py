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

import os
import time
import threading
from collections import defaultdict, deque

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timedelta

from psycopg2.extras import Json

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["ingestion"])

MAX_EVENTS_PER_CAMERA_PER_MIN = int(
    os.getenv("INGEST_MAX_EVENTS_PER_CAMERA_PER_MIN", "120")
)
RATE_WINDOW_SECONDS = 60
_rate_limit_lock = threading.Lock()
_camera_event_windows = defaultdict(deque)


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


_activity_type_cache: dict = {}


def resolve_activity_type_id(code: str) -> int:
    if code in _activity_type_cache:
        return _activity_type_cache[code]

    with get_cursor() as cur:
        cur.execute("SELECT id FROM activity_type WHERE code=%s", (code,))
        row = cur.fetchone()

    if not row:
        raise HTTPException(400, f"Unknown activity_type {code}")

    _activity_type_cache[code] = row["id"]
    return row["id"]


_camera_farm_cache: dict = {}


def resolve_camera_farm_id(camera_id: str) -> Optional[str]:
    """Look up which farm owns this camera, so a device's payload-supplied
    camera_id can be checked against its server-derived farm_id (device_ctx)
    before insert. Cached like _activity_type_cache — cameras are rarely
    reassigned across farms."""
    if camera_id in _camera_farm_cache:
        return _camera_farm_cache[camera_id]

    with get_cursor() as cur:
        cur.execute("SELECT farm_id FROM farm_camera WHERE id = %s", (camera_id,))
        row = cur.fetchone()

    if not row:
        return None

    _camera_farm_cache[camera_id] = str(row["farm_id"])
    return _camera_farm_cache[camera_id]


_zone_farm_cache: dict = {}


def resolve_zone_farm_id(zone_id: str) -> Optional[str]:
    """Same check as resolve_camera_farm_id, for the optional zone_id."""
    if zone_id in _zone_farm_cache:
        return _zone_farm_cache[zone_id]

    with get_cursor() as cur:
        cur.execute("SELECT farm_id FROM farm_zone WHERE id = %s", (zone_id,))
        row = cur.fetchone()

    if not row:
        return None

    _zone_farm_cache[zone_id] = str(row["farm_id"])
    return _zone_farm_cache[zone_id]


def rate_exceeded(camera_id: str):
    if MAX_EVENTS_PER_CAMERA_PER_MIN <= 0:
        return False, None

    now = time.monotonic()
    cutoff = now - RATE_WINDOW_SECONDS

    with _rate_limit_lock:
        window = _camera_event_windows[camera_id]

        while window and window[0] <= cutoff:
            window.popleft()

        if len(window) >= MAX_EVENTS_PER_CAMERA_PER_MIN:
            retry_after = max(1, int(RATE_WINDOW_SECONDS - (now - window[0])))
            return True, retry_after

        window.append(now)
        return False, None


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

    # Camera identity is client-supplied; farm/device identity is not. Confirm
    # this camera actually belongs to the authenticated device's farm before
    # inserting — otherwise a misconfigured/compromised device key for one
    # farm could write events tagged with another farm's camera_id.
    camera_farm_id = resolve_camera_farm_id(str(payload.camera_id))
    if camera_farm_id is None:
        raise HTTPException(400, f"Unknown camera_id {payload.camera_id}")
    if camera_farm_id != str(device_ctx["farm_id"]):
        raise HTTPException(
            400, "camera_id does not belong to the authenticated device's farm"
        )

    # Normalize lifecycle so aggregation always begins from START_CANDIDATE.
    incoming_event_type = (payload.event_type or "").strip().upper()
    event_type_for_insert = (
        "START_CANDIDATE" if incoming_event_type == "START" else incoming_event_type
    )
    print(
        f"[INGEST] in={incoming_event_type} stored={event_type_for_insert} | "
        f"{payload.camera_id} | {payload.event_time}"
    )

    exceeded, retry_after = rate_exceeded(str(payload.camera_id))
    if exceeded:
        raise HTTPException(
            status_code=429,
            detail=(
                "Event rate exceeded for camera. "
                "Reduce edge event volume or increase ingest limit."
            ),

            headers={"Retry-After": str(retry_after)},
        )

    # Extract zone_id from payload.zones.primary
    zone_id = None
    if payload.zones and "primary" in payload.zones:
        zone_id = payload.zones["primary"]

    if zone_id is not None:
        zone_farm_id = resolve_zone_farm_id(str(zone_id))
        if zone_farm_id is None:
            raise HTTPException(400, f"Unknown zone_id {zone_id}")
        if zone_farm_id != str(device_ctx["farm_id"]):
            raise HTTPException(
                400, "zone_id does not belong to the authenticated device's farm"
            )

    with get_cursor() as cur:
        # ON CONFLICT DO NOTHING instead of catching IntegrityError: the Jetson
        # retry queue resubmits the same event_id verbatim after a request
        # timeout (timeout-after-commit race), so duplicates here are expected.
        # A caught exception still gets logged by Postgres as a raw 23505 even
        # though the app handles it; ON CONFLICT never raises in the first place.
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
            ON CONFLICT (event_id) DO NOTHING
            RETURNING event_id
            """,
            (
                str(payload.event_id),
                str(payload.session_id),
                device_ctx["farm_id"],
                device_ctx["device_id"],
                str(payload.camera_id),
                activity_type_id,
                event_type_for_insert,
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
        inserted = cur.fetchone()

    if not inserted:
        return {"status": "duplicate_ignored"}

    return {"status": "ok"}
