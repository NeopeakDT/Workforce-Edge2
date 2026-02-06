"""
Event Ingest API — STEP-2

Accepts detection events from Jetson devices and writes to
activity_detection_event table with:
- DB-level idempotency (event_id unique constraint)
- Transactional processing
- Session-scoped aggregation

STEP-2 — IDEMPOTENCY + TRANSACTION
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime, timezone, timedelta

from psycopg2.extras import Json
from psycopg2 import IntegrityError

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["ingestion"])


# -------------------------------------------------
# Request Schema
# -------------------------------------------------

class DetectionEventIn(BaseModel):
    event_id: UUID  # Required: unique per event (used for DB-level idempotency)
    session_id: UUID  # Required: groups events from same activity run
    camera_id: UUID
    activity_type: str
    event_type: str  # detection_event_type enum: START_CANDIDATE, FRAME_AGGREGATE, END_CANDIDATE
    confidence: float = Field(ge=0.0, le=1.0)
    event_time: datetime  # UTC timestamptz

    objects: Dict[str, Any]
    zones: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None


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
    """
    STEP-2: Idempotent, transactional event ingestion.
    
    Contract:
    - DB-level idempotency via event_id unique constraint
    - Single transaction for all operations
    - Session-scoped aggregation logic
    """
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
    # STEP-2: Single transaction for all operations
    # Explicit transaction control with manual commit/rollback
    # -------------------------------------------------
    with get_cursor() as cur:
        try:
            # 1. Insert event with DB-level idempotency (event_id unique constraint)
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
                    payload,
                    created_at
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                """,
                (
                    str(payload.event_id),
                    str(payload.session_id),
                    str(farm_id),
                    str(device_id),
                    str(payload.camera_id),
                    activity_type_id,
                    validated_event_type,
                    payload.event_time,
                    payload.confidence,
                    event_payload,
                    utc_now(),
                ),
            )
        except IntegrityError as e:
            # DB-level idempotency: event_id already exists (unique constraint violation)
            # Check if it's a unique constraint violation on event_id
            if "uq_event_id" in str(e) or "event_id" in str(e):
                # Duplicate event - rollback and return (context manager will handle cleanup)
                cur.connection.rollback()
                return {"status": "duplicate_ignored"}
            # Re-raise if it's a different integrity error (context manager will rollback)
            raise

        try:
            # 2. Apply session-scoped aggregation logic
            _apply_session_aggregation(
                cur=cur,
                event_id=payload.event_id,
                session_id=payload.session_id,
                farm_id=farm_id,
                activity_type_id=activity_type_id,
                event_type=validated_event_type,
                event_time=payload.event_time,
            )
            # Explicit commit on success (before context manager commit)
            cur.connection.commit()
        except Exception:
            # Rollback on aggregation failure to maintain atomicity
            # Context manager will also rollback, but explicit rollback ensures state
            cur.connection.rollback()
            raise

    return {"status": "ok"}


def _apply_session_aggregation(
    cur,
    event_id: UUID,
    session_id: UUID,
    farm_id: UUID,
    activity_type_id: int,
    event_type: str,
    event_time: datetime,
):
    """
    STEP-2: Session-scoped aggregation logic.
    
    Rules:
    - START_CANDIDATE: Create instance if session doesn't exist (first wins)
    - FRAME_AGGREGATE: Append if session exists and has START (ignore if no START)
    - END_CANDIDATE: Mark end if session exists and has START (last wins, first END wins)
    
    NOTE:
    FRAME_AGGREGATE and END_CANDIDATE events without START_CANDIDATE
    are intentionally kept unlinked.
    Do NOT create instances from them.
    """
    # Fetch session state
    cur.execute(
        """
        SELECT 
            id,
            status,
            actual_start_at,
            actual_end_at
        FROM activity_instance
        WHERE session_id = %s
        LIMIT 1
        """,
        (str(session_id),),
    )
    session_row = cur.fetchone()

    if event_type == "START_CANDIDATE":
        # START: Create instance if session doesn't exist (first wins)
        # Status is IN_PROGRESS (no pending states in STEP-2)
        if not session_row:
            cur.execute(
                """
                INSERT INTO activity_instance (
                    session_id,
                    farm_id,
                    activity_type_id,
                    status,
                    actual_start_at,
                    source,
                    created_at,
                    updated_at
                )
                VALUES (%s, %s, %s, 'IN_PROGRESS', %s, 'AI', %s, %s)
                RETURNING id
                """,
                (
                    str(session_id),
                    str(farm_id),
                    activity_type_id,
                    event_time,
                    utc_now(),
                    utc_now(),
                ),
            )
            instance_id = cur.fetchone()["id"]
            
            # Link event to instance
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id = %s
                WHERE event_id = %s
                """,
                (instance_id, str(event_id)),
            )
        # Else: duplicate START, ignore (already handled by event_id unique constraint)

    elif event_type == "FRAME_AGGREGATE":
        # FRAME: Append if session exists and is IN_PROGRESS (ignore if no START)
        # FRAMEs only extend liveness, nothing else
        if session_row and session_row["status"] == "IN_PROGRESS":
            # Link event to instance
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id = %s
                WHERE event_id = %s
                """,
                (session_row["id"], str(event_id)),
            )
        # Else: FRAME without START, ignore (keep event unlinked)

    elif event_type == "END_CANDIDATE":
        # END: Set end time if session exists and is IN_PROGRESS (first END wins)
        # DO NOT change status - END_CANDIDATE only means "edge says activity stopped"
        # Final outcome (EARLY/LATE/COMPLETED) is resolved later in STEP-3/5
        # Guard: Only apply END if not already ended (prevent duplicate END overwrites)
        if (
            session_row
            and session_row["status"] == "IN_PROGRESS"
            and session_row["actual_end_at"] is None
        ):
            # Calculate duration (clamp to >= 0 to handle clock jitter)
            duration_sec = max(
                0,
                int((event_time - session_row["actual_start_at"]).total_seconds())
            )
            
            cur.execute(
                """
                UPDATE activity_instance
                SET 
                    actual_end_at = %s,
                    actual_duration_sec = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    event_time,
                    duration_sec,
                    utc_now(),
                    session_row["id"],
                ),
            )
            
            # Link event to instance
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id = %s
                WHERE event_id = %s
                """,
                (session_row["id"], str(event_id)),
            )
        # Else: END without START or duplicate END, ignore (keep event unlinked)
