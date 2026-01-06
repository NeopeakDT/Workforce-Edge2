"""
Event Ingest API

Accepts detection events from Jetson devices and writes to activity_detection_event table.

Responsibilities:
    - Device authentication via X-DEVICE-KEY header
    - Payload validation using Pydantic models
    - Optional idempotency support
    - Insert-only writes (append-only event log)
    - Hard-fail on invalid input (no silent failures)

Architecture:
    - Edge devices send detection events with YOLO/tracker output
    - Backend validates and stores events atomically
    - Idempotency prevents duplicate processing
    - Backend aggregation layer processes events to create activity instances
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel, Field
from typing import Optional, Dict, Any
from uuid import UUID
from datetime import datetime

from common.db import get_cursor
from common.idempotency import is_duplicate, mark_processed
from common.time_utils import utc_now

router = APIRouter(prefix="/ingest", tags=["ingestion"])


# -------------------- SCHEMA --------------------

class DetectionEventIn(BaseModel):
    camera_id: UUID
    activity_type: str
    confidence: float = Field(ge=0.0, le=1.0)
    frame_ts: datetime
    objects: Dict[str, Any]        # raw YOLO + tracker output
    zones: Optional[Dict[str, Any]] = None
    metadata: Optional[Dict[str, Any]] = None
    idempotency_key: Optional[str] = None


# -------------------- HELPERS --------------------

def resolve_device(device_key: str) -> UUID:
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

@router.post("/event")
def ingest_event(
    payload: DetectionEventIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    """
    Ingest detection event from edge device.
    
    Process:
    1. Authenticate device via X-DEVICE-KEY header
    2. Validate payload (Pydantic handles this automatically)
    3. Check idempotency if idempotency_key provided
    4. Insert event into activity_detection_event table
    5. Return success
    
    Hard-fails on:
    - Invalid device key (401)
    - Invalid payload format (422)
    - Database errors (500)
    
    Args:
        payload: Detection event payload
        x_device_key: Device API key from X-DEVICE-KEY header
        
    Returns:
        dict: {"status": "ok"} on success, {"status": "duplicate_ignored"} if duplicate
    """
    device_id = resolve_device(x_device_key)

    # ---- Idempotency (optional but supported) ----
    if payload.idempotency_key:
        if is_duplicate(payload.idempotency_key):
            return {"status": "duplicate_ignored"}
        mark_processed(payload.idempotency_key)

    # ---- Insert-only write ----
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO activity_detection_event (
                device_id,
                camera_id,
                activity_type,
                confidence,
                frame_ts,
                objects,
                zones,
                metadata,
                created_at
            )
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """,
            (
                device_id,
                payload.camera_id,
                payload.activity_type,
                payload.confidence,
                payload.frame_ts,
                payload.objects,
                payload.zones,
                payload.metadata,
                utc_now(),
            ),
        )

    return {"status": "ok"}
