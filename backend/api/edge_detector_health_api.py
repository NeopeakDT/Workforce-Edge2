"""
backend/api/edge_detector_health_api.py
Detector-Process Health Ingest API (Step C)

Accepts a periodic pulse from jetson/edge_watchdog.py proving the workforce
detection pipeline (not just the board) is making progress. Deliberately a
separate endpoint/table-column from /ingest/heartbeat (edge_heartbeat_agent.py,
board telemetry) -- see docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md
for why these must stay distinct signals.

NO alerting here -- this endpoint only records the pulse. WORKFORCE_DETECTOR_OFFLINE
is evaluated separately by aggregation/alerts_cron.py based on staleness of
what gets written here, never on this request's payload content.
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["detector-health"])


class DetectorHeartbeatIn(BaseModel):
    detector_healthy: bool
    total_frames: int | None = None
    camera_count: int | None = None


@router.post("/detector-heartbeat")
def ingest_detector_heartbeat(
    payload: DetectorHeartbeatIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    """
    Ingest a detector-health pulse. Always accepted and always updates
    detector_last_seen_at regardless of payload.detector_healthy's value --
    the ALERT decision (WORKFORCE_DETECTOR_OFFLINE) is driven by whether
    pulses keep arriving at all, not by what any single pulse says. A
    detector_healthy=false pulse still proves the watchdog itself is alive
    and reporting; only silence (no pulse for 5 min) is the alert trigger.
    """
    try:
        device_ctx = resolve_device_from_headers({"X-DEVICE-KEY": x_device_key})
    except DeviceAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))

    device_id = device_ctx["device_id"]
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            "UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s",
            (now, str(device_id)),
        )

    return {"status": "recorded", "detector_healthy": payload.detector_healthy}
