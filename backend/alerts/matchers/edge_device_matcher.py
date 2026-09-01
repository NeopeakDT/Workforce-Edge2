"""
backend/alerts/matchers/edge_device_matcher.py
STEP B — EDGE_DEVICE alert evaluator.

Implements only EDGE_DEVICE_OFFLINE -- the sole EDGE_DEVICE alert approved
for Step B (the HIGH_CPU/GPU_TEMPERATURE/DISK/MEMORY alerts are excluded:
their numeric thresholds are still pending business approval per the A4
review; EDGE_DEVICE_HIGH_GPU_USAGE was excluded entirely in A4 -- no GPU
utilization field exists in edge_device_heartbeat, only gpu_temp_c).

Freshness is read from edge_device.last_seen_at -- the same field
aggregation/device_health_monitor.py already uses for its own offline
detection. This module does not redefine the 10-minute threshold; the
threshold is supplied via the alert_rule's own condition JSON (callers
should seed it from device_health_monitor.OFFLINE_THRESHOLD_MIN for
consistency, per A4's "reuse existing constants" instruction), so nothing
here special-cases or hardcodes that number.

Not wired into heartbeat_ingest_api.py or device_health_monitor.py yet --
that wiring is Step C.
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

from alerts.alert_conditions import (
    condition_matches,
    upsert_active_alert,
    resolve_active_occurrence,
)

_HEARTBEAT_AGE_METRIC = "heartbeat_age_minutes"


def _load_edge_device_rules(cur, farm_id):
    cur.execute(
        """
        SELECT id, alert_type, name, severity, condition
        FROM alert_rule
        WHERE farm_id = %s AND alert_type = 'EDGE_DEVICE' AND is_active = true
        """,
        (farm_id,),
    )
    return cur.fetchall()


def evaluate_device_offline(device_id):
    """
    STEP B entry point, callable standalone per device.
    """
    with get_cursor() as cur:
        cur.execute(
            "SELECT id, farm_id, last_seen_at FROM edge_device WHERE id = %s",
            (device_id,),
        )
        device = cur.fetchone()
        if not device:
            return {"device_found": False}
        rules = _load_edge_device_rules(cur, device["farm_id"])

    if device["last_seen_at"] is None:
        # Device has never sent a heartbeat -- not the same thing as
        # "went offline". Not alertable from this signal alone.
        return {"device_found": True, "last_seen_at": None}

    age_minutes = (utc_now() - device["last_seen_at"]).total_seconds() / 60.0

    created = []
    resolved = 0
    dedup_key = str(device_id)
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _HEARTBEAT_AGE_METRIC:
            continue

        if condition_matches(condition, age_minutes):
            inserted = upsert_active_alert(
                farm_id=device["farm_id"],
                rule=rule,
                dedup_key=dedup_key,
                message=f"{rule['name']}: device has not reported a heartbeat in {age_minutes:.1f} minutes.",
                details={"device_id": str(device_id), "heartbeat_age_minutes": round(age_minutes, 2)},
                device_id=device_id,
            )
            if inserted:
                created.append(rule["id"])
        else:
            resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {
        "device_found": True,
        "age_minutes": age_minutes,
        "alerts_created": created,
        "alerts_resolved": resolved,
    }


_DETECTOR_AGE_METRIC = "detector_heartbeat_age_minutes"


def evaluate_detector_offline(device_id):
    """
    STEP C — WORKFORCE_DETECTOR_OFFLINE. Driven purely by freshness/absence
    of edge_device.detector_last_seen_at (set by the new detector-heartbeat
    ingest endpoint, see api/edge_detector_health_api.py) -- never by a
    received payload's detector_healthy value. A NULL detector_last_seen_at
    (new device, or pre-Step-C rollout) is "not yet observed", not "stale":
    do not alert on it, matching evaluate_device_offline's identical
    handling of last_seen_at IS NULL.
    """
    with get_cursor() as cur:
        cur.execute(
            "SELECT id, farm_id, detector_last_seen_at FROM edge_device WHERE id = %s",
            (device_id,),
        )
        device = cur.fetchone()
        if not device:
            return {"device_found": False}
        rules = _load_edge_device_rules(cur, device["farm_id"])

    if device["detector_last_seen_at"] is None:
        return {"device_found": True, "age_minutes": None, "alerts_created": []}

    age_minutes = (utc_now() - device["detector_last_seen_at"]).total_seconds() / 60.0

    created = []
    resolved = 0
    dedup_key = str(device_id)
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _DETECTOR_AGE_METRIC:
            continue

        if condition_matches(condition, age_minutes):
            inserted = upsert_active_alert(
                farm_id=device["farm_id"],
                rule=rule,
                dedup_key=dedup_key,
                message=f"{rule['name']}: no detector-health pulse in {age_minutes:.1f} minutes.",
                details={"device_id": str(device_id), "detector_heartbeat_age_minutes": round(age_minutes, 2)},
                device_id=device_id,
            )
            if inserted:
                created.append(rule["id"])
        else:
            resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {
        "device_found": True,
        "age_minutes": age_minutes,
        "alerts_created": created,
        "alerts_resolved": resolved,
    }
