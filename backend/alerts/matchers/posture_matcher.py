"""
backend/alerts/matchers/posture_matcher.py
STEP B — POSTURE alert evaluator.

Implements only POSTURE_DATA_STALE -- the sole POSTURE alert approved for
Step B (POSTURE_HIGH_RESTING_PERCENTAGE / POSTURE_LOW_STANDING_PERCENTAGE
are excluded: their numeric thresholds are still pending business approval
per the A4 review).

Freshness is read from the MOST RECENT posture_observation row of ANY mode,
mirroring dashboard_query_service.get_latest_observation_and_heartbeat --
a MILKING-window synthetic row still counts as evidence the pipeline is
alive, so staleness is never falsely raised just because no NORMAL-mode
row has landed recently.

Not wired into posture_scheduler.py yet -- that wiring is Step C.

TASK 2 -- MILKING-aware message: when the MOST RECENT posture_observation
row's metadata->>'mode' is 'MILKING', the STALE message is replaced with
"Cows are in milking for X minutes/seconds." instead of the generic
staleness wording. This is message-text-only -- it does not change when
the alert fires, its threshold, severity, lifecycle, or dedup (those all
still key purely off observation_age_minutes / age_minutes, exactly as
before).

The MILKING duration is derived exclusively from posture_observation
history (_find_current_milking_start): walk backward from the most recent
row while metadata->>'mode' stays 'MILKING', stop at the first row that
isn't (or when history runs out), and use the earliest such row's
observed_at as the start of the CURRENT CONTINUOUS milking period.
Deliberately never sourced from activity_instance.actual_start_at -- the
posture pipeline's MILKING state and the activity pipeline's actual_start_at
are two independently-timestamped signals (see this module's docstring
context in the Task 2 audit) and must not be conflated. If the freshly
re-queried latest row is somehow not MILKING by the time
_find_current_milking_start runs (a same-process re-read; not expected to
differ from the initial read), no duration is fabricated -- the generic
staleness message is used instead.
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

_STALENESS_METRIC = "observation_age_minutes"
_MILKING_MODE = "MILKING"
# Bound on how far back to scan for the start of the current continuous
# MILKING run. posture_scheduler.py flushes one MILKING row roughly every
# DB_WRITE_INTERVAL_SECONDS (300s); 200 rows is comfortably more than any
# realistic milking window while still keeping the query bounded.
_MILKING_HISTORY_SCAN_LIMIT = 200

# Maximum gap between two consecutive MILKING rows that still counts as
# the SAME continuous MILKING period. posture_scheduler.py's
# DB_WRITE_INTERVAL_SECONDS (300s) is the expected cadence for both
# NORMAL and scheduled-MILKING flushes; 600s (2x) tolerates exactly one
# missed/delayed flush without breaking continuity, while a gap beyond
# that no longer reflects a healthy, uninterrupted pipeline -- and not
# coincidentally matches the existing POSTURE_DATA_STALE 10-minute
# threshold, so "continuous" here means the same thing "not stale" means
# elsewhere in this module.
_MAX_MILKING_CONTINUITY_GAP_SECONDS = 600


def _load_posture_rules(cur, farm_id):
    cur.execute(
        """
        SELECT id, alert_type, name, severity, condition
        FROM alert_rule
        WHERE farm_id = %s AND alert_type = 'POSTURE' AND is_active = true
        """,
        (farm_id,),
    )
    return cur.fetchall()


def _find_current_milking_start(cur, farm_id, zone_id):
    """
    Return the observed_at of the earliest row in the unbroken run of
    metadata->>'mode' = 'MILKING' posture_observation rows ending at the
    most recent row for this zone, or None if the most recent row itself
    is not MILKING (or no rows exist).

    A run is "unbroken" only while BOTH hold between consecutive rows:
      - mode stays 'MILKING'
      - the gap to the previously-accepted (more recent) row's
        observed_at is <= _MAX_MILKING_CONTINUITY_GAP_SECONDS
    A gap larger than that (e.g. a 30-minute hole between two MILKING
    rows) ends the walk there -- the newer MILKING observation after the
    gap is treated as the start of a NEW continuous period, not a
    continuation of the older one.

    Never touches activity_instance -- this is derived purely from
    posture_observation's own historical rows, per the CRITICAL
    requirement that actual_start_at not be used as a stand-in for the
    posture pipeline's own MILKING-state timestamps.
    """
    cur.execute(
        """
        SELECT observed_at, metadata ->> 'mode' AS mode
        FROM posture_observation
        WHERE farm_id = %s AND zone_id = %s
        ORDER BY observed_at DESC
        LIMIT %s
        """,
        (farm_id, zone_id, _MILKING_HISTORY_SCAN_LIMIT),
    )
    rows = cur.fetchall()

    milking_start = None
    previous_observed_at = None
    for row in rows:
        if row["mode"] != _MILKING_MODE:
            break
        if previous_observed_at is not None:
            gap_seconds = (previous_observed_at - row["observed_at"]).total_seconds()
            if gap_seconds > _MAX_MILKING_CONTINUITY_GAP_SECONDS:
                break
        milking_start = row["observed_at"]
        previous_observed_at = row["observed_at"]
    return milking_start


def _format_milking_duration_message(duration_seconds: float) -> str:
    """
    "Cows are in milking for X minutes." (or "X seconds." under a minute).
    """
    if duration_seconds < 60:
        value = max(int(round(duration_seconds)), 0)
        unit = "second" if value == 1 else "seconds"
    else:
        value = int(round(duration_seconds / 60.0))
        unit = "minute" if value == 1 else "minutes"
    return f"Cows are in milking for {value} {unit}."


def _build_stale_message(*, rule_name, age_minutes, farm_id, zone_id, latest_mode):
    """
    Preserves the existing generic staleness message exactly as before
    unless the most recent observation's mode is MILKING, in which case
    the MILKING-duration message (Task 2) is used instead. Only called
    at the moment an alert is about to be created/kept ACTIVE -- does not
    affect whether/when that happens.
    """
    if latest_mode == _MILKING_MODE:
        with get_cursor() as cur:
            milking_start = _find_current_milking_start(cur, farm_id, zone_id)
        if milking_start is not None:
            duration_seconds = (utc_now() - milking_start).total_seconds()
            return _format_milking_duration_message(duration_seconds)
        # Defensive fallback only -- see docstring above. Not expected to
        # be reachable in practice since latest_mode already came from the
        # same 'MILKING' row this function re-reads.
    return f"{rule_name}: no posture data received for {age_minutes:.1f} minutes."


def evaluate_zone_staleness(farm_id, zone_id):
    """
    STEP B entry point, callable standalone per zone.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, metadata
            FROM posture_observation
            WHERE farm_id = %s AND zone_id = %s
            ORDER BY observed_at DESC
            LIMIT 1
            """,
            (farm_id, zone_id),
        )
        latest = cur.fetchone()
        rules = _load_posture_rules(cur, farm_id)

    if not latest:
        # No observation has EVER been recorded for this zone -- not the
        # same thing as "went stale". Per A4: don't turn missing data into
        # an alert here (NO_DATA != a real reading). A zone with zero
        # history is a configuration gap, a separate future concern.
        return {"observation_found": False}

    age_minutes = (utc_now() - latest["observed_at"]).total_seconds() / 60.0
    latest_mode = (latest["metadata"] or {}).get("mode")

    created = []
    resolved = 0
    dedup_key = str(zone_id)
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _STALENESS_METRIC:
            continue

        if condition_matches(condition, age_minutes):
            message = _build_stale_message(
                rule_name=rule["name"],
                age_minutes=age_minutes,
                farm_id=farm_id,
                zone_id=zone_id,
                latest_mode=latest_mode,
            )
            inserted = upsert_active_alert(
                farm_id=farm_id,
                rule=rule,
                dedup_key=dedup_key,
                message=message,
                details={"zone_id": str(zone_id), "observation_age_minutes": round(age_minutes, 2)},
                zone_id=zone_id,
            )
            if inserted:
                created.append(rule["id"])
        else:
            resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {
        "observation_found": True,
        "age_minutes": age_minutes,
        "alerts_created": created,
        "alerts_resolved": resolved,
    }
