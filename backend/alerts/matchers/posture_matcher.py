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


def evaluate_zone_staleness(farm_id, zone_id):
    """
    STEP B entry point, callable standalone per zone.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at
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

    created = []
    resolved = 0
    dedup_key = str(zone_id)
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _STALENESS_METRIC:
            continue

        if condition_matches(condition, age_minutes):
            inserted = upsert_active_alert(
                farm_id=farm_id,
                rule=rule,
                dedup_key=dedup_key,
                message=f"{rule['name']}: no posture data received for {age_minutes:.1f} minutes.",
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
