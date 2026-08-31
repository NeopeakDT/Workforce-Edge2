"""
backend/alerts/alert_conditions.py
STEP B — Shared alert_log persistence helpers for the type-specific evaluators.

Used by:
    alerts/matchers/activity_matcher.py
    alerts/matchers/posture_matcher.py
    alerts/matchers/edge_device_matcher.py

This module owns the one INSERT/UPDATE shape every matcher needs against
alert_log:
    - idempotent insert of a new ACTIVE (or immediately-RESOLVED) occurrence,
      guarded by the Step A3 partial unique index
      uq_alert_rule_dedup_active (alert_rule_id, dedup_key)
      WHERE lifecycle_state = 'ACTIVE'. Re-evaluating an already-ACTIVE
      condition is therefore a no-op insert -- this IS the anti-storm
      mechanism approved in A3/A4; this module does not reimplement it.
    - resolving one occurrence, or a schedule-wide batch of occurrences
      (the LATE/MISSED recovery rule approved in the A4 review).

Does NOT decide which alert_rule rows apply or what a "metric" means for a
given alert_type -- that is each matcher's job. This module only compares
an already-resolved observed value against a rule's condition JSON
(condition_matches), and persists the result.
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json

from common.db import get_cursor
from common.time_utils import utc_now


# Canonical A4 condition format:
#   {"metric": ..., "operator": "=" | "!=" | ">" | ">=" | "<" | "<=",
#    "value": ..., "duration_minutes": ...}
_OPERATORS = {
    "=": lambda observed, value: observed == value,
    "!=": lambda observed, value: observed != value,
    ">": lambda observed, value: observed is not None and value is not None and observed > value,
    ">=": lambda observed, value: observed is not None and value is not None and observed >= value,
    "<": lambda observed, value: observed is not None and value is not None and observed < value,
    "<=": lambda observed, value: observed is not None and value is not None and observed <= value,
}


def condition_matches(condition: dict, observed_value) -> bool:
    """
    Evaluate operator/value from a rule's condition JSON against an
    already-resolved observed_value. Resolving *what* the observed value is
    (which DB column or computation a "metric" name maps to) is each
    matcher's responsibility, not this function's.
    """
    if not condition:
        return False
    operator = condition.get("operator")
    if operator not in _OPERATORS:
        return False
    return _OPERATORS[operator](observed_value, condition.get("value"))


def upsert_active_alert(
    *,
    farm_id,
    rule,
    dedup_key: str,
    message: str,
    details: dict,
    activity_instance_id=None,
    zone_id=None,
    device_id=None,
    immediately_resolve: bool = False,
) -> bool:
    """
    Idempotent insert of one alert occurrence.

    immediately_resolve=True is for point-in-time alerts (ACTIVITY_UNSCHEDULED,
    per the A4 review decision) that have no ongoing condition to watch --
    the row is inserted already RESOLVED so it never blocks a future
    occurrence via the partial unique index, while still remaining visible
    in history/recent-alerts queries (nothing filters those by
    lifecycle_state).

    Returns True if a new row was inserted, False if an ACTIVE occurrence
    with the same (alert_rule_id, dedup_key) already existed (dedup no-op).
    """
    now = utc_now()
    lifecycle_state = "RESOLVED" if immediately_resolve else "ACTIVE"
    resolved_at = now if immediately_resolve else None

    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_log (
                farm_id, alert_rule_id, alert_type, activity_instance_id,
                zone_id, device_id, triggered_at, status, lifecycle_state,
                resolved_at, dedup_key, message, details
            )
            SELECT
                %(farm_id)s, %(rule_id)s, %(alert_type)s, %(activity_instance_id)s,
                %(zone_id)s, %(device_id)s, %(now)s, 'SENT', %(lifecycle_state)s,
                %(resolved_at)s, %(dedup_key)s, %(message)s, %(details)s
            WHERE NOT EXISTS (
                SELECT 1 FROM alert_log
                WHERE alert_rule_id = %(rule_id)s
                  AND dedup_key = %(dedup_key)s
                  AND lifecycle_state = 'ACTIVE'
            )
            """,
            {
                "farm_id": farm_id,
                "rule_id": rule["id"],
                "alert_type": rule["alert_type"],
                "activity_instance_id": activity_instance_id,
                "zone_id": zone_id,
                "device_id": device_id,
                "now": now,
                "lifecycle_state": lifecycle_state,
                "resolved_at": resolved_at,
                "dedup_key": dedup_key,
                "message": message,
                "details": Json(details),
            },
        )
        return cur.rowcount > 0


def resolve_active_occurrence(*, rule_id, dedup_key) -> int:
    """
    Clears one ACTIVE occurrence when its own condition stops being true
    (e.g. ACTIVITY_RUNNING_LONG's instance finalizes, a device's heartbeat
    freshens up, a zone's posture data arrives again). Returns rows affected
    (0 or 1).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE alert_log
            SET lifecycle_state = 'RESOLVED', resolved_at = %s
            WHERE alert_rule_id = %s AND dedup_key = %s AND lifecycle_state = 'ACTIVE'
            """,
            (utc_now(), rule_id, dedup_key),
        )
        return cur.rowcount


def resolve_active_alerts_for_schedule_rules(*, farm_id, activity_schedule_id, metric, values) -> int:
    """
    A4-review decision: when a scheduled activity finalizes as EARLY/ON_TIME,
    resolve every currently-ACTIVE alert belonging to a rule scoped to this
    SAME (farm_id, activity_schedule_id) whose condition targets one of
    `values` for `metric` (session_classification LATE/MISSED).

    Deliberately strict: scoped by activity_schedule_id only, never by
    activity_type_id alone. A farm+type-wide rule with
    activity_schedule_id IS NULL will never match here -- NULL never equals
    activity_schedule_id in the subquery below (standard SQL NULL
    semantics), so no broad activity_type_id fallback is implemented, per
    the A4 review's explicit instruction not to allow one automatically.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE alert_log
            SET lifecycle_state = 'RESOLVED', resolved_at = %(now)s
            WHERE lifecycle_state = 'ACTIVE'
              AND alert_type = 'ACTIVITY'
              AND farm_id = %(farm_id)s
              AND alert_rule_id IN (
                    SELECT id FROM alert_rule
                    WHERE farm_id = %(farm_id)s
                      AND activity_schedule_id = %(schedule_id)s
                      AND condition ->> 'metric' = %(metric)s
                      AND condition ->> 'value' = ANY(%(values)s)
              )
            """,
            {
                "now": utc_now(),
                "farm_id": farm_id,
                "schedule_id": activity_schedule_id,
                "metric": metric,
                "values": list(values),
            },
        )
        return cur.rowcount
