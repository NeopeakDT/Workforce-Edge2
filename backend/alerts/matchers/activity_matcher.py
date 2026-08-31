"""
backend/alerts/matchers/activity_matcher.py
STEP B — ACTIVITY alert evaluator.

Implements exactly the four ACTIVITY alerts approved in Step A4:
    ACTIVITY_LATE, ACTIVITY_MISSED, ACTIVITY_UNSCHEDULED, ACTIVITY_RUNNING_LONG

Two independent entry points, matching how each alert can actually be
observed given the current data model (see A4 findings: session_classification,
not status, carries EARLY/ON_TIME/LATE/MISSED/UNSCHEDULED; both are only set
at finalization):

    evaluate_finalized_instance(activity_instance_id)
        Called once an activity_instance has status='ENDED' and a
        session_classification. Handles LATE / MISSED / UNSCHEDULED triggers,
        this instance's own RUNNING_LONG self-recovery (it can no longer be
        "running long" once it has ended), and the A4-review LATE/MISSED
        schedule-wide recovery: a later EARLY/ON_TIME occurrence of the SAME
        (farm_id, activity_schedule_id) resolves earlier ACTIVE LATE/MISSED
        alerts for that schedule.

    evaluate_in_progress_instance(activity_instance_id)
        Called for a still-open (status='IN_PROGRESS') instance. Handles
        ACTIVITY_RUNNING_LONG only -- the sole ACTIVITY alert that can be
        evaluated live, since started_offset_min/session_classification stay
        NULL until finalization.

Neither function is wired into activity_aggregator.py or
missed_activity_cron.py yet -- that wiring is Step C (Pipeline Integration).
Step B only implements and proves the evaluator logic itself, callable
standalone (matching how scripts/test_step6_alerts.py already called the
STEP 6.1 evaluator directly).
"""

from pathlib import Path
import sys
from datetime import datetime, timedelta, timezone

import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor

from alerts.alert_conditions import (
    condition_matches,
    upsert_active_alert,
    resolve_active_occurrence,
    resolve_active_alerts_for_schedule_rules,
)

_SCHEDULE_ADHERENCE_METRIC = "session_classification"
_RUNNING_LONG_METRIC = "elapsed_minutes_since_start"


def _load_instance(cur, activity_instance_id):
    cur.execute(
        """
        SELECT id, farm_id, activity_type_id, activity_schedule_id,
               status, session_classification, actual_start_at, zone_id
        FROM activity_instance
        WHERE id = %s
        """,
        (activity_instance_id,),
    )
    return cur.fetchone()


def _load_activity_rules(cur, farm_id, activity_type_id, activity_schedule_id):
    cur.execute(
        """
        SELECT id, alert_type, name, severity, condition
        FROM alert_rule
        WHERE farm_id = %s
          AND alert_type = 'ACTIVITY'
          AND is_active = true
          AND (activity_type_id IS NULL OR activity_type_id = %s)
          AND (activity_schedule_id IS NULL OR activity_schedule_id = %s)
        """,
        (farm_id, activity_type_id, activity_schedule_id),
    )
    return cur.fetchall()


def _build_finalized_message(rule_name, classification_value):
    if classification_value == "LATE":
        return f"{rule_name}: activity completed late."
    if classification_value == "MISSED":
        return f"{rule_name}: scheduled activity was missed."
    if classification_value == "UNSCHEDULED":
        return f"{rule_name}: unscheduled activity detected."
    return rule_name


def evaluate_finalized_instance(activity_instance_id):
    """
    STEP B entry point for ACTIVITY_LATE / ACTIVITY_MISSED /
    ACTIVITY_UNSCHEDULED, plus RUNNING_LONG self-recovery and the
    LATE/MISSED schedule-wide recovery approved in the A4 review.
    """
    with get_cursor() as cur:
        instance = _load_instance(cur, activity_instance_id)
        if not instance:
            return {"instance_found": False}
        rules = _load_activity_rules(
            cur, instance["farm_id"], instance["activity_type_id"], instance["activity_schedule_id"]
        )

    classification = instance["session_classification"]
    created = []

    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _SCHEDULE_ADHERENCE_METRIC:
            continue
        if not condition_matches(condition, classification):
            continue

        target_value = condition.get("value")
        immediately_resolve = target_value == "UNSCHEDULED"

        inserted = upsert_active_alert(
            farm_id=instance["farm_id"],
            rule=rule,
            dedup_key=str(instance["id"]),
            message=_build_finalized_message(rule["name"], target_value),
            details={
                "activity_type_id": instance["activity_type_id"],
                "activity_schedule_id": str(instance["activity_schedule_id"]) if instance["activity_schedule_id"] else None,
                "session_classification": classification,
            },
            activity_instance_id=instance["id"],
            zone_id=instance["zone_id"],
            immediately_resolve=immediately_resolve,
        )
        if inserted:
            created.append(rule["id"])

    # Self-recovery: this instance is no longer IN_PROGRESS, so any
    # RUNNING_LONG alert raised while it was still open is now stale.
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") == _RUNNING_LONG_METRIC:
            resolve_active_occurrence(rule_id=rule["id"], dedup_key=str(instance["id"]))

    # Schedule-wide recovery (A4 review decision #2): a successful
    # occurrence resolves earlier LATE/MISSED alerts for the SAME schedule.
    # Strictly scoped to activity_schedule_id -- see alert_conditions.py
    # docstring for why no activity_type_id fallback is implemented.
    if classification in ("EARLY", "ON_TIME") and instance["activity_schedule_id"]:
        resolve_active_alerts_for_schedule_rules(
            farm_id=instance["farm_id"],
            activity_schedule_id=instance["activity_schedule_id"],
            metric=_SCHEDULE_ADHERENCE_METRIC,
            values=["LATE", "MISSED"],
        )

    return {"instance_found": True, "rules_evaluated": len(rules), "alerts_created": created}


def evaluate_in_progress_instance(activity_instance_id):
    """
    STEP B entry point for ACTIVITY_RUNNING_LONG -- the only ACTIVITY alert
    evaluable while an instance is still open (status='IN_PROGRESS').
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id, ai.farm_id, ai.activity_type_id, ai.activity_schedule_id,
                ai.status, ai.actual_start_at, ai.zone_id,
                s.ideal_end_time, s.tolerance_late_min, f.timezone
            FROM activity_instance ai
            JOIN activity_schedule s ON s.id = ai.activity_schedule_id
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.id = %s AND ai.status = 'IN_PROGRESS'
            """,
            (activity_instance_id,),
        )
        row = cur.fetchone()
        if not row:
            return {"instance_found": False}
        rules = _load_activity_rules(cur, row["farm_id"], row["activity_type_id"], row["activity_schedule_id"])

    farm_tz = pytz.timezone(row["timezone"])
    start_local = row["actual_start_at"].astimezone(farm_tz)
    ideal_end_naive = datetime.combine(start_local.date(), row["ideal_end_time"])
    ideal_end_local = farm_tz.localize(ideal_end_naive)
    if ideal_end_local <= start_local:
        ideal_end_local += timedelta(days=1)
    overdue_cutoff_local = ideal_end_local + timedelta(minutes=row["tolerance_late_min"] or 0)

    now_local = datetime.now(timezone.utc).astimezone(farm_tz)
    is_overdue = now_local > overdue_cutoff_local

    created = []
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _RUNNING_LONG_METRIC or condition.get("operator") != ">":
            continue
        if not is_overdue:
            continue

        inserted = upsert_active_alert(
            farm_id=row["farm_id"],
            rule=rule,
            dedup_key=str(row["id"]),
            message=f"{rule['name']}: activity is still in progress past its expected end time.",
            details={
                "activity_type_id": row["activity_type_id"],
                "activity_schedule_id": str(row["activity_schedule_id"]),
                "actual_start_at": row["actual_start_at"].isoformat(),
            },
            activity_instance_id=row["id"],
            zone_id=row["zone_id"],
        )
        if inserted:
            created.append(rule["id"])

    return {"instance_found": True, "is_overdue": is_overdue, "alerts_created": created}
