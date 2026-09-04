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
_EARLY_START_METRIC = "minutes_before_ideal_start"
_LATE_START_METRIC = "minutes_since_ideal_start"


def _ideal_start_local(schedule_row, activity_date, farm_tz):
    """schedule_row needs ideal_start_time; farm_tz is a pytz timezone."""
    naive = datetime.combine(activity_date, schedule_row["ideal_start_time"])
    return farm_tz.localize(naive)


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
        if target_value == "LATE":
            # STEP C: ACTIVITY_LATE is now generated exclusively by the
            # schedule-keyed evaluate_late_start_sweep() operational evaluator
            # (see docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md
            # §2). Finalization must not create a second, separate ACTIVITY_LATE
            # alert for the historical session_classification='LATE' fact --
            # that remains purely historical/reporting.
            continue
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


def evaluate_early_start(activity_instance_id):
    """
    STEP C — ACTIVITY_EARLY. Point-in-time, instance-keyed: fires (already
    RESOLVED) the moment an instance's actual_start_at is more than
    alert_early_start_min before its schedule's ideal_start_time. Never
    derived from session_classification -- that stays historical/reporting.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT ai.id, ai.farm_id, ai.activity_type_id, ai.activity_schedule_id,
                   ai.actual_start_at, ai.zone_id, ai.activity_date,
                   s.ideal_start_time, f.timezone
            FROM activity_instance ai
            JOIN activity_schedule s ON s.id = ai.activity_schedule_id
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.id = %s
            """,
            (activity_instance_id,),
        )
        row = cur.fetchone()
        if not row or row["actual_start_at"] is None or row["activity_schedule_id"] is None:
            return {"instance_found": bool(row), "alerts_created": []}
        rules = _load_activity_rules(cur, row["farm_id"], row["activity_type_id"], row["activity_schedule_id"])

    farm_tz = pytz.timezone(row["timezone"])
    start_local = row["actual_start_at"].astimezone(farm_tz)
    # Use the instance's own activity_date (already resolved correctly at
    # instance-creation time by activity_aggregator.compute_activity_date,
    # which handles midnight-crossing schedules) rather than recomputing it
    # from start_local.date() here.
    ideal_start_local = _ideal_start_local(row, row["activity_date"], farm_tz)
    minutes_before = (ideal_start_local - start_local).total_seconds() / 60.0

    created = []
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _EARLY_START_METRIC:
            continue
        if not condition_matches(condition, minutes_before):
            continue

        # upsert_active_alert's idempotency guard only blocks a duplicate
        # when an ACTIVE row with the same (rule_id, dedup_key) already
        # exists. immediately_resolve=True inserts an already-RESOLVED row,
        # so that guard never fires here and repeated evaluation of the same
        # instance would otherwise insert a new RESOLVED row every time.
        # This local, instance-scoped check covers that gap without touching
        # the shared helper (which stays correct for every other matcher).
        with get_cursor() as cur2:
            cur2.execute(
                "SELECT 1 FROM alert_log WHERE alert_rule_id = %s AND dedup_key = %s LIMIT 1",
                (rule["id"], str(row["id"])),
            )
            already_recorded = cur2.fetchone() is not None
        if already_recorded:
            continue

        inserted = upsert_active_alert(
            farm_id=row["farm_id"],
            rule=rule,
            dedup_key=str(row["id"]),
            message=f"{rule['name']}: activity started {minutes_before:.0f} minutes early.",
            details={
                "activity_type_id": row["activity_type_id"],
                "activity_schedule_id": str(row["activity_schedule_id"]),
                "minutes_before_ideal_start": round(minutes_before, 1),
            },
            activity_instance_id=row["id"],
            zone_id=row["zone_id"],
            immediately_resolve=True,
        )
        if inserted:
            created.append(rule["id"])

    return {"instance_found": True, "alerts_created": created}


def evaluate_late_start_sweep(farm_id=None):
    """
    STEP C — ACTIVITY_LATE. Schedule-keyed periodic sweep (called from
    aggregation/alerts_cron.py every 60s), not instance-keyed -- by
    definition no activity_instance exists yet when this should first fire.
    Deliberately ignores activity_schedule.tolerance_late_min (that's
    ACTIVITY_MISSED's mechanism); uses alert_rule.condition's own
    minutes_since_ideal_start threshold instead.

    Three occurrence states per (farm, schedule, activity_date) -- there can
    be at most one activity_instance row per that triple
    (uq_missed_schedule_per_day is a full, non-partial unique index):

      1. No row at all -- the occurrence is still undecided. Ordinary LATE
         create/dedup logic applies.
      2. A row with actual_start_at IS NOT NULL -- a genuine occurrence
         (source='AI' today; also 'MANUAL'/'AI_WITH_MANUAL_OVERRIDE' per the
         activity_source enum, not source-specific by design). The activity
         actually started -- LATE may resolve.
      3. A row with session_classification='MISSED' AND source='SYSTEM' --
         missed_activity_cron.py's placeholder, inserted once an occurrence
         is confirmed missed (always actual_start_at=NULL). This occurrence
         is CLOSED: missed_activity_cron.py's own
         resolve_late_start_alerts_for_occurrence() already resolved LATE
         synchronously the moment this row was created, and
         evaluate_finalized_instance() already had its chance to create the
         separate ACTIVITY_MISSED alert. The sweep must take no action at
         all here -- neither create (the occurrence isn't undecided) nor
         resolve (already done once; re-resolving here would just be a
         harmless no-op, but re-CREATING on a later tick, after some
         hypothetical future resolution of that already-closed LATE row,
         would not be -- see the investigation report this fix closes).

    Any other, unrecognized shape (actual_start_at NULL but not matching the
    MISSED/SYSTEM signature -- not currently possible against this schema,
    but not asserted against) falls through to state 1's undecided handling,
    preserving prior behavior for anything not explicitly covered.
    """
    with get_cursor() as cur:
        query = """
            SELECT s.id AS schedule_id, s.farm_id, s.activity_type_id,
                   s.ideal_start_time, f.timezone
            FROM activity_schedule s
            JOIN farm f ON f.id = s.farm_id
            WHERE s.is_active = true
        """
        params = ()
        if farm_id:
            query += " AND s.farm_id = %s"
            params = (farm_id,)
        cur.execute(query, params)
        schedules = cur.fetchall()

    created = []
    resolved = 0
    for sched in schedules:
        farm_tz = pytz.timezone(sched["timezone"])
        now_local = datetime.now(timezone.utc).astimezone(farm_tz)
        today_local = now_local.date()
        ideal_start_local = _ideal_start_local(sched, today_local, farm_tz)
        minutes_since = (now_local - ideal_start_local).total_seconds() / 60.0
        if minutes_since < 0:
            continue  # ideal start hasn't happened yet today

        with get_cursor() as cur:
            cur.execute(
                """
                SELECT actual_start_at, session_classification, source
                FROM activity_instance
                WHERE farm_id = %s AND activity_schedule_id = %s AND activity_date = %s
                LIMIT 1
                """,
                (sched["farm_id"], sched["schedule_id"], today_local),
            )
            existing = cur.fetchone()
            rules = _load_activity_rules(cur, sched["farm_id"], sched["activity_type_id"], sched["schedule_id"])

        instance_exists = existing is not None and existing["actual_start_at"] is not None
        occurrence_closed = (
            existing is not None
            and existing["actual_start_at"] is None
            and existing["session_classification"] == "MISSED"
            and existing["source"] == "SYSTEM"
        )

        dedup_key = f"{sched['schedule_id']}:{today_local.isoformat()}"
        for rule in rules:
            condition = rule["condition"] or {}
            if condition.get("metric") != _LATE_START_METRIC:
                continue

            if occurrence_closed:
                # STATE 3: already conclusively MISSED. LATE was already
                # resolved once by resolve_late_start_alerts_for_occurrence()
                # at the moment the MISSED row was created -- take no action
                # here, in either direction.
                continue

            if not instance_exists and condition_matches(condition, minutes_since):
                inserted = upsert_active_alert(
                    farm_id=sched["farm_id"],
                    rule=rule,
                    dedup_key=dedup_key,
                    message=f"{rule['name']}: no activity started {minutes_since:.0f} minutes after ideal start.",
                    details={
                        "activity_type_id": sched["activity_type_id"],
                        "activity_schedule_id": str(sched["schedule_id"]),
                        "activity_date": today_local.isoformat(),
                        "minutes_since_ideal_start": round(minutes_since, 1),
                    },
                )
                if inserted:
                    # Part F(b): close (not eliminate -- full atomicity would
                    # need cross-process locking, out of scope) the race
                    # window between this function's own read of
                    # instance_exists/occurrence_closed above and this
                    # INSERT: re-check right now, cheaply, whether the
                    # occurrence has since resolved itself either way --
                    # a genuine occurrence appearing (self-correct: resolve),
                    # or missed_activity_cron.py's MISSED insert landing in
                    # this exact window (also self-correct: resolve, since
                    # its own resolve_late_start_alerts_for_occurrence() call
                    # would have found nothing yet to resolve and this row
                    # would otherwise be left ACTIVE for an already-closed
                    # occurrence). Otherwise, leave the new row ACTIVE.
                    with get_cursor() as recheck_cur:
                        recheck_cur.execute(
                            """
                            SELECT actual_start_at, session_classification, source
                            FROM activity_instance
                            WHERE farm_id = %s AND activity_schedule_id = %s AND activity_date = %s
                            LIMIT 1
                            """,
                            (sched["farm_id"], sched["schedule_id"], today_local),
                        )
                        recheck_row = recheck_cur.fetchone()
                    instance_now_exists = recheck_row is not None and recheck_row["actual_start_at"] is not None
                    now_closed = (
                        recheck_row is not None
                        and recheck_row["actual_start_at"] is None
                        and recheck_row["session_classification"] == "MISSED"
                        and recheck_row["source"] == "SYSTEM"
                    )
                    if instance_now_exists or now_closed:
                        resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)
                    else:
                        created.append(rule["id"])
            elif instance_exists:
                resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {"schedules_evaluated": len(schedules), "alerts_created": created, "alerts_resolved": resolved}


def resolve_late_start_alerts_for_occurrence(farm_id, activity_type_id, activity_schedule_id, activity_date):
    """
    STEP C -- called by missed_activity_cron.py after a new ACTIVITY_MISSED
    row commits, to resolve any still-ACTIVE ACTIVITY_LATE for the SAME
    occurrence (MISSED supersedes LATE). Reuses the exact same rule-scoping
    mechanism as evaluate_late_start_sweep() (_load_activity_rules) rather
    than a separate/duplicated lookup -- never falls back to matching by
    activity_type_id alone; the schedule_id + activity_date pairing (via
    the dedup_key) is the sole occurrence identity.
    """
    if activity_schedule_id is None:
        return 0
    with get_cursor() as cur:
        rules = _load_activity_rules(cur, farm_id, activity_type_id, activity_schedule_id)
    dedup_key = f"{activity_schedule_id}:{activity_date.isoformat()}"
    resolved = 0
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _LATE_START_METRIC:
            continue
        resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)
    return resolved


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
