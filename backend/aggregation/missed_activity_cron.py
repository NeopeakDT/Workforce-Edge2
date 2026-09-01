#!/usr/bin/env python3
"""
STEP-5 — FINALIZATION + MISSED (AUTHORITATIVE)

This is the ONLY place where activity_instance.status is finalized.

Responsibilities:
1. Finalize completed activities:
   - EARLY / ON_TIME / LATE
2. Create MISSED activities:
   - Exactly one MISSED per (farm, schedule, activity_date)

Rules:
- Only process instances with actual_end_at IS NOT NULL
- Never touch IN_PROGRESS instances
- Use FARM LOCAL TIMEZONE
- Fully idempotent and safe to rerun
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import pytz

# -------------------------------------------------
# Bootstrap backend path
# -------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now
from aggregation.activity_schedule_resolver import resolve as resolve_completed_activities


# -------------------------------------------------
# STEP-5A — FINALIZE COMPLETED ACTIVITIES
# -------------------------------------------------
# This below logic already implemented in activity_schedule_resolver.py
# def finalize_completed_activities():
#     """
#     Finalize EARLY / ON_TIME / LATE for ended activities.

#     Preconditions:
#     - actual_end_at IS NOT NULL
#     - activity_schedule_id IS NOT NULL
#     - status is still IN_PROGRESS
#     """
#     now = utc_now()

#     with get_cursor() as cur:
#         cur.execute(
#             """
#             SELECT
#                 ai.id,
#                 ai.started_offset_min,
#                 s.tolerance_early_min,
#                 s.tolerance_late_min
#             FROM activity_instance ai
#             JOIN activity_schedule s
#               ON s.id = ai.activity_schedule_id
#             WHERE ai.actual_end_at IS NOT NULL
#               AND ai.activity_schedule_id IS NOT NULL
#               AND ai.status = 'IN_PROGRESS'
#             """
#         )

#         for row in cur.fetchall():
#             offset = row["started_offset_min"]

#             if offset is None:
#                 # Safety guard: should not happen, but skip if offsets missing
#                 continue

#             if offset < -row["tolerance_early_min"]:
#                 status = "EARLY"
#             elif offset > row["tolerance_late_min"]:
#                 status = "LATE"
#             else:
#                 status = "ON_TIME"

#             cur.execute(
#                 """
#                 UPDATE activity_instance
#                 SET status = %s,
#                     updated_at = %s
#                 WHERE id = %s
#                 """,
#                 (status, now, row["id"]),
#             )


# -------------------------------------------------
# STEP-5B — CREATE MISSED ACTIVITIES
# -------------------------------------------------
def detect_missed_activities():
    """
    Create MISSED activity_instance rows when:
    - Ideal end + late tolerance has passed (full window plus grace elapsed)
    - No activity_instance exists for that (farm, schedule, activity_date)

    Idempotent against uq_missed_schedule_per_day: any existing row for the
    same (farm, schedule, date) blocks MISSED insert (ON CONFLICT DO NOTHING).
    """
    
    now_utc = utc_now()

    # STEP C: (instance_id, farm_id, activity_type_id, schedule_id, activity_date)
    # for every newly-created MISSED row this run, evaluated for alerts only
    # after the outer commit below (see module-level note in task-4-brief.md
    # -- evaluating inline would read across an uncommitted connection).
    newly_missed = []

    with get_cursor() as cur:
        # 1. Load all active schedules with farm timezone
        cur.execute(
            """
            SELECT
                s.id AS schedule_id,
                s.farm_id,
                s.activity_type_id,
                s.ideal_start_time,
                s.ideal_end_time,
                s.tolerance_late_min,
                f.timezone
            FROM activity_schedule s
            JOIN farm f ON f.id = s.farm_id
            WHERE s.is_active = true
            """
        )
    
        schedules = cur.fetchall()
    
        for s in schedules:
            farm_id = s["farm_id"]
            schedule_id = s["schedule_id"]
            activity_type_id = s["activity_type_id"]
    
            farm_tz = pytz.timezone(s["timezone"])
            local_now = now_utc.astimezone(farm_tz)

            # Check today and yesterday.
            for days_back in range(2):
                activity_date = local_now.date() - timedelta(days=days_back)

                # -------------------------------------------------
                # Compute late cutoff from IDEAL END (LOCAL → UTC)
                # -------------------------------------------------
                ideal_start_naive = datetime.combine(
                    activity_date,
                    s["ideal_start_time"],
                )
                ideal_end_naive = datetime.combine(
                    activity_date,
                    s["ideal_end_time"],
                )

                ideal_start_local = farm_tz.localize(ideal_start_naive)
                ideal_end_local = farm_tz.localize(ideal_end_naive)

                # Cross-midnight handling
                if ideal_end_local <= ideal_start_local:
                    ideal_end_local += timedelta(days=1)

                # MISSED cutoff = ideal_end + late tolerance
                late_cutoff_local = ideal_end_local + timedelta(
                    minutes=s["tolerance_late_min"]
                )

                late_cutoff_utc = late_cutoff_local.astimezone(timezone.utc)

                print(
                    f"[MISSED_CHECK] "
                    f"schedule={schedule_id} "
                    f"date={activity_date} "
                    f"cutoff={late_cutoff_utc} "
                    f"now={now_utc}"
                )

                # Wait until ideal_end + late_tolerance before marking MISSED.
                if now_utc <= late_cutoff_utc:
                    continue

                # Unique index uq_missed_schedule_per_day is on
                # (farm_id, activity_schedule_id, activity_date) for ALL rows.
                # Skip if ANY instance already covers this schedule/day
                # (MISSED, AI with NULL classification, EARLY/ON_TIME/LATE, etc.).
                cur.execute(
                    """
                    SELECT id, source, status, session_classification
                    FROM activity_instance
                    WHERE farm_id = %s
                      AND activity_schedule_id = %s
                      AND activity_date = %s
                    LIMIT 1
                    """,
                    (
                        farm_id,
                        schedule_id,
                        activity_date,
                    ),
                )
                existing = cur.fetchone()

                print(
                    f"[MISSED_EXISTS] "
                    f"schedule={schedule_id} "
                    f"date={activity_date} "
                    f"already_exists={bool(existing)} "
                    f"source={existing['source'] if existing else None} "
                    f"status={existing['status'] if existing else None} "
                    f"classification={existing['session_classification'] if existing else None}"
                )

                if existing:
                    continue

                cur.execute(
                    """
                    INSERT INTO activity_instance
                    (
                        id,
                        farm_id,
                        activity_type_id,
                        activity_schedule_id,
                        activity_date,

                        actual_start_at,
                        actual_end_at,
                        actual_duration_sec,

                        started_offset_min,
                        ended_offset_min,
                        within_ideal_window,

                        status,
                        session_classification,

                        source,

                        created_at,
                        updated_at
                    )
                    VALUES
                    (
                        gen_random_uuid(),
                        %s,
                        %s,
                        %s,
                        %s,

                        NULL,
                        NULL,
                        0,

                        NULL,
                        NULL,
                        FALSE,

                        'ENDED',
                        'MISSED',

                        'SYSTEM',

                        %s,
                        %s
                    )
                    ON CONFLICT (farm_id, activity_schedule_id, activity_date)
                    DO NOTHING
                    RETURNING id
                    """,
                    (
                        farm_id,
                        activity_type_id,
                        schedule_id,
                        activity_date,
                        now_utc,
                        now_utc,
                    ),
                )

                if cur.rowcount:
                    new_row = cur.fetchone()
                    print(
                        f"[MISSED_CREATED] "
                        f"schedule={schedule_id} "
                        f"activity_type={activity_type_id} "
                        f"date={activity_date}"
                    )
                    newly_missed.append(
                        (new_row["id"], farm_id, activity_type_id, schedule_id, activity_date)
                    )
                else:
                    print(
                        f"[MISSED_SKIPPED] "
                        f"schedule={schedule_id} "
                        f"date={activity_date} "
                        f"reason=conflict"
                    )

        cur.connection.commit()

    # STEP C: now that the MISSED rows are committed and visible to other
    # connections, resolve any still-ACTIVE ACTIVITY_LATE for the same
    # (schedule, date) -- MISSED supersedes it -- and let the finalized-
    # instance matcher fire ACTIVITY_MISSED on each new row. Never let an
    # alert-layer failure block missed-activity detection itself, which has
    # already fully committed by this point regardless of what happens below.
    if newly_missed:
        from alerts.matchers import activity_matcher as _activity_matcher

        for instance_id, farm_id, activity_type_id, schedule_id, activity_date in newly_missed:
            try:
                _activity_matcher.resolve_late_start_alerts_for_occurrence(
                    farm_id, activity_type_id, schedule_id, activity_date
                )
                _activity_matcher.evaluate_finalized_instance(instance_id)
            except Exception as e:
                print(f"[MISSED][ALERT_WARN] evaluator failed for instance={instance_id}: {e}")


# -------------------------------------------------
# RUNNER
# -------------------------------------------------
if __name__ == "__main__":
    # If invoked directly, run STEP-5A before STEP-5B.
    resolve_completed_activities()
    detect_missed_activities()
