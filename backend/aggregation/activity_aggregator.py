#!/usr/bin/env python3
"""
PHASE-5 — ACTIVITY AGGREGATOR (FINAL, TIMEZONE-SAFE)

Responsibilities:
STEP-4:
- Finalize END_CANDIDATE
- Set actual_end_at, duration
- DO NOT classify status

STEP-5:
- Bind activity_schedule
- Compute started_offset_min using FARM TIMEZONE
- Classify EARLY / ON_TIME / LATE
"""

from datetime import datetime, timezone
from pathlib import Path
import sys

# -------------------------------------------------------------------
# Bootstrap
# -------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

# -------------------------------------------------------------------
# STEP-4 — FINALIZE ENDED ACTIVITIES
# -------------------------------------------------------------------
def finalize_ended_activities():
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id AS activity_instance_id,
                ai.actual_start_at,
                MAX(ade.event_time) AS actual_end_at
            FROM activity_instance ai
            JOIN activity_detection_event ade
              ON ade.activity_instance_id = ai.id
            WHERE ai.status = 'IN_PROGRESS'
              AND ai.actual_start_at IS NOT NULL
              AND ai.actual_end_at IS NULL
              AND ade.event_type = 'END_CANDIDATE'
            GROUP BY ai.id, ai.actual_start_at
            """
        )

        rows = cur.fetchall()

        for row in rows:
            instance_id = row["activity_instance_id"]
            start_at = row["actual_start_at"]
            end_at = row["actual_end_at"]

            # Guard against bad data
            if end_at <= start_at:
                continue

            duration_sec = int((end_at - start_at).total_seconds())

            cur.execute(
                """
                UPDATE activity_instance
                SET
                    actual_end_at = %s,
                    actual_duration_sec = %s,
                    updated_at = %s
                WHERE id = %s
                  AND status = 'IN_PROGRESS'
                """,
                (end_at, duration_sec, now, instance_id),
            )

            print(
                f"[FINALIZED]"
                f"[INSTANCE={instance_id}]"
                f"[DURATION_SEC={duration_sec}]"
            )

# -------------------------------------------------------------------
# STEP-5 — CLASSIFY COMPLETED ACTIVITIES (TIMEZONE SAFE)
# -------------------------------------------------------------------
def classify_completed_activities():
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id,
                ai.farm_id,
                ai.activity_type_id,
                ai.activity_date,
                ai.actual_start_at,
                f.timezone AS farm_timezone
            FROM activity_instance ai
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.status = 'IN_PROGRESS'
              AND ai.actual_end_at IS NOT NULL
            """
        )

        instances = cur.fetchall()

        for ai in instances:
            instance_id = ai["id"]
            farm_id = ai["farm_id"]
            activity_type_id = ai["activity_type_id"]
            activity_date = ai["activity_date"]
            actual_start_at = ai["actual_start_at"]
            farm_tz = ai["farm_timezone"]

            # ---------------------------------------------------------
            # Find matching schedule (ONE PER SCHEDULE PER DAY)
            # ---------------------------------------------------------
            cur.execute(
                """
                SELECT *
                FROM activity_schedule
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND is_active = true
                ORDER BY ideal_start_time
                """,
                (farm_id, activity_type_id),
            )

            schedules = cur.fetchall()
            if not schedules:
                continue

            matched_schedule = None
            started_offset_min = None
            within_ideal = False
            status = "LATE"  # default

            for s in schedules:
                tol_early = s["tolerance_early_min"]
                tol_late = s["tolerance_late_min"]

                # -------------------------------------------------
                # Build IDEAL START UTC (CRITICAL LOGIC)
                # -------------------------------------------------
                cur.execute(
                    """
                    SELECT
                        (
                            ( %s::date + %s )
                            AT TIME ZONE %s
                        ) AS ideal_start_utc
                    """,
                    (
                        activity_date,
                        s["ideal_start_time"],
                        farm_tz,
                    ),
                )

                ideal_start_utc = cur.fetchone()["ideal_start_utc"]

                offset_min = int(
                    (actual_start_at - ideal_start_utc).total_seconds() / 60
                )

                if -tol_early <= offset_min <= tol_late:
                    matched_schedule = s
                    started_offset_min = offset_min
                    within_ideal = True
                    status = "ON_TIME"
                    break

                if offset_min < -tol_early:
                    matched_schedule = s
                    started_offset_min = offset_min
                    status = "EARLY"
                    break

                if offset_min > tol_late:
                    matched_schedule = s
                    started_offset_min = offset_min
                    status = "LATE"
                    # continue checking later schedules

            if not matched_schedule:
                continue

            cur.execute(
                """
                UPDATE activity_instance
                SET
                    activity_schedule_id = %s,
                    status = %s,
                    started_offset_min = %s,
                    within_ideal_window = %s,
                    updated_at = %s
                WHERE id = %s
                  AND status = 'IN_PROGRESS'
                """,
                (
                    matched_schedule["id"],
                    status,
                    started_offset_min,
                    within_ideal,
                    now,
                    instance_id,
                ),
            )

            print(
                f"[CLASSIFIED]"
                f"[INSTANCE={instance_id}]"
                f"[STATUS={status}]"
                f"[OFFSET_MIN={started_offset_min}]"
            )

# -------------------------------------------------------------------
# RUNNER
# -------------------------------------------------------------------
def run():
    finalize_ended_activities()
    classify_completed_activities()


if __name__ == "__main__":
    run()
