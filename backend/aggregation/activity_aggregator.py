#!/usr/bin/env python3
"""
STEP-4 + STEP-5 Activity Aggregator (AUTHORITATIVE)

STEP-4:
- Finalize end time & duration using END_CANDIDATE
- MUST NOT update status

STEP-5:
- Bind activity_schedule
- Compute offsets vs ideal window
- Update status (ON_TIME / EARLY / LATE / MISSED)
"""

from datetime import datetime, timezone, timedelta  

from pathlib import Path
import sys

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


# ---------------------------------------------------------
# STEP-4 — finalize ended activities (NO STATUS UPDATE)
# ---------------------------------------------------------
def finalize_ended_activities():
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id,
                ai.actual_start_at,
                MAX(ade.event_time) AS end_time
            FROM activity_instance ai
            JOIN activity_detection_event ade
              ON ade.activity_type_id = ai.activity_type_id
             AND ade.farm_id = ai.farm_id
            WHERE ade.event_type = 'END_CANDIDATE'
              AND ai.actual_end_at IS NULL
              AND ai.status = 'IN_PROGRESS'
            GROUP BY ai.id, ai.actual_start_at
            """
        )

        rows = cur.fetchall()

        for row in rows:
            instance_id = row["id"]
            start_at = row["actual_start_at"]
            end_at = row["end_time"]

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
                f"[END FINALIZED]"
                f"[INSTANCE={instance_id}]"
                f"[END_AT={end_at}]"
                f"[DURATION_SEC={duration_sec}]"
            )


# ---------------------------------------------------------
# STEP-5 — classify activity vs schedule (STATUS UPDATE)
# ---------------------------------------------------------
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
                ai.actual_start_at
            FROM activity_instance ai
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
            actual_start = ai["actual_start_at"]

            day_of_week = activity_date.weekday()  # 0 = Monday

            cur.execute(
                """
                SELECT *
                FROM activity_schedule
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND is_active = true
                  AND %s = ANY(days_of_week)
                LIMIT 1
                """,
                (farm_id, activity_type_id, day_of_week),
            )

            schedule = cur.fetchone()

            if not schedule:
                continue

            sched_id = schedule["id"]
            tol_early = schedule["tolerance_early_min"]
            tol_late = schedule["tolerance_late_min"]

            ideal_start = datetime.combine(
                activity_date,
                schedule["ideal_start_time"],
                tzinfo=timezone.utc,
            )

            delta_min = int((actual_start - ideal_start).total_seconds() / 60)

            if delta_min < -tol_early:
                status = "EARLY"
                within = False
            elif delta_min > tol_late:
                status = "LATE"
                within = False
            else:
                status = "ON_TIME"
                within = True

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
                    sched_id,
                    status,
                    delta_min,
                    within,
                    now,
                    instance_id,
                ),
            )

            print(
                f"[CLASSIFIED]"
                f"[INSTANCE={instance_id}]"
                f"[STATUS={status}]"
                f"[OFFSET_MIN={delta_min}]"
                f"[WITHIN_IDEAL={within}]"
            )


# ---------------------------------------------------------
# RUNNER
# ---------------------------------------------------------
def run():
    finalize_ended_activities()
    classify_completed_activities()


if __name__ == "__main__":
    run()
