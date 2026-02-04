#!/usr/bin/env python3
"""
PHASE-5 — TEST ACTIVITY AGGREGATOR (FINAL, DISAMBIGUATED)

STEP-4:
- Finalize END_CANDIDATE

STEP-5:
- Select closest schedule by ideal_start_time
- Classify EARLY / ON_TIME / LATE relative to THAT schedule only

NOTE:
- days_of_week intentionally ignored in Phase-5
"""

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

        for row in cur.fetchall():
            start_at = row["actual_start_at"]
            end_at = row["actual_end_at"]

            if not end_at or end_at <= start_at:
                continue

            duration_sec = int((end_at - start_at).total_seconds())

            cur.execute(
                """
                UPDATE activity_instance
                SET actual_end_at = %s,
                    actual_duration_sec = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (end_at, duration_sec, now, row["activity_instance_id"]),
            )

            print(
                f"[FINALIZED]"
                f"[INSTANCE={row['activity_instance_id']}]"
                f"[DURATION_SEC={duration_sec}]"
            )

# -------------------------------------------------------------------
# STEP-5 — CLASSIFY COMPLETED ACTIVITIES (FIXED)
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

        for ai in cur.fetchall():
            instance_id = ai["id"]
            farm_id = ai["farm_id"]
            activity_type_id = ai["activity_type_id"]
            activity_date = ai["activity_date"]
            actual_start_at = ai["actual_start_at"]
            farm_tz = ai["farm_timezone"]

            # ---------------------------------------------------------
            # Load schedules
            # ---------------------------------------------------------
            cur.execute(
                """
                SELECT *
                FROM activity_schedule
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND is_active = true
                """,
                (farm_id, activity_type_id),
            )

            schedules = cur.fetchall()
            if not schedules:
                print(f"[NO_SCHEDULES][INSTANCE={instance_id}]")
                continue

            # ---------------------------------------------------------
            # Compute closest schedule by ideal_start_utc
            # ---------------------------------------------------------
            candidates = []

            for s in schedules:
                cur.execute(
                    """
                    SELECT
                        ((%s::date + %s) AT TIME ZONE %s)
                        AS ideal_start_utc
                    """,
                    (activity_date, s["ideal_start_time"], farm_tz),
                )

                ideal_start_utc = cur.fetchone()["ideal_start_utc"]

                diff_sec = abs(
                    (actual_start_at - ideal_start_utc).total_seconds()
                )

                candidates.append((diff_sec, s, ideal_start_utc))

            candidates.sort(key=lambda x: x[0])
            _, matched_schedule, ideal_start_utc = candidates[0]

            # ---------------------------------------------------------
            # Classify relative to chosen schedule ONLY
            # ---------------------------------------------------------
            tol_early = matched_schedule["tolerance_early_min"]
            tol_late = matched_schedule["tolerance_late_min"]

            started_offset_min = int(
                (actual_start_at - ideal_start_utc).total_seconds() / 60
            )

            if started_offset_min < -tol_early:
                status = "EARLY"
                within_ideal = False
            elif started_offset_min > tol_late:
                status = "LATE"
                within_ideal = False
            else:
                status = "ON_TIME"
                within_ideal = True

            cur.execute(
                """
                UPDATE activity_instance
                SET activity_schedule_id = %s,
                    status = %s,
                    started_offset_min = %s,
                    within_ideal_window = %s,
                    updated_at = %s
                WHERE id = %s
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
                f"[SCHEDULE={matched_schedule['label']}]"
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
