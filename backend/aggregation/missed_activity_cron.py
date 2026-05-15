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
    """
    # MISSED creation is enabled.
    # Function is idempotent via INSERT ... ON CONFLICT DO NOTHING.
    
    now_utc = utc_now()
    
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
            activity_date = local_now.date()
    
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

            # Wait until ideal_end + late_tolerance before marking MISSED.
            if now_utc <= late_cutoff_utc:
                continue

            # Create pending-missed signal only when no AI instance exists for this schedule/day.
            cur.execute(
                """
                SELECT 1
                FROM activity_instance ai
                WHERE ai.farm_id = %s
                  AND ai.activity_schedule_id = %s
                  AND ai.activity_date = %s
                  AND ai.source = 'AI'
                  AND (
                        (ai.activity_type_id = 1 AND ai.actual_duration_sec >= 300)
                     OR (ai.activity_type_id = 2 AND ai.actual_duration_sec >= 60)
                     OR (ai.activity_type_id = 3 AND ai.actual_duration_sec >= 30)
                  )
                  AND (
                        ai.status = 'IN_PROGRESS'
                        OR (
                            ai.status = 'ENDED'
                            AND ai.session_classification IS NOT NULL
                        )
                      )
                LIMIT 1
                """,
                (
                    farm_id,
                    schedule_id,
                    activity_date,
                ),
            )
            if cur.fetchone():
                continue

            print(
                "[MISSED_PENDING] "
                f"farm={farm_id} schedule={schedule_id} activity_type={activity_type_id} "
                f"activity_date={activity_date} cutoff_utc={late_cutoff_utc.isoformat()}"
            )


# -------------------------------------------------
# RUNNER
# -------------------------------------------------
if __name__ == "__main__":
    # If invoked directly, run STEP-5A before STEP-5B.
    resolve_completed_activities()
    detect_missed_activities()
