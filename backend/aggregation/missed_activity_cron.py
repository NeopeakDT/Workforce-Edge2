#!/usr/bin/env python3
"""
STEP-5b — MISSED Activity Detector (AUTHORITATIVE)

Creates exactly ONE MISSED activity_instance per:
- farm
- activity_schedule
- activity_date

When:
- Schedule window + late tolerance has fully passed
- No activity_instance exists for that schedule/date

This script is:
- Idempotent
- Safe to run repeatedly
- Required for Phase-5 completeness
"""

from datetime import datetime, timezone, timedelta
from pathlib import Path
import sys

# -------------------------------------------------
# Bootstrap backend path
# -------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


# -------------------------------------------------
# MISSED activity detection
# -------------------------------------------------
def detect_missed_activities():
    now_utc = utc_now()
    activity_date = now_utc.date()

    with get_cursor() as cur:
        # 1. Load all active schedules
        cur.execute(
            """
            SELECT
                s.id AS schedule_id,
                s.farm_id,
                s.activity_type_id,
                s.ideal_end_time,
                s.tolerance_late_min
            FROM activity_schedule s
            WHERE s.is_active = true
            """
        )

        schedules = cur.fetchall()

        for s in schedules:
            schedule_id = s["schedule_id"]
            farm_id = s["farm_id"]
            activity_type_id = s["activity_type_id"]

            # -------------------------------------------------
            # Compute cutoff time (UTC)
            # -------------------------------------------------
            ideal_end_utc = datetime.combine(
                activity_date,
                s["ideal_end_time"],
                tzinfo=timezone.utc,
            )

            late_cutoff_utc = ideal_end_utc + timedelta(
                minutes=s["tolerance_late_min"]
            )

            # If window still open → skip
            if now_utc <= late_cutoff_utc:
                continue

            # -------------------------------------------------
            # INSERT MISSED (HARD GUARDED)
            # -------------------------------------------------
            cur.execute(
                """
                INSERT INTO activity_instance (
                    farm_id,
                    activity_type_id,
                    activity_schedule_id,
                    activity_date,
                    status,
                    source,
                    created_at,
                    updated_at
                )
                SELECT
                    %s,
                    %s,
                    %s,
                    %s,
                    'MISSED',
                    'SYSTEM',
                    %s,
                    %s
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM activity_instance ai
                    WHERE ai.farm_id = %s
                      AND ai.activity_schedule_id = %s
                      AND ai.activity_date = %s
                )
                """,
                (
                    farm_id,
                    activity_type_id,
                    schedule_id,
                    activity_date,
                    now_utc,
                    now_utc,
                    farm_id,
                    schedule_id,
                    activity_date,
                ),
            )

            if cur.rowcount > 0:
                print(
                    f"[MISSED CREATED]"
                    f"[FARM={farm_id}]"
                    f"[SCHEDULE={schedule_id}]"
                    f"[ACTIVITY_TYPE={activity_type_id}]"
                    f"[DATE={activity_date}]"
                )


# -------------------------------------------------
# Runner
# -------------------------------------------------
if __name__ == "__main__":
    detect_missed_activities()
