#!/usr/bin/env python3
"""
STEP-5b — MISSED Activity Detector (AUTHORITATIVE)

Creates MISSED activity_instance rows when:
- Schedule window has fully passed
- No activity_instance exists for that schedule/date
"""

from datetime import datetime, time, timezone, timedelta
from pathlib import Path
import sys

# Add backend root
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def detect_missed_activities():
    now_utc = utc_now()

    with get_cursor() as cur:
        # 1. Fetch all active schedules
        cur.execute(
            """
            SELECT
                s.id AS schedule_id,
                s.farm_id,
                s.activity_type_id,
                s.ideal_start_time,
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

            # activity_date is evaluated per-day
            activity_date = now_utc.date()

            # Compute ideal end (UTC)
            ideal_end = datetime.combine(
                activity_date,
                s["ideal_end_time"],
                tzinfo=timezone.utc,
            )

            late_cutoff = ideal_end + timedelta(
                minutes=s["tolerance_late_min"]
            )

            # If window not over yet → skip
            if now_utc <= late_cutoff:
                continue

            # 2. Check if any instance exists for this schedule
            cur.execute(
                """
                SELECT 1
                FROM activity_instance
                WHERE activity_schedule_id = %s
                AND activity_date = %s

                LIMIT 1
                """,
                (schedule_id, activity_date),
            )

            exists = cur.fetchone()
            if exists:
                continue

            # 3. Create MISSED instance
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
                VALUES (%s, %s, %s, %s, 'MISSED', 'SYSTEM', %s, %s)
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

            print(
                f"[MISSED CREATED]"
                f"[FARM={farm_id}]"
                f"[ACTIVITY_TYPE={activity_type_id}]"
                f"[DATE={activity_date}]"
            )


if __name__ == "__main__":
    detect_missed_activities()
