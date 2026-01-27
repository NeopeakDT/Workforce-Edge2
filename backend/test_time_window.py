#!/usr/bin/env python3
"""
Test the time-window matching logic
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor

def test_time_window_logic():
    print("🧪 Testing Time-Window Matching Logic")

    with get_cursor() as cur:
        # Get a test activity
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
            WHERE ai.status != 'IN_PROGRESS'
            ORDER BY ai.created_at DESC
            LIMIT 1
            """
        )

        test_activity = cur.fetchone()
        if not test_activity:
            print("❌ No test activity found")
            return

        print(f"📊 Test Activity: {test_activity['id']}")
        print(f"   Start Time: {test_activity['actual_start_at']}")
        print(f"   Activity Date: {test_activity['activity_date']}")
        print(f"   Farm TZ: {test_activity['farm_timezone']}")

        # Test time-window matching logic
        farm_id = test_activity['farm_id']
        activity_type_id = test_activity['activity_type_id']
        activity_date = test_activity['activity_date']
        actual_start_at = test_activity['actual_start_at']

        # Get candidate schedules
        cur.execute(
            """
            SELECT
                s.*,
                f.timezone AS farm_timezone
            FROM activity_schedule s
            JOIN farm f ON f.id = s.farm_id
            WHERE s.farm_id = %s
              AND s.activity_type_id = %s
              AND s.is_active = true
            """,
            (farm_id, activity_type_id),
        )

        schedules = cur.fetchall()
        print(f"\n📅 Found {len(schedules)} candidate schedules:")

        matched_schedule = None

        for s in schedules:
            print(f"\n🔍 Testing Schedule: {s['label']} ({s['ideal_start_time']} - {s['ideal_end_time']})")

            # Calculate ideal times
            cur.execute(
                """
                SELECT
                  (
                    ( %s::date + %s )
                    AT TIME ZONE %s
                  ) AS ideal_start_utc,
                  (
                    ( %s::date + %s )
                    AT TIME ZONE %s
                  ) AS ideal_end_utc
                """,
                (
                    activity_date,
                    s["ideal_start_time"],
                    s["farm_timezone"],
                    activity_date,
                    s["ideal_end_time"],
                    s["farm_timezone"],
                ),
            )

            row = cur.fetchone()
            ideal_start_utc = row["ideal_start_utc"]
            ideal_end_utc = row["ideal_end_utc"]

            window_start = ideal_start_utc - timedelta(minutes=s["tolerance_early_min"])
            window_end = ideal_end_utc + timedelta(minutes=s["tolerance_late_min"])

            print(f"   Ideal Start UTC: {ideal_start_utc}")
            print(f"   Ideal End UTC: {ideal_end_utc}")
            print(f"   Window: {window_start} to {window_end}")
            print(f"   Activity Start: {actual_start_at}")

            # Check if activity falls within window
            if window_start <= actual_start_at <= window_end:
                matched_schedule = s
                started_offset_min = int((actual_start_at - ideal_start_utc).total_seconds() / 60)
                print(f"   ✅ MATCH! Offset: {started_offset_min} minutes")

                # Classify
                if started_offset_min < -s["tolerance_early_min"]:
                    status = "EARLY"
                elif started_offset_min > s["tolerance_late_min"]:
                    status = "LATE"
                else:
                    status = "ON_TIME"

                print(f"   📊 Status: {status}")
                break
            else:
                print("   ❌ No match")

        if not matched_schedule:
            print("\n❌ No schedule matched this activity (correct behavior!)")
        else:
            print(f"\n✅ Matched Schedule: {matched_schedule['label']}")

if __name__ == "__main__":
    test_time_window_logic()