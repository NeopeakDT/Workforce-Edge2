#!/usr/bin/env python3
"""
================================================================================
DEPRECATED (Architecture v2)

Reason:
The activity_compliance table has been removed from the new architecture.

Compliance information is now derived directly from activity_instance:

status
session_classification
activity_schedule_id
activity_date

This file is intentionally kept only for historical reference.

DO NOT USE.

Date:
2026-05-31

================================================================================
"""

from datetime import timedelta, timezone, datetime
from pathlib import Path
import sys
import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def _table_exists(cur, table_name: str) -> bool:
    cur.execute(
        """
        SELECT EXISTS (
          SELECT 1
          FROM information_schema.tables
          WHERE table_schema = 'public'
            AND table_name = %s
        ) AS ok
        """,
        (table_name,),
    )
    row = cur.fetchone()
    return bool(row and row["ok"])


# def build_activity_compliance():
#     now_utc = utc_now()
#
#     with get_cursor() as cur:
#         if not _table_exists(cur, "activity_compliance"):
#             print(
#                 "[STEP-6] activity_compliance table not found; "
#                 "skipping compliance build."
#             )
#             return
#
#         cur.execute(
#             """
#             SELECT
#                 s.id AS schedule_id,
#                 s.farm_id,
#                 s.activity_type_id,
#                 f.timezone
#             FROM activity_schedule s
#             JOIN farm f ON f.id = s.farm_id
#             WHERE s.is_active = true
#             """
#         )
#         schedules = cur.fetchall()
#
#         cur.execute(
#             """
#             SELECT id, id_uuid
#             FROM activity_type
#             """
#         )
#         activity_type_uuid_map = {
#             row["id"]: row["id_uuid"] for row in cur.fetchall()
#         }
#
#         upserts = 0
#         for s in schedules:
#             farm_id = s["farm_id"]
#             schedule_id = s["schedule_id"]
#             activity_type_id = s["activity_type_id"]
#             print(f"[STEP-6] schedule={schedule_id}")
#             farm_tz = pytz.timezone(s["timezone"])
#             local_today = now_utc.astimezone(farm_tz).date()
#             dates = [
#                 local_today - timedelta(days=1),
#                 local_today,
#             ]
#
#             for activity_date in dates:
#                 cur.execute(
#                     """
#                     SELECT
#                         ideal_start_time,
#                         ideal_end_time,
#                         tolerance_late_min
#                     FROM activity_schedule
#                     WHERE id = %s
#                     """,
#                     (schedule_id,),
#                 )
#                 schedule_window = cur.fetchone()
#                 if not schedule_window:
#                     continue
#
#                 ideal_start_naive = datetime.combine(
#                     activity_date,
#                     schedule_window["ideal_start_time"],
#                 )
#                 ideal_end_naive = datetime.combine(
#                     activity_date,
#                     schedule_window["ideal_end_time"],
#                 )
#                 ideal_start_local = farm_tz.localize(ideal_start_naive)
#                 ideal_end_local = farm_tz.localize(ideal_end_naive)
#                 if ideal_end_local <= ideal_start_local:
#                     ideal_end_local += timedelta(days=1)
#                 late_cutoff_local = ideal_end_local + timedelta(
#                     minutes=schedule_window["tolerance_late_min"]
#                 )
#                 late_cutoff_utc = late_cutoff_local.astimezone(timezone.utc)
#
#                 cur.execute(
#                     """
#                     SELECT
#                         id,
#                         activity_type_id,
#                         session_classification,
#                         actual_start_at,
#                         actual_end_at,
#                         COALESCE(actual_duration_sec, 0) AS actual_duration_sec
#                     FROM activity_instance
#                     WHERE farm_id = %s
#                       AND activity_schedule_id = %s
#                       AND activity_schedule_id IS NOT NULL
#                       AND activity_date = %s
#                       AND status = 'ENDED'
#                       AND source = 'AI'
#                       AND session_classification IN ('EARLY', 'ON_TIME', 'LATE')
#                       AND (
#                             (activity_type_id = 1 AND actual_duration_sec >= 300)
#                          OR (activity_type_id = 2 AND actual_duration_sec >= 60)
#                          OR (activity_type_id = 3 AND actual_duration_sec >= 30)
#                       )
#                     """,
#                     (farm_id, schedule_id, activity_date),
#                 )
#                 rows = cur.fetchall()
#                 print(f"[STEP-6 DEBUG] date={activity_date} rows={len(rows)}")
#                 activity_type_id_int = (
#                     rows[0]["activity_type_id"] if rows else activity_type_id
#                 )
#
#                 activity_type_id_for_upsert = activity_type_uuid_map.get(activity_type_id_int)
#
#                 if not activity_type_id_for_upsert:
#                     print(
#                         f"[STEP-6 ERROR] Missing UUID mapping for activity_type_id={activity_type_id_int}"
#                     )
#                     continue
#
#                 total_sessions = len(rows)
#                 early_sessions = sum(
#                     1 for r in rows if r.get("session_classification") == "EARLY"
#                 )
#                 on_time_sessions = sum(
#                     1 for r in rows if r.get("session_classification") == "ON_TIME"
#                 )
#                 late_sessions = sum(
#                     1 for r in rows if r.get("session_classification") == "LATE"
#                 )
#
#                 if total_sessions == 0:
#                     if now_utc <= late_cutoff_utc:
#                         continue
#                     final_status = "MISSED"
#                     primary_instance_id = None
#                     first_activity_at = None
#                     last_activity_at = None
#                 else:
#                     if on_time_sessions > 0:
#                         final_status = "ON_TIME"
#                     elif late_sessions > 0:
#                         final_status = "LATE"
#                     else:
#                         final_status = "EARLY"
#
#                     primary_row = max(rows, key=lambda r: r["actual_duration_sec"] or 0)
#                     primary_instance_id = primary_row["id"]
#
#                     starts = [
#                         r["actual_start_at"].astimezone(timezone.utc)
#                         for r in rows
#                         if r.get("actual_start_at") is not None
#                     ]
#                     ends = [
#                         r["actual_end_at"].astimezone(timezone.utc)
#                         for r in rows
#                         if r.get("actual_end_at") is not None
#                     ]
#                     first_activity_at = min(starts) if starts else None
#                     last_activity_at = max(ends) if ends else None
#
#                 cur.execute(
#                     """
#                     INSERT INTO activity_compliance (
#                         farm_id,
#                         activity_schedule_id,
#                         activity_type_id,
#                         activity_date,
#                         final_status,
#                         primary_instance_id,
#                         total_sessions,
#                         early_sessions,
#                         on_time_sessions,
#                         late_sessions,
#                         first_activity_at,
#                         last_activity_at,
#                         created_at,
#                         updated_at
#                     )
#                     VALUES (
#                         %s, %s, %s, %s, %s, %s,
#                         %s, %s, %s, %s, %s, %s, %s, %s
#                     )
#                     ON CONFLICT (farm_id, activity_schedule_id, activity_date)
#                     DO UPDATE SET
#                         final_status = EXCLUDED.final_status,
#                         primary_instance_id = EXCLUDED.primary_instance_id,
#                         total_sessions = EXCLUDED.total_sessions,
#                         early_sessions = EXCLUDED.early_sessions,
#                         on_time_sessions = EXCLUDED.on_time_sessions,
#                         late_sessions = EXCLUDED.late_sessions,
#                         first_activity_at = EXCLUDED.first_activity_at,
#                         last_activity_at = EXCLUDED.last_activity_at,
#                         updated_at = EXCLUDED.updated_at
#                     WHERE
#                         activity_compliance.final_status IS DISTINCT FROM EXCLUDED.final_status
#                         OR activity_compliance.primary_instance_id IS DISTINCT FROM EXCLUDED.primary_instance_id
#                         OR activity_compliance.total_sessions IS DISTINCT FROM EXCLUDED.total_sessions
#                         OR activity_compliance.early_sessions IS DISTINCT FROM EXCLUDED.early_sessions
#                         OR activity_compliance.on_time_sessions IS DISTINCT FROM EXCLUDED.on_time_sessions
#                         OR activity_compliance.late_sessions IS DISTINCT FROM EXCLUDED.late_sessions
#                         OR activity_compliance.first_activity_at IS DISTINCT FROM EXCLUDED.first_activity_at
#                         OR activity_compliance.last_activity_at IS DISTINCT FROM EXCLUDED.last_activity_at
#                     """,
#                     (
#                         farm_id,
#                         schedule_id,
#                         activity_type_id_for_upsert,
#                         activity_date,
#                         final_status,
#                         primary_instance_id,
#                         total_sessions,
#                         early_sessions,
#                         on_time_sessions,
#                         late_sessions,
#                         first_activity_at,
#                         last_activity_at,
#                         now_utc,
#                         now_utc,
#                     ),
#                 )
#                 upserts += 1
#
#         print(f"[STEP-6] Upserted compliance rows: {upserts}")
#         cur.connection.commit()


# if __name__ == "__main__":
#     build_activity_compliance()
