#!/usr/bin/env python3
"""
PHASE-5 — ACTIVITY AGGREGATOR (FINAL, CORRECT)

DB ENUMS:
START_PENDING → IN_PROGRESS → END_PENDING → EARLY | ON_TIME | LATE
"""

from pathlib import Path
import sys
from datetime import timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

# ------------------------------------------------------------------
# RULES
# ------------------------------------------------------------------
ACTIVITY_RULES = {
    "MILKING":  {"START_CONFIRM": 60, "GAP": 900, "SCHEDULE": True},
    "FEEDING":  {"START_CONFIRM": 30, "GAP": 300, "SCHEDULE": True},
    "SCRAPPING": {"START_CONFIRM": 20, "GAP": 180, "SCHEDULE": False},
}

def load_activity_code_map(cur):
    cur.execute("SELECT id, code FROM activity_type")
    return {r["id"]: r["code"] for r in cur.fetchall()}

# ------------------------------------------------------------------
# STEP 1 — START_PENDING
# ------------------------------------------------------------------
def create_start_pending_instances():
    with get_cursor() as cur:
        cur.execute("""
            SELECT e.*
            FROM activity_detection_event e
            LEFT JOIN activity_instance ai
              ON ai.id = e.activity_instance_id
            WHERE e.event_type = 'START_CANDIDATE'
              AND ai.id IS NULL
        """)

        for e in cur.fetchall():
            cur.execute("""
                INSERT INTO activity_instance (
                    farm_id, activity_type_id, activity_date,
                    status, source, created_at, updated_at,
                    edge_device_id, camera_id
                )
                VALUES (%s,%s,%s,'START_PENDING','AI',%s,%s,%s,%s)
                RETURNING id
            """, (
                e["farm_id"],
                e["activity_type_id"],
                e["activity_date"],
                e["event_time"],
                e["event_time"],
                e["edge_device_id"],
                e["camera_id"],
            ))
            iid = cur.fetchone()["id"]
            cur.execute(
                "UPDATE activity_detection_event SET activity_instance_id=%s WHERE id=%s",
                (iid, e["id"])
            )

# ------------------------------------------------------------------
# STEP 2 — ADVANCE STATES
# ------------------------------------------------------------------
def advance_activity_states():
    now = utc_now()

    with get_cursor() as cur:
        code_map = load_activity_code_map(cur)

        cur.execute("""
            SELECT *
            FROM activity_instance
            WHERE status IN ('START_PENDING','IN_PROGRESS','END_PENDING')
        """)

        for ai in cur.fetchall():
            code = code_map[ai["activity_type_id"]]
            rules = ACTIVITY_RULES[code]

            cur.execute("""
                SELECT MAX(event_time) AS last_seen
                FROM activity_detection_event
                WHERE activity_instance_id=%s
                  AND event_type='FRAME_AGGREGATE'
            """, (ai["id"],))
            last_seen = cur.fetchone()["last_seen"] or ai["created_at"]
            gap = (now - last_seen).total_seconds()

            if ai["status"] == "START_PENDING":
                if (now - ai["created_at"]).total_seconds() >= rules["START_CONFIRM"]:
                    cur.execute("""
                        UPDATE activity_instance
                        SET status='IN_PROGRESS',
                            actual_start_at=%s,
                            updated_at=%s
                        WHERE id=%s
                    """, (ai["created_at"], now, ai["id"]))
                continue

            if ai["status"] == "IN_PROGRESS":
                if gap > rules["GAP"]:
                    cur.execute("""
                        UPDATE activity_instance
                        SET status='END_PENDING', updated_at=%s
                        WHERE id=%s
                    """, (now, ai["id"]))
                continue

            if ai["status"] == "END_PENDING":
                if gap <= rules["GAP"]:
                    cur.execute("""
                        UPDATE activity_instance
                        SET status='IN_PROGRESS', updated_at=%s
                        WHERE id=%s
                    """, (now, ai["id"]))
                    continue

                duration = int((last_seen - ai["actual_start_at"]).total_seconds())
                cur.execute("""
                    UPDATE activity_instance
                    SET actual_end_at=%s,
                        actual_duration_sec=%s,
                        updated_at=%s
                    WHERE id=%s
                """, (last_seen, duration, now, ai["id"]))

# ------------------------------------------------------------------
# STEP 3 — CLASSIFY
# ------------------------------------------------------------------
def classify_completed_activities():
    now = utc_now()

    with get_cursor() as cur:
        code_map = load_activity_code_map(cur)

        cur.execute("""
            SELECT ai.*, f.timezone
            FROM activity_instance ai
            JOIN farm f ON f.id=ai.farm_id
            WHERE ai.status='END_PENDING'
              AND ai.actual_end_at IS NOT NULL
        """)

        for ai in cur.fetchall():
            code = code_map[ai["activity_type_id"]]
            rules = ACTIVITY_RULES[code]

            if not rules["SCHEDULE"]:
                cur.execute("""
                    UPDATE activity_instance
                    SET status='ON_TIME', updated_at=%s
                    WHERE id=%s
                """, (now, ai["id"]))
                continue

            cur.execute("""
                SELECT *
                FROM activity_schedule
                WHERE farm_id=%s
                  AND activity_type_id=%s
                  AND is_active=true
                ORDER BY ideal_start_time
            """, (ai["farm_id"], ai["activity_type_id"]))

            for s in cur.fetchall():
                cur.execute("""
                    SELECT ((%s::date + %s) AT TIME ZONE %s) AS ideal_start
                """, (ai["activity_date"], s["ideal_start_time"], ai["timezone"]))
                ideal = cur.fetchone()["ideal_start"]

                offset = int((ai["actual_start_at"] - ideal).total_seconds() / 60)

                if -s["tolerance_early_min"] <= offset <= s["tolerance_late_min"]:
                    status = (
                        "EARLY" if offset < 0 else
                        "LATE" if offset > 0 else
                        "ON_TIME"
                    )
                    cur.execute("""
                        UPDATE activity_instance
                        SET status=%s,
                            activity_schedule_id=%s,
                            started_offset_min=%s,
                            within_ideal_window=%s,
                            updated_at=%s
                        WHERE id=%s
                    """, (
                        status,
                        s["id"],
                        offset,
                        status == "ON_TIME",
                        now,
                        ai["id"],
                    ))
                    break

# ------------------------------------------------------------------
def run():
    create_start_pending_instances()
    advance_activity_states()
    classify_completed_activities()

if __name__ == "__main__":
    run()
 