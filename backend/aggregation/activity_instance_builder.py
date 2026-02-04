#!/usr/bin/env python3
"""
STEP-3 — Activity Instance Builder (FINAL, GAP-AWARE, PRODUCTION)

Responsibilities:
- Create activity_instance rows from detection events
- Support MULTIPLE instances per day per activity
- Link ALL unlinked events (START / FRAME / END)
- Use FARM-TIMEZONE for activity_date
- Attach events to the latest OPEN instance if within gap
- Otherwise create a NEW instance

IMPORTANT:
- This file does NOT classify or match schedules
- Gap logic here must align with STEP-4 aggregator
"""

from pathlib import Path
import sys
import pytz
from datetime import timedelta

# -------------------------------------------------------------------
# Bootstrap
# -------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------
# Must be >= FRAME_AGGREGATE max silence window
INSTANCE_GAP_MINUTES = 15


# -------------------------------------------------------------------
# STEP-3 LOGIC
# -------------------------------------------------------------------
def build_activity_instances():
    now = utc_now()
    gap_delta = timedelta(minutes=INSTANCE_GAP_MINUTES)

    with get_cursor() as cur:
        # 1️⃣ Fetch ALL unlinked detection events (ordered strictly)
        cur.execute(
            """
            SELECT
                id AS event_id,
                farm_id,
                activity_type_id,
                event_type,
                event_time,
                device_id,
                camera_id
            FROM activity_detection_event
            WHERE activity_instance_id IS NULL
            ORDER BY farm_id, activity_type_id, event_time
            """
        )
        events = cur.fetchall()

        for e in events:
            farm_id = e["farm_id"]
            activity_type_id = e["activity_type_id"]
            event_time_utc = e["event_time"]

            # ---------------------------------------------------------
            # FARM-LOCAL activity_date (CRITICAL)
            # ---------------------------------------------------------
            cur.execute(
                "SELECT timezone FROM farm WHERE id = %s",
                (farm_id,),
            )
            farm_tz = cur.fetchone()["timezone"]
            local_dt = event_time_utc.astimezone(pytz.timezone(farm_tz))
            activity_date = local_dt.date()

            # ---------------------------------------------------------
            # Find latest OPEN instance (gap-aware, robust)
            # ---------------------------------------------------------
            cur.execute(
                """
                SELECT
                    ai.id,
                    ai.actual_start_at,
                    COALESCE(
                        MAX(ade.event_time),
                        ai.actual_start_at
                    ) AS last_signal_time
                FROM activity_instance ai
                LEFT JOIN activity_detection_event ade
                  ON ade.activity_instance_id = ai.id
                WHERE ai.farm_id = %s
                  AND ai.activity_type_id = %s
                  AND ai.status = 'IN_PROGRESS'
                GROUP BY ai.id, ai.actual_start_at
                ORDER BY last_signal_time DESC
                LIMIT 1
                """,
                (farm_id, activity_type_id),
            )

            row = cur.fetchone()
            attach_to_existing = False
            instance_id = None

            if row:
                last_signal_time = row["last_signal_time"]
                if last_signal_time and event_time_utc - last_signal_time <= gap_delta:
                    instance_id = row["id"]
                    attach_to_existing = True

            # ---------------------------------------------------------
            # Create NEW instance if needed
            # ---------------------------------------------------------
            if not attach_to_existing:
                cur.execute(
                    """
                    INSERT INTO activity_instance (
                        farm_id,
                        activity_type_id,
                        activity_date,
                        actual_start_at,
                        status,
                        source,
                        edge_device_id,
                        camera_id,
                        created_at,
                        updated_at
                    )
                    VALUES (
                        %s, %s, %s, %s,
                        'IN_PROGRESS',
                        'AI',
                        %s,
                        %s,
                        %s,
                        %s
                    )
                    RETURNING id
                    """,
                    (
                        farm_id,
                        activity_type_id,
                        activity_date,
                        event_time_utc,
                        e["device_id"],
                        e["camera_id"],
                        now,
                        now,
                    ),
                )
                instance_id = cur.fetchone()["id"]

                print(
                    f"[INSTANCE CREATED]"
                    f"[ID={instance_id}]"
                    f"[ACTIVITY_TYPE={activity_type_id}]"
                    f"[DATE={activity_date}]"
                    f"[START={event_time_utc}]"
                )

            # ---------------------------------------------------------
            # Link event → instance (MANDATORY, ALL TYPES)
            # ---------------------------------------------------------
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id = %s
                WHERE id = %s
                """,
                (instance_id, e["event_id"]),
            )


# -------------------------------------------------------------------
# RUNNER
# -------------------------------------------------------------------
def run():
    build_activity_instances()


if __name__ == "__main__":
    run()





# #!/usr/bin/env python3
# """
# STEP-3 — Activity Instance Builder (FINAL, FARM-REALITY SAFE)

# Rules:
# - EVERY START_CANDIDATE creates a NEW activity_instance
# - END / FRAME events attach to the latest open instance
# - activity_date is FARM-LOCAL date of START
# """

# from pathlib import Path
# import sys
# import pytz

# BACKEND_ROOT = Path(__file__).resolve().parent.parent
# if str(BACKEND_ROOT) not in sys.path:
#     sys.path.insert(0, str(BACKEND_ROOT))

# from common.db import get_cursor
# from common.time_utils import utc_now


# def run():
#     now = utc_now()

#     with get_cursor() as cur:
#         # Fetch unlinked events in time order
#         cur.execute(
#             """
#             SELECT *
#             FROM activity_detection_event
#             WHERE activity_instance_id IS NULL
#             ORDER BY event_time
#             """
#         )

#         events = cur.fetchall()

#         for e in events:
#             farm_id = e["farm_id"]
#             activity_type_id = e["activity_type_id"]
#             event_type = e["event_type"]
#             event_time_utc = e["event_time"]

#             # Fetch farm timezone
#             cur.execute("SELECT timezone FROM farm WHERE id = %s", (farm_id,))
#             farm_tz = cur.fetchone()["timezone"]

#             local_dt = event_time_utc.astimezone(pytz.timezone(farm_tz))
#             activity_date = local_dt.date()

#             # --------------------------------------------
#             # START_CANDIDATE → NEW INSTANCE (ALWAYS)
#             # --------------------------------------------
#             if event_type == "START_CANDIDATE":
#                 cur.execute(
#                     """
#                     INSERT INTO activity_instance (
#                         farm_id,
#                         activity_type_id,
#                         activity_date,
#                         actual_start_at,
#                         status,
#                         source,
#                         edge_device_id,
#                         camera_id,
#                         created_at,
#                         updated_at
#                     )
#                     VALUES (
#                         %s, %s, %s, %s,
#                         'IN_PROGRESS',
#                         'AI',
#                         %s,
#                         %s,
#                         %s,
#                         %s
#                     )
#                     RETURNING id
#                     """,
#                     (
#                         farm_id,
#                         activity_type_id,
#                         activity_date,
#                         event_time_utc,
#                         e["device_id"],
#                         e["camera_id"],
#                         now,
#                         now,
#                     ),
#                 )

#                 instance_id = cur.fetchone()["id"]

#                 print(f"[INSTANCE CREATED] {instance_id}")

#             else:
#                 # --------------------------------------------
#                 # END / FRAME → attach to latest open instance
#                 # --------------------------------------------
#                 cur.execute(
#                     """
#                     SELECT id
#                     FROM activity_instance
#                     WHERE farm_id = %s
#                       AND activity_type_id = %s
#                       AND status = 'IN_PROGRESS'
#                     ORDER BY actual_start_at DESC
#                     LIMIT 1
#                     """,
#                     (farm_id, activity_type_id),
#                 )

#                 row = cur.fetchone()
#                 if not row:
#                     continue  # orphan END, ignore safely

#                 instance_id = row["id"]

#             # Link event → instance
#             cur.execute(
#                 """
#                 UPDATE activity_detection_event
#                 SET activity_instance_id = %s
#                 WHERE id = %s
#                 """,
#                 (instance_id, e["id"]),
#             )


# if __name__ == "__main__":
#     run()
