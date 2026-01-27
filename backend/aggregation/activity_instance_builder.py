#!/usr/bin/env python3
"""
STEP-3 — Activity Instance Builder (FINAL, ROBUST, TZ-SAFE)

Responsibilities:
- Ensure exactly one activity_instance per (farm, activity_type, activity_date)
- Link ALL unlinked detection events (START / END / FRAME)
- Compute activity_date in FARM TIMEZONE (NOT UTC)
- Set actual_start_at ONLY from START_CANDIDATE
- Idempotent and safe to run repeatedly
"""

from pathlib import Path
import sys
import pytz

# -------------------------------------------------------------------
# Bootstrap
# -------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def build_activity_instances():
    now = utc_now()

    with get_cursor() as cur:
        # 1️⃣ Fetch ALL unlinked detection events (START / END / FRAME)
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
            ORDER BY event_time
            """
        )

        events = cur.fetchall()

        for e in events:
            farm_id = e["farm_id"]
            activity_type_id = e["activity_type_id"]
            event_type = e["event_type"]
            event_time_utc = e["event_time"]

            # ---------------------------------------------------------
            # FARM-LOCAL activity_date (CRITICAL)
            # ---------------------------------------------------------
            cur.execute(
                "SELECT timezone FROM farm WHERE id = %s",
                (farm_id,),
            )
            tz_row = cur.fetchone()
            if not tz_row or not tz_row["timezone"]:
                raise RuntimeError(
                    f"Farm timezone missing for farm_id={farm_id}"
                )

            farm_tz = tz_row["timezone"]
            local_dt = event_time_utc.astimezone(pytz.timezone(farm_tz))
            activity_date = local_dt.date()

            # ---------------------------------------------------------
            # Find existing instance for (farm, activity, date)
            # ---------------------------------------------------------
            cur.execute(
                """
                SELECT id, actual_start_at
                FROM activity_instance
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND activity_date = %s
                LIMIT 1
                """,
                (farm_id, activity_type_id, activity_date),
            )

            row = cur.fetchone()

            if row:
                instance_id = row["id"]
            else:
                # -------------------------------------------------
                # Create new instance
                # actual_start_at ONLY if START_CANDIDATE
                # -------------------------------------------------
                actual_start_at = (
                    event_time_utc
                    if event_type == "START_CANDIDATE"
                    else None
                )

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
                        actual_start_at,
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
                )

            # ---------------------------------------------------------
            # Link event → instance (MANDATORY)
            # ---------------------------------------------------------
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id = %s
                WHERE id = %s
                """,
                (instance_id, e["event_id"]),
            )


def run():
    build_activity_instances()


if __name__ == "__main__":
    run()
