#!/usr/bin/env python3
"""
STEP-3 — Activity Instance Builder (AUTHORITATIVE)

Creates activity_instance rows from START_CANDIDATE events.

Rules:
- One instance per (farm_id, activity_type_id, activity_date)
- Idempotent (safe to run repeatedly)
- Does NOT classify, schedule, or close activities
"""

from datetime import timezone
from pathlib import Path
import sys

# Add backend root to path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


def build_activity_instances():
    now = utc_now()

    with get_cursor() as cur:
        # 1️⃣ Fetch START_CANDIDATE events that are not yet linked
        cur.execute(
            """
            SELECT
                ade.id AS event_id,
                ade.farm_id,
                ade.activity_type_id,
                ade.event_time,
                ade.device_id,
                ade.camera_id
            FROM activity_detection_event ade
            WHERE ade.event_type = 'START_CANDIDATE'
              AND ade.activity_instance_id IS NULL
            ORDER BY ade.event_time
            """
        )

        events = cur.fetchall()

        for e in events:
            farm_id = e["farm_id"]
            activity_type_id = e["activity_type_id"]
            start_time = e["event_time"]
            activity_date = start_time.date()

            # 2️⃣ Check if instance already exists (idempotency)
            cur.execute(
                """
                SELECT id
                FROM activity_instance
                WHERE farm_id = %s
                  AND activity_type_id = %s
                  AND activity_date = %s
                LIMIT 1
                """,
                (farm_id, activity_type_id, activity_date),
            )

            existing = cur.fetchone()

            if existing:
                instance_id = existing["id"]
            else:
                # 3️⃣ Create new activity_instance
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
                        start_time,
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
                    f"[START={start_time}]"
                )

            # 4️⃣ Link event → instance
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
