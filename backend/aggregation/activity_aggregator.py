#!/usr/bin/env python3
"""
STEP-4 — ACTIVITY AGGREGATOR (FINAL, PRODUCTION-GRADE)

Creates and manages activity_instance from START_CANDIDATE only.

Rules:
- ONE active instance per (farm, zone, activity_type)
- session_id is edge-scoped, NOT instance-scoped
- FRAME/END never create instances
"""

from pathlib import Path
import sys
from datetime import timedelta
import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

MERGE_GAP_MINUTES = {
    1: 10,   # MILKING
    2: 15,   # FEEDING
    3: 15,  # SCRAPPING
}


def resolve_zone_id(cur, farm_id, camera_id, activity_type_id):
    cur.execute(
        """
        SELECT zone_id
        FROM camera_activity_zone
        WHERE farm_id = %s
          AND camera_id = %s
          AND activity_type_id = %s
          AND is_active = true
        LIMIT 1
        """,
        (farm_id, camera_id, activity_type_id),
    )
    row = cur.fetchone()
    return row["zone_id"] if row else None


def compute_activity_date(cur, farm_id, event_time_utc):
    cur.execute("SELECT timezone FROM farm WHERE id = %s", (farm_id,))
    tz = pytz.timezone(cur.fetchone()["timezone"])
    return event_time_utc.astimezone(tz).date()


STALE_TIMEOUT_MIN = 10  # must be > 2x FRAME_AGGREGATE interval


def cleanup_stale_instances(cur):
    """
    Auto-close zombie IN_PROGRESS instances.

    Rules:
    - status = IN_PROGRESS
    - actual_end_at IS NULL
    - last_seen_at older than timeout
    - Close using last_seen_at (NOT now)
    - Do NOT change status here (resolver will finalize)
    """

    now = utc_now()
    cutoff = now - timedelta(minutes=STALE_TIMEOUT_MIN)

    cur.execute(
        """
        SELECT id, actual_start_at, last_seen_at
        FROM activity_instance
        WHERE status = 'IN_PROGRESS'
          AND actual_end_at IS NULL
          AND last_seen_at IS NOT NULL
          AND last_seen_at < %s
        """,
        (cutoff,),
    )

    stale = cur.fetchall()

    if stale:
        print(f"[CLEANUP] Closing {len(stale)} stale instances")

    for row in stale:
        duration = max(
            0,
            int(
                (row["last_seen_at"] - row["actual_start_at"]).total_seconds()
            ),
        )

        cur.execute(
            """
            UPDATE activity_instance
            SET actual_end_at = %s,
                actual_duration_sec = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                row["last_seen_at"],
                duration,
                now,
                row["id"],
            ),
        )


def run():
    print("[PHASE-5] Activity aggregator starting…")

    BATCH_SIZE = 1000

    # In-memory caching for performance
    zone_cache = {}
    farm_tz_cache = {}

    with get_cursor() as cur:
        while True:
            cur.execute(
                """
                SELECT
                    e.id AS event_row_id,
                    e.event_type,
                    e.event_time,
                    e.farm_id,
                    e.camera_id,
                    e.activity_type_id,
                    e.session_id
                FROM activity_detection_event e
                WHERE e.activity_instance_id IS NULL
                ORDER BY e.event_time
                LIMIT %s
                """,
                (BATCH_SIZE,),
            )

            events = cur.fetchall()

            if not events:
                break

            print(f"[AGGREGATOR] Processing batch of {len(events)} events")

            for e in events:
                etype = e["event_type"]
                event_time = e["event_time"]
                farm_id = e["farm_id"]
                camera_id = e["camera_id"]
                activity_type_id = e["activity_type_id"]
                session_id = e["session_id"]

                # Cached zone lookup
                zone_key = (farm_id, camera_id, activity_type_id)
                if zone_key not in zone_cache:
                    cur.execute(
                        """
                        SELECT zone_id
                        FROM camera_activity_zone
                        WHERE farm_id = %s
                          AND camera_id = %s
                          AND activity_type_id = %s
                          AND is_active = true
                        LIMIT 1
                        """,
                        zone_key,
                    )
                    row = cur.fetchone()
                    zone_cache[zone_key] = row["zone_id"] if row else None

                zone_id = zone_cache[zone_key]
                if not zone_id:
                    continue  # cannot build instance without zone

                # -------------------------------------------------
                # START_CANDIDATE
                # -------------------------------------------------
                if etype == "START_CANDIDATE":
                    # 1. Active instance exists → reuse
                    cur.execute(
                        """
                        SELECT id
                        FROM activity_instance
                        WHERE farm_id = %s
                          AND zone_id = %s
                          AND activity_type_id = %s
                          AND status = 'IN_PROGRESS'
                        LIMIT 1
                        """,
                        (farm_id, zone_id, activity_type_id),
                    )
                    active = cur.fetchone()

                    if active:
                        instance_id = active["id"]
                    else:
                        # 2. Merge-on-start (recent ended)
                        gap = timedelta(
                            minutes=MERGE_GAP_MINUTES.get(activity_type_id, 10)
                        )

                        cur.execute(
                            """
                            SELECT id, actual_end_at
                            FROM activity_instance
                            WHERE farm_id = %s
                              AND zone_id = %s
                              AND activity_type_id = %s
                              AND status != 'IN_PROGRESS'
                              AND actual_end_at IS NOT NULL
                            ORDER BY actual_end_at DESC
                            LIMIT 1
                            """,
                            (farm_id, zone_id, activity_type_id),
                        )
                        prev = cur.fetchone()

                        if prev and event_time - prev["actual_end_at"] <= gap:
                            cur.execute(
                                """
                                UPDATE activity_instance
                                SET status = 'IN_PROGRESS',
                                    actual_end_at = NULL,
                                    last_seen_at = %s,
                                    updated_at = %s
                                WHERE id = %s
                                """,
                                (event_time, utc_now(), prev["id"]),
                            )
                            instance_id = prev["id"]
                        else:
                            # Cached timezone lookup
                            if farm_id not in farm_tz_cache:
                                cur.execute(
                                    "SELECT timezone FROM farm WHERE id = %s",
                                    (farm_id,),
                                )
                                farm_tz_cache[farm_id] = pytz.timezone(
                                    cur.fetchone()["timezone"]
                                )

                            tz = farm_tz_cache[farm_id]
                            activity_date = event_time.astimezone(tz).date()

                            cur.execute(
                                """
                                INSERT INTO activity_instance (
                                    farm_id,
                                    zone_id,
                                    activity_type_id,
                                    activity_date,
                                    status,
                                    actual_start_at,
                                    last_seen_at,
                                    source,
                                    created_at,
                                    updated_at
                                )
                                VALUES (%s,%s,%s,%s,
                                        'IN_PROGRESS',
                                        %s,%s,
                                        'AI',
                                        %s,%s)
                                RETURNING id
                                """,
                                (
                                    farm_id,
                                    zone_id,
                                    activity_type_id,
                                    activity_date,
                                    event_time,
                                    event_time,
                                    utc_now(),
                                    utc_now(),
                                ),
                            )
                            instance_id = cur.fetchone()["id"]

                    cur.execute(
                        """
                        UPDATE activity_detection_event
                        SET activity_instance_id = %s
                        WHERE id = %s
                        """,
                        (instance_id, e["event_row_id"]),
                    )

                # -------------------------------------------------
                # FRAME / END
                # -------------------------------------------------
                else:
                    cur.execute(
                        """
                        SELECT id, actual_start_at
                        FROM activity_instance
                        WHERE farm_id = %s
                          AND zone_id = %s
                          AND activity_type_id = %s
                          AND status = 'IN_PROGRESS'
                        LIMIT 1
                        """,
                        (farm_id, zone_id, activity_type_id),
                    )
                    row = cur.fetchone()
                    if not row:
                        continue

                    if etype == "FRAME_AGGREGATE":
                        cur.execute(
                            """
                            UPDATE activity_instance
                            SET last_seen_at = %s,
                                updated_at = %s
                            WHERE id = %s
                            """,
                            (event_time, utc_now(), row["id"]),
                        )

                    elif etype == "END_CANDIDATE":
                        duration = max(
                            0,
                            int(
                                (event_time - row["actual_start_at"]).total_seconds()
                            ),
                        )
                        cur.execute(
                            """
                            UPDATE activity_instance
                            SET actual_end_at = %s,
                                actual_duration_sec = %s,
                                last_seen_at = %s,
                                updated_at = %s
                            WHERE id = %s
                            """,
                            (
                                event_time,
                                duration,
                                event_time,
                                utc_now(),
                                row["id"],
                            ),
                        )

                    cur.execute(
                        """
                        UPDATE activity_detection_event
                        SET activity_instance_id = %s
                        WHERE id = %s
                        """,
                        (row["id"], e["event_row_id"]),
                    )

            cur.connection.commit()

        # After processing all batches, close stale sessions
        cleanup_stale_instances(cur)
        cur.connection.commit()

    print("[PHASE-5] Activity aggregation complete")
