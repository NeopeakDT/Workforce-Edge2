#!/usr/bin/env python3
"""
STEP-3 — INSTANCE LIFECYCLE RESOLVER

Responsibilities (ONLY):
- Resolve zone_id
- Update last_seen_at
- Merge-on-start (pause/resume)
- Bind activity_schedule_id + offsets

Hard rules:
- START_CANDIDATE is the ONLY event allowed to create/merge instances
- FRAME_AGGREGATE only updates last_seen_at
- END_CANDIDATE only closes instances (already done in ingest)
- NO status finalization here (EARLY / LATE / ON_TIME is STEP-5)

--------------------------------------------------------------------------
Edge (state machine)
   ↓
Ingest API (transactional, idempotent)
   ↓
activity_aggregator.py (STEP-3 resolver)
   ↓
missed_activity_cron.py (STEP-5 finalizer)


"""

from pathlib import Path
import sys
import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


# -------------------------------------------------
# CONFIG (FROZEN)
# -------------------------------------------------

MERGE_GAP_MINUTES = {
    "MILKING": 3,
    "FEEDING": 5,
    "SCRAPPING": 12,
}


# -------------------------------------------------
# HELPERS
# -------------------------------------------------

def load_activity_code_map(cur):
    cur.execute("SELECT id, code FROM activity_type")
    return {r["id"]: r["code"] for r in cur.fetchall()}


def resolve_zone(cur, farm_id, camera_id, activity_type_id):
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
    if not row:
        raise RuntimeError(
            f"No zone mapping for farm={farm_id}, camera={camera_id}, activity={activity_type_id}"
        )
    return row["zone_id"]


def compute_activity_date(event_time_utc, farm_timezone):
    local_dt = event_time_utc.astimezone(pytz.timezone(farm_timezone))
    return local_dt.date()


# -------------------------------------------------
# STEP-3 LOGIC
# -------------------------------------------------

def resolve_zones():
    """Resolve zone_id using START_CANDIDATE camera mapping."""
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id AS instance_id,
                ai.farm_id,
                ai.activity_type_id,
                e.camera_id
            FROM activity_instance ai
            JOIN activity_detection_event e
              ON e.activity_instance_id = ai.id
            WHERE ai.zone_id IS NULL
              AND e.event_type = 'START_CANDIDATE'
            """
        )

        for r in cur.fetchall():
            try:
                zone_id = resolve_zone(
                    cur,
                    r["farm_id"],
                    r["camera_id"],
                    r["activity_type_id"],
                )
                cur.execute(
                    """
                    UPDATE activity_instance
                    SET zone_id=%s, updated_at=%s
                    WHERE id=%s
                    """,
                    (zone_id, now, r["instance_id"]),
                )
            except RuntimeError as e:
                print(f"[WARN] {e}")


def update_last_seen_at():
    """Update liveness for START / FRAME / END events."""
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE activity_instance ai
            SET last_seen_at = sub.max_event_time
            FROM (
                SELECT
                    activity_instance_id,
                    MAX(event_time) AS max_event_time
                FROM activity_detection_event
                WHERE event_type IN (
                    'START_CANDIDATE',
                    'FRAME_AGGREGATE',
                    'END_CANDIDATE'
                )
                GROUP BY activity_instance_id
            ) sub
            WHERE ai.id = sub.activity_instance_id
              AND (ai.last_seen_at IS NULL
                   OR sub.max_event_time > ai.last_seen_at)
            """
        )


def merge_on_start():
    """
    Merge-on-start (pause/resume).

    Triggered ONLY by START_CANDIDATE events.
    """
    now = utc_now()

    with get_cursor() as cur:
        code_map = load_activity_code_map(cur)

        cur.execute(
            """
            SELECT
                e.id AS event_id,
                e.event_time,
                ai.id AS new_instance_id,
                ai.farm_id,
                ai.zone_id,
                ai.activity_type_id,
                ai.activity_schedule_id
            FROM activity_detection_event e
            JOIN activity_instance ai
              ON ai.id = e.activity_instance_id
            WHERE e.event_type = 'START_CANDIDATE'
              AND e.merge_processed = false
              AND ai.merged_into_instance_id IS NULL
            """
        )

        for r in cur.fetchall():
            code = code_map[r["activity_type_id"]]
            gap_min = MERGE_GAP_MINUTES.get(code)
            if gap_min is None or r["zone_id"] is None:
                continue

            if r["activity_schedule_id"]:
                cur.execute(
                    """
                    SELECT id
                    FROM activity_instance
                    WHERE farm_id=%s
                      AND zone_id=%s
                      AND activity_schedule_id=%s
                      AND actual_end_at IS NOT NULL
                      AND actual_end_at < %s
                      AND actual_end_at >= %s - INTERVAL '%s minutes'
                    ORDER BY actual_end_at DESC
                    LIMIT 1
                    """,
                    (
                        r["farm_id"],
                        r["zone_id"],
                        r["activity_schedule_id"],
                        r["event_time"],
                        r["event_time"],
                        gap_min,
                    ),
                )
            else:
                cur.execute(
                    """
                    SELECT id
                    FROM activity_instance
                    WHERE farm_id=%s
                      AND zone_id=%s
                      AND activity_type_id=%s
                      AND activity_schedule_id IS NULL
                      AND actual_end_at IS NOT NULL
                      AND actual_end_at < %s
                      AND actual_end_at >= %s - INTERVAL '%s minutes'
                    ORDER BY actual_end_at DESC
                    LIMIT 1
                    """,
                    (
                        r["farm_id"],
                        r["zone_id"],
                        r["activity_type_id"],
                        r["event_time"],
                        r["event_time"],
                        gap_min,
                    ),
                )

            prev = cur.fetchone()
            if not prev:
                # No merge candidate - mark event as processed anyway
                cur.execute(
                    """
                    UPDATE activity_detection_event
                    SET merge_processed = true
                    WHERE id = %s
                    """,
                    (r["event_id"],),
                )
                continue

            old_id = prev["id"]
            new_id = r["new_instance_id"]

            # Reopen old instance
            cur.execute(
                """
                UPDATE activity_instance
                SET actual_end_at=NULL,
                    actual_duration_sec=NULL,
                    updated_at=%s
                WHERE id=%s
                """,
                (now, old_id),
            )

            # Move events from new instance to old instance
            cur.execute(
                """
                UPDATE activity_detection_event
                SET activity_instance_id=%s
                WHERE activity_instance_id=%s
                """,
                (old_id, new_id),
            )

            # Mark new instance as merged (do NOT delete, do NOT change status)
            # Use merged_into_instance_id to track merge relationship
            cur.execute(
                """
                UPDATE activity_instance
                SET merged_into_instance_id=%s,
                    updated_at=%s
                WHERE id=%s
                """,
                (old_id, now, new_id),
            )

            # Mark START event as processed (idempotency guard)
            cur.execute(
                """
                UPDATE activity_detection_event
                SET merge_processed = true
                WHERE id = %s
                """,
                (r["event_id"],),
            )


def bind_schedules():
    """
    Bind schedule + compute offsets.
    NO status finalization here.
    """
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT ai.*, f.timezone
            FROM activity_instance ai
            JOIN farm f ON f.id=ai.farm_id
            WHERE ai.actual_end_at IS NOT NULL
              AND ai.activity_schedule_id IS NULL
              AND ai.zone_id IS NOT NULL
              AND ai.merged_into_instance_id IS NULL
            """
        )

        for ai in cur.fetchall():
            activity_date = (
                ai["activity_date"]
                or compute_activity_date(ai["actual_start_at"], ai["timezone"])
            )

            cur.execute(
                """
                SELECT *
                FROM activity_schedule
                WHERE farm_id=%s
                  AND activity_type_id=%s
                  AND is_active=true
                """,
                (ai["farm_id"], ai["activity_type_id"]),
            )

            for s in cur.fetchall():
                cur.execute(
                    """
                    SELECT ((%s::date + %s) AT TIME ZONE %s) AS ideal_start
                    """,
                    (activity_date, s["ideal_start_time"], ai["timezone"]),
                )
                ideal = cur.fetchone()["ideal_start"]

                offset = int(
                    (ai["actual_start_at"] - ideal).total_seconds() / 60
                )

                if (
                    -s["tolerance_early_min"]
                    <= offset
                    <= s["tolerance_late_min"]
                ):
                    cur.execute(
                        """
                        UPDATE activity_instance
                        SET activity_schedule_id=%s,
                            activity_date=%s,
                            started_offset_min=%s,
                            within_ideal_window=%s,
                            updated_at=%s
                        WHERE id=%s
                        """,
                        (
                            s["id"],
                            activity_date,
                            offset,
                            offset == 0,
                            now,
                            ai["id"],
                        ),
                    )
                    # Break inner loop and continue to next instance
                    break


# -------------------------------------------------
# RUNNER
# -------------------------------------------------

def run():
    resolve_zones()
    update_last_seen_at()
    merge_on_start()
    bind_schedules()


if __name__ == "__main__":
    run()
