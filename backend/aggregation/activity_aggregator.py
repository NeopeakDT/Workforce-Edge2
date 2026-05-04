#!/usr/bin/env python3
"""
STEP-4 - Activity Aggregator (schedule-aware)

What this does:
- Consumes unlinked rows from `activity_detection_event` where `activity_instance_id` is NULL.
- Creates/updates `activity_instance` records for real-world activity sessions.
- Groups events by `(farm, zone, activity_type, schedule_id)` to avoid cross-schedule merges.
- Attaches `FRAME_AGGREGATE` and `END_CANDIDATE` to the correct instance using `session_id`.
- Auto-closes stale in-progress instances using `last_seen_at`.

How to use:
- Continuous worker (recommended for production):
  `python backend/aggregation/activity_aggregator.py`
- One-pass loop (useful in debugging/tests):
  `python backend/aggregation/activity_aggregator.py --max-loops 1`

Operational notes:
- This file is STEP-4 only. STEP-5A and STEP-5B are run by `run_phase5.py`.
- Designed to run as a long-lived systemd service.
- Idempotent behavior is achieved by linking each processed event to an instance.

Critical rules:
- One active instance per `(farm, zone, activity_type, schedule_id)`.
- `session_id` is used for correctness tracking (session -> instance mapping).
- `schedule_id` is used for merge boundaries (same schedule window -> same instance).
- `FRAME_AGGREGATE` and `END_CANDIDATE` normally attach to an existing instance.
  If START is missing, a new instance is created as a fallback when no active row matches.
- END_CANDIDATE refreshes `last_seen_at` only; `cleanup_stale_instances` closes after `CLOSE_DELAY_SEC`
  and then applies duration rules (including NOISE when finally shorter than `MIN_ACTIVITY_DURATION_SEC`).
- START merge uses MIN logic: `actual_start_at = min(existing_start, event_time)`.
"""

from pathlib import Path
import sys
import os
import time
from datetime import timedelta, datetime, timezone
import pytz
from psycopg2 import errors as pg_errors
from psycopg2.extras import execute_values

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now

from aggregation.activity_schedule_resolver import (
    ideal_window_utc_bounds,
    is_actual_start_within_ideal_window,
)

MERGE_GAP_MINUTES = {
    1: 10,  # MILKING
    2: 10,  # FEEDING
    3: 10,  # SCRAPPING
}

MAX_DURATION_SEC = int(os.getenv("AGG_MAX_ACTIVITY_DURATION_SEC", str(90 * 60)))
MIN_VALID_DURATION_SEC = int(os.getenv("AGG_MIN_VALID_DURATION_SEC", "60"))
MIN_ACTIVITY_DURATION_SEC = int(os.getenv("AGG_MIN_ACTIVITY_DURATION_SEC", "60"))
NOISE_DURATION_SEC = int(
    os.getenv("AGG_NOISE_DURATION_SEC", str(MIN_ACTIVITY_DURATION_SEC))
)
UNCLEAR_STALE_SEC = int(os.getenv("AGG_UNCLEAR_STALE_SEC", "1800"))
MAX_EVENT_DELAY_SEC = int(os.getenv("AGG_MAX_EVENT_DELAY_SEC", "300"))
SCHEDULE_BIND_GRACE_SEC = int(os.getenv("AGG_SCHEDULE_BIND_GRACE_SEC", "40"))
UNSCHEDULED_CREATE_DELAY_SEC = int(os.getenv("AGG_UNSCHEDULED_CREATE_DELAY_SEC", "60"))
STALE_START_SESSION_EVENT_MIN = int(os.getenv("AGG_STALE_START_SESSION_EVENT_MIN", "3"))
SOFT_DEDUPE_WINDOW_SEC = int(os.getenv("AGG_SOFT_DEDUPE_WINDOW_SEC", "30"))

_NOISE_COLUMN_WARNED = False


def compute_activity_date(cur, farm_id, event_time_utc):
    cur.execute("SELECT timezone FROM farm WHERE id = %s", (farm_id,))
    tz = pytz.timezone(cur.fetchone()["timezone"])
    return event_time_utc.astimezone(tz).date()


def resolve_schedule_for_event(cur, farm_id, activity_type_id, event_time_utc, farm_tz):
    """
    Resolve the best-matching activity_schedule for a given event.
    
    Returns: schedule_id (or None if no schedule window match)
    
    This ensures:
    - Same schedule window → same instance_id → coherent merging
    - Different schedule windows → different instances
    - Activities that span midnight handled correctly
    """
    
    # Convert event time to farm local
    event_local = event_time_utc.astimezone(farm_tz)
    activity_date = event_local.date()
    
    # Get all active schedules
    cur.execute(
        """
        SELECT
            id,
            ideal_start_time,
            ideal_end_time,
            tolerance_early_min,
            tolerance_late_min
        FROM activity_schedule
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND is_active = true
        ORDER BY ideal_start_time
        """,
        (farm_id, activity_type_id),
    )
    
    schedules = cur.fetchall()
    
    if not schedules:
        return None
    
    best_schedule = None
    best_score = None
    
    for sched in schedules:
        try:
            # Build ideal window in farm LOCAL time
            ideal_start_naive = datetime.combine(activity_date, sched["ideal_start_time"])
            ideal_end_naive = datetime.combine(activity_date, sched["ideal_end_time"])
            
            ideal_start_local = farm_tz.localize(ideal_start_naive)
            ideal_end_local = farm_tz.localize(ideal_end_naive)
            
            # Handle cross-midnight schedules
            if ideal_end_local <= ideal_start_local:
                ideal_end_local += timedelta(days=1)
            
            # Expand window by tolerances
            window_start = ideal_start_local - timedelta(
                minutes=(sched["tolerance_early_min"] or 0)
            )
            window_end = ideal_end_local + timedelta(
                minutes=(sched["tolerance_late_min"] or 0)
            )
            
            print(
                f"[WINDOW_DEBUG] event={event_local}, "
                f"start={window_start}, end={window_end}"
            )

            # Check if event falls within tolerance window
            if window_start <= event_local <= window_end:
                distance_from_start = int((event_local - ideal_start_local).total_seconds() / 60)
                score = abs(distance_from_start)
                if best_schedule is None or score < best_score:
                    best_schedule = sched["id"]
                    best_score = score
                    
        except Exception as ex:
            print(f"[WARN] Schedule resolution error for schedule {sched['id']}: {ex}")
            continue
    
    return best_schedule


def resolve_schedule_from_rows(schedules, event_time_utc, farm_tz):
    """
    Resolve schedule_id from preloaded schedule rows.
    `schedules` must contain rows with:
      id, ideal_start_time, ideal_end_time, tolerance_early_min, tolerance_late_min
    """
    if not schedules:
        return None

    event_local = event_time_utc.astimezone(farm_tz)
    activity_date = event_local.date()

    best_schedule = None
    best_score = None

    for sched in schedules:
        try:
            ideal_start_naive = datetime.combine(activity_date, sched["ideal_start_time"])
            ideal_end_naive = datetime.combine(activity_date, sched["ideal_end_time"])

            ideal_start_local = farm_tz.localize(ideal_start_naive)
            ideal_end_local = farm_tz.localize(ideal_end_naive)

            if ideal_end_local <= ideal_start_local:
                ideal_end_local += timedelta(days=1)

            window_start = ideal_start_local - timedelta(
                minutes=(sched["tolerance_early_min"] or 0)
            )
            window_end = ideal_end_local + timedelta(
                minutes=(sched["tolerance_late_min"] or 0)
            )

            print(
                f"[WINDOW_DEBUG] event={event_local}, "
                f"start={window_start}, end={window_end}"
            )

            if window_start <= event_local <= window_end:
                distance_from_start = int((event_local - ideal_start_local).total_seconds() / 60)
                score = abs(distance_from_start)
                if best_schedule is None or score < best_score:
                    best_schedule = sched["id"]
                    best_score = score
        except Exception as ex:
            print(f"[WARN] Cached schedule resolution error for schedule {sched.get('id')}: {ex}")
            continue

    return best_schedule


def bulk_link_events(cur, event_links):
    """
    Bulk update activity_detection_event.activity_instance_id.
    event_links: list[(event_row_id, instance_id)]
    """
    if not event_links:
        return

    execute_values(
        cur,
        """
        UPDATE activity_detection_event AS e
        SET activity_instance_id = v.activity_instance_id
        FROM (VALUES %s) AS v(id, activity_instance_id)
        WHERE e.id = v.id
        """,
        event_links,
        template="(%s::bigint, %s::uuid)",
    )


def mark_event_skipped(cur, event_row_id, event_columns):
    if "merge_processed" not in event_columns:
        return
    cur.execute(
        """
        UPDATE activity_detection_event
        SET merge_processed = TRUE
        WHERE id = %s
        """,
        (event_row_id,),
    )


# Timeout-driven closure control. END_CANDIDATE does not close sessions immediately.
# Sessions are closed only when inactivity exceeds CLOSE_DELAY_SEC.
END_CONFIRMATION_WINDOW_SEC = int(
    os.getenv("AGG_END_CONFIRMATION_WINDOW_SEC", "20")
)
CLOSE_DELAY_SEC = int(
    os.getenv(
        "AGG_CLOSE_DELAY_SEC",
        os.getenv("AGG_END_GAP_SEC", "60"),
    )
)
REOPEN_WINDOW_SEC = int(os.getenv("AGG_REOPEN_WINDOW_SEC", "180"))


def load_activity_instance_columns(cur):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'activity_instance'
        """
    )
    return {row["column_name"] for row in cur.fetchall()}


def load_event_columns(cur):
    cur.execute(
        """
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = 'public'
          AND table_name = 'activity_detection_event'
        """
    )
    return {row["column_name"] for row in cur.fetchall()}


def mark_noise_if_short(cur, instance_id, duration_sec, ai_columns):
    global _NOISE_COLUMN_WARNED
    if duration_sec >= MIN_ACTIVITY_DURATION_SEC:
        return

    assignments = []
    params = []

    if "instance_type" in ai_columns:
        assignments.append("instance_type = 'NOISE'")
    if "activity_schedule_id" in ai_columns:
        # Keep DB invariant: NOISE rows must not have schedule_id.
        assignments.append("activity_schedule_id = NULL")
    if "status" in ai_columns:
        assignments.append("status = 'NOISE'")
    if "is_valid" in ai_columns:
        assignments.append("is_valid = FALSE")
    if "anomaly_reason" in ai_columns:
        if duration_sec < MIN_VALID_DURATION_SEC:
            assignments.append("anomaly_reason = 'TOO_SHORT'")
        else:
            assignments.append("anomaly_reason = 'SHORT_DURATION'")
    if "within_ideal_window" in ai_columns:
        assignments.append("within_ideal_window = FALSE")
    if "started_offset_min" in ai_columns:
        assignments.append("started_offset_min = NULL")
    if "ended_offset_min" in ai_columns:
        assignments.append("ended_offset_min = NULL")
    if "updated_at" in ai_columns:
        assignments.append("updated_at = %s")
        params.append(utc_now())

    if not assignments:
        if not _NOISE_COLUMN_WARNED:
            print(
                "[WARN] SHORT_DURATION noise columns missing on activity_instance; "
                "skipping NOISE tagging."
            )
            _NOISE_COLUMN_WARNED = True
        return

    cur.execute(
        f"""
        UPDATE activity_instance
        SET {", ".join(assignments)}
        WHERE id = %s
        """,
        (*params, instance_id),
    )
    print(
        f"[DEBUG] Marked instance as NOISE "
        f"instance_id={instance_id} duration_sec={duration_sec}"
    )


def create_fallback_activity_instance(
    cur,
    farm_id,
    zone_id,
    activity_type_id,
    schedule_id,
    activity_date,
    session_id,
    event_time,
    farm_tz,
    matched_schedule,
    event_row_id,
):
    """
    Create IN_PROGRESS activity_instance when FRAME/END arrive with no mapping.
    Matches START-path insert semantics (schedule ideal window, instance_type).
    """
    resolved_schedule_id = schedule_id
    if schedule_id is not None and not matched_schedule:
        print(
            f"[WARN] activity_schedule {schedule_id} missing in cache "
            f"→ create as UNSCHEDULED for event {event_row_id}"
        )
        resolved_schedule_id = None

    within_ideal_window = False
    if resolved_schedule_id is not None and matched_schedule:
        ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
            farm_tz,
            activity_date,
            matched_schedule["ideal_start_time"],
            matched_schedule["ideal_end_time"],
        )
        within_ideal_window = is_actual_start_within_ideal_window(
            event_time, ideal_start_utc, ideal_end_utc
        )

    instance_type = "SCHEDULED" if resolved_schedule_id is not None else "UNSCHEDULED"
    cur.execute("SAVEPOINT ai_fb_insert_sp")
    try:
        cur.execute(
            """
            INSERT INTO activity_instance (
                farm_id,
                zone_id,
                activity_type_id,
                activity_schedule_id,
                activity_date,
                session_id,
                instance_type,
                status,
                actual_start_at,
                last_seen_at,
                source,
                within_ideal_window,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,
                    %s,%s,
                    'IN_PROGRESS',
                    %s,%s,
                    'AI',
                    %s,
                    %s,%s)
            RETURNING id
            """,
            (
                farm_id,
                zone_id,
                activity_type_id,
                resolved_schedule_id,
                activity_date,
                session_id,
                instance_type,
                event_time,
                event_time,
                within_ideal_window,
                utc_now(),
                utc_now(),
            ),
        )
        instance_id = cur.fetchone()["id"]
        cur.execute("RELEASE SAVEPOINT ai_fb_insert_sp")
        print(
            f"[DEBUG] FRAME/END fallback CREATED-NEW → instance_id={instance_id} "
            f"type={instance_type} event_row_id={event_row_id}"
        )
        return instance_id
    except pg_errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT ai_fb_insert_sp")
        cur.execute("RELEASE SAVEPOINT ai_fb_insert_sp")
        cur.execute(
            """
            SELECT id
            FROM activity_instance
            WHERE farm_id = %s
              AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
              AND activity_type_id = %s
              AND activity_date = %s
              AND status = 'IN_PROGRESS'
              AND actual_end_at IS NULL
            ORDER BY updated_at DESC
            LIMIT 1
            """,
            (farm_id, zone_id, zone_id, activity_type_id, activity_date),
        )
        row = cur.fetchone()
        if row:
            print(
                f"[DEBUG] FRAME/END fallback recovered concurrent row "
                f"→ instance_id={row['id']} event_row_id={event_row_id}"
            )
            return row["id"]
        return None


def cleanup_stale_instances(cur, ai_columns):
    """
    Auto-close zombie IN_PROGRESS instances.

    Rules:
    - status = IN_PROGRESS
    - actual_end_at IS NULL
    - last_seen_at older than close delay
    - Close using last_seen_at (NOT now)
    - Never keep IN_PROGRESS after close
    """

    now = utc_now()
    cutoff = now - timedelta(seconds=CLOSE_DELAY_SEC)

    cur.execute(
        """
        SELECT id, actual_start_at, last_seen_at, activity_schedule_id, instance_type
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
        print(
            f"[CLEANUP] Closing {len(stale)} inactive instances "
            f"(CLOSE_DELAY_SEC={CLOSE_DELAY_SEC})"
        )

    for row in stale:
        instance_type = row["instance_type"] or "UNSCHEDULED"
        duration = max(
            0,
            int(
                (row["last_seen_at"] - row["actual_start_at"]).total_seconds()
            ),
        )

        if duration <= 0:
            cur.execute(
                "DELETE FROM activity_instance WHERE id = %s",
                (row["id"],),
            )
            print(f"[CLEANUP] Deleted zero-duration instance {row['id']}")
            continue

        # Duration classification must run before lifecycle close status.
        if duration < MIN_ACTIVITY_DURATION_SEC:
            cur.execute(
                """
                UPDATE activity_instance
                SET actual_end_at = %s,
                    actual_duration_sec = %s,
                    status = 'NOISE',
                    instance_type = 'NOISE',
                    activity_schedule_id = NULL,
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
            mark_noise_if_short(
                cur,
                row["id"],
                duration,
                ai_columns,
            )
            continue

        stale_age_sec = (now - row["last_seen_at"]).total_seconds()
        if instance_type == "SCHEDULED":
            closed_status = "ENDED"
        elif instance_type == "NOISE":
            closed_status = "NOISE"
        elif stale_age_sec > UNCLEAR_STALE_SEC:
            closed_status = "UNCLEAR"
        else:
            closed_status = "UNSCHEDULED"

        cur.execute(
            """
            UPDATE activity_instance
            SET actual_end_at = %s,
                actual_duration_sec = %s,
                status = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                row["last_seen_at"],
                duration,
                closed_status,
                now,
                row["id"],
            ),
        )

    # Hard close runaway sessions even if events keep trickling in.
    cur.execute(
        """
        SELECT id, actual_start_at, last_seen_at, activity_schedule_id, instance_type
        FROM activity_instance
        WHERE status = 'IN_PROGRESS'
          AND actual_end_at IS NULL
          AND actual_start_at IS NOT NULL
        """
    )
    open_rows = cur.fetchall()
    forced_closed = 0

    for row in open_rows:
        instance_type = row["instance_type"] or "UNSCHEDULED"
        close_at = row["last_seen_at"] or now
        duration = max(
            0,
            int((close_at - row["actual_start_at"]).total_seconds()),
        )
        if duration <= 0:
            cur.execute("DELETE FROM activity_instance WHERE id = %s", (row["id"],))
            print(f"[CLEANUP] Deleted zero-duration runaway instance {row['id']}")
            continue
        if duration <= MAX_DURATION_SEC:
            continue

        if instance_type == "SCHEDULED":
            closed_status = "ENDED"
        elif instance_type == "NOISE":
            closed_status = "NOISE"
        else:
            closed_status = "UNSCHEDULED"

        cur.execute(
            """
            UPDATE activity_instance
            SET actual_end_at = %s,
                actual_duration_sec = %s,
                status = %s,
                last_seen_at = %s,
                updated_at = %s
            WHERE id = %s
            """,
            (
                close_at,
                duration,
                closed_status,
                close_at,
                now,
                row["id"],
            ),
        )
        forced_closed += 1

    if forced_closed:
        print(f"[CLEANUP] Force-closed {forced_closed} over-duration instances")


def run(max_loops=None):
    print("[AGGREGATOR] Starting continuous worker (SCHEDULE-AWARE)")

    BATCH_SIZE = int(os.getenv("AGG_BATCH_SIZE", "500"))
    MAX_SESSION_AGE_SEC = int(os.getenv("AGG_MAX_SESSION_AGE_SEC", "3600"))
    PENDING_LOG_EVERY_LOOPS = int(os.getenv("AGG_PENDING_LOG_EVERY_LOOPS", "10"))
    loops = 0

    # In-memory caching for performance
    # zone_cache: farm_id -> {(camera_id, activity_type_id): zone_id}
    zone_cache = {}
    farm_tz_cache = {}
    # schedule_cache: (farm_id, activity_type_id) -> [schedule rows]
    schedule_cache = {}
    
    # CRITICAL: Maps session_id → (instance_id, last_touch_unix_sec)
    # This ensures END events attach to the correct instance
    session_map = {}
    ai_columns = None
    event_columns = None

    while True:
        if max_loops and loops >= max_loops:
            print("[AGGREGATOR] Max loops reached, stopping")
            break

        loops += 1
        processed = False
        now_ts = time.time()

        # Cleanup stale session map entries to prevent unbounded memory growth.
        for sid, (_, last_touch_ts) in list(session_map.items()):
            if now_ts - last_touch_ts > MAX_SESSION_AGE_SEC:
                session_map.pop(sid, None)

        with get_cursor() as cur:
            if ai_columns is None:
                ai_columns = load_activity_instance_columns(cur)
            if event_columns is None:
                event_columns = load_event_columns(cur)

            if loops % PENDING_LOG_EVERY_LOOPS == 0:
                if "merge_processed" in event_columns:
                    cur.execute(
                        """
                        SELECT count(*) AS pending
                        FROM activity_detection_event
                        WHERE activity_instance_id IS NULL
                          AND COALESCE(merge_processed, FALSE) = FALSE
                        """
                    )
                else:
                    cur.execute(
                        """
                        SELECT count(*) AS pending
                        FROM activity_detection_event
                        WHERE activity_instance_id IS NULL
                        """
                    )
                pending = cur.fetchone()["pending"]
                print(f"[AGGREGATOR] Pending events: {pending}")

            if "merge_processed" in event_columns:
                cur.execute(
                    """
                    SELECT
                        e.id AS event_row_id,
                        e.event_type,
                        e.event_time,
                        e.farm_id,
                        e.camera_id,
                        e.activity_type_id,
                        e.session_id,
                        e.zone_id
                    FROM activity_detection_event e
                    WHERE e.activity_instance_id IS NULL
                      AND COALESCE(e.merge_processed, FALSE) = FALSE
                    ORDER BY event_time, e.id
                    LIMIT %s
                    """,
                    (BATCH_SIZE,),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        e.id AS event_row_id,
                        e.event_type,
                        e.event_time,
                        e.farm_id,
                        e.camera_id,
                        e.activity_type_id,
                        e.session_id,
                        e.zone_id
                    FROM activity_detection_event e
                    WHERE e.activity_instance_id IS NULL
                    ORDER BY event_time, e.id
                    LIMIT %s
                    """,
                    (BATCH_SIZE,),
                )

            events = cur.fetchall()

            if events:
                processed = True
                print(f"[AGGREGATOR] Processing {len(events)} events")
                event_links = []

                for e in events:
                    etype = e["event_type"]
                    event_time = e["event_time"]
                    farm_id = e["farm_id"]
                    camera_id = e["camera_id"]
                    activity_type_id = e["activity_type_id"]
                    session_id = e["session_id"]
                    
                    print(f"[DEBUG] {etype} | event_id={e['event_row_id']} | session={session_id}")

                    # --------------------------------------------------
                    # Get farm timezone
                    # --------------------------------------------------
                    if farm_id not in farm_tz_cache:
                        cur.execute("SELECT timezone FROM farm WHERE id = %s", (farm_id,))
                        farm_tz_cache[farm_id] = pytz.timezone(cur.fetchone()["timezone"])

                    farm_tz = farm_tz_cache[farm_id]
                    activity_date = event_time.astimezone(farm_tz).date()

                    # Cached zone lookup
                    zone_key = (farm_id, camera_id, activity_type_id)
                    if farm_id not in zone_cache:
                        cur.execute(
                            """
                            SELECT camera_id, activity_type_id, zone_id
                            FROM camera_activity_zone
                            WHERE farm_id = %s
                              AND is_active = true
                            """,
                            (farm_id,),
                        )
                        farm_zone_map = {}
                        for row in cur.fetchall():
                            farm_zone_map[(row["camera_id"], row["activity_type_id"])] = row["zone_id"]
                        zone_cache[farm_id] = farm_zone_map

                    zone_id = e["zone_id"] or zone_cache[farm_id].get(zone_key[1:])
                    if not zone_id:
                        print(f"[WARN] No zone mapping → skip event {e['event_row_id']}")
                        continue

                    # --------------------------------------------------
                    # CRITICAL: Resolve schedule for THIS event
                    # This is the KEY to schedule-aware grouping
                    # --------------------------------------------------
                    schedule_key = (farm_id, activity_type_id)
                    if schedule_key not in schedule_cache:
                        cur.execute(
                            """
                            SELECT
                                id,
                                ideal_start_time,
                                ideal_end_time,
                                tolerance_early_min,
                                tolerance_late_min
                            FROM activity_schedule
                            WHERE farm_id = %s
                              AND activity_type_id = %s
                              AND is_active = true
                            ORDER BY ideal_start_time
                            """,
                            schedule_key,
                        )
                        schedule_cache[schedule_key] = cur.fetchall()

                    schedule_id = resolve_schedule_from_rows(
                        schedule_cache[schedule_key], event_time, farm_tz
                    )
                    if schedule_id is None:
                        print(f"[SCHEDULE_MISS] event={event_time} farm={farm_id}")

                    # Strict schedule binding: if event is outside tolerance window,
                    # force UNSCHEDULED so we never merge across schedule windows.
                    matched_schedule = None
                    within_window = False
                    if schedule_id is not None:
                        matched_schedule = next(
                            (
                                s for s in schedule_cache[schedule_key]
                                if s["id"] == schedule_id
                            ),
                            None,
                        )
                        if matched_schedule:
                            ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
                                farm_tz,
                                activity_date,
                                matched_schedule["ideal_start_time"],
                                matched_schedule["ideal_end_time"],
                            )
                            early_tol = matched_schedule["tolerance_early_min"] or 0
                            late_tol = matched_schedule["tolerance_late_min"] or 0
                            within_window = (
                                ideal_start_utc - timedelta(minutes=early_tol)
                                <= event_time
                                <= ideal_end_utc + timedelta(minutes=late_tol)
                            )

                    # Keep best matched schedule_id even when outside strict tolerance.
                    # We only track the strict-window flag via within_window.
                    if schedule_id is None:
                        matched_schedule = None
                    print(
                        f"[SCHEDULE_DEBUG] event_local={event_time.astimezone(farm_tz)}, "
                        f"schedule_id={schedule_id}, within_window={within_window}"
                    )

                    # -------------------------------------------------
                    # START_CANDIDATE
                    # -------------------------------------------------
                    if etype == "START_CANDIDATE":
                        instance_id = None
                        event_age_sec = (utc_now() - event_time).total_seconds()
                        print(
                            f"[START_DEBUG] session={session_id} "
                            f"schedule={schedule_id} zone={zone_id} time={event_time}"
                        )

                        # STEP 0: DUPLICATE PROTECTION
                        # If this session already started, skip duplicate START
                        if session_id in session_map:
                            existing_instance_id, _ = session_map[session_id]
                            session_map[session_id] = (existing_instance_id, time.time())
                            print(f"[DEBUG] START already processed for session {session_id} → skip duplicate")
                            event_links.append((e["event_row_id"], existing_instance_id))
                            continue

                        # STEP 1: Look for active instance in same bucket
                        active = None
                        cur.execute(
                            """
                            SELECT id, last_seen_at, actual_start_at, instance_type
                            FROM activity_instance
                            WHERE farm_id = %s
                              AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
                              AND activity_type_id = %s
                              AND status = 'IN_PROGRESS'
                              AND actual_end_at IS NULL
                            ORDER BY actual_start_at DESC
                            LIMIT 1
                            """,
                            (farm_id, zone_id, zone_id, activity_type_id),
                        )
                        active = cur.fetchone()

                        if active:
                            last_seen_at = active["last_seen_at"]
                            active_start_at = active["actual_start_at"]
                            active_duration_sec = None
                            if active_start_at is not None:
                                active_duration_sec = int((event_time - active_start_at).total_seconds())
                            if active_duration_sec is not None and active_duration_sec > MAX_DURATION_SEC:
                                close_at = last_seen_at or event_time
                                status_on_close = (
                                    "ENDED"
                                    if (active.get("instance_type") == "SCHEDULED")
                                    else "UNSCHEDULED"
                                )
                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET actual_end_at = %s,
                                        actual_duration_sec = %s,
                                        status = %s,
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        close_at,
                                        max(0, int((close_at - active_start_at).total_seconds())),
                                        status_on_close,
                                        utc_now(),
                                        active["id"],
                                    ),
                                )
                                print(
                                    f"[DEBUG] Force-closed runaway active instance {active['id']} "
                                    f"duration={active_duration_sec}s cap={MAX_DURATION_SEC}s"
                                )
                                active = None
                            elif last_seen_at is None:
                                print("[DEBUG] GAP BREAK → active instance has no last_seen_at")
                                active = None
                            else:
                                gap_sec = (event_time - last_seen_at).total_seconds()
                                if gap_sec <= REOPEN_WINDOW_SEC:
                                    instance_id = active["id"]
                                    print(f"[DEBUG] Found active instance → instance_id={instance_id}")
                                else:
                                    print(
                                        f"[DEBUG] HARD GAP BREAK → new instance "
                                        f"(gap_sec={gap_sec:.1f}, reopen_limit={REOPEN_WINDOW_SEC})"
                                    )
                                    active = None

                        if not active:
                            # STEP 2: Re-open a recently closed row in the same bucket so fragmented
                            # sessions (new session_id / premature END) stitch into one instance.
                            # Previous bug: queried status = IN_PROGRESS AND actual_end_at IS NOT NULL,
                            # which violates lifecycle constraints and matched zero rows.
                            cur.execute(
                                """
                                SELECT id, actual_end_at, actual_start_at,
                                       activity_schedule_id, status
                                FROM activity_instance
                                WHERE farm_id = %s
                                  AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
                                  AND activity_type_id = %s
                                  AND activity_date = %s
                                  AND actual_end_at IS NOT NULL
                                  AND status <> 'MISSED'
                                  AND status <> 'IN_PROGRESS'
                                ORDER BY actual_end_at DESC
                                LIMIT 1
                                """,
                                (
                                    farm_id,
                                    zone_id,
                                    zone_id,
                                    activity_type_id,
                                    activity_date,
                                ),
                            )
                            prev = cur.fetchone()

                            if prev:
                                gap_sec = (event_time - prev["actual_end_at"]).total_seconds()
                                if gap_sec <= REOPEN_WINDOW_SEC:
                                    instance_id = prev["id"]
                                else:
                                    prev = None
                            if prev:
                                merged_schedule_id = prev["activity_schedule_id"] or schedule_id
                                merged_type = (
                                    "SCHEDULED"
                                    if merged_schedule_id is not None
                                    else "UNSCHEDULED"
                                )

                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET actual_start_at = LEAST(actual_start_at, %s),
                                        last_seen_at = %s,
                                        actual_end_at = NULL,
                                        actual_duration_sec = NULL,
                                        started_offset_min = NULL,
                                        ended_offset_min = NULL,
                                        status = 'IN_PROGRESS',
                                        activity_schedule_id = %s,
                                        instance_type = %s,
                                        session_id = %s,
                                        zone_id = COALESCE(zone_id, %s),
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        event_time,
                                        event_time,
                                        merged_schedule_id,
                                        merged_type,
                                        session_id,
                                        zone_id,
                                        utc_now(),
                                        instance_id,
                                    ),
                                )
                                print(
                                    f"[DEBUG] MERGED-ON-START (reopened) → instance_id={instance_id} "
                                    f"prev_status={prev['status']}"
                                )
                            else:
                                # STEP 3: CREATE new instance
                                if event_age_sec > MAX_EVENT_DELAY_SEC:
                                    print(
                                        f"[REPLAY] Processing delayed event "
                                        f"(age_sec={int(event_age_sec)}): {event_time}"
                                    )

                                # Do not blindly reuse latest row here.
                                # Reuse is only allowed via active attach, gap-based merge,
                                # or soft dedupe guard below.

                                # Soft dedupe across session_id churn/restarts.
                                cur.execute(
                                    """
                                    SELECT id
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
                                      AND activity_type_id = %s
                                      AND activity_date = %s
                                      AND status = 'IN_PROGRESS'
                                      AND actual_end_at IS NULL
                                      AND actual_start_at IS NOT NULL
                                      AND ABS(EXTRACT(EPOCH FROM (actual_start_at - %s))) <= %s
                                    ORDER BY updated_at DESC
                                    LIMIT 1
                                    """,
                                    (
                                        farm_id,
                                        zone_id,
                                        zone_id,
                                        activity_type_id,
                                        activity_date,
                                        event_time,
                                        SOFT_DEDUPE_WINDOW_SEC,
                                    ),
                                )
                                soft_dupe = cur.fetchone()
                                if soft_dupe:
                                    instance_id = soft_dupe["id"]
                                    cur.execute(
                                        """
                                        UPDATE activity_instance
                                        SET last_seen_at = %s,
                                            session_id = %s,
                                            updated_at = %s
                                        WHERE id = %s
                                        """,
                                        (event_time, session_id, utc_now(), instance_id),
                                    )
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[DEBUG] Soft-dedupe reused active instance "
                                        f"→ instance_id={instance_id}"
                                    )
                                    event_links.append((e["event_row_id"], instance_id))
                                    continue

                                within_ideal_window = False
                                if schedule_id is not None:
                                    if not matched_schedule:
                                        print(
                                            f"[WARN] activity_schedule {schedule_id} missing in cache "
                                            f"→ create as UNSCHEDULED for event {e['event_row_id']}"
                                        )
                                        schedule_id = None
                                    else:
                                        ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
                                            farm_tz,
                                            activity_date,
                                            matched_schedule["ideal_start_time"],
                                            matched_schedule["ideal_end_time"],
                                        )
                                        within_ideal_window = is_actual_start_within_ideal_window(
                                            event_time, ideal_start_utc, ideal_end_utc
                                        )

                                instance_type = "SCHEDULED" if schedule_id is not None else "UNSCHEDULED"
                                cur.execute("SAVEPOINT ai_insert_sp")
                                try:
                                    cur.execute(
                                        """
                                        INSERT INTO activity_instance (
                                            farm_id,
                                            zone_id,
                                            activity_type_id,
                                            activity_schedule_id,
                                            activity_date,
                                            session_id,
                                            instance_type,
                                            status,
                                            actual_start_at,
                                            last_seen_at,
                                            source,
                                            within_ideal_window,
                                            created_at,
                                            updated_at
                                        )
                                        VALUES (%s,%s,%s,%s,%s,
                                                %s,%s,
                                                'IN_PROGRESS',
                                                %s,%s,
                                                'AI',
                                                %s,
                                                %s,%s)
                                        RETURNING id
                                        """,
                                        (
                                            farm_id,
                                            zone_id,
                                            activity_type_id,
                                            schedule_id,
                                            activity_date,
                                            session_id,
                                            instance_type,
                                            event_time,
                                            event_time,
                                            within_ideal_window,
                                            utc_now(),
                                            utc_now(),
                                        ),
                                    )
                                    instance_id = cur.fetchone()["id"]
                                    cur.execute("RELEASE SAVEPOINT ai_insert_sp")
                                    print(
                                        f"[DEBUG] CREATED-NEW → instance_id={instance_id} "
                                        f"type={instance_type}"
                                    )
                                except pg_errors.UniqueViolation as ex:
                                    # Another row already exists for this key/day. Recover deterministically.
                                    cur.execute("ROLLBACK TO SAVEPOINT ai_insert_sp")
                                    cur.execute("RELEASE SAVEPOINT ai_insert_sp")
                                    print(f"[ERROR] UniqueViolation details: {ex}")

                                    if schedule_id is None:
                                        cur.execute(
                                            """
                                            SELECT id, status, source
                                            FROM activity_instance
                                            WHERE farm_id = %s
                                              AND zone_id = %s
                                              AND activity_type_id = %s
                                              AND activity_schedule_id IS NULL
                                              AND activity_date = %s
                                              AND actual_start_at IS NOT NULL
                                              AND ABS(EXTRACT(EPOCH FROM (actual_start_at - %s))) < 600
                                            ORDER BY created_at DESC
                                            LIMIT 1
                                            """,
                                            (
                                                farm_id,
                                                zone_id,
                                                activity_type_id,
                                                activity_date,
                                                event_time,
                                            ),
                                        )
                                    else:
                                        cur.execute(
                                            """
                                            SELECT id, status, source
                                            FROM activity_instance
                                            WHERE farm_id = %s
                                              AND zone_id = %s
                                              AND activity_type_id = %s
                                              AND activity_schedule_id = %s
                                              AND activity_date = %s
                                            ORDER BY created_at DESC
                                            LIMIT 1
                                            """,
                                            (farm_id, zone_id, activity_type_id, schedule_id, activity_date),
                                        )
                                    existing = cur.fetchone()

                                    # Fallback: conflicting row can be SYSTEM/MISSED with zone_id NULL.
                                    if not existing and schedule_id is not None:
                                        cur.execute(
                                            """
                                            SELECT id, status, source
                                            FROM activity_instance
                                            WHERE farm_id = %s
                                              AND activity_type_id = %s
                                              AND activity_schedule_id = %s
                                              AND activity_date = %s
                                            ORDER BY created_at DESC
                                            LIMIT 1
                                            """,
                                            (farm_id, activity_type_id, schedule_id, activity_date),
                                        )
                                        existing = cur.fetchone()

                                    if not existing:
                                        print(
                                            f"[WARN] Duplicate insert but no row found on recovery "
                                            f"→ retry later {e['event_row_id']}"
                                        )
                                        continue
                                    instance_id = existing["id"]
                                    existing_status = existing["status"]

                                    if existing_status != "IN_PROGRESS":
                                        # Finalized rows (including MISSED) are terminal.
                                        # Never reopen in-place; create a fresh AI row.
                                        cur.execute("SAVEPOINT ai_fresh_insert_sp")
                                        try:
                                            cur.execute(
                                                """
                                                INSERT INTO activity_instance (
                                                    farm_id,
                                                    zone_id,
                                                    activity_type_id,
                                                    activity_schedule_id,
                                                    activity_date,
                                                    session_id,
                                                    instance_type,
                                                    status,
                                                    actual_start_at,
                                                    last_seen_at,
                                                    source,
                                                    within_ideal_window,
                                                    created_at,
                                                    updated_at
                                                )
                                                VALUES (%s,%s,%s,%s,%s,
                                                        %s,%s,
                                                        'IN_PROGRESS',
                                                        %s,%s,
                                                        'AI',
                                                        %s,
                                                        %s,%s)
                                                RETURNING id
                                                """,
                                                (
                                                    farm_id,
                                                    zone_id,
                                                    activity_type_id,
                                                    schedule_id,
                                                    activity_date,
                                                    session_id,
                                                    instance_type,
                                                    event_time,
                                                    event_time,
                                                    within_ideal_window,
                                                    utc_now(),
                                                    utc_now(),
                                                ),
                                            )
                                            instance_id = cur.fetchone()["id"]
                                            cur.execute("RELEASE SAVEPOINT ai_fresh_insert_sp")
                                            print(
                                                f"[DEBUG] CREATED-NEW after finalized-row conflict "
                                                f"→ instance_id={instance_id}"
                                            )
                                        except pg_errors.UniqueViolation:
                                            cur.execute("ROLLBACK TO SAVEPOINT ai_fresh_insert_sp")
                                            cur.execute("RELEASE SAVEPOINT ai_fresh_insert_sp")
                                            print(
                                                f"[WARN] Finalized row conflict prevented fresh insert "
                                                f"→ retry later {e['event_row_id']}"
                                            )
                                            continue
                                    else:
                                        cur.execute(
                                            """
                                            UPDATE activity_instance
                                            SET actual_start_at = LEAST(COALESCE(actual_start_at, %s), %s),
                                                last_seen_at = GREATEST(COALESCE(last_seen_at, %s), %s),
                                                session_id = COALESCE(session_id, %s),
                                                zone_id = COALESCE(zone_id, %s),
                                                updated_at = %s
                                            WHERE id = %s
                                            """,
                                            (
                                                event_time,
                                                event_time,
                                                event_time,
                                                event_time,
                                                session_id,
                                                zone_id,
                                                utc_now(),
                                                instance_id,
                                            ),
                                        )
                                        print(
                                            f"[DEBUG] REUSED-EXISTING IN_PROGRESS (duplicate guard) "
                                            f"→ instance_id={instance_id}"
                                        )

                        # Map this session to the instance
                        session_map[session_id] = (instance_id, time.time())
                        print(f"[DEBUG] MAPPED session {session_id} → instance_id={instance_id}")

                        event_links.append((e["event_row_id"], instance_id))

                    # -------------------------------------------------
                    # FRAME / END
                    # -------------------------------------------------
                    else:
                        # Session-first attach for FRAME/END to prevent retry loops.
                        cache_row = session_map.get(session_id)
                        if cache_row:
                            instance_id = cache_row[0]
                            session_map[session_id] = (instance_id, time.time())
                            if schedule_id is not None:
                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET last_seen_at = %s,
                                        updated_at = %s,
                                        activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                        instance_type = CASE
                                            WHEN activity_schedule_id IS NULL THEN 'SCHEDULED'
                                            ELSE instance_type
                                        END
                                    WHERE id = %s
                                    """,
                                    (event_time, utc_now(), schedule_id, instance_id),
                                )
                            else:
                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET last_seen_at = %s,
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (event_time, utc_now(), instance_id),
                                )
                            event_links.append((e["event_row_id"], instance_id))
                            continue

                        instance_id = None

                        if instance_id is None:
                            # 🔥 CRITICAL FIX (ISSUE 3): DB Fallback for Restart Safety
                            # If session_map lost (aggregator restart), recover from DB
                            cur.execute(
                                """
                                SELECT id FROM activity_instance
                                WHERE session_id = %s
                                  AND status = 'IN_PROGRESS'
                                  AND actual_end_at IS NULL
                                ORDER BY updated_at DESC
                                LIMIT 1
                                """,
                                (session_id,),
                            )
                            row = cur.fetchone()
                            
                            if row:
                                instance_id = row["id"]
                                # Restore to memory cache
                                session_map[session_id] = (instance_id, time.time())
                                print(f"[DEBUG] Restored session map from DB: {session_id} → {instance_id}")
                            else:
                                # Try business-key recovery (session_id churn), then fallback create.
                                cur.execute(
                                    """
                                    SELECT id
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
                                      AND activity_type_id = %s
                                      AND activity_date = %s
                                      AND status = 'IN_PROGRESS'
                                      AND actual_end_at IS NULL
                                    ORDER BY updated_at DESC
                                    LIMIT 1
                                    """,
                                    (
                                        farm_id,
                                        zone_id,
                                        zone_id,
                                        activity_type_id,
                                        activity_date,
                                    ),
                                )
                                by_key = cur.fetchone()
                                if by_key:
                                    instance_id = by_key["id"]
                                    if schedule_id is not None:
                                        cur.execute(
                                            """
                                            UPDATE activity_instance
                                            SET last_seen_at = %s,
                                                updated_at = %s,
                                                activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                                instance_type = CASE
                                                    WHEN activity_schedule_id IS NULL THEN 'SCHEDULED'
                                                    ELSE instance_type
                                                END
                                            WHERE id = %s
                                            """,
                                            (event_time, utc_now(), schedule_id, instance_id),
                                        )
                                    else:
                                        cur.execute(
                                            """
                                            UPDATE activity_instance
                                            SET last_seen_at = %s,
                                                updated_at = %s
                                            WHERE id = %s
                                            """,
                                            (event_time, utc_now(), instance_id),
                                        )
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[DEBUG] Recovered active instance by business key "
                                        f"→ session={session_id} instance_id={instance_id}"
                                    )
                                else:
                                    instance_id = create_fallback_activity_instance(
                                        cur,
                                        farm_id,
                                        zone_id,
                                        activity_type_id,
                                        schedule_id,
                                        activity_date,
                                        session_id,
                                        event_time,
                                        farm_tz,
                                        matched_schedule,
                                        e["event_row_id"],
                                    )
                                    if instance_id is None:
                                        print(
                                            f"[WARN] TEMP SKIP (will retry) {etype} "
                                            f"→ event_id={e['event_row_id']} session={session_id}"
                                        )
                                        continue
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[DEBUG] FRAME/END fallback mapped session "
                                        f"{session_id} → instance_id={instance_id}"
                                    )
                        else:
                            # Refresh last-touch timestamp for long sessions.
                            session_map[session_id] = (instance_id, time.time())

                        # Refresh instance state (get latest values)
                        cur.execute(
                            """
                            SELECT actual_start_at, actual_end_at, activity_schedule_id
                            FROM activity_instance
                            WHERE id = %s
                            """,
                            (instance_id,),
                        )
                        row = cur.fetchone()
                        if not row:
                            session_map.pop(session_id, None)
                            print(f"[WARN] Instance disappeared → retry later {e['event_row_id']}")
                            continue
                        actual_start_at = row["actual_start_at"]
                        current_end_at = row["actual_end_at"]

                        if etype == "FRAME_AGGREGATE":
                            cur.execute(
                                """
                                UPDATE activity_instance
                                SET last_seen_at = %s,
                                    updated_at = %s
                                WHERE id = %s
                                """,
                                (event_time, utc_now(), instance_id),
                            )

                        elif etype == "END_CANDIDATE":
                            # Always defer real closure to cleanup_stale_instances after
                            # CLOSE_DELAY_SEC of inactivity. Classifying NOISE from the *first*
                            # END_CANDIDATE while duration < NOISE_DURATION_SEC falsely splits real
                            # sessions (edge often emits END during brief detection gaps).
                            if actual_start_at is None:
                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET last_seen_at = %s,
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (event_time, utc_now(), instance_id),
                                )
                                print(
                                    "[WARN] END_CANDIDATE but no start -> fallback update "
                                    f"instance_id={instance_id}"
                                )
                            else:
                                duration_sec = int((event_time - actual_start_at).total_seconds())
                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET last_seen_at = %s,
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (event_time, utc_now(), instance_id),
                                )
                                print(
                                    "[DEBUG] END_CANDIDATE -> deferred close (stitch mode) "
                                    f"instance_id={instance_id} duration={duration_sec}s"
                                )

                        event_links.append((e["event_row_id"], instance_id))

                bulk_link_events(cur, event_links)
                cur.connection.commit()

            cleanup_stale_instances(cur, ai_columns)
            cur.connection.commit()

        if not processed:
            time.sleep(2)   # no load → slow polling

    print("[AGGREGATOR] Activity aggregation complete")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(
        description="Run STEP-4 activity aggregation worker."
    )
    parser.add_argument(
        "--max-loops",
        type=int,
        default=None,
        help="Stop after N polling loops (default: run forever).",
    )
    args = parser.parse_args()
    run(max_loops=args.max_loops)
