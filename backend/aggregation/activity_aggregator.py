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
  If START is missing, a synthetic IN_PROGRESS instance is created.
- END uses MAX logic: `actual_end_at = max(existing_end, event_time)`.
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

MAX_DURATION_SEC = 90 * 60
MIN_VALID_DURATION_SEC = int(os.getenv("AGG_MIN_VALID_DURATION_SEC", "60"))
NOISE_DURATION_SEC = int(os.getenv("AGG_NOISE_DURATION_SEC", "300"))
START_CONFIRMATION_WINDOW_SEC = int(os.getenv("AGG_START_CONFIRMATION_WINDOW_SEC", "10"))
MIN_START_EVENTS = int(os.getenv("AGG_MIN_START_EVENTS", "15"))
UNCLEAR_STALE_SEC = int(os.getenv("AGG_UNCLEAR_STALE_SEC", "600"))
MAX_EVENT_DELAY_SEC = int(os.getenv("AGG_MAX_EVENT_DELAY_SEC", "300"))

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


# Timeout-driven closure control. END_CANDIDATE does not close sessions immediately.
# Sessions are closed only when inactivity exceeds CLOSE_DELAY_SEC.
CLOSE_DELAY_SEC = int(
    os.getenv(
        "AGG_CLOSE_DELAY_SEC",
        os.getenv("AGG_END_GAP_SEC", "300"),
    )
)
REOPEN_WINDOW_SEC = int(os.getenv("AGG_REOPEN_WINDOW_SEC", "300"))


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


def mark_noise_if_short(cur, instance_id, duration_sec, ai_columns):
    global _NOISE_COLUMN_WARNED
    if duration_sec >= NOISE_DURATION_SEC:
        return

    assignments = []
    params = []

    if "instance_type" in ai_columns:
        assignments.append("instance_type = 'NOISE'")
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


def has_stable_start_signal(cur, session_id, event_time):
    """
    Require temporal persistence before converting START_CANDIDATE into an instance.
    This avoids one-frame false starts polluting activity_instance.
    """
    window_start = event_time - timedelta(seconds=START_CONFIRMATION_WINDOW_SEC)
    cur.execute(
        """
        SELECT
            count(*) AS evt_count,
            min(event_time) AS first_seen,
            max(event_time) AS last_seen
        FROM activity_detection_event
        WHERE session_id = %s
          AND activity_instance_id IS NULL
          AND event_type IN ('START_CANDIDATE', 'FRAME_AGGREGATE')
          AND event_time BETWEEN %s AND %s
        """,
        (session_id, window_start, event_time),
    )
    row = cur.fetchone()
    evt_count = row["evt_count"] or 0
    if evt_count < MIN_START_EVENTS:
        return False

    first_seen = row["first_seen"]
    last_seen = row["last_seen"]
    if not first_seen or not last_seen:
        return False

    span_sec = (last_seen - first_seen).total_seconds()
    return span_sec >= START_CONFIRMATION_WINDOW_SEC


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
        if duration < NOISE_DURATION_SEC:
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

            if loops % PENDING_LOG_EVERY_LOOPS == 0:
                cur.execute(
                    """
                    SELECT count(*) AS pending
                    FROM activity_detection_event
                    WHERE activity_instance_id IS NULL
                    """
                )
                pending = cur.fetchone()["pending"]
                print(f"[AGGREGATOR] Pending events: {pending}")

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
                        print(f"[WARN] No zone mapping → retry later {e['event_row_id']}")
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

                        # STEP 0: DUPLICATE PROTECTION
                        # If this session already started, skip duplicate START
                        if session_id in session_map:
                            existing_instance_id, _ = session_map[session_id]
                            session_map[session_id] = (existing_instance_id, time.time())
                            print(f"[DEBUG] START already processed for session {session_id} → skip duplicate")
                            event_links.append((e["event_row_id"], existing_instance_id))
                            continue

                        if not has_stable_start_signal(cur, session_id, event_time):
                            print(
                                f"[DEBUG] START deferred (unstable) session={session_id} "
                                f"window={START_CONFIRMATION_WINDOW_SEC}s min_events={MIN_START_EVENTS}"
                            )
                            # Keep event unlinked; it will be retried next loop.
                            continue

                        max_gap_sec = REOPEN_WINDOW_SEC

                        # STEP 1: Look for active instance in same bucket
                        active = None
                        cur.execute(
                            """
                            SELECT id, last_seen_at
                            FROM activity_instance
                            WHERE farm_id = %s
                              AND zone_id = %s
                              AND activity_type_id = %s
                              AND (
                                    activity_schedule_id = %s
                                    OR (activity_schedule_id IS NULL AND %s IS NULL)
                              )
                              AND status = 'IN_PROGRESS'
                              AND actual_end_at IS NULL
                            ORDER BY actual_start_at DESC
                            LIMIT 1
                            """,
                            (farm_id, zone_id, activity_type_id, schedule_id, schedule_id),
                        )
                        active = cur.fetchone()

                        if active:
                            last_seen_at = active["last_seen_at"]
                            if last_seen_at is None:
                                print("[DEBUG] GAP BREAK → active instance has no last_seen_at")
                                active = None
                            else:
                                gap_sec = (event_time - last_seen_at).total_seconds()
                                if gap_sec <= max_gap_sec:
                                    instance_id = active["id"]
                                    print(f"[DEBUG] Found active instance → instance_id={instance_id}")
                                elif gap_sec <= REOPEN_WINDOW_SEC:
                                    # Reopen same active session for short pauses/flicker gaps.
                                    instance_id = active["id"]
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
                                        f"[DEBUG] REOPENED active session → instance_id={instance_id} "
                                        f"(gap_sec={gap_sec:.1f}, reopen_limit={REOPEN_WINDOW_SEC})"
                                    )
                                else:
                                    print(
                                        f"[DEBUG] HARD GAP BREAK → new instance "
                                        f"(gap_sec={gap_sec:.1f}, reopen_limit={REOPEN_WINDOW_SEC})"
                                    )
                                    active = None

                        if not active:
                            # STEP 2: Check for merge window (recent ended in same bucket)
                            gap = timedelta(seconds=REOPEN_WINDOW_SEC)

                            if schedule_id is None:
                                cur.execute(
                                    """
                                    SELECT id, actual_end_at, actual_start_at
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND zone_id = %s
                                      AND activity_type_id = %s
                                      AND activity_schedule_id IS NULL
                                      AND actual_end_at IS NOT NULL
                                      AND status = 'IN_PROGRESS'
                                    ORDER BY actual_end_at DESC
                                    LIMIT 1
                                    """,
                                    (farm_id, zone_id, activity_type_id),
                                )
                            else:
                                cur.execute(
                                    """
                                    SELECT id, actual_end_at, actual_start_at
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND zone_id = %s
                                      AND activity_type_id = %s
                                      AND activity_schedule_id = %s
                                      AND actual_end_at IS NOT NULL
                                      AND status = 'IN_PROGRESS'
                                    ORDER BY actual_end_at DESC
                                    LIMIT 1
                                    """,
                                    (farm_id, zone_id, activity_type_id, schedule_id),
                                )
                            prev = cur.fetchone()

                            if prev and event_time - prev["actual_end_at"] <= gap:
                                # MERGE without reopening.
                                instance_id = prev["id"]

                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET actual_start_at = LEAST(actual_start_at, %s),
                                        last_seen_at = %s,
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (event_time, event_time, utc_now(), instance_id),
                                )
                                print(f"[DEBUG] MERGED-ON-START → instance_id={instance_id} (no reopen)")
                            else:
                                # STEP 3: CREATE new instance
                                if event_age_sec > MAX_EVENT_DELAY_SEC:
                                    print(
                                        f"[REPLAY] Processing delayed event "
                                        f"(age_sec={int(event_age_sec)}): {event_time}"
                                    )

                                cur.execute(
                                    """
                                    SELECT id, status, actual_end_at
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND zone_id = %s
                                      AND activity_type_id = %s
                                      AND activity_date = %s
                                      AND (
                                            activity_schedule_id = %s
                                            OR (activity_schedule_id IS NULL AND %s IS NULL)
                                      )
                                    ORDER BY updated_at DESC
                                    LIMIT 1
                                    """,
                                    (
                                        farm_id,
                                        zone_id,
                                        activity_type_id,
                                        activity_date,
                                        schedule_id,
                                        schedule_id,
                                    ),
                                )
                                existing = cur.fetchone()
                                if existing:
                                    instance_id = existing["id"]
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[DEBUG] Duplicate guard reused existing instance "
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
                                        if existing_status == "MISSED":
                                            # Reuse MISSED row in-place; never delete during aggregation.
                                            print("[DEBUG] Reusing MISSED row as active AI instance")
                                            cur.execute(
                                                """
                                                UPDATE activity_instance
                                                SET zone_id = %s,
                                                    session_id = COALESCE(session_id, %s),
                                                    instance_type = %s,
                                                    status = 'IN_PROGRESS',
                                                    source = 'AI',
                                                    actual_start_at = LEAST(COALESCE(actual_start_at, %s), %s),
                                                    actual_end_at = NULL,
                                                    actual_duration_sec = NULL,
                                                    last_seen_at = GREATEST(COALESCE(last_seen_at, %s), %s),
                                                    within_ideal_window = %s,
                                                    started_offset_min = NULL,
                                                    ended_offset_min = NULL,
                                                    updated_at = %s
                                                WHERE id = %s
                                                """,
                                                (
                                                    zone_id,
                                                    session_id,
                                                    instance_type,
                                                    event_time,
                                                    event_time,
                                                    event_time,
                                                    event_time,
                                                    within_ideal_window,
                                                    utc_now(),
                                                    instance_id,
                                                ),
                                            )
                                            print(
                                                f"[DEBUG] REUSED-MISSED as IN_PROGRESS "
                                                f"→ instance_id={instance_id}"
                                            )
                                        else:
                                            # COMPLETED (and other finalized) rows are terminal.
                                            # Never reopen; attempt a fresh insert instead.
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
                        # For FRAME/END, look up instance via session_map (correctness layer)
                        cache_row = session_map.get(session_id)
                        instance_id = cache_row[0] if cache_row else None

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
                                # START can be dropped under load/restarts; synthesize session start.
                                event_age_sec = (utc_now() - event_time).total_seconds()
                                if event_age_sec > MAX_EVENT_DELAY_SEC:
                                    print(
                                        f"[REPLAY] Processing delayed event "
                                        f"(age_sec={int(event_age_sec)}): {event_time}"
                                    )

                                synthetic_schedule_id = schedule_id
                                if synthetic_schedule_id is None:
                                    s_key = (farm_id, activity_type_id)
                                    schedules = schedule_cache.get(s_key)
                                    if schedules is None:
                                        cur.execute(
                                            """
                                            SELECT
                                                id, ideal_start_time, ideal_end_time,
                                                tolerance_early_min, tolerance_late_min
                                            FROM activity_schedule
                                            WHERE farm_id = %s
                                              AND activity_type_id = %s
                                              AND is_active = true
                                            ORDER BY ideal_start_time
                                            """,
                                            (farm_id, activity_type_id),
                                        )
                                        schedules = cur.fetchall()
                                        schedule_cache[s_key] = schedules
                                    synthetic_schedule_id = resolve_schedule_from_rows(
                                        schedules, event_time, farm_tz
                                    )

                                synthetic_activity_date = compute_activity_date(cur, farm_id, event_time)
                                synthetic_within_ideal_window = False
                                if synthetic_schedule_id is not None:
                                    cur.execute(
                                        """
                                        SELECT
                                            ideal_start_time,
                                            ideal_end_time,
                                            tolerance_early_min,
                                            tolerance_late_min
                                        FROM activity_schedule
                                        WHERE id = %s
                                        """,
                                        (synthetic_schedule_id,),
                                    )
                                    srow = cur.fetchone()
                                    if srow:
                                        ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
                                            farm_tz,
                                            synthetic_activity_date,
                                            srow["ideal_start_time"],
                                            srow["ideal_end_time"],
                                        )
                                        early_tol = srow["tolerance_early_min"] or 0
                                        late_tol = srow["tolerance_late_min"] or 0
                                        synthetic_within_ideal_window = (
                                            ideal_start_utc - timedelta(minutes=early_tol)
                                            <= event_time
                                            <= ideal_end_utc + timedelta(minutes=late_tol)
                                        )
                                # Enforce one-active-instance-per-key rule before any synthetic insert.
                                # NOTE: intentionally do not filter by schedule_id here, because
                                # uniq_active_instance enforces uniqueness on the business key only.
                                cur.execute(
                                    """
                                    SELECT id
                                    FROM activity_instance
                                    WHERE farm_id = %s
                                      AND zone_id = %s
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
                                        activity_type_id,
                                        synthetic_activity_date,
                                    ),
                                )
                                existing_active = cur.fetchone()

                                if existing_active:
                                    instance_id = existing_active["id"]
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
                                        f"[DEBUG] Reused existing active instance instead of synthetic insert "
                                        f"→ instance_id={instance_id}"
                                    )
                                else:
                                    synthetic_instance_type = (
                                        "SCHEDULED" if synthetic_schedule_id is not None else "UNSCHEDULED"
                                    )
                                    cur.execute("SAVEPOINT ai_synth_insert_sp")
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
                                                synthetic_schedule_id,
                                                synthetic_activity_date,
                                                session_id,
                                                synthetic_instance_type,
                                                event_time,
                                                event_time,
                                                synthetic_within_ideal_window,
                                                utc_now(),
                                                utc_now(),
                                            ),
                                        )
                                        instance_id = cur.fetchone()["id"]
                                        cur.execute("RELEASE SAVEPOINT ai_synth_insert_sp")
                                        session_map[session_id] = (instance_id, time.time())
                                        print(
                                            f"[WARN] No START for session {session_id} "
                                            f"→ created synthetic instance {instance_id}"
                                        )
                                    except pg_errors.UniqueViolation:
                                        # Race-safe fallback: another worker/path created the active row.
                                        cur.execute("ROLLBACK TO SAVEPOINT ai_synth_insert_sp")
                                        cur.execute("RELEASE SAVEPOINT ai_synth_insert_sp")
                                        cur.execute(
                                            """
                                            SELECT id
                                            FROM activity_instance
                                            WHERE farm_id = %s
                                              AND zone_id = %s
                                              AND activity_type_id = %s
                                              AND activity_date = %s
                                              AND status = 'IN_PROGRESS'
                                              AND actual_end_at IS NULL
                                            ORDER BY updated_at DESC
                                            LIMIT 1
                                            FOR UPDATE
                                            """,
                                            (
                                                farm_id,
                                                zone_id,
                                                activity_type_id,
                                                synthetic_activity_date,
                                            ),
                                        )
                                        race_row = cur.fetchone()
                                        if race_row:
                                            instance_id = race_row["id"]
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
                                                f"[DEBUG] Recovered from synthetic insert race "
                                                f"→ instance_id={instance_id}"
                                            )
                                        else:
                                            print(
                                                f"[WARN] Synthetic insert raced but no active row found "
                                                f"→ retry later {e['event_row_id']}"
                                            )
                                            continue
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
                            # END is advisory; do not close immediately.
                            # Keep session open so short pauses can stitch into one session.
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
                                f"[DEBUG] END_CANDIDATE observed; deferred close by timeout "
                                f"(instance_id={instance_id})"
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
