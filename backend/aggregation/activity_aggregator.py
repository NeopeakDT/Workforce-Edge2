#!/usr/bin/env python3
"""
STEP-4 — ACTIVITY AGGREGATOR (SCHEDULE-AWARE, PRODUCTION-GRADE)

Creates and manages activity_instance from START_CANDIDATE only.

CRITICAL RULES:
✅ ONE active instance per (farm, zone, activity_type, schedule_id)
✅ session_id used ONLY for correctness (session_map tracking)
✅ schedule_id used for MERGING (same schedule window → same instance)
✅ FRAME/END never create instances
✅ END uses MAX logic: actual_end_at = max(existing_end, event_time)
✅ START uses MIN logic: actual_start_at = min(existing_start, event_time)
"""

from pathlib import Path
import sys
from datetime import timedelta, datetime, timezone
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


def compute_activity_date(cur, farm_id, event_time_utc):
    cur.execute("SELECT timezone FROM farm WHERE id = %s", (farm_id,))
    tz = pytz.timezone(cur.fetchone()["timezone"])
    return event_time_utc.astimezone(tz).date()


def resolve_schedule_for_event(cur, farm_id, activity_type_id, event_time_utc, farm_tz):
    """
    Resolve the best-matching activity_schedule for a given event.
    
    Returns: schedule_id (or None if no schedule found)
    
    LOGIC:
    1. Get all active schedules for farm/activity
    2. Convert event_time to farm local timezone
    3. For each schedule, check if event falls within [ideal_start - early_tolerance, ideal_end + late_tolerance]
    4. Return the schedule_id of the best match
    5. If no match in tolerance window, return the closest schedule
    
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
            window_start = ideal_start_local - timedelta(minutes=sched["tolerance_early_min"])
            window_end = ideal_end_local + timedelta(minutes=sched["tolerance_late_min"])
            
            # Check if event falls within tolerance window
            if window_start <= event_local <= window_end:
                # Event is within this schedule's window - perfect match!
                # Score by distance from ideal_start (prefer early in window)
                distance_from_start = int((event_local - ideal_start_local).total_seconds() / 60)
                score = abs(distance_from_start)
                
                if best_schedule is None or score < best_score:
                    best_schedule = sched["id"]
                    best_score = score
            else:
                # Event outside this schedule's tolerance window
                # Use as fallback: score by distance from closest boundary
                if event_local < window_start:
                    distance = int((window_start - event_local).total_seconds() / 60)
                else:
                    distance = int((event_local - window_end).total_seconds() / 60)
                
                score = distance  # Fallback score = distance from window
                
                if best_schedule is None:
                    best_schedule = sched["id"]
                    best_score = score
                elif score < best_score:
                    best_schedule = sched["id"]
                    best_score = score
                    
        except Exception as ex:
            print(f"[WARN] Schedule resolution error for schedule {sched['id']}: {ex}")
            continue
    
    return best_schedule


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


def run(max_loops=None):
    print("[AGGREGATOR] Starting continuous worker (SCHEDULE-AWARE)")

    BATCH_SIZE = 1000
    loops = 0

    # In-memory caching for performance
    zone_cache = {}
    farm_tz_cache = {}
    schedule_cache = {}  # (farm_id, activity_type_id) → schedule_id
    
    # CRITICAL: Maps session_id → instance_id (for correctness)
    # This ensures END events attach to the correct instance
    session_map = {}

    while True:
        if max_loops and loops >= max_loops:
            print("[AGGREGATOR] Max loops reached, stopping")
            break

        loops += 1
        processed = False

        with get_cursor() as cur:
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
                ORDER BY session_id, event_time, e.id
                LIMIT %s
                """,
                (BATCH_SIZE,),
            )

            events = cur.fetchall()

            if events:
                processed = True
                print(f"[AGGREGATOR] Processing {len(events)} events")

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

                    zone_id = e["zone_id"] or zone_cache[zone_key]
                    if not zone_id:
                        print(f"[WARN] No zone mapping → retry later {e['event_row_id']}")
                        continue

                    # --------------------------------------------------
                    # CRITICAL: Resolve schedule for THIS event
                    # This is the KEY to schedule-aware grouping
                    # --------------------------------------------------
                    schedule_id = resolve_schedule_for_event(
                        cur, farm_id, activity_type_id, event_time, farm_tz
                    )
                    
                    if schedule_id is None:
                        print(f"[WARN] No schedule found for activity_type {activity_type_id} → retry later {e['event_row_id']}")
                        continue

                    instance_key = (farm_id, zone_id, activity_type_id, schedule_id)

                    # -------------------------------------------------
                    # START_CANDIDATE
                    # -------------------------------------------------
                    if etype == "START_CANDIDATE":
                        instance_id = None

                        # STEP 0: DUPLICATE PROTECTION
                        # If this session already started, skip duplicate START
                        if session_id in session_map:
                            print(f"[DEBUG] START already processed for session {session_id} → skip duplicate")
                            cur.execute(
                                """
                                UPDATE activity_detection_event
                                SET activity_instance_id = %s
                                WHERE id = %s
                                """,
                                (session_map[session_id], e["event_row_id"]),
                            )
                            continue

                        # STEP 1: Look for active instance in this schedule/zone
                        cur.execute(
                            """
                            SELECT id
                            FROM activity_instance
                            WHERE farm_id = %s
                              AND zone_id = %s
                              AND activity_type_id = %s
                              AND activity_schedule_id = %s
                              AND status = 'IN_PROGRESS'
                              AND actual_end_at IS NULL
                            ORDER BY actual_start_at DESC
                            LIMIT 1
                            """,
                            (farm_id, zone_id, activity_type_id, schedule_id),
                        )
                        active = cur.fetchone()

                        if active:
                            instance_id = active["id"]
                            print(f"[DEBUG] Found active instance → instance_id={instance_id}")
                        else:
                            # STEP 2: Check for merge window (recent ended in same schedule)
                            gap = timedelta(
                                minutes=MERGE_GAP_MINUTES.get(activity_type_id, 10)
                            )

                            cur.execute(
                                """
                                SELECT id, actual_end_at, actual_start_at
                                FROM activity_instance
                                WHERE farm_id = %s
                                  AND zone_id = %s
                                  AND activity_type_id = %s
                                  AND activity_schedule_id = %s
                                  AND actual_end_at IS NOT NULL
                                ORDER BY actual_end_at DESC
                                LIMIT 1
                                """,
                                (farm_id, zone_id, activity_type_id, schedule_id),
                            )
                            prev = cur.fetchone()

                            if prev and event_time - prev["actual_end_at"] <= gap:
                                # 🔥 CRITICAL FIX: MERGE without reopening
                                # DO NOT update status or actual_end_at
                                # Just reuse instance_id
                                instance_id = prev["id"]
                                
                                # Use MIN Logic: ensure actual_start_at stays earlier
                                # In case this session started before the other
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
                                cur.execute(
                                    """
                                    INSERT INTO activity_instance (
                                        farm_id,
                                        zone_id,
                                        activity_type_id,
                                        activity_schedule_id,
                                        activity_date,
                                        session_id,
                                        status,
                                        actual_start_at,
                                        last_seen_at,
                                        source,
                                        created_at,
                                        updated_at
                                    )
                                    VALUES (%s,%s,%s,%s,%s,
                                            %s,
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
                                        schedule_id,
                                        activity_date,
                                        session_id,
                                        event_time,
                                        event_time,
                                        utc_now(),
                                        utc_now(),
                                    ),
                                )
                                instance_id = cur.fetchone()["id"]
                                print(f"[DEBUG] CREATED-NEW → instance_id={instance_id}")

                        # Map this session to the instance
                        session_map[session_id] = instance_id
                        print(f"[DEBUG] MAPPED session {session_id} → instance_id={instance_id}")

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
                        # For FRAME/END, look up instance via session_map (correctness layer)
                        instance_id = session_map.get(session_id)

                        if instance_id is None:
                            # 🔥 CRITICAL FIX (ISSUE 3): DB Fallback for Restart Safety
                            # If session_map lost (aggregator restart), recover from DB
                            cur.execute(
                                """
                                SELECT id FROM activity_instance
                                WHERE session_id = %s
                                LIMIT 1
                                """,
                                (session_id,),
                            )
                            row = cur.fetchone()
                            
                            if row:
                                instance_id = row["id"]
                                # Restore to memory cache
                                session_map[session_id] = instance_id
                                print(f"[DEBUG] Restored session map from DB: {session_id} → {instance_id}")
                            else:
                                # NO START event received for this session
                                # This is an edge case: END/FRAME without START
                                print(f"[WARN] No START for session {session_id} → skipping {etype} (event {e['event_row_id']})")
                                continue

                        # Refresh instance state (get latest values)
                        cur.execute(
                            """
                            SELECT actual_start_at, actual_end_at
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
                            # 🔴 CRITICAL: Use MAX logic to prevent end < start
                            # existing_end could be NULL or already set
                            # We take the GREATEST to handle merge-on-start correctly
                            
                            # Get fresh data
                            cur.execute(
                                """
                                SELECT actual_start_at, actual_end_at
                                FROM activity_instance
                                WHERE id = %s
                                """,
                                (instance_id,),
                            )
                            curr = cur.fetchone()
                            curr_start = curr["actual_start_at"]
                            curr_end = curr["actual_end_at"]
                            
                            # Determine final end time (use MAX logic)
                            if curr_end is not None:
                                final_end = max(curr_end, event_time)
                                print(f"[DEBUG] END merging: curr_end={curr_end}, event_time={event_time} → final_end={final_end}")
                            else:
                                final_end = event_time
                            
                            # Validate: end should NOT be before start
                            if final_end < curr_start:
                                print(f"[ERROR] END BEFORE START! start={curr_start}, end={final_end}, session={session_id}")
                                # Skip this update to prevent data corruption
                                # Event will be retried next loop
                                continue
                            
                            duration = max(
                                0,
                                int(
                                    (final_end - curr_start).total_seconds()
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
                                    final_end,
                                    duration,
                                    final_end,
                                    utc_now(),
                                    instance_id,
                                ),
                            )
                            
                            # Remove from session map (closed instance)
                            session_map.pop(session_id, None)
                            print(f"[DEBUG] CLOSED instance via END → session {session_id} removed from map")

                        cur.execute(
                            """
                            UPDATE activity_detection_event
                            SET activity_instance_id = %s
                            WHERE id = %s
                            """,
                            (instance_id, e["event_row_id"]),
                        )

                cur.connection.commit()

                # Close stale instances only when work was done
                cleanup_stale_instances(cur)
                cur.connection.commit()

        if not processed:
            import time
            time.sleep(2)   # no load → slow polling

    print("[AGGREGATOR] Activity aggregation complete")

