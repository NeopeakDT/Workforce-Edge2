#!/usr/bin/env python3
"""
Edge2 device
STEP-4 - Activity Aggregator (schedule-aware)

What this does:
- Consumes unlinked rows from `activity_detection_event` where `activity_instance_id` is NULL.
- Creates/updates `activity_instance` records for real-world activity sessions.
- Uses optional schedule binding per event (tolerance) without merging across inactive gaps.
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
- Every event link runs `backfill_instance_schedule_date` + `normalize_instance_status_for_row`;
  each loop calls `normalize_null_instance_statuses` before commit (repairs historical NULL `status`).
- Env `AGG_MAX_EVENT_AGE_SEC` (>0): skip stale events with `merge_processed` set so they are not retried forever.
  Default `86400` (1 day). Set `0` to disable. This bounds the main fetch path (oldest-first).
- Env `AGG_DISABLE_FALLBACK_INSTANCE_CREATE`: when truthy, skip synthetic INSERT in `resolve_fallback_instance_or_skip`.
- Env `AGG_ENDED_RECOVERY_WINDOW_SEC`: optional override for late FRAME/END stitch into `ENDED` rows
  (see `ended_recovery_window_sec` for activity-type defaults; separate from live attach guard).
- Default `AGG_CLOSE_DELAY_SEC` / `AGG_END_GAP_SEC` is 900s unless overridden (use 1800+ for heavy replay).
- Live attach uses `MERGE_GAP_SEC` / `live_attach_guard_sec` (strict + soft `IN_PROGRESS` only).
  Historical replay uses nearest-neighbor on `activity_date` then bounded contiguity
  (`AGG_HISTORICAL_REPLAY_GAP_SEC`, default 1800s). When `event_age_sec > live_attach_guard_sec`,
  resolver skips live paths and uses replay mode only.
- Periodic reconciliation: `AGG_RECONCILE_EVERY_LOOPS` (default 5) pulls NULL-linked events via
  `retry_unlinked_events`; lookback `AGG_RECONCILE_LOOKBACK_DAYS` (default 1, float OK e.g. 0.5);
  batch `AGG_RECONCILE_BATCH_SIZE`. Orphans stay retriable (`merge_processed` unchanged).
- A MISSED row at `(farm_id, activity_schedule_id, activity_date)` blocks INSERT via unique index;
  START converts that row back to `IN_PROGRESS` instead of inserting another row.

Critical rules:
- Attach logic: live = strict + soft `IN_PROGRESS` (same day). Historical = nearest contiguous bucket on
  `activity_date` with replay gap (default 1800s). `ended_recovery_window_sec` (7200s) is for same-day ENDED stitch only.
- `recover_ended_instance_for_late_frame_end` extends **ENDED** rows in-place (`actual_end_at` / duration /
  `last_seen_at`) without setting `status = 'IN_PROGRESS'`, preserving `check_valid_lifecycle`.
- `session_id` is a hint for ordering/cache; bucket keys are farm / zone-soft / activity_type / day.
- `FRAME_AGGREGATE` / `END_CANDIDATE` / `START_CANDIDATE` bucket paths share the resolver rules above.
- `cleanup_stale_instances` selects `IN_PROGRESS` rows stale by `last_seen_at` or runaway span (including
  rows with provisional non-null `actual_end_at`).
- END_CANDIDATE logging uses deferred close; cleanup assigns final `ENDED` after `CLOSE_DELAY_SEC`.
- Session open is optimistic on first `START_CANDIDATE` (no pre-frame count gate); short sessions
  are filtered at cleanup via `MIN_VALID_DURATION_SEC`. Merge uses MIN logic on `actual_start_at`.
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

# Max gap (seconds) between events to treat as one activity instance.
# A schedule may contain many instances; compliance aggregates across them.
MERGE_GAP_SEC = {
    1: 180,  # milking
    2: 900,  # feeding / TRM — match CLOSE_DELAY_SEC (sparse frames / inference gaps)
    3: 300,  # scrapping / cleaning
}

MIN_VALID_DURATION_SEC = {
    1: 120,  # MILKING
    2: 60,  # FEEDING
    3: 30,  # SCRAPPING
}

# Lifecycle: finalized rows retain `actual_end_at`; FRAME/END may extend in attach paths.
FINALIZED_LIFECYCLE_STATUSES = (
    "ENDED",
)
FINALIZED_ATTACH_STATUSES = frozenset(FINALIZED_LIFECYCLE_STATUSES)


def merge_gap_sec(activity_type_id: int) -> int:
    """Seconds allowed between last activity and attach/reopen continuity."""
    return MERGE_GAP_SEC.get(activity_type_id, 600)


def reopen_window_sec(activity_type_id: int) -> int:
    """Alias for merge gap (reopen / merge-on-start use the same continuity bound)."""
    return merge_gap_sec(activity_type_id)


def continuity_gap_exceeded(
    activity_type_id: int, event_time, anchor_at
) -> bool:
    """Hard reject attach/reopen when gap since anchor exceeds activity merge gap."""
    if anchor_at is None:
        return False
    gap_sec = abs((event_time - anchor_at).total_seconds())
    return gap_sec > merge_gap_sec(activity_type_id)


def continuity_anchor_at(row) -> datetime | None:
    """Reference time for continuity: last_seen on open rows, actual_end on finalized."""
    if row.get("status") == "IN_PROGRESS":
        return row.get("last_seen_at")
    if row.get("actual_end_at") is not None:
        return row["actual_end_at"]
    return row.get("last_seen_at")


def reopen_missed_activity_instance(
    cur,
    instance_id,
    zone_id,
    session_id,
    event_time,
    within_ideal_window,
):
    """
    Convert a placeholder MISSED row into a live AI session. Avoids INSERT collisions with
    uq_missed_schedule_per_day (farm_id, activity_schedule_id, activity_date).
    session_id is set here because MISSED placeholders typically had none.
    """
    cur.execute(
        """
        UPDATE activity_instance
        SET status = 'IN_PROGRESS',
            zone_id = COALESCE(zone_id, %s),
            actual_start_at = %s,
            actual_end_at = NULL,
            actual_duration_sec = NULL,
            last_seen_at = %s,
            source = 'AI',
            session_id = %s,
            within_ideal_window = %s,
            started_offset_min = NULL,
            ended_offset_min = NULL,
            session_classification = NULL,
            updated_at = %s
        WHERE id = %s
          AND status = 'ENDED'
          AND session_classification = 'MISSED'
        RETURNING id
        """,
        (
            zone_id,
            event_time,
            event_time,
            session_id,
            within_ideal_window,
            utc_now(),
            instance_id,
        ),
    )
    row = cur.fetchone()
    return row["id"] if row else None


MAX_DURATION_SEC = int(os.getenv("AGG_MAX_ACTIVITY_DURATION_SEC", str(90 * 60)))
MAX_EVENT_DELAY_SEC = int(os.getenv("AGG_MAX_EVENT_DELAY_SEC", "300"))
SCHEDULE_BIND_GRACE_SEC = int(os.getenv("AGG_SCHEDULE_BIND_GRACE_SEC", "40"))
UNSCHEDULED_CREATE_DELAY_SEC = int(os.getenv("AGG_UNSCHEDULED_CREATE_DELAY_SEC", "60"))
STALE_START_SESSION_EVENT_MIN = int(os.getenv("AGG_STALE_START_SESSION_EVENT_MIN", "3"))
SOFT_DEDUPE_WINDOW_SEC = int(os.getenv("AGG_SOFT_DEDUPE_WINDOW_SEC", "30"))
# When > 0, skip processing events older than this many seconds (marks merge_processed).
# Default 1 day — matches AGG_RECONCILE_LOOKBACK_DAYS. Set 0 to disable age gate.
MAX_EVENT_AGE_SEC = int(os.getenv("AGG_MAX_EVENT_AGE_SEC", "86400"))


def historical_replay_gap_sec(activity_type_id: int) -> int:
    """
    Max continuity gap (seconds) for replay attach to an existing bucket — repairs outages,
    not separate operational sessions. Override: AGG_HISTORICAL_REPLAY_GAP_SEC or _<type_id>
    (legacy: AGG_HISTORICAL_MAX_DISTANCE_SEC / _<type_id>).
    """
    env = os.getenv("AGG_HISTORICAL_REPLAY_GAP_SEC")
    if env:
        return int(env)
    per = os.getenv(f"AGG_HISTORICAL_REPLAY_GAP_SEC_{activity_type_id}")
    if per:
        return int(per)
    legacy = os.getenv("AGG_HISTORICAL_MAX_DISTANCE_SEC")
    if legacy:
        return int(legacy)
    per_legacy = os.getenv(f"AGG_HISTORICAL_MAX_DISTANCE_SEC_{activity_type_id}")
    if per_legacy:
        return int(per_legacy)
    defaults = {
        1: 1800,  # milking 30 min replay continuity
        2: 1800,  # feeding 30 min
        3: 1800,  # scrapping 30 min
    }
    return defaults.get(activity_type_id, 1800)


def historical_max_distance_sec(activity_type_id: int) -> int:
    """Alias for `historical_replay_gap_sec` (backward-compatible name)."""
    return historical_replay_gap_sec(activity_type_id)


def historical_start_tolerance_sec(activity_type_id: int) -> int:
    """How far before `actual_start_at` a replay event may still attach."""
    env = os.getenv("AGG_HISTORICAL_START_TOLERANCE_SEC")
    if env:
        return int(env)
    per = os.getenv(f"AGG_HISTORICAL_START_TOLERANCE_SEC_{activity_type_id}")
    if per:
        return int(per)
    return live_attach_guard_sec(activity_type_id)


# SQL fragment: temporal anchor for historical nearest-neighbor (no live window in WHERE).
_HISTORICAL_ANCHOR_SQL = "COALESCE(last_seen_at, actual_end_at, actual_start_at)"


def historical_instance_anchor(row) -> datetime | None:
    """Temporal anchor for nearest-neighbor historical matching."""
    return row.get("last_seen_at") or row.get("actual_end_at") or row.get("actual_start_at")


def historical_contiguity_allows(row, event_time, activity_type_id: int):
    """
  Replay attach only to a contiguous bucket: bounded gap vs last activity, not whole-day chaining.
  Returns (allowed, reject_reason, log_fields).
    """
    replay_gap = historical_replay_gap_sec(activity_type_id)
    start_tol = historical_start_tolerance_sec(activity_type_id)
    start_at = row.get("actual_start_at")
    last_seen = row.get("last_seen_at")
    actual_end = row.get("actual_end_at")
    anchor = historical_instance_anchor(row)
    log_fields = {
        "replay_gap_sec": replay_gap,
        "start_tolerance_sec": start_tol,
    }

    if anchor is None:
        return False, "historical_no_anchor", log_fields

    gap_sec = abs((event_time - anchor).total_seconds())
    log_fields["gap_sec"] = int(gap_sec)

    if gap_sec > replay_gap:
        return False, "historical_gap_exceeded", log_fields

    if start_at and event_time < start_at - timedelta(seconds=start_tol):
        log_fields["actual_start_at"] = start_at
        return False, "historical_before_start", log_fields

    activity_ceiling = last_seen or actual_end or start_at
    if activity_ceiling and event_time > activity_ceiling + timedelta(
        seconds=replay_gap
    ):
        log_fields["activity_ceiling"] = activity_ceiling
        return False, "historical_after_window", log_fields

    return True, None, log_fields


def is_historical_replay_event(event_time, activity_type_id: int) -> bool:
    """True when ingestion lag exceeds the live attach window → replay mode only."""
    age_sec = (utc_now() - event_time).total_seconds()
    return age_sec > live_attach_guard_sec(activity_type_id)


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


def attach_event_to_instance(cur, event_row_id, instance_id):
    """Persist detection-event → instance link immediately (crash-safe before batch flush)."""
    cur.execute(
        """
        UPDATE activity_detection_event
        SET activity_instance_id = %s
        WHERE id = %s
          AND activity_instance_id IS NULL
        """,
        (instance_id, event_row_id),
    )


def queue_event_link(
    cur,
    event_row_id,
    instance_id,
    event_links,
    *,
    schedule_id=None,
    activity_date=None,
):
    """Link event to instance; backfill NULL schedule/date on the instance from this event's resolution."""
    attach_event_to_instance(cur, event_row_id, instance_id)
    event_links.append((event_row_id, instance_id))
    backfill_instance_schedule_date(cur, instance_id, schedule_id, activity_date)
    normalize_instance_status_for_row(cur, instance_id)


def backfill_instance_schedule_date(cur, instance_id, schedule_id, activity_date):
    """COALESCE-fill `activity_schedule_id` / `activity_date` after attach (replay / recovery safe)."""
    if not instance_id:
        return
    if schedule_id is None and activity_date is None:
        return
    cur.execute(
        """
        UPDATE activity_instance
        SET activity_schedule_id = COALESCE(activity_schedule_id, %s),
            activity_date = COALESCE(activity_date, %s)
        WHERE id = %s
        """,
        (schedule_id, activity_date, instance_id),
    )


def normalize_instance_status_for_row(cur, instance_id):
    """Repair lifecycle status from actual_end_at (skip write if already correct)."""
    if not instance_id:
        return

    cur.execute(
        """
        UPDATE activity_instance
        SET
            status = (
                CASE
                    WHEN actual_end_at IS NULL
                        THEN 'IN_PROGRESS'
                    ELSE 'ENDED'
                END
            )::activity_status,
            updated_at = NOW()
        WHERE id = %s
          AND status IS DISTINCT FROM (
            CASE
                WHEN actual_end_at IS NULL
                    THEN 'IN_PROGRESS'
                ELSE 'ENDED'
            END
          )::activity_status
        """,
        (instance_id,),
    )


def normalize_null_instance_statuses(cur):
    """
    Repair rows where lifecycle writers left status NULL.
    """
    cur.execute(
        """
        UPDATE activity_instance
        SET
            status = (
                CASE
                    WHEN actual_end_at IS NULL
                        THEN 'IN_PROGRESS'
                    ELSE 'ENDED'
                END
            )::activity_status,
            updated_at = %s
        WHERE status IS NULL
        """,
        (utc_now(),),
    )


def bulk_link_events(cur, event_links):
    """
    Optional bulk replay for rows still NULL (e.g. race); queue_event_link already attached most.
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
          AND e.activity_instance_id IS NULL
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


def resolve_fallback_instance_or_skip(
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
    event_columns,
    fatal_label,
):
    """
    Last-resort INSERT when session restore, bucket attach, and ENDED extension all miss (backlog /
    fragmentation). Retry via `merge_processed=FALSE` semantics on failure paths.
    Disabled when `AGG_DISABLE_FALLBACK_INSTANCE_CREATE` is truthy.
    """
    if os.getenv("AGG_DISABLE_FALLBACK_INSTANCE_CREATE", "").lower() in (
        "1",
        "true",
        "yes",
    ):
        print(
            f"[ORPHAN_EVENT] skipped (fallback disabled) "
            f"event_row_id={event_row_id} type={fatal_label}"
        )
        return None

    within_ideal_window = False
    if schedule_id is not None and matched_schedule:
        ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
            farm_tz,
            activity_date,
            matched_schedule["ideal_start_time"],
            matched_schedule["ideal_end_time"],
        )
        within_ideal_window = is_actual_start_within_ideal_window(
            event_time,
            ideal_start_utc,
            ideal_end_utc,
        )

    cur.execute("SAVEPOINT fallback_instance_sp")
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
                status,
                actual_start_at,
                last_seen_at,
                source,
                within_ideal_window,
                created_at,
                updated_at
            )
            VALUES (%s,%s,%s,%s,%s,
                    %s,
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
                event_time,
                event_time,
                within_ideal_window,
                utc_now(),
                utc_now(),
            ),
        )
        row = cur.fetchone()
        cur.execute("RELEASE SAVEPOINT fallback_instance_sp")
        iid = row["id"]
        print(
            f"[FALLBACK_INSTANCE] created instance_id={iid} event_row_id={event_row_id} "
            f"reason={fatal_label}"
        )
        return iid
    except pg_errors.UniqueViolation:
        cur.execute("ROLLBACK TO SAVEPOINT fallback_instance_sp")
        cur.execute("RELEASE SAVEPOINT fallback_instance_sp")
        replay = is_historical_replay_event(event_time, activity_type_id)
        if not replay:
            alt = find_in_progress_bucket_attach(
                cur,
                farm_id,
                zone_id,
                activity_type_id,
                activity_date,
                schedule_id,
                event_time,
                event_row_id=event_row_id,
            )
            if alt:
                print(
                    f"[FALLBACK_INSTANCE] uniq race → reuse instance_id={alt} "
                    f"event_row_id={event_row_id}"
                )
                return alt
        hist_row = recover_historical_instance_attach(
            cur,
            farm_id,
            zone_id,
            activity_type_id,
            activity_date,
            schedule_id,
            event_time,
            event_row_id=event_row_id,
        )
        if hist_row:
            alt_hist = _attach_from_historical_row(
                cur,
                hist_row,
                session_id,
                farm_id,
                activity_date,
                zone_id,
                activity_type_id,
                schedule_id,
                event_time,
            )
            if alt_hist:
                print(
                    f"[FALLBACK_INSTANCE] historical → reuse instance_id={alt_hist} "
                    f"event_row_id={event_row_id}"
                )
                return alt_hist
        print(
            f"[ORPHAN_EVENT] event_row_id={event_row_id} type={fatal_label} "
            "(UniqueViolation; no attach row)"
        )
        return None


# Timeout-driven closure control. END_CANDIDATE does not close sessions immediately.
# Sessions are closed only when inactivity exceeds CLOSE_DELAY_SEC.
END_CONFIRMATION_WINDOW_SEC = int(
    os.getenv("AGG_END_CONFIRMATION_WINDOW_SEC", "20")
)
CLOSE_DELAY_SEC = int(
    os.getenv(
        "AGG_CLOSE_DELAY_SEC",
        os.getenv("AGG_END_GAP_SEC", "900"),
    )
)


def ended_recovery_window_sec(activity_type_id: int):
    """
    Max allowed delay for stitching late FRAME/END events
    into an already ENDED instance.
    """

    env_override = os.getenv("AGG_ENDED_RECOVERY_WINDOW_SEC")
    if env_override:
        return int(env_override)

    # activity-specific defaults (historical / ENDED replay — separate from live attach guard)
    defaults = {
        1: 7200,  # milking 2 h
        2: 7200,  # feeding 2 h
        3: 7200,  # scrapping 2 h
    }

    return defaults.get(activity_type_id, 7200)


def attach_guard_window_sec(activity_type_id: int) -> int:
    """Legacy: max(reopen gap, close delay). Prefer `live_attach_guard_sec` for IN_PROGRESS attach."""
    return max(reopen_window_sec(activity_type_id), CLOSE_DELAY_SEC)


def live_attach_guard_sec(activity_type_id: int) -> int:
    """
    Live IN_PROGRESS attach window (seconds), time-first vs schedule.
    Separate from `ended_recovery_window_sec` (ENDED / replay stitch).
    Override: AGG_LIVE_ATTACH_GUARD_SEC or AGG_LIVE_ATTACH_GUARD_SEC_<type_id>.
    """
    env = os.getenv("AGG_LIVE_ATTACH_GUARD_SEC")
    if env:
        return int(env)
    per = os.getenv(f"AGG_LIVE_ATTACH_GUARD_SEC_{activity_type_id}")
    if per:
        return int(per)
    return merge_gap_sec(activity_type_id)


def log_attach_reject(reason: str, **fields):
    """Structured attach failure log (grep-friendly)."""
    parts = " ".join(f"{k}={v}" for k, v in fields.items() if v is not None)
    print(f"[ATTACH_REJECT] reason={reason} {parts}".rstrip())


def _live_attach_schedule_params(schedule_id):
    """Bind params for schedule-soft WHERE + ORDER BY (8 placeholders)."""
    return (
        schedule_id,
        schedule_id,
        schedule_id,
        schedule_id,
        schedule_id,
        schedule_id,
        schedule_id,
        schedule_id,
    )


def _finalize_live_attach_row(row, schedule_id, event_row_id, *, soft_label=None):
    inst_sched = row["activity_schedule_id"]
    if (
        schedule_id is not None
        and inst_sched is not None
        and inst_sched != schedule_id
    ):
        print(
            f"[ATTACH_SOFT] schedule_soft_match_used "
            f"event_row_id={event_row_id} event_schedule={schedule_id} "
            f"instance_schedule={inst_sched}"
        )
    if soft_label:
        print(
            f"[{soft_label}] instance_id={row['id']} "
            f"event_row_id={event_row_id} last_seen_at={row.get('last_seen_at')}"
        )
    return row["id"]


def find_in_progress_bucket_attach(
    cur,
    farm_id,
    zone_id,
    activity_type_id,
    activity_date,
    schedule_id,
    event_time,
    *,
    event_row_id=None,
):
    """
    Live bucket attach only (same `activity_date`): strict `IN_PROGRESS` within
    `live_attach_guard_sec`, then soft `IN_PROGRESS` (most recent `last_seen_at`).
    Does not attach to `ENDED` — use `recover_historical_instance_attach` for replay.
    """
    guard_sec = live_attach_guard_sec(activity_type_id)
    sched = _live_attach_schedule_params(schedule_id)

    # STEP 1 — strict live attach
    cur.execute(
        """
        SELECT id, activity_schedule_id, status, last_seen_at, actual_end_at
        FROM activity_instance
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND activity_date = %s
          AND (zone_id = %s OR zone_id IS NULL)
          AND status = 'IN_PROGRESS'
          AND last_seen_at IS NOT NULL
          AND ABS(
                EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at))
              ) <= %s
          AND (
                (
                  %s IS NOT NULL
                  AND (
                        activity_schedule_id = %s
                        OR activity_schedule_id IS NULL
                      )
                )
                OR (
                  %s IS NULL
                  AND (
                        activity_schedule_id IS NOT NULL
                        OR activity_schedule_id IS NULL
                      )
                )
              )
        ORDER BY
          CASE
            WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
            WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
            WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
            WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
            ELSE 2
          END,
          ABS(EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at)))
        LIMIT 1
        """,
        (
            farm_id,
            activity_type_id,
            activity_date,
            zone_id,
            event_time,
            guard_sec,
            *sched,
            event_time,
        ),
    )
    row = cur.fetchone()
    if row:
        anchor = continuity_anchor_at(row)
        if continuity_gap_exceeded(activity_type_id, event_time, anchor):
            log_attach_reject(
                "merge_gap_exceeded",
                event_row_id=event_row_id,
                farm_id=farm_id,
                activity_type_id=activity_type_id,
                activity_date=activity_date,
                zone_id=zone_id,
                event_schedule=schedule_id,
                instance_id=row["id"],
                gap_sec=int(abs((event_time - anchor).total_seconds()))
                if anchor
                else None,
                merge_gap_sec=merge_gap_sec(activity_type_id),
            )
        else:
            return _finalize_live_attach_row(row, schedule_id, event_row_id)

    # STEP 2 — soft live attach (fragmented inference gaps; no time guard)
    cur.execute(
        """
        SELECT id, activity_schedule_id, status, last_seen_at, actual_end_at
        FROM activity_instance
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND activity_date = %s
          AND (zone_id = %s OR zone_id IS NULL)
          AND status = 'IN_PROGRESS'
          AND last_seen_at IS NOT NULL
          AND (
                (
                  %s IS NOT NULL
                  AND (
                        activity_schedule_id = %s
                        OR activity_schedule_id IS NULL
                      )
                )
                OR (
                  %s IS NULL
                  AND (
                        activity_schedule_id IS NOT NULL
                        OR activity_schedule_id IS NULL
                      )
                )
              )
        ORDER BY last_seen_at DESC
        LIMIT 1
        """,
        (
            farm_id,
            activity_type_id,
            activity_date,
            zone_id,
            *sched[:4],
        ),
    )
    row = cur.fetchone()
    if row:
        return _finalize_live_attach_row(
            row, schedule_id, event_row_id, soft_label="ATTACH_SOFT_LIVE"
        )

    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM activity_instance
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND activity_date = %s
          AND (zone_id = %s OR zone_id IS NULL)
          AND status = 'IN_PROGRESS'
        """,
        (farm_id, activity_type_id, activity_date, zone_id),
    )
    any_open = int(cur.fetchone()["c"] or 0)

    cur.execute(
        """
        SELECT COUNT(*) AS c
        FROM activity_instance
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND activity_date = %s
          AND (zone_id = %s OR zone_id IS NULL)
          AND status = 'IN_PROGRESS'
          AND last_seen_at IS NOT NULL
          AND ABS(
                EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at))
              ) <= %s
        """,
        (farm_id, activity_type_id, activity_date, zone_id, event_time, guard_sec),
    )
    in_guard = int(cur.fetchone()["c"] or 0)

    if any_open <= 0:
        log_attach_reject(
            "no_attach_bucket",
            event_row_id=event_row_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            zone_id=zone_id,
            event_schedule=schedule_id,
            guard_sec=guard_sec,
        )
    elif in_guard <= 0:
        log_attach_reject(
            "outside_attach_guard",
            event_row_id=event_row_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            zone_id=zone_id,
            event_schedule=schedule_id,
            guard_sec=guard_sec,
            open_rows=any_open,
        )
    else:
        log_attach_reject(
            "schedule_mismatch_in_window",
            event_row_id=event_row_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            zone_id=zone_id,
            event_schedule=schedule_id,
            guard_sec=guard_sec,
            in_guard_rows=in_guard,
        )
    return None


def recover_historical_instance_attach(
    cur,
    farm_id,
    zone_id,
    activity_type_id,
    activity_date,
    schedule_id,
    event_time,
    *,
    event_row_id=None,
):
    """
    Historical / replay attach: nearest contiguous `IN_PROGRESS` or `ENDED` on `activity_date`.
    No temporal SQL pre-filter. Post-check: `historical_contiguity_allows` (default replay gap 1800s).
    """
    params = (
        farm_id,
        activity_type_id,
        activity_date,
        zone_id,
        schedule_id,
        event_time,
    )

    cur.execute(
        f"""
        SELECT
            id,
            status,
            actual_start_at,
            actual_end_at,
            last_seen_at,
            activity_schedule_id
        FROM activity_instance
        WHERE farm_id = %s
          AND activity_type_id = %s
          AND activity_date = %s
          AND status IN ('IN_PROGRESS', 'ENDED')
          AND (
                zone_id = %s
                OR zone_id IS NULL
              )
        ORDER BY
          CASE
            WHEN activity_schedule_id = %s THEN 0
            WHEN activity_schedule_id IS NULL THEN 1
            ELSE 2
          END,
          ABS(
            EXTRACT(
              EPOCH FROM (
                %s::timestamptz - {_HISTORICAL_ANCHOR_SQL}
              )
            )
          ) ASC
        LIMIT 1
        """,
        params,
    )
    row = cur.fetchone()
    if not row:
        log_attach_reject(
            "historical_miss",
            event_row_id=event_row_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            zone_id=zone_id,
            event_schedule=schedule_id,
        )
        return None

    allowed, reject_reason, log_fields = historical_contiguity_allows(
        row, event_time, activity_type_id
    )
    if not allowed:
        log_attach_reject(
            reject_reason,
            event_row_id=event_row_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            zone_id=zone_id,
            instance_id=row["id"],
            event_schedule=schedule_id,
            **log_fields,
        )
        return None

    print(
        f"[ATTACH_HISTORICAL] instance_id={row['id']} status={row['status']} "
        f"event_row_id={event_row_id} gap_sec={log_fields.get('gap_sec')} "
        f"replay_gap_sec={log_fields.get('replay_gap_sec')}"
    )
    return row


def recover_ended_instance_for_late_frame_end(
    cur,
    session_id,
    farm_id,
    activity_date,
    zone_id,
    activity_type_id,
    schedule_id,
    event_time,
    recovery_sec,
    *,
    target_instance_id=None,
    temporal_limit_sec=None,
    relaxed_select=False,
):
    """
    Extend finalized instances in-place (`actual_end_at` / duration / `last_seen_at`) for late FRAME/END
    replays — never sets `status = 'IN_PROGRESS'` while `actual_end_at` remains set (lifecycle-safe).
    When `event_time` falls within `[actual_start_at, actual_end_at]`, link only (no lifecycle mutation).
    `temporal_limit_sec` overrides `recovery_sec` for distance checks (replay uses historical max).
    `relaxed_select`: nearest-neighbor on `activity_date` without live window in SQL.
    """
    distance_limit = (
        temporal_limit_sec if temporal_limit_sec is not None else recovery_sec
    )

    def _extend_ended_row(evt, now_ts, sid, zid, sched_id, row_id):
        cur.execute(
            f"""
            UPDATE activity_instance
            SET
                status = COALESCE(status, 'ENDED'),
                actual_end_at = GREATEST(actual_end_at, %s::timestamptz),
                last_seen_at = GREATEST(
                    COALESCE(last_seen_at, %s::timestamptz),
                    %s::timestamptz
                ),
                actual_duration_sec = CASE
                    WHEN actual_start_at IS NOT NULL THEN
                        CAST(
                            EXTRACT(
                                EPOCH FROM (
                                    GREATEST(actual_end_at, %s::timestamptz) - actual_start_at
                                )
                            ) AS INTEGER
                        )
                    ELSE actual_duration_sec
                END,
                updated_at = %s::timestamptz,
                session_id = COALESCE(session_id, %s),
                zone_id = COALESCE(zone_id, %s),
                activity_schedule_id = COALESCE(activity_schedule_id, %s),
                activity_date = COALESCE(activity_date, %s::date),
                session_classification = NULL,
                started_offset_min = NULL,
                ended_offset_min = NULL
            WHERE id = %s
              AND status IN ({",".join("%s" for _ in FINALIZED_LIFECYCLE_STATUSES)})
              AND actual_end_at IS NOT NULL
            RETURNING id
            """,
            (
                evt,
                evt,
                evt,
                evt,
                now_ts,
                sid,
                zid,
                sched_id,
                activity_date,
                row_id,
                *FINALIZED_LIFECYCLE_STATUSES,
            ),
        )
        row = cur.fetchone()
        return row["id"] if row else None

    if target_instance_id is not None:
        cur.execute(
            f"""
            SELECT
                id,
                actual_start_at,
                actual_end_at,
                last_seen_at,
                zone_id,
                activity_schedule_id,
                status
            FROM activity_instance
            WHERE id = %s
              AND farm_id = %s
              AND activity_type_id = %s
              AND status IN ({",".join("%s" for _ in FINALIZED_LIFECYCLE_STATUSES)})
              AND actual_end_at IS NOT NULL
            """,
            (
                target_instance_id,
                farm_id,
                activity_type_id,
                *FINALIZED_LIFECYCLE_STATUSES,
            ),
        )
        hit = cur.fetchone()
        if hit:
            allowed, _, _ = historical_contiguity_allows(
                hit, event_time, activity_type_id
            )
            if not allowed:
                hit = None
    elif relaxed_select:
        relaxed_params = (
            farm_id,
            activity_type_id,
            *FINALIZED_LIFECYCLE_STATUSES,
            activity_date,
            zone_id,
            schedule_id,
            event_time,
        )
        cur.execute(
            f"""
            SELECT
                id,
                actual_start_at,
                actual_end_at,
                last_seen_at,
                zone_id,
                activity_schedule_id,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND activity_type_id = %s
              AND status IN ({",".join("%s" for _ in FINALIZED_LIFECYCLE_STATUSES)})
              AND actual_end_at IS NOT NULL
              AND activity_date = %s
              AND (
                    zone_id = %s
                    OR zone_id IS NULL
                  )
            ORDER BY
              CASE
                WHEN activity_schedule_id = %s THEN 0
                WHEN activity_schedule_id IS NULL THEN 1
                ELSE 2
              END,
              ABS(
                EXTRACT(
                  EPOCH FROM (
                    %s::timestamptz - {_HISTORICAL_ANCHOR_SQL}
                  )
                )
              ) ASC
            LIMIT 1
            """,
            relaxed_params,
        )
        hit = cur.fetchone()
        if hit:
            allowed, _, _ = historical_contiguity_allows(
                hit, event_time, activity_type_id
            )
            if not allowed:
                hit = None
    else:
        cur.execute(
            f"""
            SELECT
                id,
                actual_start_at,
                actual_end_at,
                last_seen_at,
                zone_id,
                activity_schedule_id,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND activity_type_id = %s
              AND status IN ({",".join("%s" for _ in FINALIZED_LIFECYCLE_STATUSES)})
              AND actual_end_at IS NOT NULL
              AND (
                    zone_id = %s
                    OR zone_id IS NULL
                  )
              AND activity_date = %s
              AND ABS(
                    EXTRACT(
                        EPOCH FROM (%s::timestamptz - actual_end_at)
                    )
                  ) <= %s
            ORDER BY
              CASE
                WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
                WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
                WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
                WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
                ELSE 2
              END,
              ABS(EXTRACT(EPOCH FROM (%s::timestamptz - actual_end_at)))
            LIMIT 1
            """,
            (
                farm_id,
                activity_type_id,
                *FINALIZED_LIFECYCLE_STATUSES,
                zone_id,
                activity_date,
                event_time,
                recovery_sec,
                schedule_id,
                schedule_id,
                schedule_id,
                schedule_id,
                schedule_id,
                event_time,
            ),
        )
        hit = cur.fetchone()

    if hit:
        start_at = hit["actual_start_at"]
        end_at = hit["actual_end_at"]
        if start_at is not None and event_time < start_at - timedelta(minutes=30):
            hit = None
        elif (
            start_at is not None
            and end_at is not None
            and start_at <= event_time <= end_at
        ):
            print(
                f"[ENDED_RECOVERY] link-only within span instance_id={hit['id']} "
                f"event_row_time={event_time}"
            )
            return hit["id"]
        elif start_at is not None and end_at is not None and event_time < start_at:
            anchor = historical_instance_anchor(hit)
            if (
                anchor is not None
                and abs((event_time - anchor).total_seconds()) > distance_limit
            ):
                hit = None
            elif hit:
                print(
                    f"[ENDED_RECOVERY] link-only before span instance_id={hit['id']} "
                    f"event_row_time={event_time}"
                )
                return hit["id"]

    if not hit:
        return None

    if hit["actual_end_at"] is not None and event_time <= hit["actual_end_at"]:
        print(
            f"[ENDED_RECOVERY] link-only (no extension) instance_id={hit['id']} "
            f"event_row_time={event_time}"
        )
        return hit["id"]

    now = utc_now()
    rid = _extend_ended_row(
        event_time,
        now,
        session_id,
        zone_id,
        schedule_id,
        hit["id"],
    )
    if rid:
        print(
            f"[ENDED_RECOVERY] extended finalized instance_id={rid} "
            f"farm={farm_id} type={activity_type_id} session_hint={session_id} "
            f"(window={recovery_sec}s)"
        )
    return rid


def _attach_from_historical_row(
    cur,
    hist_row,
    session_id,
    farm_id,
    activity_date,
    zone_id,
    activity_type_id,
    schedule_id,
    event_time,
):
    """Apply ENDED lifecycle rules or return IN_PROGRESS id from historical match."""
    replay_gap = historical_replay_gap_sec(activity_type_id)
    if hist_row["status"] == "ENDED":
        return recover_ended_instance_for_late_frame_end(
            cur,
            session_id,
            farm_id,
            activity_date,
            zone_id,
            activity_type_id,
            schedule_id,
            event_time,
            replay_gap,
            target_instance_id=hist_row["id"],
            temporal_limit_sec=replay_gap,
        )
    return hist_row["id"]


def resolve_attachable_instance(
    cur,
    farm_id,
    zone_id,
    activity_type_id,
    activity_date,
    schedule_id,
    event_time,
    session_id,
    *,
    try_session_restore=True,
    try_ended_recovery_sec=None,
    event_row_id=None,
    event_age_sec=None,
):
    """
    Resolver for FRAME / END / session hint. When `event_age_sec` exceeds the live attach window,
    uses replay mode only (historical nearest-neighbor + relaxed ENDED). Otherwise: session restore,
    live attach, historical, then same-day ENDED fallback.
    """
    guard_sec = live_attach_guard_sec(activity_type_id)
    ended_sec = ended_recovery_window_sec(activity_type_id)
    max_hist = historical_replay_gap_sec(activity_type_id)
    if event_age_sec is None:
        event_age_sec = (utc_now() - event_time).total_seconds()

    # Replay: historical nearest-neighbor only — no session restore / strict / soft live.
    if is_historical_replay_event(event_time, activity_type_id):
        print(
            f"[REPLAY_MODE] event_row_id={event_row_id} age_sec={int(event_age_sec)} "
            f"live_window_sec={guard_sec}"
        )
        hist_row = recover_historical_instance_attach(
            cur,
            farm_id,
            zone_id,
            activity_type_id,
            activity_date,
            schedule_id,
            event_time,
            event_row_id=event_row_id,
        )
        if hist_row:
            attached = _attach_from_historical_row(
                cur,
                hist_row,
                session_id,
                farm_id,
                activity_date,
                zone_id,
                activity_type_id,
                schedule_id,
                event_time,
            )
            if attached:
                return attached
        if try_ended_recovery_sec is not None and try_ended_recovery_sec > 0:
            return recover_ended_instance_for_late_frame_end(
                cur,
                session_id,
                farm_id,
                activity_date,
                zone_id,
                activity_type_id,
                schedule_id,
                event_time,
                max_hist,
                temporal_limit_sec=max_hist,
                relaxed_select=True,
            )
        return None

    if try_session_restore and session_id:
        cur.execute(
            f"""
            SELECT id, activity_schedule_id, status, last_seen_at, actual_end_at
            FROM activity_instance
            WHERE session_id = %s
              AND farm_id = %s
              AND activity_date = %s
              AND activity_type_id = %s
              AND (
                    (
                      status = 'IN_PROGRESS'
                      AND last_seen_at IS NOT NULL
                      AND ABS(
                            EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at))
                          ) <= %s
                    )
                    OR (
                      status IN ({",".join("%s" for _ in FINALIZED_LIFECYCLE_STATUSES)})
                      AND actual_end_at IS NOT NULL
                      AND ABS(
                            EXTRACT(EPOCH FROM (%s::timestamptz - actual_end_at))
                          ) <= %s
                    )
                  )
              AND (
                    zone_id = %s
                    OR zone_id IS NULL
                  )
            ORDER BY
              CASE WHEN status = 'IN_PROGRESS' THEN 0 ELSE 1 END,
              CASE
                WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
                WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
                WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
                WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
                ELSE 2
              END,
              CASE
                WHEN status = 'IN_PROGRESS' THEN
                  ABS(EXTRACT(EPOCH FROM (%s::timestamptz - last_seen_at)))
                ELSE
                  ABS(EXTRACT(EPOCH FROM (%s::timestamptz - actual_end_at)))
              END,
              created_at DESC
            LIMIT 1
            """,
            (
                session_id,
                farm_id,
                activity_date,
                activity_type_id,
                event_time,
                guard_sec,
                *FINALIZED_LIFECYCLE_STATUSES,
                event_time,
                ended_sec,
                zone_id,
                schedule_id,
                schedule_id,
                schedule_id,
                schedule_id,
                schedule_id,
                event_time,
                event_time,
            ),
        )
        row = cur.fetchone()
        if row:
            anchor = continuity_anchor_at(row)
            if continuity_gap_exceeded(activity_type_id, event_time, anchor):
                log_attach_reject(
                    "merge_gap_exceeded",
                    event_row_id=event_row_id,
                    session_id=session_id,
                    farm_id=farm_id,
                    activity_type_id=activity_type_id,
                    activity_date=activity_date,
                    instance_id=row["id"],
                    gap_sec=int(abs((event_time - anchor).total_seconds()))
                    if anchor
                    else None,
                    merge_gap_sec=merge_gap_sec(activity_type_id),
                )
            else:
                inst_sched = row["activity_schedule_id"]
                if (
                    schedule_id is not None
                    and inst_sched is not None
                    and inst_sched != schedule_id
                ):
                    print(
                        f"[ATTACH_SOFT] session_restore_schedule_soft_match "
                        f"event_row_id={event_row_id} event_schedule={schedule_id} "
                        f"instance_schedule={inst_sched}"
                    )
                if row.get("status") and row["status"] != "IN_PROGRESS":
                    print(
                        f"[SESSION_RESTORE_FINALIZED] instance_id={row['id']} "
                        f"status={row['status']} event_row_id={event_row_id}"
                    )
                return row["id"]
        log_attach_reject(
            "session_restore_miss",
            event_row_id=event_row_id,
            session_id=session_id,
            farm_id=farm_id,
            activity_type_id=activity_type_id,
            activity_date=activity_date,
            guard_sec=guard_sec,
            ended_sec=ended_sec,
            event_schedule=schedule_id,
        )

    bid = find_in_progress_bucket_attach(
        cur,
        farm_id,
        zone_id,
        activity_type_id,
        activity_date,
        schedule_id,
        event_time,
        event_row_id=event_row_id,
    )
    if bid:
        return bid

    hist_row = recover_historical_instance_attach(
        cur,
        farm_id,
        zone_id,
        activity_type_id,
        activity_date,
        schedule_id,
        event_time,
        event_row_id=event_row_id,
    )
    if hist_row:
        return _attach_from_historical_row(
            cur,
            hist_row,
            session_id,
            farm_id,
            activity_date,
            zone_id,
            activity_type_id,
            schedule_id,
            event_time,
        )

    if try_ended_recovery_sec is not None and try_ended_recovery_sec > 0:
        return recover_ended_instance_for_late_frame_end(
            cur,
            session_id,
            farm_id,
            activity_date,
            zone_id,
            activity_type_id,
            schedule_id,
            event_time,
            try_ended_recovery_sec,
        )
    return None


def _reconcile_lookback_days(value=None) -> float:
    """AGG_RECONCILE_LOOKBACK_DAYS: days of NULL-event requeue window (float, e.g. 0.5)."""
    if value is not None:
        return float(value)
    return float(os.getenv("AGG_RECONCILE_LOOKBACK_DAYS", "1"))


def retry_unlinked_events(cur, event_columns, limit=500, lookback_days=None):
    """
    Periodic reconciliation: NULL-linked events in lookback window get another attempt.
    Does not set merge_processed on failure so retries remain possible.
    """
    merge_clause = ""
    if "merge_processed" in event_columns:
        merge_clause = " AND COALESCE(e.merge_processed, FALSE) = FALSE"

    lookback_days = _reconcile_lookback_days(lookback_days)

    cur.execute(
        f"""
        SELECT
            e.event_id,
            e.id AS event_row_id,
            e.event_type,
            e.event_time,
            e.farm_id,
            e.device_id,
            e.camera_id,
            e.activity_type_id,
            e.session_id,
            e.zone_id,
            e.ai_confidence,
            e.payload
        FROM activity_detection_event e
        WHERE e.activity_instance_id IS NULL
          AND e.event_time >= NOW() - (%s * INTERVAL '1 day')
          {merge_clause}
        ORDER BY e.event_time
        LIMIT %s
        """,
        (lookback_days, limit),
    )
    return cur.fetchall()


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


def _cleanup_pop_session_map(cur, instance_id, session_map):
    """Drop stale session→instance cache entries after closing an instance."""
    if not session_map:
        return
    cur.execute(
        """
        SELECT session_id
        FROM activity_instance
        WHERE id = %s
        """,
        (instance_id,),
    )
    sess = cur.fetchone()
    if sess and sess.get("session_id"):
        session_map.pop(sess["session_id"], None)


def cleanup_stale_instances(cur, session_map=None):
    """
    Auto-close zombie IN_PROGRESS instances (single pass).

    Rows selected when either:
    - last_seen older than CLOSE_DELAY (inactive), or
    - span since actual_start exceeds MAX_DURATION_SEC (runaway), without a second scan pass.
    """

    now = utc_now()
    cutoff = now - timedelta(seconds=CLOSE_DELAY_SEC)

    cur.execute(
        """
        SELECT
            id,
            activity_type_id,
            actual_start_at,
            last_seen_at,
            activity_schedule_id,
            (
                last_seen_at IS NOT NULL
                AND last_seen_at < %(cutoff)s
            ) AS inactive_stale
        FROM activity_instance
        WHERE status = 'IN_PROGRESS'
          AND (
                (
                    last_seen_at IS NOT NULL
                    AND last_seen_at < %(cutoff)s
                )
                OR (
                    actual_start_at IS NOT NULL
                    AND EXTRACT(
                        EPOCH FROM (
                            COALESCE(last_seen_at, %(now)s) - actual_start_at
                        )
                    ) > %(max_dur)s
                )
              )
        """,
        {"cutoff": cutoff, "now": now, "max_dur": MAX_DURATION_SEC},
    )

    stale = cur.fetchall()

    if stale:
        print(
            f"[CLEANUP] Processing {len(stale)} close candidates "
            f"(CLOSE_DELAY_SEC={CLOSE_DELAY_SEC}, MAX_DURATION_SEC={MAX_DURATION_SEC})"
        )

    forced_closed = 0

    for row in stale:
        inactive_stale = bool(row["inactive_stale"])

        if (
            row["last_seen_at"] is not None
            and row["actual_start_at"] is not None
            and row["last_seen_at"] < row["actual_start_at"]
        ):
            print(
                f"[ERROR] Invalid timeline last_seen_at < actual_start_at "
                f"→ skip finalize id={row['id']}"
            )
            continue

        close_at = row["last_seen_at"] or now

        if row["actual_start_at"] is None:
            duration = 0
        else:
            duration = max(
                0,
                int((close_at - row["actual_start_at"]).total_seconds()),
            )

        if duration <= 0:
            print(
                f"[CLEANUP] skip_finalize duration<=0 instance_id={row['id']} "
                f"activity_type={row['activity_type_id']}"
            )
            continue

        print(
            f"[SESSION_FINALIZED] "
            f"instance_id={row['id']} "
            f"activity_type={row['activity_type_id']} "
            f"duration={duration}"
        )

        min_required = MIN_VALID_DURATION_SEC.get(row["activity_type_id"], 10)
        if duration < min_required:
            end_anchor = row["last_seen_at"] or row["actual_start_at"] or now
            cur.execute(
                """
                UPDATE activity_instance
                SET status = 'ENDED',
                    actual_end_at = %s,
                    actual_duration_sec = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (end_anchor, duration, now, row["id"]),
            )
            print(
                f"[CLEANUP] Closed short-duration instance as ENDED "
                f"id={row['id']} duration={duration}s min_required={min_required}s"
            )
            _cleanup_pop_session_map(cur, row["id"], session_map)
            continue

        closed_status = "ENDED"

        if inactive_stale:
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
            _cleanup_pop_session_map(cur, row["id"], session_map)
        elif duration > MAX_DURATION_SEC:
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
            _cleanup_pop_session_map(cur, row["id"], session_map)

    if forced_closed:
        print(f"[CLEANUP] Force-closed {forced_closed} over-duration instances")


def run(max_loops=None):
    print("[AGGREGATOR] Starting continuous worker (SCHEDULE-AWARE)")

    BATCH_SIZE = int(os.getenv("AGG_BATCH_SIZE", "500"))
    RECONCILE_EVERY_LOOPS = int(os.getenv("AGG_RECONCILE_EVERY_LOOPS", "5"))
    RECONCILE_BATCH_SIZE = int(os.getenv("AGG_RECONCILE_BATCH_SIZE", "500"))
    RECONCILE_LOOKBACK_DAYS = _reconcile_lookback_days()
    MAX_SESSION_AGE_SEC = int(os.getenv("AGG_MAX_SESSION_AGE_SEC", "3600"))
    PENDING_LOG_EVERY_LOOPS = int(os.getenv("AGG_PENDING_LOG_EVERY_LOOPS", "10"))
    loops = 0
    print(
        f"[AGGREGATOR] lookback_days={RECONCILE_LOOKBACK_DAYS} "
        f"max_event_age_sec={MAX_EVENT_AGE_SEC}"
    )

    # In-memory caching for performance
    # zone_cache: farm_id -> {(camera_id, activity_type_id): zone_id}
    zone_cache = {}
    farm_tz_cache = {}
    # schedule_cache: (farm_id, activity_type_id) -> [schedule rows]
    schedule_cache = {}
    
    # CRITICAL: Maps session_id → (instance_id, last_touch_unix_sec)
    # This ensures END events attach to the correct instance
    session_map = {}
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
                        e.event_id,
                        e.id AS event_row_id,
                        e.event_type,
                        e.event_time,
                        e.farm_id,
                        e.device_id,
                        e.camera_id,
                        e.activity_type_id,
                        e.session_id,
                        e.zone_id,
                        e.ai_confidence,
                        e.payload
                    FROM activity_detection_event e
                    WHERE e.activity_instance_id IS NULL
                      AND COALESCE(e.merge_processed, FALSE) = FALSE
                    ORDER BY e.event_time,
                             CASE e.event_type::text
                               WHEN 'START_CANDIDATE' THEN 0
                               WHEN 'FRAME_AGGREGATE' THEN 1
                               WHEN 'END_CANDIDATE' THEN 2
                               ELSE 3
                             END,
                             e.id
                    LIMIT %s
                    """,
                    (BATCH_SIZE,),
                )
            else:
                cur.execute(
                    """
                    SELECT
                        e.event_id,
                        e.id AS event_row_id,
                        e.event_type,
                        e.event_time,
                        e.farm_id,
                        e.device_id,
                        e.camera_id,
                        e.activity_type_id,
                        e.session_id,
                        e.zone_id,
                        e.ai_confidence,
                        e.payload
                    FROM activity_detection_event e
                    WHERE e.activity_instance_id IS NULL
                    ORDER BY e.event_time,
                             CASE e.event_type::text
                               WHEN 'START_CANDIDATE' THEN 0
                               WHEN 'FRAME_AGGREGATE' THEN 1
                               WHEN 'END_CANDIDATE' THEN 2
                               ELSE 3
                             END,
                             e.id
                    LIMIT %s
                    """,
                    (BATCH_SIZE,),
                )

            events = cur.fetchall()

            if (
                RECONCILE_EVERY_LOOPS > 0
                and loops % RECONCILE_EVERY_LOOPS == 0
            ):
                retry_batch = retry_unlinked_events(
                    cur,
                    event_columns,
                    limit=RECONCILE_BATCH_SIZE,
                    lookback_days=RECONCILE_LOOKBACK_DAYS,
                )
                if retry_batch:
                    print(
                        f"[RECONCILE] prepended {len(retry_batch)} unlinked events "
                        f"(lookback_days={RECONCILE_LOOKBACK_DAYS}, limit={RECONCILE_BATCH_SIZE})"
                    )
                    events = list(retry_batch) + list(events)

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
                        mark_event_skipped(cur, e["event_row_id"], event_columns)
                        continue

                    if MAX_EVENT_AGE_SEC > 0:
                        ev_age = (utc_now() - event_time).total_seconds()
                        if ev_age > MAX_EVENT_AGE_SEC:
                            print(
                                f"[SKIP_STALE_EVENT] event_row_id={e['event_row_id']} "
                                f"age_sec={int(ev_age)} max={MAX_EVENT_AGE_SEC}"
                            )
                            mark_event_skipped(cur, e["event_row_id"], event_columns)
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
                    # force no schedule binding so we never merge across schedule windows.
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
                    # START_CANDIDATE: open session immediately (optimistic); FRAME/END attach
                    # afterward. No synthetic START row (enum: START_CANDIDATE / FRAME / END only).
                    # -------------------------------------------------
                    if etype == "START_CANDIDATE":
                        cur.execute(
                            """
                            SELECT 1
                            FROM activity_detection_event
                            WHERE session_id = %s
                              AND event_type IN (
                                    'START_CANDIDATE',
                                    'FRAME_AGGREGATE'
                              )
                              AND activity_instance_id IS NOT NULL
                            LIMIT 1
                            """,
                            (session_id,),
                        )
                        if cur.fetchone():
                            mark_event_skipped(cur, e["event_row_id"], event_columns)
                            print(
                                f"[SKIP_START_CANDIDATE] session already has linked evidence "
                                f"session={session_id} event_id={e['event_row_id']}"
                            )
                            continue

                        merge_gap = merge_gap_sec(activity_type_id)
                        instance_id = None
                        event_age_sec = (utc_now() - event_time).total_seconds()
                        print(
                            f"[START_DEBUG] session={session_id} "
                            f"schedule={schedule_id} zone={zone_id} time={event_time}"
                        )

                        # STEP 0: DUPLICATE PROTECTION
                        # If this session already mapped, skip duplicate open (only if row still open).
                        if session_id in session_map:
                            existing_instance_id, _ = session_map[session_id]
                            cur.execute(
                                """
                                SELECT activity_date, status
                                FROM activity_instance
                                WHERE id = %s
                                """,
                                (existing_instance_id,),
                            )
                            row = cur.fetchone()
                            if (
                                row
                                and row["activity_date"] == activity_date
                                and row["status"] == "IN_PROGRESS"
                            ):
                                session_map[session_id] = (existing_instance_id, time.time())
                                print(
                                    f"[DEBUG] START_CANDIDATE already mapped for session {session_id} "
                                    "→ skip duplicate"
                                )
                                queue_event_link(cur, e["event_row_id"], existing_instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                continue
                            session_map.pop(session_id, None)

                        cur.execute(
                            """
                            SELECT id
                            FROM activity_instance
                            WHERE session_id = %s
                              AND activity_date = %s
                            ORDER BY created_at DESC
                            LIMIT 1
                            """,
                            (session_id, activity_date),
                        )
                        existing_session_instance = cur.fetchone()
                        if existing_session_instance:
                            cur.execute(
                                """
                                SELECT status, actual_end_at, last_seen_at
                                FROM activity_instance
                                WHERE id = %s
                                """,
                                (existing_session_instance["id"],),
                            )
                            row_sess = cur.fetchone()
                            if row_sess and row_sess["status"] == "IN_PROGRESS":
                                if continuity_gap_exceeded(
                                    activity_type_id,
                                    event_time,
                                    row_sess.get("last_seen_at"),
                                ):
                                    log_attach_reject(
                                        "merge_gap_exceeded",
                                        event_row_id=e["event_row_id"],
                                        session_id=session_id,
                                        instance_id=existing_session_instance["id"],
                                        merge_gap_sec=merge_gap,
                                    )
                                else:
                                    instance_id = existing_session_instance["id"]
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[DEBUG] Existing session instance reused "
                                        f"→ instance_id={instance_id}"
                                    )
                                    queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                    continue

                        # Bucket IN_PROGRESS attach (skipped in replay mode — use historical).
                        replay_start = is_historical_replay_event(
                            event_time, activity_type_id
                        )
                        bucket_attach_id = None
                        if not replay_start:
                            bucket_attach_id = find_in_progress_bucket_attach(
                                cur,
                                farm_id,
                                zone_id,
                                activity_type_id,
                                activity_date,
                                schedule_id,
                                event_time,
                                event_row_id=e["event_row_id"],
                            )
                        active = None
                        if bucket_attach_id:
                            cur.execute(
                                """
                                SELECT id, last_seen_at, actual_start_at, activity_schedule_id
                                FROM activity_instance
                                WHERE id = %s
                                """,
                                (bucket_attach_id,),
                            )
                            active = cur.fetchone()

                        if active:
                            active_start_at = active["actual_start_at"]
                            active_duration_sec = None
                            if active_start_at is not None:
                                active_duration_sec = int((event_time - active_start_at).total_seconds())
                            if active_duration_sec is not None and active_duration_sec > MAX_DURATION_SEC:
                                close_at = active["last_seen_at"] or event_time
                                dur_close = max(
                                    0,
                                    int((close_at - active_start_at).total_seconds()),
                                )
                                if dur_close <= 0:
                                    print(
                                        f"[DEBUG] skip force-close runaway (duration<=0) "
                                        f"instance_id={active['id']}"
                                    )
                                else:
                                    status_on_close = "ENDED"
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
                                            dur_close,
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
                            elif continuity_gap_exceeded(
                                activity_type_id,
                                event_time,
                                active.get("last_seen_at"),
                            ):
                                log_attach_reject(
                                    "merge_gap_exceeded",
                                    event_row_id=e["event_row_id"],
                                    session_id=session_id,
                                    instance_id=active["id"],
                                    merge_gap_sec=merge_gap,
                                )
                                active = None
                            else:
                                instance_id = active["id"]
                                cur.execute(
                                    """
                                    SELECT session_id
                                    FROM activity_instance
                                    WHERE id = %s
                                    """,
                                    (instance_id,),
                                )
                                existing = cur.fetchone()
                                if existing and existing["session_id"]:
                                    if existing["session_id"] != session_id:
                                        print(
                                            f"[SESSION_CONFLICT] "
                                            f"existing={existing['session_id']} "
                                            f"incoming={session_id} "
                                            f"→ reuse bucket active instance_id={instance_id}"
                                        )
                                    else:
                                        print(
                                            f"[DEBUG] Found active instance → instance_id={instance_id}"
                                        )
                                else:
                                    print(
                                        f"[DEBUG] Found active instance → instance_id={instance_id}"
                                    )

                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET last_seen_at = %s,
                                        actual_start_at = LEAST(COALESCE(actual_start_at, %s), %s),
                                        actual_end_at = CASE
                                            WHEN actual_end_at IS NOT NULL THEN
                                                GREATEST(actual_end_at, %s::timestamptz)
                                            ELSE actual_end_at
                                        END,
                                        zone_id = COALESCE(zone_id, %s),
                                        session_id = COALESCE(session_id, %s),
                                        activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                        activity_date = COALESCE(activity_date, %s),
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        event_time,
                                        event_time,
                                        event_time,
                                        event_time,
                                        zone_id,
                                        session_id,
                                        schedule_id,
                                        activity_date,
                                        utc_now(),
                                        instance_id,
                                    ),
                                )
                                session_map[session_id] = (instance_id, time.time())
                                print(
                                    f"[BUCKET_ACTIVE_REUSE] session={session_id} "
                                    f"→ instance_id={instance_id}"
                                )
                                queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                continue

                        if not active:
                            # STEP 2: Re-open a recently closed row in the same bucket so fragmented
                            # sessions (same session_id / premature END) stitch into one instance.
                            # Different edge session_id ⇒ new activity; do not reopen (prevents duration inflation).
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
                                  AND (
                                        (
                                          %s IS NOT NULL
                                          AND (
                                                activity_schedule_id = %s
                                                OR activity_schedule_id IS NULL
                                              )
                                        )
                                        OR (
                                          %s IS NULL
                                          AND (
                                                activity_schedule_id IS NOT NULL
                                                OR activity_schedule_id IS NULL
                                              )
                                        )
                                      )
                                  AND actual_end_at IS NOT NULL
                                  AND actual_end_at <= %s::timestamptz
                                  AND EXTRACT(EPOCH FROM (%s::timestamptz - actual_end_at)) <= %s
                                  AND NOT (
                                    status = 'ENDED'
                                    AND session_classification = 'MISSED'
                                  )
                                  AND status <> 'IN_PROGRESS'
                                  AND (session_id IS NULL OR session_id = %s)
                                ORDER BY
                                  CASE
                                    WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
                                    WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
                                    WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
                                    WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
                                    ELSE 2
                                  END,
                                  ABS(EXTRACT(EPOCH FROM (%s::timestamptz - actual_end_at)))
                                LIMIT 1
                                """,
                                (
                                    farm_id,
                                    zone_id,
                                    zone_id,
                                    activity_type_id,
                                    activity_date,
                                    schedule_id,
                                    schedule_id,
                                    schedule_id,
                                    event_time,
                                    event_time,
                                    merge_gap,
                                    session_id,
                                    schedule_id,
                                    schedule_id,
                                    schedule_id,
                                    schedule_id,
                                    schedule_id,
                                    event_time,
                                ),
                            )
                            prev = cur.fetchone()

                            if prev:
                                prev_date = prev["actual_end_at"].astimezone(farm_tz).date()
                                current_date = event_time.astimezone(farm_tz).date()
                                if prev_date != current_date:
                                    prev = None
                                elif continuity_gap_exceeded(
                                    activity_type_id,
                                    event_time,
                                    prev["actual_end_at"],
                                ):
                                    log_attach_reject(
                                        "merge_gap_exceeded",
                                        event_row_id=e["event_row_id"],
                                        session_id=session_id,
                                        instance_id=prev["id"],
                                        merge_gap_sec=merge_gap,
                                    )
                                    prev = None

                            if prev:
                                instance_id = prev["id"]
                                merged_schedule_id = prev["activity_schedule_id"] or schedule_id

                                cur.execute(
                                    """
                                    UPDATE activity_instance
                                    SET actual_start_at = LEAST(actual_start_at, %s),
                                        last_seen_at = %s,
                                        actual_end_at = NULL,
                                        actual_duration_sec = NULL,
                                        started_offset_min = NULL,
                                        ended_offset_min = NULL,
                                        session_classification = NULL,
                                        status = 'IN_PROGRESS',
                                        activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                        activity_date = COALESCE(activity_date, %s),
                                        zone_id = COALESCE(zone_id, %s),
                                        session_id = COALESCE(session_id, %s),
                                        updated_at = %s
                                    WHERE id = %s
                                    """,
                                    (
                                        event_time,
                                        event_time,
                                        merged_schedule_id,
                                        activity_date,
                                        zone_id,
                                        session_id,
                                        utc_now(),
                                        instance_id,
                                    ),
                                )
                                print(
                                    f"[DEBUG] MERGED-ON-START (reopened) → instance_id={instance_id} "
                                    f"prev_status={prev['status']}"
                                )
                            else:
                                # Historical / ENDED recovery before any INSERT.
                                max_hist_start = historical_replay_gap_sec(
                                    activity_type_id
                                )
                                recovery_sec_start = ended_recovery_window_sec(
                                    activity_type_id
                                )
                                hist_start = recover_historical_instance_attach(
                                    cur,
                                    farm_id,
                                    zone_id,
                                    activity_type_id,
                                    activity_date,
                                    schedule_id,
                                    event_time,
                                    event_row_id=e["event_row_id"],
                                )
                                if hist_start:
                                    recovered_start = _attach_from_historical_row(
                                        cur,
                                        hist_start,
                                        session_id,
                                        farm_id,
                                        activity_date,
                                        zone_id,
                                        activity_type_id,
                                        schedule_id,
                                        event_time,
                                    )
                                else:
                                    recovered_start = (
                                        recover_ended_instance_for_late_frame_end(
                                            cur,
                                            session_id,
                                            farm_id,
                                            activity_date,
                                            zone_id,
                                            activity_type_id,
                                            schedule_id,
                                            event_time,
                                            max_hist_start
                                            if replay_start
                                            else recovery_sec_start,
                                            temporal_limit_sec=max_hist_start
                                            if replay_start
                                            else None,
                                            relaxed_select=replay_start,
                                        )
                                    )
                                if recovered_start:
                                    instance_id = recovered_start
                                    session_map[session_id] = (instance_id, time.time())
                                    print(
                                        f"[START_ENDED_RECOVERY] session={session_id} "
                                        f"→ instance_id={instance_id}"
                                    )
                                    queue_event_link(
                                        cur,
                                        e["event_row_id"],
                                        instance_id,
                                        event_links,
                                        schedule_id=schedule_id,
                                        activity_date=activity_date,
                                    )
                                    continue

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
                                      AND (
                                            (
                                              %s IS NOT NULL
                                              AND (
                                                    activity_schedule_id = %s
                                                    OR activity_schedule_id IS NULL
                                                  )
                                            )
                                            OR (
                                              %s IS NULL
                                              AND (
                                                    activity_schedule_id IS NOT NULL
                                                    OR activity_schedule_id IS NULL
                                                  )
                                            )
                                          )
                                      AND status = 'IN_PROGRESS'
                                      AND actual_end_at IS NULL
                                      AND actual_start_at IS NOT NULL
                                      AND ABS(EXTRACT(EPOCH FROM (actual_start_at - %s))) <= %s
                                    ORDER BY
                                      CASE
                                        WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
                                        WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
                                        WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
                                        WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
                                        ELSE 2
                                      END,
                                      ABS(EXTRACT(EPOCH FROM (actual_start_at - %s))),
                                      updated_at DESC
                                    LIMIT 1
                                    """,
                                    (
                                        farm_id,
                                        zone_id,
                                        zone_id,
                                        activity_type_id,
                                        activity_date,
                                        schedule_id,
                                        schedule_id,
                                        schedule_id,
                                        event_time,
                                        SOFT_DEDUPE_WINDOW_SEC,
                                        schedule_id,
                                        schedule_id,
                                        schedule_id,
                                        schedule_id,
                                        schedule_id,
                                        event_time,
                                    ),
                                )
                                soft_dupe = cur.fetchone()
                                if soft_dupe:
                                    instance_id = soft_dupe["id"]
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
                                        f"[DEBUG] Soft-dedupe reused active instance "
                                        f"→ instance_id={instance_id}"
                                    )
                                    queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                    continue

                                within_ideal_window = False
                                if schedule_id is not None:
                                    if not matched_schedule:
                                        print(
                                            f"[WARN] activity_schedule {schedule_id} missing in cache "
                                            f"→ drop schedule binding for event {e['event_row_id']}"
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

                                if schedule_id is not None:
                                    cur.execute(
                                        """
                                        SELECT id
                                        FROM activity_instance
                                        WHERE farm_id = %s
                                          AND activity_schedule_id = %s
                                          AND activity_date = %s
                                          AND status = 'ENDED'
                                          AND session_classification = 'MISSED'
                                        LIMIT 1
                                        """,
                                        (farm_id, schedule_id, activity_date),
                                    )
                                    missed_slot = cur.fetchone()
                                    if missed_slot:
                                        reopened_id = reopen_missed_activity_instance(
                                            cur,
                                            missed_slot["id"],
                                            zone_id,
                                            session_id,
                                            event_time,
                                            within_ideal_window,
                                        )
                                        if reopened_id:
                                            instance_id = reopened_id
                                            session_map[session_id] = (instance_id, time.time())
                                            print(
                                                f"[DEBUG] Reopened MISSED slot as AI session "
                                                f"→ instance_id={instance_id} schedule_id={schedule_id}"
                                            )
                                            queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                            continue

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
                                            status,
                                            actual_start_at,
                                            last_seen_at,
                                            source,
                                            within_ideal_window,
                                            created_at,
                                            updated_at
                                        )
                                        VALUES (%s,%s,%s,%s,%s,
                                                %s,
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
                                        f"schedule_id={schedule_id}"
                                    )
                                except pg_errors.UniqueViolation as ex:
                                    cur.execute("ROLLBACK TO SAVEPOINT ai_insert_sp")
                                    cur.execute("RELEASE SAVEPOINT ai_insert_sp")
                                    print(
                                        f"[RECOVERY] uniq_active_instance hit "
                                        f"session={session_id}: {ex}"
                                    )

                                    replay_uv = is_historical_replay_event(
                                        event_time, activity_type_id
                                    )
                                    existing_uv_id = None
                                    if not replay_uv:
                                        existing_uv_id = find_in_progress_bucket_attach(
                                            cur,
                                            farm_id,
                                            zone_id,
                                            activity_type_id,
                                            activity_date,
                                            schedule_id,
                                            event_time,
                                            event_row_id=e["event_row_id"],
                                        )
                                    if not existing_uv_id:
                                        hist_uv = recover_historical_instance_attach(
                                            cur,
                                            farm_id,
                                            zone_id,
                                            activity_type_id,
                                            activity_date,
                                            schedule_id,
                                            event_time,
                                            event_row_id=e["event_row_id"],
                                        )
                                        if hist_uv:
                                            existing_uv_id = _attach_from_historical_row(
                                                cur,
                                                hist_uv,
                                                session_id,
                                                farm_id,
                                                activity_date,
                                                zone_id,
                                                activity_type_id,
                                                schedule_id,
                                                event_time,
                                            )
                                    if existing_uv_id:
                                        instance_id = existing_uv_id
                                        print(
                                            f"[RECOVERY_ATTACH] session={session_id} "
                                            f"→ instance_id={instance_id}"
                                        )
                                        cur.execute(
                                            """
                                            UPDATE activity_instance
                                            SET actual_start_at = LEAST(COALESCE(actual_start_at, %s), %s),
                                                last_seen_at = GREATEST(COALESCE(last_seen_at, %s), %s),
                                                actual_end_at = CASE
                                                    WHEN actual_end_at IS NOT NULL THEN
                                                        GREATEST(actual_end_at, %s::timestamptz)
                                                    ELSE actual_end_at
                                                END,
                                                zone_id = COALESCE(zone_id, %s),
                                                session_id = COALESCE(session_id, %s),
                                                activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                                activity_date = COALESCE(activity_date, %s),
                                                updated_at = %s
                                            WHERE id = %s
                                            """,
                                            (
                                                event_time,
                                                event_time,
                                                event_time,
                                                event_time,
                                                event_time,
                                                zone_id,
                                                session_id,
                                                schedule_id,
                                                activity_date,
                                                utc_now(),
                                                instance_id,
                                            ),
                                        )
                                        session_map[session_id] = (instance_id, time.time())
                                        queue_event_link(
                                            cur,
                                            e["event_row_id"],
                                            instance_id,
                                            event_links,
                                            schedule_id=schedule_id,
                                            activity_date=activity_date,
                                        )
                                        continue

                                    print(f"[WARN] UniqueViolation — no bucket IN_PROGRESS row: {ex}")

                                    cur.execute(
                                        """
                                        SELECT id, status, session_classification, source, activity_schedule_id
                                        FROM activity_instance
                                        WHERE farm_id = %s
                                          AND (zone_id = %s OR (zone_id IS NULL AND %s IS NULL))
                                          AND activity_type_id = %s
                                          AND activity_date = %s
                                          AND actual_start_at IS NOT NULL
                                          AND ABS(
                                                EXTRACT(EPOCH FROM (actual_start_at - %s))
                                              ) < 600
                                        ORDER BY
                                          CASE
                                            WHEN %s IS NOT NULL AND activity_schedule_id = %s THEN 0
                                            WHEN %s IS NULL AND activity_schedule_id IS NULL THEN 0
                                            WHEN %s IS NULL AND activity_schedule_id IS NOT NULL THEN 1
                                            WHEN %s IS NOT NULL AND activity_schedule_id IS NULL THEN 1
                                            ELSE 2
                                          END,
                                          ABS(EXTRACT(EPOCH FROM (actual_start_at - %s))),
                                          created_at DESC
                                        LIMIT 1
                                        """,
                                        (
                                            farm_id,
                                            zone_id,
                                            zone_id,
                                            activity_type_id,
                                            activity_date,
                                            event_time,
                                            schedule_id,
                                            schedule_id,
                                            schedule_id,
                                            schedule_id,
                                            schedule_id,
                                            event_time,
                                        ),
                                    )
                                    existing = cur.fetchone()

                                    if existing and schedule_id is not None:
                                        es = existing.get("activity_schedule_id")
                                        if es is not None and es != schedule_id:
                                            log_attach_reject(
                                                "uniq_recovery_schedule_rank",
                                                event_row_id=e["event_row_id"],
                                                event_schedule=schedule_id,
                                                instance_schedule=es,
                                            )

                                    if not existing:
                                        instance_id = resolve_fallback_instance_or_skip(
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
                                            event_columns,
                                            "START UniqueViolation recovery miss",
                                        )
                                        if instance_id is None:
                                            continue
                                        session_map[session_id] = (instance_id, time.time())
                                        queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                        continue
                                    instance_id = existing["id"]
                                    existing_status = existing["status"]
                                    existing_classification = existing.get("session_classification")

                                    if existing_status == "IN_PROGRESS":
                                        cur.execute(
                                            """
                                            SELECT last_seen_at
                                            FROM activity_instance
                                            WHERE id = %s
                                            """,
                                            (instance_id,),
                                        )
                                        ls_row = cur.fetchone()
                                        if continuity_gap_exceeded(
                                            activity_type_id,
                                            event_time,
                                            ls_row["last_seen_at"] if ls_row else None,
                                        ):
                                            log_attach_reject(
                                                "merge_gap_exceeded",
                                                event_row_id=e["event_row_id"],
                                                instance_id=instance_id,
                                                merge_gap_sec=merge_gap,
                                            )
                                            instance_id = resolve_fallback_instance_or_skip(
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
                                                event_columns,
                                                "START duplicate guard gap exceeded",
                                            )
                                            if instance_id is None:
                                                continue
                                        else:
                                            cur.execute(
                                                """
                                                UPDATE activity_instance
                                                SET actual_start_at = LEAST(COALESCE(actual_start_at, %s), %s),
                                                    last_seen_at = GREATEST(COALESCE(last_seen_at, %s), %s),
                                                    zone_id = COALESCE(zone_id, %s),
                                                    activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                                    activity_date = COALESCE(activity_date, %s),
                                                    updated_at = %s
                                                WHERE id = %s
                                                """,
                                                (
                                                    event_time,
                                                    event_time,
                                                    event_time,
                                                    event_time,
                                                    zone_id,
                                                    schedule_id,
                                                    activity_date,
                                                    utc_now(),
                                                    instance_id,
                                                ),
                                            )
                                            print(
                                                f"[DEBUG] REUSED-EXISTING IN_PROGRESS (duplicate guard) "
                                                f"→ instance_id={instance_id}"
                                            )
                                    elif (
                                        existing_status == "ENDED"
                                        and existing_classification == "MISSED"
                                    ):
                                        reopened_id = reopen_missed_activity_instance(
                                            cur,
                                            existing["id"],
                                            zone_id,
                                            session_id,
                                            event_time,
                                            within_ideal_window,
                                        )
                                        if reopened_id:
                                            instance_id = reopened_id
                                            print(
                                                f"[DEBUG] UniqueViolation → reopened MISSED row "
                                                f"→ instance_id={instance_id}"
                                            )
                                        else:
                                            instance_id = resolve_fallback_instance_or_skip(
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
                                                event_columns,
                                                "START MISSED reopen raced",
                                            )
                                            if instance_id is None:
                                                continue
                                            session_map[session_id] = (instance_id, time.time())
                                            queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)
                                            continue
                                    else:
                                        # --------------------------------------------------
                                        # REPLAY RECOVERY FOR FINALIZED ENDED ROWS
                                        # --------------------------------------------------
                                        # Delayed replay events may arrive after instance already
                                        # finalized as ENDED.
                                        #
                                        # In this case:
                                        # - DO NOT orphan
                                        # - DO NOT create new instance
                                        # - Reuse finalized ENDED instance
                                        # - Reopen into IN_PROGRESS so FRAME/END can continue stitching
                                        # --------------------------------------------------

                                        if existing_status == "ENDED":
                                            cur.execute(
                                                """
                                                SELECT actual_end_at, last_seen_at
                                                FROM activity_instance
                                                WHERE id = %s
                                                """,
                                                (existing["id"],),
                                            )
                                            ended_row = cur.fetchone()
                                            ended_anchor = None
                                            if ended_row:
                                                ended_anchor = (
                                                    ended_row.get("actual_end_at")
                                                    or ended_row.get("last_seen_at")
                                                )
                                            if continuity_gap_exceeded(
                                                activity_type_id,
                                                event_time,
                                                ended_anchor,
                                            ):
                                                log_attach_reject(
                                                    "merge_gap_exceeded",
                                                    event_row_id=e["event_row_id"],
                                                    instance_id=existing["id"],
                                                    merge_gap_sec=merge_gap,
                                                )
                                                instance_id = resolve_fallback_instance_or_skip(
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
                                                    event_columns,
                                                    "START ENDED replay gap exceeded",
                                                )
                                                if instance_id is None:
                                                    continue
                                            else:
                                                instance_id = existing["id"]

                                                cur.execute(
                                                    """
                                                    UPDATE activity_instance
                                                    SET status = 'IN_PROGRESS',
                                                        actual_end_at = NULL,
                                                        actual_duration_sec = NULL,
                                                        last_seen_at = %s,
                                                        session_classification = NULL,
                                                        started_offset_min = NULL,
                                                        ended_offset_min = NULL,
                                                        activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                                        activity_date = COALESCE(activity_date, %s),
                                                        updated_at = %s
                                                    WHERE id = %s
                                                    """,
                                                    (
                                                        event_time,
                                                        schedule_id,
                                                        activity_date,
                                                        utc_now(),
                                                        instance_id,
                                                    ),
                                                )

                                                print(
                                                    f"[REPLAY_RECOVERY] reopened ENDED instance "
                                                    f"→ instance_id={instance_id}"
                                                )

                                            session_map[session_id] = (instance_id, time.time())

                                            queue_event_link(
                                                cur,
                                                e["event_row_id"],
                                                instance_id,
                                                event_links,
                                                schedule_id=schedule_id,
                                                activity_date=activity_date,
                                            )

                                            continue

                                        # --------------------------------------------------
                                        # FINALIZED NON-RECOVERABLE ROW
                                        # --------------------------------------------------

                                        instance_id = resolve_fallback_instance_or_skip(
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
                                            event_columns,
                                            "START finalized unrecoverable",
                                        )

                                        if instance_id is None:
                                            continue

                                        session_map[session_id] = (instance_id, time.time())

                                        queue_event_link(
                                            cur,
                                            e["event_row_id"],
                                            instance_id,
                                            event_links,
                                            schedule_id=schedule_id,
                                            activity_date=activity_date,
                                        )

                                        continue

                        if instance_id is None:
                            instance_id = resolve_fallback_instance_or_skip(
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
                                event_columns,
                                "START path exhausted",
                            )
                            if instance_id is None:
                                continue

                        # Map this session to the instance
                        session_map[session_id] = (instance_id, time.time())
                        print(f"[DEBUG] MAPPED session {session_id} → instance_id={instance_id}")

                        queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)

                    # -------------------------------------------------
                    # FRAME / END
                    # -------------------------------------------------
                    elif etype in ("FRAME_AGGREGATE", "END_CANDIDATE"):
                        instance_id = None
                        cache_row = session_map.get(session_id)
                        if cache_row:
                            instance_id = cache_row[0]

                        if instance_id is None:
                            fe_age_sec = (utc_now() - event_time).total_seconds()
                            resolved_fe = resolve_attachable_instance(
                                cur,
                                farm_id,
                                zone_id,
                                activity_type_id,
                                activity_date,
                                schedule_id,
                                event_time,
                                session_id,
                                try_session_restore=True,
                                try_ended_recovery_sec=ended_recovery_window_sec(
                                    activity_type_id
                                ),
                                event_row_id=e["event_row_id"],
                                event_age_sec=fe_age_sec,
                            )
                            if resolved_fe:
                                instance_id = resolved_fe
                                session_map[session_id] = (instance_id, time.time())
                                print(
                                    f"[DEBUG] FRAME/END resolve_attach "
                                    f"session={session_id} → instance_id={instance_id}"
                                )

                        if instance_id is None:
                            instance_id = resolve_fallback_instance_or_skip(
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
                                event_columns,
                                f"{etype} attach exhausted",
                            )
                            if instance_id:
                                session_map[session_id] = (instance_id, time.time())
                                print(
                                    f"[DEBUG] FRAME/END fallback instance "
                                    f"session={session_id} → instance_id={instance_id}"
                                )
                            else:
                                print(
                                    f"[ORPHAN_EVENT] "
                                    f"event_id={e['event_row_id']} "
                                    f"event_type={etype}"
                                )
                                continue

                        cur.execute(
                            """
                            SELECT
                                id,
                                farm_id,
                                activity_type_id,
                                activity_date,
                                status
                            FROM activity_instance
                            WHERE id = %s
                            """,
                            (instance_id,),
                        )
                        inst = cur.fetchone()
                        if not inst:
                            session_map.pop(session_id, None)
                            continue
                        if inst["status"] == "IN_PROGRESS":
                            pass
                        elif inst["status"] in FINALIZED_ATTACH_STATUSES:
                            print(
                                f"[FRAME_END_FINALIZED] updating instance_id={instance_id} "
                                f"status={inst['status']} event_row_id={e['event_row_id']}"
                            )
                        else:
                            reopened = resolve_attachable_instance(
                                cur,
                                farm_id,
                                zone_id,
                                activity_type_id,
                                activity_date,
                                schedule_id,
                                event_time,
                                session_id,
                                try_session_restore=False,
                                try_ended_recovery_sec=ended_recovery_window_sec(
                                    activity_type_id
                                ),
                                event_row_id=e["event_row_id"],
                                event_age_sec=(utc_now() - event_time).total_seconds(),
                            )
                            if reopened:
                                instance_id = reopened
                                session_map[session_id] = (instance_id, time.time())
                                cur.execute(
                                    """
                                    SELECT
                                        id,
                                        farm_id,
                                        activity_type_id,
                                        activity_date,
                                        status
                                    FROM activity_instance
                                    WHERE id = %s
                                    """,
                                    (instance_id,),
                                )
                                inst = cur.fetchone()
                            if not inst or (
                                inst["status"] != "IN_PROGRESS"
                                and inst["status"] not in FINALIZED_ATTACH_STATUSES
                            ):
                                session_map.pop(session_id, None)
                                continue
                        if inst["activity_date"] != activity_date:
                            session_map.pop(session_id, None)
                            continue
                        if inst["farm_id"] != farm_id:
                            session_map.pop(session_id, None)
                            continue
                        if inst["activity_type_id"] != activity_type_id:
                            session_map.pop(session_id, None)
                            continue

                        session_map[session_id] = (instance_id, time.time())

                        finalized_extend = inst["status"] in FINALIZED_ATTACH_STATUSES
                        cls_reset_sql = (
                            """
                                    , session_classification = NULL
                                    , started_offset_min = NULL
                                    , ended_offset_min = NULL"""
                            if finalized_extend
                            else ""
                        )

                        if schedule_id is not None:
                            cur.execute(
                                f"""
                                UPDATE activity_instance
                                SET last_seen_at = %s,
                                    updated_at = %s,
                                    activity_schedule_id = COALESCE(activity_schedule_id, %s),
                                    activity_date = COALESCE(activity_date, %s),
                                    actual_end_at = CASE
                                        WHEN actual_end_at IS NOT NULL THEN
                                            GREATEST(actual_end_at, %s::timestamptz)
                                        ELSE actual_end_at
                                    END,
                                    actual_duration_sec = CASE
                                        WHEN actual_start_at IS NOT NULL THEN
                                            CAST(
                                                EXTRACT(
                                                    EPOCH FROM (
                                                        GREATEST(actual_end_at, %s::timestamptz)
                                                        - actual_start_at
                                                    )
                                                ) AS INTEGER
                                            )
                                        ELSE actual_duration_sec
                                    END
                                    {cls_reset_sql}
                                WHERE id = %s
                                """,
                                (
                                    event_time,
                                    utc_now(),
                                    schedule_id,
                                    activity_date,
                                    event_time,
                                    event_time,
                                    instance_id,
                                ),
                            )
                        else:
                            cur.execute(
                                f"""
                                UPDATE activity_instance
                                SET last_seen_at = %s,
                                    updated_at = %s,
                                    activity_date = COALESCE(activity_date, %s),
                                    actual_end_at = CASE
                                        WHEN actual_end_at IS NOT NULL THEN
                                            GREATEST(actual_end_at, %s::timestamptz)
                                        ELSE actual_end_at
                                    END,
                                    actual_duration_sec = CASE
                                        WHEN actual_start_at IS NOT NULL THEN
                                            CAST(
                                                EXTRACT(
                                                    EPOCH FROM (
                                                        GREATEST(actual_end_at, %s::timestamptz)
                                                        - actual_start_at
                                                    )
                                                ) AS INTEGER
                                            )
                                        ELSE actual_duration_sec
                                    END
                                    {cls_reset_sql}
                                WHERE id = %s
                                """,
                                (
                                    event_time,
                                    utc_now(),
                                    activity_date,
                                    event_time,
                                    event_time,
                                    instance_id,
                                ),
                            )

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
                            print(
                                f"[ORPHAN_EVENT] "
                                f"event_id={e['event_row_id']} "
                                f"event_type={etype}"
                            )
                            continue
                        actual_start_at = row["actual_start_at"]

                        if etype == "END_CANDIDATE":
                            if actual_start_at is None:
                                print(
                                    "[WARN] END_CANDIDATE but no start -> fallback update "
                                    f"instance_id={instance_id}"
                                )
                            else:
                                duration_sec = int(
                                    (event_time - actual_start_at).total_seconds()
                                )
                                print(
                                    "[DEBUG] END_CANDIDATE -> deferred close (stitch mode) "
                                    f"instance_id={instance_id} duration={duration_sec}s"
                                )

                        queue_event_link(cur, e["event_row_id"], instance_id, event_links, schedule_id=schedule_id, activity_date=activity_date)

                bulk_link_events(cur, event_links)
                normalize_null_instance_statuses(cur)
                cur.connection.commit()

            cleanup_stale_instances(cur, session_map)
            normalize_null_instance_statuses(cur)
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
