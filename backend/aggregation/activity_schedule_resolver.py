#!/usr/bin/env python3
"""
backend/aggregation/activity_schedule_resolver.py
STEP-5A — ACTIVITY SCHEDULE RESOLVER (AUTHORITATIVE)

Responsibilities:
- Bind activity_instance → activity_schedule
- Compute timing offsets
- Finalize status: EARLY / ON_TIME / LATE

Rules:
- Only instances with actual_end_at IS NOT NULL
- Includes ended rows waiting for schedule resolution
- Uses FARM LOCAL TIMEZONE
- Fully idempotent
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import os
import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

MAX_ACTIVITY_DURATION_SEC = int(os.getenv("AGG_MAX_ACTIVITY_DURATION_SEC", str(90 * 60)))
MIN_ACTIVITY_DURATION_SEC = int(os.getenv("AGG_MIN_ACTIVITY_DURATION_SEC", "60"))
ON_TIME_BUFFER_MIN = int(os.getenv("AGG_ON_TIME_BUFFER_MIN", "10"))
STABLE_END_DELAY_SEC = int(
    os.getenv("AGG_STABLE_END_DELAY_SEC", os.getenv("AGG_CLOSE_DELAY_SEC", "60"))
)


def ideal_window_utc_bounds(farm_tz, activity_date, ideal_start_time, ideal_end_time):
    """
    Build the strict ideal window for activity_date in farm local time, return UTC bounds.

    Used by the resolver and aggregator so within_ideal_window matches:
    ideal_start_utc <= actual_start_at <= ideal_end_utc
    """
    ideal_start_naive = datetime.combine(activity_date, ideal_start_time)
    ideal_end_naive = datetime.combine(activity_date, ideal_end_time)

    ideal_start_local = farm_tz.localize(ideal_start_naive)
    ideal_end_local = farm_tz.localize(ideal_end_naive)

    if ideal_end_local <= ideal_start_local:
        ideal_end_local += timedelta(days=1)

    ideal_start_utc = ideal_start_local.astimezone(timezone.utc)
    ideal_end_utc = ideal_end_local.astimezone(timezone.utc)
    return ideal_start_utc, ideal_end_utc


def is_actual_start_within_ideal_window(actual_start_utc, ideal_start_utc, ideal_end_utc):
    """Strict ideal window (no tolerance): inclusive on both ends, compared in UTC."""
    actual_utc = actual_start_utc.astimezone(timezone.utc)
    return ideal_start_utc <= actual_utc <= ideal_end_utc


def resolve():
    now_utc = utc_now()
    stable_end_cutoff_utc = now_utc - timedelta(seconds=STABLE_END_DELAY_SEC)

    with get_cursor() as cur:
        # --------------------------------------------------
        # Pick ended, unresolved AI instances
        # --------------------------------------------------
        # We resolve only "stable ended" rows to avoid finalizing too early
        # while late FRAME/END events are still arriving.
        # --------------------------------------------------
        cur.execute(
            """
            SELECT
                ai.id,
                ai.farm_id,
                ai.activity_type_id,
                ai.activity_schedule_id,
                ai.status,
                ai.actual_start_at,
                ai.actual_end_at,
                ai.activity_date,
                f.timezone
            FROM activity_instance ai
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.actual_end_at IS NOT NULL
              AND ai.instance_type = 'SCHEDULED'
              AND ai.status = 'ENDED'
              AND ai.source = 'AI'
              AND ai.last_seen_at IS NOT NULL
              AND ai.last_seen_at < %s
              AND ai.updated_at < %s
            """,
            (stable_end_cutoff_utc, stable_end_cutoff_utc),
        )

        instances = cur.fetchall()

        for ai in instances:
            # Guard: NOISE rows must never be schedule-resolved.
            if ai["status"] == "NOISE":
                continue

            # --------------------------------------------------
            # Duration sanity check
            # --------------------------------------------------
            duration_sec = int(
                (ai["actual_end_at"] - ai["actual_start_at"]).total_seconds()
            )

            if duration_sec > MAX_ACTIVITY_DURATION_SEC:
                # Over-duration rows are treated as anomalies; do not resolve/finalize here.
                continue

            farm_tz = pytz.timezone(ai["timezone"])

            # Convert actual times once (UTC is canonical)
            # Fail fast if naive datetimes detected (indicates ingestion bug)
            if ai["actual_start_at"].tzinfo is None or ai["actual_end_at"].tzinfo is None:
                raise ValueError(
                    f"Naive datetime detected in activity_instance {ai['id']}. "
                    f"This indicates an ingestion bug - all timestamps must be timezone-aware."
                )

            actual_start_utc = ai["actual_start_at"].astimezone(timezone.utc)
            actual_end_utc = ai["actual_end_at"].astimezone(timezone.utc)

            # Use persisted activity_date from STEP-4 to avoid cross-midnight drift.
            activity_date = ai["activity_date"]
            if activity_date is None:
                # Safety fallback for legacy/bad rows.
                start_local = actual_start_utc.astimezone(farm_tz)
                activity_date = start_local.date()

            # --------------------------------------------------
            # CRITICAL: Use existing activity_schedule_id
            # (set by aggregator, not recomputed here)
            # --------------------------------------------------
            schedule_id = ai["activity_schedule_id"]
            if schedule_id is None:
                # UNSCHEDULED rows are intentionally excluded from schedule resolution.
                continue
            
            cur.execute(
                """
                SELECT
                    s.id,
                    s.ideal_start_time,
                    s.ideal_end_time,
                    s.tolerance_early_min,
                    s.tolerance_late_min
                FROM activity_schedule s
                WHERE s.id = %s
                """,
                (schedule_id,)
            )
            
            schedule_row = cur.fetchone()
            if not schedule_row:
                # Schedule deleted? Mark as LATE and skip
                cur.execute(
                    """
                    UPDATE activity_instance
                    SET status = 'LATE',
                        updated_at = %s
                    WHERE id = %s
                    """,
                    (now_utc, ai["id"]),
                )
                continue

            s = schedule_row

            # ------- Compute offsets --------
            ideal_start_utc, ideal_end_utc = ideal_window_utc_bounds(
                farm_tz,
                activity_date,
                s["ideal_start_time"],
                s["ideal_end_time"],
            )

            within_ideal_window = is_actual_start_within_ideal_window(
                actual_start_utc, ideal_start_utc, ideal_end_utc
            )

            # Compute offsets in UTC (authoritative)
            started_offset = int(
                (actual_start_utc - ideal_start_utc).total_seconds() / 60
            )
            ended_offset = int(
                (actual_end_utc - ideal_end_utc).total_seconds() / 60
            )

            # --------------------------------------------------
            # Final Status Resolution (schedule-tolerance model)
            # --------------------------------------------------
            early_tol_min = s["tolerance_early_min"] or 0
            late_tol_min = s["tolerance_late_min"] or 0
            early_limit = ideal_start_utc - timedelta(minutes=early_tol_min)
            late_limit = ideal_start_utc + timedelta(minutes=late_tol_min)
            buffer_start = ideal_start_utc - timedelta(minutes=ON_TIME_BUFFER_MIN)
            buffer_end = ideal_start_utc + timedelta(minutes=ON_TIME_BUFFER_MIN)

            if actual_start_utc < early_limit:
                final_status = "EARLY"
            elif actual_start_utc < buffer_start:
                final_status = "EARLY"
            elif buffer_start <= actual_start_utc <= buffer_end:
                final_status = "ON_TIME"
            elif actual_start_utc <= late_limit:
                final_status = "LATE"
            else:
                final_status = "LATE"

            # --------------------------------------------------
            # Persist resolution
            # --------------------------------------------------
            cur.execute(
                """
                UPDATE activity_instance
                SET started_offset_min = %s,
                    ended_offset_min = %s,
                    status = %s,
                    within_ideal_window = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    started_offset,
                    ended_offset,
                    final_status,
                    within_ideal_window,
                    now_utc,
                    ai["id"],
                ),
            )

        # --------------------------------------------------
        # Finalize valid UNSCHEDULED rows
        # --------------------------------------------------
        # Rows shorter than MIN_ACTIVITY_DURATION_SEC are expected to be tagged as NOISE
        # by STEP-4 and are intentionally excluded here.
        cur.execute(
            """
            UPDATE activity_instance
            SET status = 'UNSCHEDULE',
                updated_at = %s
            WHERE actual_end_at IS NOT NULL
              AND instance_type = 'UNSCHEDULED'
              AND status = 'IN_PROGRESS'
              AND source = 'AI'
              AND actual_duration_sec IS NOT NULL
              AND actual_duration_sec >= %s
              AND last_seen_at IS NOT NULL
              AND last_seen_at < %s
              AND updated_at < %s
            """,
            (
                now_utc,
                MIN_ACTIVITY_DURATION_SEC,
                stable_end_cutoff_utc,
                stable_end_cutoff_utc,
            ),
        )

        cur.connection.commit()


if __name__ == "__main__":
    resolve()
