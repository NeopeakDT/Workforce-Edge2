#!/usr/bin/env python3
"""
Edge2 device
backend/aggregation/activity_schedule_resolver.py
STEP-5A — ACTIVITY SCHEDULE RESOLVER (AUTHORITATIVE)

Responsibilities:
- Bind activity_instance → activity_schedule
- Compute timing offsets
- Finalize session_classification: EARLY / ON_TIME / LATE (overlap of actual interval vs ideal window)

Selection:
- Primary: stable ENDED AI rows (last_seen / updated_at before stable cutoff) with min duration.
- Repair: any AI row with schedule + ended interval + min duration where session_classification or
  offset columns are still NULL (historical replay / partial writes), without requiring the stable
  cutoff on those rows.
- Refresh: rows touched within `AGG_PHASE5_CLASSIFICATION_REFRESH_DAYS` (default 0; set to 1 if replay
  extension reclassification is needed) so replay-extended intervals are reclassified even when offsets
  were previously populated.
- Lookback: only instances with `actual_end_at` within `AGG_PHASE5_RESOLVER_LOOKBACK_DAYS` (default 1)
  are scanned, so historical rows are not revisited indefinitely.

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

MAX_ACTIVITY_DURATION_SEC = int(os.getenv("AGG_MAX_ACTIVITY_DURATION_SEC", str(120 * 60)))
MIN_ON_TIME_OVERLAP_SEC = int(os.getenv("AGG_MIN_ON_TIME_OVERLAP_SEC", "60"))
STABLE_END_DELAY_SEC = int(
    os.getenv(
        "AGG_STABLE_END_DELAY_SEC",
        os.getenv("AGG_CLOSE_DELAY_SEC", os.getenv("AGG_END_GAP_SEC", "900")),
    )
)
# Re-resolve session_classification when row was touched recently (replay extends finalized rows).
# Default 0 = disabled; set AGG_PHASE5_CLASSIFICATION_REFRESH_DAYS=1 if replay extension is needed.
CLASSIFICATION_REFRESH_DAYS = int(os.getenv("AGG_PHASE5_CLASSIFICATION_REFRESH_DAYS", "0"))
# Only resolve instances ended within this many days (avoids indefinite historical rescans).
RESOLVER_LOOKBACK_DAYS = int(os.getenv("AGG_PHASE5_RESOLVER_LOOKBACK_DAYS", "1"))


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
    classification_refresh_cutoff_utc = now_utc - timedelta(days=CLASSIFICATION_REFRESH_DAYS)
    resolver_lookback_cutoff_utc = now_utc - timedelta(days=RESOLVER_LOOKBACK_DAYS)

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
              AND ai.actual_end_at >= %s
              AND ai.activity_schedule_id IS NOT NULL
              AND (
                    (ai.activity_type_id = 1 AND ai.actual_duration_sec >= 300)
                 OR (ai.activity_type_id = 2 AND ai.actual_duration_sec >= 60)
                 OR (ai.activity_type_id = 3 AND ai.actual_duration_sec >= 30)
              )
              AND ai.source = 'AI'
              AND (
                    (
                      ai.status = 'ENDED'
                      AND ai.last_seen_at IS NOT NULL
                      AND ai.last_seen_at < %s
                      AND ai.updated_at < %s
                    )
                    OR
                    (
                      (
                        ai.session_classification IS NULL
                        OR ai.started_offset_min IS NULL
                        OR ai.ended_offset_min IS NULL
                      )
                      AND ai.status = 'ENDED'
                    )
                    OR
                    (
                      ai.updated_at >= %s
                      AND ai.status = 'ENDED'
                    )
                  )
            """,
            (
                resolver_lookback_cutoff_utc,
                stable_end_cutoff_utc,
                stable_end_cutoff_utc,
                classification_refresh_cutoff_utc,
            ),
        )

        instances = cur.fetchall()

        for ai in instances:
            span_sec = int(
                (ai["actual_end_at"] - ai["actual_start_at"]).total_seconds()
            )

            if span_sec <= 0:
                continue

            if span_sec > MAX_ACTIVITY_DURATION_SEC:
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
                continue
            
            cur.execute(
                """
                SELECT
                    s.id,
                    s.ideal_start_time,
                    s.ideal_end_time
                FROM activity_schedule s
                WHERE s.id = %s
                """,
                (schedule_id,)
            )
            
            schedule_row = cur.fetchone()
            if not schedule_row:
                # Schedule deleted? Preserve as unscheduled classification.
                cur.execute(
                    """
                    UPDATE activity_instance
                    SET session_classification = 'UNSCHEDULED',
                        updated_at = %s
                    WHERE id = %s
                      AND session_classification IS DISTINCT FROM 'UNSCHEDULED'
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
            # OVERLAP-BASED STATUS
            # --------------------------------------------------
            duration_sec = max(
                1,
                int((actual_end_utc - actual_start_utc).total_seconds()),
            )

            overlap_start = max(actual_start_utc, ideal_start_utc)
            overlap_end = min(actual_end_utc, ideal_end_utc)

            overlap_sec = max(
                0,
                int((overlap_end - overlap_start).total_seconds()),
            )

            overlap_ratio = overlap_sec / duration_sec

            print(
                f"[OVERLAP_DEBUG] instance={ai['id']} "
                f"duration={duration_sec}s overlap={overlap_sec}s ratio={overlap_ratio:.2f}"
            )

            if overlap_sec >= MIN_ON_TIME_OVERLAP_SEC:
                final_status = "ON_TIME"
            elif actual_end_utc <= ideal_start_utc:
                final_status = "EARLY"
            elif actual_start_utc >= ideal_end_utc:
                final_status = "LATE"
            else:
                final_status = "ON_TIME"

            # --------------------------------------------------
            # Persist resolution
            # --------------------------------------------------
            cur.execute(
                """
                UPDATE activity_instance
                SET started_offset_min = %s,
                    ended_offset_min = %s,
                    session_classification = %s,
                    within_ideal_window = %s,
                    updated_at = %s
                WHERE id = %s
                  AND (
                    started_offset_min IS DISTINCT FROM %s
                    OR ended_offset_min IS DISTINCT FROM %s
                    OR session_classification IS DISTINCT FROM %s
                    OR within_ideal_window IS DISTINCT FROM %s
                  )
                """,
                (
                    started_offset,
                    ended_offset,
                    final_status,
                    within_ideal_window,
                    now_utc,
                    ai["id"],
                    started_offset,
                    ended_offset,
                    final_status,
                    within_ideal_window,
                ),
            )

        # --------------------------------------------------
        # Finalize ended rows with no schedule binding → UNSCHEDULED classification
        # --------------------------------------------------
        cur.execute(
            """
            UPDATE activity_instance
            SET session_classification = 'UNSCHEDULED',
                updated_at = %s
            WHERE actual_end_at IS NOT NULL
              AND actual_end_at >= %s
              AND activity_schedule_id IS NULL
              AND status = 'ENDED'
              AND source = 'AI'
              AND last_seen_at IS NOT NULL
              AND last_seen_at < %s
              AND updated_at < %s
              AND session_classification IS DISTINCT FROM 'UNSCHEDULED'
            """,
            (
                now_utc,
                resolver_lookback_cutoff_utc,
                stable_end_cutoff_utc,
                stable_end_cutoff_utc,
            ),
        )

        cur.connection.commit()


if __name__ == "__main__":
    resolve()
