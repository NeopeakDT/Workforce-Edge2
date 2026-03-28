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
- Only instances with status = IN_PROGRESS
- Uses FARM LOCAL TIMEZONE
- Fully idempotent
"""

from datetime import datetime, timedelta, timezone
from pathlib import Path
import sys
import pytz

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now


# --------------------------------------------------
# CONFIG
# --------------------------------------------------

MAX_ACTIVITY_DURATION_SEC = 3 * 60 * 60  # 3 hours safety cap


def resolve():
    now_utc = utc_now()

    with get_cursor() as cur:
        # --------------------------------------------------
        # Pick ended, unresolved AI instances
        # --------------------------------------------------
        # 🔥 CRITICAL FIX: 
        # 1. Removed "activity_schedule_id IS NULL" condition
        #    (Aggregator now sets activity_schedule_id at instance creation)
        # 2. Added reprocessing guard "started_offset_min IS NULL"
        #    (Prevent recomputing already resolved instances)
        # --------------------------------------------------
        cur.execute(
            """
            SELECT
                ai.id,
                ai.farm_id,
                ai.activity_type_id,
                ai.activity_schedule_id,
                ai.actual_start_at,
                ai.actual_end_at,
                ai.activity_date,
                f.timezone
            FROM activity_instance ai
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.actual_end_at IS NOT NULL
              AND ai.status = 'IN_PROGRESS'
              AND ai.source = 'AI'
              AND ai.started_offset_min IS NULL
            """
        )

        instances = cur.fetchall()

        for ai in instances:

            # --------------------------------------------------
            # Duration sanity check
            # --------------------------------------------------
            duration_sec = int(
                (ai["actual_end_at"] - ai["actual_start_at"]).total_seconds()
            )

            if duration_sec > MAX_ACTIVITY_DURATION_SEC:
                # Mark as LATE without schedule binding (anomaly case)
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

            # Local reference only for picking correct activity_date
            start_local = actual_start_utc.astimezone(farm_tz)
            activity_date = start_local.date()

            # --------------------------------------------------
            # CRITICAL: Use existing activity_schedule_id
            # (set by aggregator, not recomputed here)
            # --------------------------------------------------
            schedule_id = ai["activity_schedule_id"]
            
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
            # Build ideal window in FARM LOCAL TIME
            ideal_start_naive = datetime.combine(
                activity_date, s["ideal_start_time"]
            )
            ideal_end_naive = datetime.combine(
                activity_date, s["ideal_end_time"]
            )

            ideal_start_local = farm_tz.localize(ideal_start_naive)
            ideal_end_local = farm_tz.localize(ideal_end_naive)

            # Cross-midnight handling
            if ideal_end_local <= ideal_start_local:
                ideal_end_local += timedelta(days=1)

            # Convert ideal window to UTC
            ideal_start_utc = ideal_start_local.astimezone(timezone.utc)
            ideal_end_utc = ideal_end_local.astimezone(timezone.utc)

            # Compute offsets in UTC (authoritative)
            started_offset = int(
                (actual_start_utc - ideal_start_utc).total_seconds() / 60
            )
            ended_offset = int(
                (actual_end_utc - ideal_end_utc).total_seconds() / 60
            )

            # --------------------------------------------------
            # Final Status Resolution
            # --------------------------------------------------
            if started_offset < -s["tolerance_early_min"]:
                final_status = "EARLY"
            elif started_offset > s["tolerance_late_min"]:
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
                    status = %s,
                    updated_at = %s
                WHERE id = %s
                """,
                (
                    started_offset,
                    ended_offset,
                    final_status,
                    now_utc,
                    ai["id"],
                ),
            )

        cur.connection.commit()


if __name__ == "__main__":
    resolve()
