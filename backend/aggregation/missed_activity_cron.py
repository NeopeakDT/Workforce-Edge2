"""
MISSED ACTIVITY DETECTOR

Creates MISSED activity instances for scheduled activities that never produced a START event.

This is NOT inference — it is truth enforcement. It detects scheduled activities that never started
by comparing activity_schedule records with activity_instance records.

Key Features:
    - Detects scheduled activities that never started
    - Creates MISSED activity instances for truth enforcement
    - Uses grace period (10 minutes) after schedule end before marking as missed
    - Idempotent: Safe to run multiple times

Architecture:
    - Reads activity_schedule table for past schedules
    - Checks if corresponding activity_instance exists
    - Creates MISSED status activity_instance if no match found
    - Runs as background cron job (called from main.py)

Process Flow:
    1. Find schedules whose end_time has passed (with grace period)
    2. Check if any activity_instance exists for that schedule window
    3. If no instance found, create MISSED activity_instance
    4. Uses ON CONFLICT DO NOTHING for idempotency
"""

from datetime import timedelta

from common.db import get_cursor
from common.time_utils import utc_now

MISSED_GRACE_MIN = 10  # minutes after schedule end


def detect_missed():
    """
    Detect and create MISSED activity instances for scheduled activities that never started.
    
    Process:
    1. Find schedules that ended more than MISSED_GRACE_MIN minutes ago
    2. Check if any activity_instance exists within the schedule window
    3. If no instance found, create a MISSED activity_instance
    4. Uses ON CONFLICT DO NOTHING for idempotency
    
    This function is idempotent and can be called repeatedly safely.
    """
    now = utc_now()
    cutoff = now - timedelta(minutes=MISSED_GRACE_MIN)
    
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT s.farm_id, s.activity_type, s.start_time, s.end_time
            FROM activity_schedule s
            WHERE s.end_time < %s
              AND NOT EXISTS (
                SELECT 1
                FROM activity_instance i
                WHERE i.farm_id = s.farm_id
                  AND i.activity_type = s.activity_type
                  AND i.started_at BETWEEN s.start_time AND s.end_time
              )
            """,
            (cutoff,),
        )
        missed = cur.fetchall()
        
        for farm_id, activity_type, start_time, end_time in missed:
            cur.execute(
                """
                INSERT INTO activity_instance (
                    farm_id,
                    activity_type,
                    status,
                    started_at,
                    ended_at
                )
                VALUES (%s, %s, 'MISSED', %s, %s)
                ON CONFLICT DO NOTHING
                """,
                (farm_id, activity_type, start_time, end_time),
            )


async def run_missed_activity_check():
    """
    Async wrapper for detect_missed() to be used as a background task.
    
    This function is called periodically from main.py's lifespan context manager.
    It runs detect_missed() in a loop with a delay between checks.
    
    Usage:
        Called from main.py as a background task
    """
    import asyncio
    
    while True:
        try:
            detect_missed()
            # Run every 5 minutes
            await asyncio.sleep(300)
        except Exception as e:
            # Log error but continue running
            print(f"Error in missed activity check: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retry


if __name__ == "__main__":
    detect_missed()
