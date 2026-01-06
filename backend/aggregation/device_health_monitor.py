"""
DEVICE HEALTH MONITOR

Detects offline Jetson devices using heartbeat timestamps.

Responsibilities:
    - Read edge_device.last_seen_at timestamps
    - Identify stale devices (no heartbeat within threshold)
    - Mark device inactive or raise alert

Architecture:
    - Monitors edge_device.last_seen_at field (updated by heartbeat endpoint)
    - Devices with no heartbeat within OFFLINE_THRESHOLD_MIN are marked inactive
    - Prevents inactive devices from sending events
    - Can be extended to raise alerts/notifications

Process Flow:
    1. Find devices with last_seen_at older than threshold
    2. Mark those devices as inactive (is_active = false)
    3. Inactive devices cannot authenticate or send events

Key Features:
    - Automatic device health monitoring
    - Prevents stale devices from sending events
    - Can be extended with alert notifications
    - Idempotent: Safe to run multiple times
"""

from datetime import timedelta

from common.db import get_cursor
from common.time_utils import utc_now

OFFLINE_THRESHOLD_MIN = 10  # minutes without heartbeat


def detect_offline_devices():
    """
    Detect and mark offline devices as inactive.
    
    Process:
    1. Find devices with last_seen_at older than OFFLINE_THRESHOLD_MIN
    2. Mark those devices as inactive (is_active = false)
    3. Inactive devices cannot authenticate or send events
    
    This function is idempotent and can be called repeatedly safely.
    """
    cutoff = utc_now() - timedelta(minutes=OFFLINE_THRESHOLD_MIN)
    
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM edge_device
            WHERE last_seen_at < %s
              AND is_active = true
            """,
            (cutoff,),
        )
        offline = cur.fetchall()
        
        for (device_id,) in offline:
            cur.execute(
                """
                UPDATE edge_device
                SET is_active = false
                WHERE id = %s
                """,
                (device_id,),
            )


async def run_device_health_check():
    """
    Async wrapper for detect_offline_devices() to be used as a background task.
    
    This function is called periodically from main.py's lifespan context manager.
    It runs detect_offline_devices() in a loop with a delay between checks.
    
    Usage:
        Called from main.py as a background task
    """
    import asyncio
    
    while True:
        try:
            detect_offline_devices()
            # Run every 5 minutes
            await asyncio.sleep(300)
        except Exception as e:
            # Log error but continue running
            print(f"Error in device health check: {e}")
            await asyncio.sleep(60)  # Wait 1 minute before retry


if __name__ == "__main__":
    detect_offline_devices()
