"""
CORE ACTIVITY AGGREGATOR

Transforms raw detection events into ground-truth activity instances.

Key guarantees:
    - Deterministic: Same events produce same results
    - Idempotent: Safe to run multiple times
    - Single IN_PROGRESS per (farm, activity): Enforces uniqueness
    - Buffer-based defragmentation: Handles gaps in detection stream

Architecture:
    - Processes unprocessed detection events from activity_detection_event table
    - Creates or updates activity_instance records
    - Auto-closes stale activities after inactivity period
    - Single source of truth for live and completed activities

Process Flow:
    1. Fetch pending events (processed_at IS NULL)
    2. For each event:
       - Filter by confidence threshold
       - Check for existing IN_PROGRESS activity
       - Start new activity or update existing
       - Mark event as processed
    3. Close stale activities (no events within buffer period)
"""

from datetime import timedelta

from common.db import get_cursor
from common.time_utils import utc_now

# ---------------- CONFIG ----------------

ACTIVITY_END_BUFFER_SEC = 120   # inactivity buffer
CONFIDENCE_MIN = 0.5

# ---------------------------------------

def fetch_pending_events(limit=500):
    """
    Fetch unprocessed detection events.
    
    Args:
        limit: Maximum number of events to fetch
        
    Returns:
        list: List of event records (id, farm_id, activity_type, event_time, confidence)
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, farm_id, activity_type, event_time, confidence
            FROM activity_detection_event
            WHERE processed_at IS NULL
            ORDER BY event_time
            LIMIT %s
            """,
            (limit,),
        )
        return cur.fetchall()


def mark_event_processed(event_id):
    """
    Mark an event as processed by setting processed_at timestamp.
    
    Args:
        event_id: UUID of the event to mark as processed
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE activity_detection_event
            SET processed_at = %s
            WHERE id = %s
            """,
            (utc_now(), event_id),
        )


def get_active_instance(farm_id, activity_type):
    """
    Get the active (IN_PROGRESS) activity instance for a farm and activity type.
    
    Args:
        farm_id: UUID of the farm
        activity_type: Type of activity (e.g., "SCRAPING", "FEEDING")
        
    Returns:
        dict or None: Active instance record (id, started_at, confidence) or None
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id, started_at, confidence
            FROM activity_instance
            WHERE farm_id = %s
              AND activity_type = %s
              AND status = 'IN_PROGRESS'
            """,
            (farm_id, activity_type),
        )
        return cur.fetchone()


def start_activity(farm_id, activity_type, event_time, confidence):
    """
    Start a new activity instance.
    
    Uses ON CONFLICT DO NOTHING to enforce single IN_PROGRESS per (farm, activity).
    Requires unique constraint on (farm_id, activity_type, status) where status='IN_PROGRESS'.
    
    Args:
        farm_id: UUID of the farm
        activity_type: Type of activity
        event_time: Timestamp when activity started
        confidence: Confidence score for the activity
    """
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO activity_instance (
                farm_id,
                activity_type,
                status,
                started_at,
                confidence
            )
            VALUES (%s, %s, 'IN_PROGRESS', %s, %s)
            ON CONFLICT DO NOTHING
            """,
            (farm_id, activity_type, event_time, confidence),
        )


def update_activity_confidence(instance_id, confidence):
    """
    Update activity confidence to the maximum of current and new confidence.
    
    Args:
        instance_id: UUID of the activity instance
        confidence: New confidence score
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE activity_instance
            SET confidence = GREATEST(confidence, %s)
            WHERE id = %s
            """,
            (confidence, instance_id),
        )


def end_activity(instance_id, end_time):
    """
    End an activity instance by setting status to COMPLETED.
    
    Args:
        instance_id: UUID of the activity instance
        end_time: Timestamp when activity ended
    """
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE activity_instance
            SET status = 'COMPLETED',
                ended_at = %s
            WHERE id = %s
            """,
            (end_time, instance_id),
        )


def close_stale_activities():
    """
    Auto-close IN_PROGRESS activities if no detections arrived
    within ACTIVITY_END_BUFFER_SEC.
    
    This handles cases where detection stream stops without an explicit END event.
    """
    cutoff = utc_now() - timedelta(seconds=ACTIVITY_END_BUFFER_SEC)
    
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id
            FROM activity_instance
            WHERE status = 'IN_PROGRESS'
              AND started_at < %s
              AND id NOT IN (
                SELECT DISTINCT activity_instance_id
                FROM activity_detection_event
                WHERE event_time >= %s
              )
            """,
            (cutoff, cutoff),
        )
        stale = cur.fetchall()
        
        for (instance_id,) in stale:
            end_activity(instance_id, utc_now())


def process_events():
    """
    Main processing function.
    
    Processes all pending detection events and manages activity lifecycle:
    1. Fetches unprocessed events
    2. For each event:
       - Filters by confidence threshold
       - Starts new activity or updates existing
       - Marks event as processed
    3. Closes stale activities
    
    This function is idempotent and can be called repeatedly safely.
    """
    events = fetch_pending_events()
    
    for event in events:
        (
            event_id,
            farm_id,
            activity_type,
            event_time,
            confidence,
        ) = event
        
        if confidence < CONFIDENCE_MIN:
            mark_event_processed(event_id)
            continue
        
        active = get_active_instance(farm_id, activity_type)
        
        if not active:
            start_activity(
                farm_id=farm_id,
                activity_type=activity_type,
                event_time=event_time,
                confidence=confidence,
            )
        else:
            instance_id, _, _ = active
            update_activity_confidence(instance_id, confidence)
        
        mark_event_processed(event_id)
    
    close_stale_activities()


if __name__ == "__main__":
    process_events()
