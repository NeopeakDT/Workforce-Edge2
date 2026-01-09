"""
PHASE 5 — STEP 1 (READ-ONLY DRY RUN)

Purpose:
- Validate farm-local activity_date computation
- Validate grouping of detection events into logical activities
- Observe multi-camera behavior
- Verify midnight-boundary correctness

STRICT RULES:
- NO inserts
- NO updates
- NO processed_at changes
- NO activity_instance usage
- NO schedule logic
"""


from pathlib import Path
import sys  

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from collections import defaultdict
from common.db import get_cursor

def dry_run_grouping(limit=500):
    """
    Read-only inspection of detection events grouped by:
    (farm_id, activity_type, activity_date)

    activity_date MUST be computed as:
    (event_time AT TIME ZONE farm.timezone)::date
    """

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                e.id AS event_id,
                e.event_time,                     -- UTC timestamptz
                at.code AS activity_type,         -- human-readable activity
                e.camera_id,
                f.id AS farm_id,
                f.timezone,
                (e.event_time AT TIME ZONE f.timezone)::date AS activity_date
            FROM activity_detection_event e
            JOIN activity_type at ON at.id = e.activity_type_id
            JOIN edge_device d ON d.id = e.device_id
            JOIN farm f ON f.id = d.farm_id
            ORDER BY e.event_time
            LIMIT %s
            """,
            (limit,),
        )
        rows = cur.fetchall()


    # ------------------------------
    # Grouping (pure in-memory)
    # ------------------------------
    buckets = defaultdict(list)

    for row in rows:
        (
            event_id,
            event_time,
            activity_type,
            camera_id,
            farm_id,
            timezone,
            activity_date,
        ) = row

        key = (farm_id, activity_type, activity_date)
        buckets[key].append(
            {
                "event_id": event_id,
                "event_time": event_time,
                "camera_id": camera_id,
                "timezone": timezone,
            }
        )

    # ------------------------------
    # Logging / Observation
    # ------------------------------
    for (farm_id, activity_type, activity_date), events in buckets.items():
        event_times = [e["event_time"] for e in events]
        cameras = sorted({e["camera_id"] for e in events})
        timezone = events[0]["timezone"]

        print(
            f"\n[FARM={farm_id}]"
            f"[ACTIVITY={activity_type}]"
            f"[DATE={activity_date}]"
            f"[TZ={timezone}]"
        )
        print(f"  Cameras involved   : {cameras}")
        print(f"  Event count        : {len(events)}")
        print(f"  First event (UTC)  : {min(event_times)}")
        print(f"  Last event  (UTC)  : {max(event_times)}")

        # Optional: print individual events for deep inspection
        for e in events:
            print(
                f"    - {e['event_time']} | camera={e['camera_id']}"
            )


if __name__ == "__main__":
    dry_run_grouping()
