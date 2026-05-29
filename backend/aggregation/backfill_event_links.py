#!/usr/bin/env python3
"""
One-time backfill utility for historical events left unlinked while STEP-4 was down.

Links activity_detection_event.activity_instance_id by matching each unlinked event
to an activity_instance time window for the same farm/activity type.
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor


def backfill_event_links():
    with get_cursor() as cur:
        cur.execute(
            """
            UPDATE activity_detection_event e
            SET activity_instance_id = m.instance_id
            FROM (
                SELECT DISTINCT ON (e.id)
                    e.id AS event_id,
                    ai.id AS instance_id
                FROM activity_detection_event e
                JOIN activity_instance ai
                  ON e.farm_id = ai.farm_id
                 AND e.activity_type_id = ai.activity_type_id
                 AND ai.actual_start_at IS NOT NULL
                 AND ai.actual_end_at IS NOT NULL
                 AND e.event_time BETWEEN ai.actual_start_at AND ai.actual_end_at
                WHERE e.activity_instance_id IS NULL
                ORDER BY
                    e.id,
                    ABS(EXTRACT(EPOCH FROM (e.event_time - ai.actual_start_at))),
                    ai.id
            ) m
            WHERE e.id = m.event_id
              AND e.activity_instance_id IS NULL
            """
        )
        print(f"[BACKFILL] linked_events={cur.rowcount}")
        cur.connection.commit()


if __name__ == "__main__":
    backfill_event_links()
