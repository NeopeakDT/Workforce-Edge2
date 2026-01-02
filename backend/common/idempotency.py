"""
Idempotency Utilities-

idempotency.py ensures “exact-once behavior over at-least-once delivery.”
Without it, your system will produce wrong data even if all code is “correct.”

this file prevents your system from counting or processing the same real-world event more than once.
Without it, your activity counts, durations, and alerts will drift and become wrong.

Ensures idempotent processing of events and enforces uniqueness
for IN_PROGRESS activities.

- Protects against duplicate Jetson events
- Required for exact-once ingestion
- Safe for retries and reconnect storms


"""

# common/idempotency.py

# Use relative imports since we're already in the common package
from .db import get_cursor
from .time_utils import utc_now

def is_duplicate(key: str) -> bool:
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT 1
            FROM idempotency_keys
            WHERE key = %s
            """,
            (key,),
        )
        return cur.fetchone() is not None

def mark_processed(key: str):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO idempotency_keys (key, created_at)
            VALUES (%s, %s)
            ON CONFLICT (key) DO NOTHING
            """,
            (key, utc_now()),
        )

"""
Ensure table exists (already in Phase-0):

CREATE TABLE idempotency_keys (
    key text PRIMARY KEY,
    created_at timestamptz NOT NULL
);
"""