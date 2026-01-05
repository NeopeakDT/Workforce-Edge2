"""
Audit Logger
Logs all important system events for audit trail.

- Records who / what / when / why
- Essential for ops, debugging, compliance
- Never update or delete audit logs
"""

# common/audit_logger.py

# Use relative imports since we're already in the common package
from .db import get_cursor
from .time_utils import utc_now

def log_event(
    actor_id: str,
    action: str,
    entity_type: str,
    entity_id: str,
    metadata: dict | None = None,
):
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO audit_log (
                actor_id,
                action,
                entity_type,
                entity_id,
                metadata,
                created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s)
            """,
            (
                actor_id,
                action,
                entity_type,
                entity_id,
                metadata,
                utc_now(),
            ),
        )
