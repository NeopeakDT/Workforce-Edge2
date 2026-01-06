"""
Alert Evaluator

Evaluates alert rules based on:
- activity_instance status transitions
- MISSED activities
- edge device offline detection

Writes:
- alert_log

Triggered by:
- activity_aggregator.py
- missed_activity_cron.py
- device_health_monitor.py

Design Principles:
- Event-driven (called by Phase 5 scripts or cron)
- Idempotent (no duplicate alerts)
- Deterministic rules only (no ML here)

Truth → Reaction: Evaluates alert conditions whenever:
- activity status changes
- MISSED activity is created
- device goes offline
"""

from common.db import get_cursor
from common.time_utils import utc_now

# ---------------- CONFIG ----------------

DEVICE_OFFLINE_THRESHOLD_MIN = 10

# --------------------------------------


def create_alert(
    farm_id,
    alert_type,
    severity,
    message,
    ref_table=None,
    ref_id=None,
):
    """
    Idempotent alert insert.
    Prevents duplicate active alerts for same ref.
    
    Args:
        farm_id: UUID of the farm
        alert_type: Type of alert (e.g., "ACTIVITY_MISSED", "DEVICE_OFFLINE")
        severity: Alert severity ("INFO", "WARNING", "CRITICAL")
        message: Human-readable alert message
        ref_table: Reference table name (e.g., "activity_instance", "edge_device")
        ref_id: Reference record ID
    """
    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO alert_log (
                farm_id,
                alert_type,
                severity,
                message,
                ref_table,
                ref_id,
                created_at
            )
            SELECT %s, %s, %s, %s, %s, %s, %s
            WHERE NOT EXISTS (
                SELECT 1
                FROM alert_log
                WHERE farm_id = %s
                  AND alert_type = %s
                  AND ref_table IS NOT DISTINCT FROM %s
                  AND ref_id IS NOT DISTINCT FROM %s
                  AND resolved_at IS NULL
            )
            """,
            (
                farm_id,
                alert_type,
                severity,
                message,
                ref_table,
                ref_id,
                utc_now(),
                farm_id,
                alert_type,
                ref_table,
                ref_id,
            ),
        )


# ---------------- ACTIVITY ALERTS ----------------

def on_activity_status_change(activity_instance_id):
    """
    Triggered when activity status changes.
    
    Args:
        activity_instance_id: UUID of the activity instance
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id,
                ai.farm_id,
                ai.activity_type,
                ai.status
            FROM activity_instance ai
            WHERE ai.id = %s
            """,
            (activity_instance_id,),
        )
        row = cur.fetchone()

    if not row:
        return

    if row["status"] == "COMPLETED":
        create_alert(
            farm_id=row["farm_id"],
            alert_type="ACTIVITY_COMPLETED",
            severity="INFO",
            message=f"{row['activity_type']} completed",
            ref_table="activity_instance",
            ref_id=row["id"],
        )


def on_activity_missed(activity_instance_id):
    """
    Triggered on MISSED activity creation.
    
    Args:
        activity_instance_id: UUID of the activity instance
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                ai.id,
                ai.farm_id,
                ai.activity_type,
                ai.scheduled_start
            FROM activity_instance ai
            WHERE ai.id = %s
              AND ai.status = 'MISSED'
            """,
            (activity_instance_id,),
        )
        row = cur.fetchone()

    if not row:
        return

    create_alert(
        farm_id=row["farm_id"],
        alert_type="ACTIVITY_MISSED",
        severity="CRITICAL",
        message=f"{row['activity_type']} missed (scheduled at {row['scheduled_start']})",
        ref_table="activity_instance",
        ref_id=row["id"],
    )


# ---------------- DEVICE ALERTS ----------------

def evaluate_device_health():
    """
    Periodic evaluation for offline devices.
    
    Checks for devices that haven't sent heartbeats within the threshold
    and creates alerts for them.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                farm_id,
                device_name,
                last_seen_at
            FROM edge_device
            WHERE is_active = true
              AND (
                last_seen_at IS NULL
                OR last_seen_at < now() - interval '%s minutes'
              )
            """,
            (DEVICE_OFFLINE_THRESHOLD_MIN,),
        )
        rows = cur.fetchall()

    for d in rows:
        create_alert(
            farm_id=d["farm_id"],
            alert_type="DEVICE_OFFLINE",
            severity="CRITICAL",
            message=f"Device {d['device_name']} is offline",
            ref_table="edge_device",
            ref_id=d["id"],
        )
