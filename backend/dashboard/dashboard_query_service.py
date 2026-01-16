"""
STEP 6.3 — Dashboard Query Service

Read-only queries for UI dashboards.
NO writes.
NO inference.
NO detection tables.
"""

from common.db import get_cursor


# -------------------------------------------------
# Farm overview
# -------------------------------------------------

def get_farm_overview(farm_id: str):
    """
    High-level farm dashboard numbers.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                f.id AS farm_id,
                f.name AS farm_name,

                COUNT(ai.id) FILTER (WHERE ai.status = 'IN_PROGRESS') AS in_progress_activities,
                COUNT(ai.id) FILTER (WHERE ai.status = 'MISSED') AS missed_activities,
                COUNT(al.id) FILTER (WHERE al.status = 'SENT') AS active_alerts

            FROM farm f
            LEFT JOIN activity_instance ai ON ai.farm_id = f.id
            LEFT JOIN alert_log al ON al.farm_id = f.id

            WHERE f.id = %s
            GROUP BY f.id
            """,
            (farm_id,),
        )
        return cur.fetchone()


# -------------------------------------------------
# Activity lists
# -------------------------------------------------

def list_today_activities(farm_id: str, activity_date):
    """
    Activities for a given date (already computed in Phase 5).
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                status,
                actual_start_at,
                actual_end_at,
                started_offset_min,
                ended_offset_min,
                source
            FROM activity_instance
            WHERE farm_id = %s
              AND activity_date = %s
            ORDER BY COALESCE(actual_start_at, created_at)
            """,
            (farm_id, activity_date),
        )
        return cur.fetchall()


def list_in_progress_activities(farm_id: str):
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                actual_start_at,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND status = 'IN_PROGRESS'
            ORDER BY actual_start_at
            """,
            (farm_id,),
        )
        return cur.fetchall()


def list_missed_activities(farm_id: str, limit=20):
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type_id,
                activity_date,
                status
            FROM activity_instance
            WHERE farm_id = %s
              AND status = 'MISSED'
            ORDER BY activity_date DESC
            LIMIT %s
            """,
            (farm_id, limit),
        )
        return cur.fetchall()


# -------------------------------------------------
# Alerts
# -------------------------------------------------

def list_recent_alerts(farm_id: str, limit=20):
    """
    Recent alerts with context for dashboard.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                al.id,
                al.triggered_at,
                al.status AS alert_status,
                al.message,
                al.channel,

                ar.severity,
                ar.name AS rule_name,

                ai.activity_type_id,
                ai.status AS activity_status

            FROM alert_log al
            JOIN alert_rule ar ON ar.id = al.alert_rule_id
            JOIN activity_instance ai ON ai.id = al.activity_instance_id

            WHERE al.farm_id = %s
            ORDER BY al.triggered_at DESC
            LIMIT %s
            """,
            (farm_id, limit),
        )
        return cur.fetchall()
