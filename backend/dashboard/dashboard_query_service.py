"""
Dashboard Query Service

Read-only queries for UI dashboards.

Reads:
- activity_instance
- alert_log
- farm

Rules:
- NO detection tables (never read activity_detection_event)
- NO inference (no computation logic)
- NO writes (read-only)
- Only serve aggregated truth

This file is safe for direct API exposure.
"""

from common.db import get_cursor


# ---------------- FARM OVERVIEW ----------------

def get_farm_overview(farm_id):
    """
    Get farm overview statistics.
    
    Returns:
        - Farm information
        - Count of active (IN_PROGRESS) activities
        - Count of missed activities
        - Count of active (unresolved) alerts
    
    Args:
        farm_id: UUID of the farm
        
    Returns:
        dict: Farm overview with activity and alert counts
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                f.id,
                f.name,
                COUNT(ai.id) FILTER (WHERE ai.status = 'IN_PROGRESS') AS active_activities,
                COUNT(ai.id) FILTER (WHERE ai.status = 'MISSED') AS missed_activities,
                COUNT(al.id) FILTER (WHERE al.resolved_at IS NULL) AS active_alerts
            FROM farm f
            LEFT JOIN activity_instance ai ON ai.farm_id = f.id
            LEFT JOIN alert_log al ON al.farm_id = f.id
            WHERE f.id = %s
            GROUP BY f.id
            """,
            (farm_id,),
        )
        return cur.fetchone()


# ---------------- ACTIVITY FEED ----------------

def list_recent_activities(farm_id, limit=50):
    """
    List recent activities for a farm.
    
    Returns activities ordered by start time (most recent first).
    Includes scheduled activities that haven't started yet.
    
    Args:
        farm_id: UUID of the farm
        limit: Maximum number of activities to return
        
    Returns:
        list: List of activity records
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                activity_type,
                status,
                started_at,
                ended_at,
                confidence
            FROM activity_instance
            WHERE farm_id = %s
            ORDER BY COALESCE(started_at, scheduled_start) DESC
            LIMIT %s
            """,
            (farm_id, limit),
        )
        return cur.fetchall()


# ---------------- ALERT FEED ----------------

def list_active_alerts(farm_id):
    """
    List active (unresolved) alerts for a farm.
    
    Returns alerts ordered by creation time (most recent first).
    
    Args:
        farm_id: UUID of the farm
        
    Returns:
        list: List of active alert records
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT
                id,
                alert_type,
                severity,
                message,
                created_at
            FROM alert_log
            WHERE farm_id = %s
              AND resolved_at IS NULL
            ORDER BY created_at DESC
            """,
            (farm_id,),
        )
        return cur.fetchall()
