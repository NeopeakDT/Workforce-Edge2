"""
STEP 6.1 — Alert Evaluation Engine (ACTIVITY ONLY)

Evaluates alert rules against FINAL activity truth.
"""

from common.db import get_cursor
from common.time_utils import utc_now


def _condition_matches(condition: dict, activity: dict) -> bool:
    """
    Deterministic rule evaluation.
    """
    if not condition:
        return False  # empty condition is invalid

    if "status" in condition:
        if activity["status"] != condition["status"]:
            return False

    if "started_offset_min_gt" in condition:
        if activity["started_offset_min"] is None:
            return False
        if activity["started_offset_min"] <= condition["started_offset_min_gt"]:
            return False

    return True


def evaluate_activity_alerts(activity_instance_id: str):
    """
    STEP 6.1 entry point
    """

    with get_cursor() as cur:
        # 1. Load activity_instance
        cur.execute(
            """
            SELECT
                id,
                farm_id,
                activity_type_id,
                activity_schedule_id,
                status,
                started_offset_min
            FROM activity_instance
            WHERE id = %s
            """,
            (activity_instance_id,),
        )
        activity = cur.fetchone()

        if not activity:
            print("No activity_instance found")
            return

        # 2. Load matching alert rules
        cur.execute(
            """
            SELECT
                id,
                condition
            FROM alert_rule
            WHERE farm_id = %s
              AND (activity_type_id IS NULL OR activity_type_id = %s)
              AND (activity_schedule_id IS NULL OR activity_schedule_id = %s)
              AND is_active = true
            """,
            (
                activity["farm_id"],
                activity["activity_type_id"],
                activity["activity_schedule_id"],
            ),
        )

        rules = cur.fetchall()

        for rule in rules:
            if not _condition_matches(rule["condition"], activity):
                continue

            # 3. Idempotent insert
            cur.execute(
                """
                INSERT INTO alert_log (
                    farm_id,
                    alert_rule_id,
                    activity_instance_id,
                    triggered_at,
                    status
                )
                SELECT %s, %s, %s, %s, 'SENT'
                WHERE NOT EXISTS (
                    SELECT 1
                    FROM alert_log
                    WHERE alert_rule_id = %s
                      AND activity_instance_id = %s
                )
                """,
                (
                    activity["farm_id"],
                    rule["id"],
                    activity["id"],
                    utc_now(),
                    rule["id"],
                    activity["id"],
                ),
            )
