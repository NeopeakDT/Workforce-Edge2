"""
backend/alerts/alert_evaluator.py
STEP B — Alert Evaluation Engine (type-specific architecture)

Replaces the STEP 6.1 single-purpose, activity-only evaluator with dispatch
across three matcher modules (alerts/matchers/activity_matcher.py,
posture_matcher.py, edge_device_matcher.py), per the architecture approved
in A2 ("type-specific evaluators", not one giant if/elif function) and
scoped exactly per the A4 catalogue + the A4-review decisions.

Backward compatibility (A3 compatibility requirement): evaluate_activity_alerts
keeps its STEP 6.1 name and signature -- scripts/test_step6_alerts.py still
calls it exactly as before. It now delegates to
alerts.matchers.activity_matcher.evaluate_finalized_instance, which returns a
result dict instead of the STEP 6.1 version's implicit None.

CAMERA evaluation is intentionally absent from this module -- deferred per
A4 and the A4-review decision (#7: "do not implement camera alert
evaluation in Step B").

Not wired into activity_aggregator.py, missed_activity_cron.py,
posture_scheduler.py, or heartbeat_ingest_api.py -- that pipeline wiring is
Step C. This module is only the evaluator engine itself, callable standalone
(see scripts/test_stepB_alert_evaluator.py).
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor

from alerts.matchers import activity_matcher, posture_matcher, edge_device_matcher


def evaluate_activity_alerts(activity_instance_id: str):
    """
    STEP 6.1 entry point, preserved for backward compatibility.

    Delegates to the finalized-instance ACTIVITY matcher: handles
    ACTIVITY_LATE / ACTIVITY_MISSED / ACTIVITY_UNSCHEDULED plus recovery.
    For a still-open (IN_PROGRESS) instance, call
    evaluate_in_progress_activity_alerts() instead -- session_classification
    is NULL until finalization, so this function has nothing to evaluate
    for an open instance.
    """
    return activity_matcher.evaluate_finalized_instance(activity_instance_id)


def evaluate_in_progress_activity_alerts(activity_instance_id: str):
    """ACTIVITY_RUNNING_LONG -- the only ACTIVITY alert evaluable pre-finalization."""
    return activity_matcher.evaluate_in_progress_instance(activity_instance_id)


def evaluate_posture_alerts(farm_id: str, zone_id: str):
    """POSTURE_DATA_STALE for one zone."""
    return posture_matcher.evaluate_zone_staleness(farm_id, zone_id)


def evaluate_edge_device_alerts(device_id: str):
    """EDGE_DEVICE_OFFLINE for one device."""
    return edge_device_matcher.evaluate_device_offline(device_id)


if __name__ == "__main__":
    # For manual testing - evaluate ACTIVITY alerts for the most recent activity instance
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT id FROM activity_instance
            ORDER BY created_at DESC
            LIMIT 1
            """
        )
        latest = cur.fetchone()

        if latest:
            print(f"Evaluating alerts for activity instance: {latest['id']}")
            result = evaluate_activity_alerts(latest['id'])
            print("Alert evaluation completed:", result)
        else:
            print("No activity instances found to evaluate")
