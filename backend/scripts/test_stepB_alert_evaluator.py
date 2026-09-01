"""
backend/scripts/test_stepB_alert_evaluator.py
STEP B — Integration tests for the type-specific alert evaluator.

Follows this repo's existing test convention (standalone script run directly,
no pytest) but, unlike scripts/test_step6_alerts.py, makes real assertions
and prints PASS/FAIL per case, against real data already in the DB
(farm/schedule/instance/device/zone rows) plus test-only alert_rule
fixtures created and deleted by this script. No production data is
mutated -- only alert_rule/alert_log rows created by this script, and one
synthetic activity_instance row for the RUNNING_LONG case, all removed in
a finally block regardless of outcome.

Run: python scripts/test_stepB_alert_evaluator.py
"""

from pathlib import Path
import sys
import uuid
from datetime import timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json

from common.db import get_cursor
from common.time_utils import utc_now
from alerts import alert_evaluator

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
POSTURE_ZONE_ID = "36259855-f8f7-4e30-93e5-84e536aef8b6"  # the only zone with real posture_observation rows
DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"

SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"  # activity_type 3, used by the real MISSED row
SCRAP_EVENING_SCHEDULE = "38c71cfc-0769-4d87-94b9-2b3447dbef90"  # activity_type 3, used by the real LATE row

EXISTING_MISSED_INSTANCE = "f8e9bae8-b379-4a25-9319-47e4e8dd6f03"
EXISTING_LATE_INSTANCE = "c71026f1-c862-4892-bf70-cbf6fbe8aac7"
EXISTING_UNSCHEDULED_INSTANCE = "b8e86d6a-e06d-4014-9aa1-2e0a6cf5971b"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status, detail))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_rule(cur, *, alert_type, name, condition, severity="WARNING",
              activity_type_id=None, activity_schedule_id=None):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, true)
        """,
        (rule_id, FARM_ID, activity_type_id, activity_schedule_id, name,
         Json(condition), severity, alert_type),
    )
    return rule_id


def fetch_alert_logs(rule_id):
    with get_cursor() as cur:
        cur.execute(
            "SELECT * FROM alert_log WHERE alert_rule_id = %s ORDER BY triggered_at",
            (rule_id,),
        )
        return cur.fetchall()


def cleanup(rule_ids, extra_instance_ids=None):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if extra_instance_ids:
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (extra_instance_ids,))


def test_activity_missed():
    rule_ids = []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(
                cur, alert_type="ACTIVITY", name="STEPB_TEST Missed Scrapping",
                condition={"metric": "session_classification", "operator": "=", "value": "MISSED", "duration_minutes": 0},
                severity="CRITICAL", activity_type_id=3, activity_schedule_id=SCRAP_MORNING_SCHEDULE,
            )
        rule_ids.append(rule_id)

        result = alert_evaluator.evaluate_activity_alerts(EXISTING_MISSED_INSTANCE)
        check("ACTIVITY_MISSED: evaluator finds the instance", result["instance_found"])
        check("ACTIVITY_MISSED: alert created", rule_id in result.get("alerts_created", []))

        rows = fetch_alert_logs(rule_id)
        check("ACTIVITY_MISSED: exactly one alert_log row", len(rows) == 1, f"got {len(rows)}")
        if rows:
            check("ACTIVITY_MISSED: lifecycle_state ACTIVE", rows[0]["lifecycle_state"] == "ACTIVE")
            check("ACTIVITY_MISSED: dedup_key is instance id", rows[0]["dedup_key"] == EXISTING_MISSED_INSTANCE)
            check("ACTIVITY_MISSED: alert_type is ACTIVITY", rows[0]["alert_type"] == "ACTIVITY")

        # Re-run: must NOT create a second row (anti-storm / dedup).
        alert_evaluator.evaluate_activity_alerts(EXISTING_MISSED_INSTANCE)
        rows_after = fetch_alert_logs(rule_id)
        check("ACTIVITY_MISSED: re-evaluation does not duplicate", len(rows_after) == 1, f"got {len(rows_after)}")
    finally:
        cleanup(rule_ids)


def test_activity_late_and_recovery():
    # STEP C (deliberate, spec-mandated change -- not a bug fix): per
    # docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md §2,
    # finalization with session_classification='LATE' must no longer create
    # a second, separate ACTIVITY_LATE alert -- that alert is now generated
    # exclusively by the new schedule-keyed evaluate_late_start_sweep()
    # operational evaluator (see activity_matcher.evaluate_finalized_instance,
    # which now `continue`s past any rule whose condition targets
    # session_classification == 'LATE'). This test's assertions are updated
    # to match: finalizing a LATE instance must create NO alert_log row for
    # a session_classification='LATE' rule, and the schedule-wide recovery
    # call (still invoked, still lists "LATE") remains a harmless no-op
    # since nothing creates such rows anymore.
    rule_ids = []
    try:
        with get_cursor() as cur:
            late_rule_id = make_rule(
                cur, alert_type="ACTIVITY", name="STEPB_TEST Late Scrapping",
                condition={"metric": "session_classification", "operator": "=", "value": "LATE", "duration_minutes": 0},
                severity="WARNING", activity_type_id=3, activity_schedule_id=SCRAP_EVENING_SCHEDULE,
            )
        rule_ids.append(late_rule_id)

        result = alert_evaluator.evaluate_activity_alerts(EXISTING_LATE_INSTANCE)
        check("ACTIVITY_LATE: finalization no longer creates this alert (Step C Correction 1)",
              late_rule_id not in result.get("alerts_created", []))

        rows = fetch_alert_logs(late_rule_id)
        check("ACTIVITY_LATE: zero alert_log rows after finalization", len(rows) == 0, f"got {len(rows)}")

        # Simulate a later successful occurrence of the SAME schedule -> the
        # LATE/MISSED schedule-wide recovery call still runs, but since no
        # LATE alert was ever created, it has nothing to resolve (no-op).
        synthetic_id = str(uuid.uuid4())
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, actual_end_at, actual_duration_sec, status,
                     session_classification, source, zone_id)
                VALUES (%s, %s, 3, %s, CURRENT_DATE, %s, %s, 60, 'ENDED', 'ON_TIME', 'SYSTEM', %s)
                """,
                (synthetic_id, FARM_ID, SCRAP_EVENING_SCHEDULE, utc_now() - timedelta(minutes=1), utc_now(), ZONE_ID),
            )

        alert_evaluator.evaluate_activity_alerts(synthetic_id)

        rows_after = fetch_alert_logs(late_rule_id)
        check(
            "ACTIVITY_LATE: still zero rows after a later ON_TIME occurrence (recovery no-op)",
            len(rows_after) == 0,
            detail=str([dict(r) for r in rows_after]),
        )

        cleanup([], extra_instance_ids=[synthetic_id])
    finally:
        cleanup(rule_ids)


def test_activity_unscheduled_point_in_time():
    rule_ids = []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(
                cur, alert_type="ACTIVITY", name="STEPB_TEST Unscheduled Scrapping",
                condition={"metric": "session_classification", "operator": "=", "value": "UNSCHEDULED", "duration_minutes": 0},
                severity="WARNING", activity_type_id=3,
            )
        rule_ids.append(rule_id)

        result = alert_evaluator.evaluate_activity_alerts(EXISTING_UNSCHEDULED_INSTANCE)
        check("ACTIVITY_UNSCHEDULED: alert created", rule_id in result.get("alerts_created", []))

        rows = fetch_alert_logs(rule_id)
        check("ACTIVITY_UNSCHEDULED: exactly one row", len(rows) == 1)
        if rows:
            check("ACTIVITY_UNSCHEDULED: immediately RESOLVED", rows[0]["lifecycle_state"] == "RESOLVED")
            check("ACTIVITY_UNSCHEDULED: resolved_at set at creation", rows[0]["resolved_at"] is not None)
            check("ACTIVITY_UNSCHEDULED: row still queryable (history)", rows[0]["id"] is not None)
    finally:
        cleanup(rule_ids)


def test_activity_running_long():
    rule_ids = []
    synthetic_id = str(uuid.uuid4())
    try:
        # Evening Milking schedule: 16:30-18:00 IST, tolerance_late_min=120 -> overdue after 20:00 IST.
        # actual_start_at set far enough in the past (36h) that "now" is always past the overdue cutoff,
        # regardless of what time this test happens to run.
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, 1, 'a866bb82-e816-4327-99cf-ec2495ca1adc', DATE '2019-01-01',
                        %s, 'IN_PROGRESS', 'SYSTEM', %s)
                """,
                (synthetic_id, FARM_ID, utc_now() - timedelta(hours=36), ZONE_ID),
            )

            rule_id = make_rule(
                cur, alert_type="ACTIVITY", name="STEPB_TEST Running Long Milking",
                condition={"metric": "elapsed_minutes_since_start", "operator": ">", "value": None, "duration_minutes": 0},
                severity="WARNING", activity_type_id=1, activity_schedule_id="a866bb82-e816-4327-99cf-ec2495ca1adc",
            )
        rule_ids.append(rule_id)

        result = alert_evaluator.evaluate_in_progress_activity_alerts(synthetic_id)
        check("ACTIVITY_RUNNING_LONG: detected as overdue", result.get("is_overdue") is True, detail=str(result))
        check("ACTIVITY_RUNNING_LONG: alert created", rule_id in result.get("alerts_created", []))

        rows = fetch_alert_logs(rule_id)
        check("ACTIVITY_RUNNING_LONG: one ACTIVE row", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        # Finalize the instance -> its own RUNNING_LONG alert must self-resolve.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE activity_instance SET status='ENDED', actual_end_at=%s, session_classification='LATE' WHERE id=%s",
                (utc_now(), synthetic_id),
            )
        alert_evaluator.evaluate_activity_alerts(synthetic_id)

        rows_after = fetch_alert_logs(rule_id)
        check(
            "ACTIVITY_RUNNING_LONG: self-resolves when instance finalizes",
            rows_after and rows_after[0]["lifecycle_state"] == "RESOLVED",
            detail=str([dict(r) for r in rows_after]),
        )
    finally:
        cleanup(rule_ids, extra_instance_ids=[synthetic_id])


def test_posture_data_stale():
    rule_ids = []
    try:
        with get_cursor() as cur:
            # duration_minutes/value=0 -> even the freshest real observation is "stale" relative to
            # this threshold, deterministically proving the trigger path without fabricating rows
            # in posture_observation.
            trigger_rule_id = make_rule(
                cur, alert_type="POSTURE", name="STEPB_TEST Posture Stale (forced trigger)",
                condition={"metric": "observation_age_minutes", "operator": ">", "value": -1, "duration_minutes": 0},
            )
            # An enormous threshold can never be exceeded -> proves the non-trigger / clean path.
            quiet_rule_id = make_rule(
                cur, alert_type="POSTURE", name="STEPB_TEST Posture Stale (forced quiet)",
                condition={"metric": "observation_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0},
            )
        rule_ids.extend([trigger_rule_id, quiet_rule_id])

        result = alert_evaluator.evaluate_posture_alerts(FARM_ID, POSTURE_ZONE_ID)
        check("POSTURE_DATA_STALE: observation found for zone", result.get("observation_found") is True, detail=str(result))
        check("POSTURE_DATA_STALE: trigger rule fires", trigger_rule_id in result.get("alerts_created", []))
        check("POSTURE_DATA_STALE: quiet rule does not fire", quiet_rule_id not in result.get("alerts_created", []))

        trigger_rows = fetch_alert_logs(trigger_rule_id)
        check("POSTURE_DATA_STALE: one ACTIVE row for trigger rule", len(trigger_rows) == 1 and trigger_rows[0]["lifecycle_state"] == "ACTIVE")
        check("POSTURE_DATA_STALE: zone_id set on row", trigger_rows and str(trigger_rows[0]["zone_id"]) == POSTURE_ZONE_ID)

        quiet_rows = fetch_alert_logs(quiet_rule_id)
        check("POSTURE_DATA_STALE: no row for quiet rule", len(quiet_rows) == 0)

        # Recovery: re-run with the trigger rule's threshold now unreachable (simulate by re-pointing
        # to the quiet condition semantics) -- verify resolve path via a rule whose condition flips.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE alert_rule SET condition = %s WHERE id = %s",
                (Json({"metric": "observation_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0}), trigger_rule_id),
            )
        alert_evaluator.evaluate_posture_alerts(FARM_ID, POSTURE_ZONE_ID)
        trigger_rows_after = fetch_alert_logs(trigger_rule_id)
        check(
            "POSTURE_DATA_STALE: resolves once condition no longer holds",
            trigger_rows_after and trigger_rows_after[0]["lifecycle_state"] == "RESOLVED",
            detail=str([dict(r) for r in trigger_rows_after]),
        )
    finally:
        cleanup(rule_ids)


def test_edge_device_offline():
    rule_ids = []
    try:
        with get_cursor() as cur:
            trigger_rule_id = make_rule(
                cur, alert_type="EDGE_DEVICE", name="STEPB_TEST Device Offline (forced trigger)",
                condition={"metric": "heartbeat_age_minutes", "operator": ">", "value": -1, "duration_minutes": 0},
                severity="CRITICAL",
            )
            quiet_rule_id = make_rule(
                cur, alert_type="EDGE_DEVICE", name="STEPB_TEST Device Offline (forced quiet)",
                condition={"metric": "heartbeat_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0},
                severity="CRITICAL",
            )
        rule_ids.extend([trigger_rule_id, quiet_rule_id])

        result = alert_evaluator.evaluate_edge_device_alerts(DEVICE_ID)
        check("EDGE_DEVICE_OFFLINE: device found", result.get("device_found") is True, detail=str(result))
        check("EDGE_DEVICE_OFFLINE: trigger rule fires", trigger_rule_id in result.get("alerts_created", []))
        check("EDGE_DEVICE_OFFLINE: quiet rule does not fire", quiet_rule_id not in result.get("alerts_created", []))

        rows = fetch_alert_logs(trigger_rule_id)
        check("EDGE_DEVICE_OFFLINE: one ACTIVE row, device_id set",
              len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE" and str(rows[0]["device_id"]) == DEVICE_ID)

        # Re-run: dedup, no duplicate row.
        alert_evaluator.evaluate_edge_device_alerts(DEVICE_ID)
        rows_after = fetch_alert_logs(trigger_rule_id)
        check("EDGE_DEVICE_OFFLINE: re-evaluation does not duplicate", len(rows_after) == 1)

        # Recovery: flip the rule's threshold to unreachable -> resolve.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE alert_rule SET condition = %s WHERE id = %s",
                (Json({"metric": "heartbeat_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0}), trigger_rule_id),
            )
        alert_evaluator.evaluate_edge_device_alerts(DEVICE_ID)
        rows_final = fetch_alert_logs(trigger_rule_id)
        check(
            "EDGE_DEVICE_OFFLINE: resolves once heartbeat is 'fresh enough'",
            rows_final and rows_final[0]["lifecycle_state"] == "RESOLVED",
            detail=str([dict(r) for r in rows_final]),
        )
    finally:
        cleanup(rule_ids)


if __name__ == "__main__":
    test_activity_missed()
    test_activity_late_and_recovery()
    test_activity_unscheduled_point_in_time()
    test_activity_running_long()
    test_posture_data_stale()
    test_edge_device_offline()

    total = len(results)
    passed = sum(1 for _, s, _ in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
