"""
backend/scripts/test_running_long_posture_device_rule_seed.py
Alert Rule Seed Completion -- ACTIVITY_RUNNING_LONG, POSTURE_DATA_STALE,
EDGE_DEVICE_OFFLINE.

Proves the newly-seeded production rules
(ops/seed_alert_rules_running_long_posture_device.sql) work end-to-end
against the real evaluators, using only synthetic fixtures (a synthetic
activity_instance, a synthetic edge_device, a synthetic farm_zone +
posture_observation) so no production data is ever used as a writable
test fixture -- this follows directly from the ACTIVITY_MISSED
test-contamination incident (alert_log row
b22a95e5-7fdb-4f61-9939-bc628d609442) and its fix in
test_activity_missed_rule_seed.py / test_stepB_alert_evaluator.py.

Each real seeded rule is read but never inserted, updated, or deleted.
All fixtures (instance, device, zone, observation, alert_log rows) are
removed in a finally block regardless of outcome. Includes an explicit
before/after production-state check (alert_log id set + count) to make
contamination immediately visible if it ever recurs.

Run: python scripts/test_running_long_posture_device_rule_seed.py
"""

from pathlib import Path
import sys
import uuid
from datetime import timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import activity_matcher, posture_matcher, edge_device_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
MILKING_SCHEDULE = "a866bb82-e816-4327-99cf-ec2495ca1adc"  # Evening Milking, tolerance_late_min=120

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def snapshot_alert_log():
    with get_cursor() as cur:
        cur.execute("SELECT id FROM alert_log ORDER BY id")
        return sorted(r["id"] for r in cur.fetchall())


def test_activity_running_long_real_rule():
    """Synthetic IN_PROGRESS instance, started 36h ago on the real Evening
    Milking schedule -- guaranteed past ideal_end_time + tolerance_late_min
    regardless of when this test runs."""
    synthetic_id = str(uuid.uuid4())
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT id, severity, is_active FROM alert_rule "
                "WHERE farm_id = %s AND activity_schedule_id = %s "
                "AND name = 'ACTIVITY_RUNNING_LONG: Evening Milking'",
                (FARM_ID, MILKING_SCHEDULE),
            )
            rule = cur.fetchone()
        check("real ACTIVITY_RUNNING_LONG rule exists for Evening Milking", rule is not None)
        check("real rule severity is WARNING", rule and rule["severity"] == "WARNING")
        if not rule:
            return

        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, 1, %s, DATE '2019-03-01', %s, 'IN_PROGRESS', 'SYSTEM', NULL)
                """,
                (synthetic_id, FARM_ID, MILKING_SCHEDULE, utc_now() - timedelta(hours=36)),
            )

        result = activity_matcher.evaluate_in_progress_instance(synthetic_id)
        check("evaluator finds the synthetic instance", result.get("instance_found"))
        check("evaluator detects it as overdue", result.get("is_overdue") is True)
        check("evaluator created an alert for the real rule", rule["id"] in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute(
                "SELECT al.lifecycle_state, al.alert_type, ar.severity FROM alert_log al "
                "JOIN alert_rule ar ON ar.id = al.alert_rule_id WHERE al.dedup_key = %s",
                (synthetic_id,),
            )
            rows = cur.fetchall()
        check("exactly one alert_log row created", len(rows) == 1, f"got {len(rows)}")
        if rows:
            check("alert lifecycle_state is ACTIVE", rows[0]["lifecycle_state"] == "ACTIVE")
            check("alert severity is WARNING (from the real rule)", rows[0]["severity"] == "WARNING")

        # Finalize -> RUNNING_LONG must self-resolve.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE activity_instance SET status='ENDED', actual_end_at=%s, "
                "session_classification='LATE' WHERE id=%s",
                (utc_now(), synthetic_id),
            )
        activity_matcher.evaluate_finalized_instance(synthetic_id)
        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state FROM alert_log WHERE dedup_key = %s AND alert_rule_id = %s",
                (synthetic_id, rule["id"]),
            )
            row_after = cur.fetchone()
        check("alert self-resolves on finalization", row_after and row_after["lifecycle_state"] == "RESOLVED")

    finally:
        with get_cursor() as cur:
            cur.execute("DELETE FROM alert_log WHERE dedup_key = %s", (synthetic_id,))
            cur.execute("DELETE FROM activity_instance WHERE id = %s", (synthetic_id,))


def test_posture_data_stale_real_rule():
    """Synthetic farm_zone + one stale posture_observation -- never touches
    a real zone or real posture history."""
    zone_id = str(uuid.uuid4())
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT id, severity, is_active FROM alert_rule "
                "WHERE farm_id = %s AND name = 'POSTURE_DATA_STALE'",
                (FARM_ID,),
            )
            rule = cur.fetchone()
        check("real POSTURE_DATA_STALE rule exists", rule is not None)
        check("real rule severity is WARNING", rule and rule["severity"] == "WARNING")
        if not rule:
            return

        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO farm_zone (id, farm_id, name) VALUES (%s, %s, 'STEPD_TEST synthetic zone')",
                (zone_id, FARM_ID),
            )
            cur.execute(
                "INSERT INTO posture_observation (farm_id, zone_id, observed_at) VALUES (%s, %s, %s)",
                (FARM_ID, zone_id, utc_now() - timedelta(minutes=25)),
            )

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("evaluator finds the synthetic observation", result.get("observation_found"))
        check("evaluator computes age > 10 min", result.get("age_minutes", 0) > 10)
        check("evaluator created an alert for the real rule", rule["id"] in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state, zone_id FROM alert_log WHERE dedup_key = %s AND alert_rule_id = %s",
                (str(zone_id), rule["id"]),
            )
            row = cur.fetchone()
        check("alert lifecycle_state is ACTIVE", row and row["lifecycle_state"] == "ACTIVE")
        check("alert zone_id matches the synthetic zone", row and str(row["zone_id"]) == zone_id)

        # Fresh observation -> resolves.
        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO posture_observation (farm_id, zone_id, observed_at) VALUES (%s, %s, %s)",
                (FARM_ID, zone_id, utc_now()),
            )
        posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state FROM alert_log WHERE dedup_key = %s AND alert_rule_id = %s",
                (str(zone_id), rule["id"]),
            )
            row_after = cur.fetchone()
        check("alert resolves once fresh data arrives", row_after and row_after["lifecycle_state"] == "RESOLVED")

    finally:
        with get_cursor() as cur:
            cur.execute("DELETE FROM alert_log WHERE dedup_key = %s", (str(zone_id),))
            cur.execute("DELETE FROM posture_observation WHERE zone_id = %s", (zone_id,))
            cur.execute("DELETE FROM farm_zone WHERE id = %s", (zone_id,))


def test_edge_device_offline_real_rule():
    """Synthetic edge_device -- never touches the real device."""
    device_id = str(uuid.uuid4())
    try:
        with get_cursor() as cur:
            cur.execute(
                "SELECT id, severity, is_active FROM alert_rule "
                "WHERE farm_id = %s AND name = 'EDGE_DEVICE_OFFLINE'",
                (FARM_ID,),
            )
            rule = cur.fetchone()
        check("real EDGE_DEVICE_OFFLINE rule exists", rule is not None)
        check("real rule severity is CRITICAL", rule and rule["severity"] == "CRITICAL")
        if not rule:
            return

        with get_cursor() as cur:
            cur.execute(
                "INSERT INTO edge_device (id, farm_id, name, code, api_key_hash, "
                "is_active, last_seen_at) VALUES (%s, %s, 'STEPD_TEST synthetic device', "
                "%s, 'x', true, %s)",
                (device_id, FARM_ID, f"STEPD_TEST_{device_id[:8]}", utc_now() - timedelta(minutes=25)),
            )

        result = edge_device_matcher.evaluate_device_offline(device_id)
        check("evaluator finds the synthetic device", result.get("device_found"))
        check("evaluator computes age > 10 min", result.get("age_minutes", 0) > 10)
        check("evaluator created an alert for the real rule", rule["id"] in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state, device_id FROM alert_log WHERE dedup_key = %s AND alert_rule_id = %s",
                (str(device_id), rule["id"]),
            )
            row = cur.fetchone()
        check("alert lifecycle_state is ACTIVE", row and row["lifecycle_state"] == "ACTIVE")
        check("alert device_id matches the synthetic device", row and str(row["device_id"]) == device_id)

        # Fresh heartbeat -> resolves.
        with get_cursor() as cur:
            cur.execute("UPDATE edge_device SET last_seen_at = %s WHERE id = %s", (utc_now(), device_id))
        edge_device_matcher.evaluate_device_offline(device_id)
        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state FROM alert_log WHERE dedup_key = %s AND alert_rule_id = %s",
                (str(device_id), rule["id"]),
            )
            row_after = cur.fetchone()
        check("alert resolves once heartbeat is fresh again", row_after and row_after["lifecycle_state"] == "RESOLVED")

    finally:
        with get_cursor() as cur:
            cur.execute("DELETE FROM alert_log WHERE dedup_key = %s", (str(device_id),))
            cur.execute("DELETE FROM edge_device WHERE id = %s", (device_id,))


if __name__ == "__main__":
    before = snapshot_alert_log()

    test_activity_running_long_real_rule()
    test_posture_data_stale_real_rule()
    test_edge_device_offline_real_rule()

    after = snapshot_alert_log()
    check("production alert_log identical before/after (no contamination)", before == after,
          detail=f"before={before} after={after}")

    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
