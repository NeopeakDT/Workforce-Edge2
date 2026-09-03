"""
backend/scripts/test_step_d1_alerts_query.py
Step D1 — dashboard_query_service.list_recent_alerts() regression test.

Proves, against the real database (no mocks — this is a JOIN-correctness
test, which a mocked cursor cannot verify):
  - ACTIVE alerts are returned by default.
  - RESOLVED alerts are returned when explicitly requested.
  - activity_instance_id IS NULL alerts are returned (the LEFT JOIN fix --
    this is the actual regression the INNER JOIN caused).
  - ACTIVITY alerts WITH an instance still return correctly (LEFT JOIN
    must not break the existing instance-keyed case).
  - lifecycle_state, alert_type, dedup_key, zone_id, device_id are present
    and correct in the returned rows.
  - lifecycle_state=None returns both ACTIVE and RESOLVED.
  - The existing caller's old-style call (positional farm_id, default
    limit, no lifecycle_state arg) still works -- backward compatible.

Creates only its own STEPD_TEST-named alert_rule + alert_log rows;
references real farm/zone/device/schedule ids as FK targets only (never
mutated), matching the existing Step B/C test convention. All fixtures
are deleted in a finally block regardless of outcome.

Run: python scripts/test_step_d1_alerts_query.py
"""

from pathlib import Path
import sys
import uuid

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from dashboard.dashboard_query_service import list_recent_alerts

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"
SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_rule(cur, *, alert_type, name, severity="WARNING"):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, true)
        """,
        (rule_id, FARM_ID, name, Json({"metric": "test", "operator": ">", "value": 0}), severity, alert_type),
    )
    return rule_id


def make_instance(cur):
    # Fixed far-past date (verified empty of real rows, matching the
    # existing Step C test convention) to avoid uq_missed_schedule_per_day
    # colliding with today's real production instance for this schedule.
    iid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, status, source, zone_id)
        VALUES (%s, %s, 3, %s, DATE '2020-06-15', %s, 'IN_PROGRESS', 'SYSTEM', %s)
        """,
        (iid, FARM_ID, SCRAP_MORNING_SCHEDULE, utc_now(), ZONE_ID),
    )
    return iid


def make_alert_log(cur, *, rule_id, alert_type, lifecycle_state, dedup_key, activity_instance_id=None,
                    zone_id=None, device_id=None):
    alert_id = str(uuid.uuid4())
    now = utc_now()
    resolved_at = now if lifecycle_state == "RESOLVED" else None
    cur.execute(
        """
        INSERT INTO alert_log
            (id, farm_id, alert_rule_id, alert_type, activity_instance_id, zone_id, device_id,
             triggered_at, status, lifecycle_state, resolved_at, dedup_key, message, details)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'SENT', %s, %s, %s, %s, %s)
        """,
        (alert_id, FARM_ID, rule_id, alert_type, activity_instance_id, zone_id, device_id,
         now, lifecycle_state, resolved_at, dedup_key, "STEPD_TEST alert", Json({})),
    )
    return alert_id


def cleanup(rule_ids, instance_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if instance_ids:
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


def test_full_matrix():
    rule_ids, instance_ids = [], []
    try:
        with get_cursor() as cur:
            # Device-keyed rule/alert -- activity_instance_id IS NULL, the exact
            # case the old INNER JOIN silently dropped.
            device_rule = make_rule(cur, alert_type="EDGE_DEVICE", name="STEPD_TEST device offline", severity="CRITICAL")
            rule_ids.append(device_rule)
            active_no_instance = make_alert_log(
                cur, rule_id=device_rule, alert_type="EDGE_DEVICE", lifecycle_state="ACTIVE",
                dedup_key=str(uuid.uuid4()), device_id=DEVICE_ID,
            )
            resolved_no_instance = make_alert_log(
                cur, rule_id=device_rule, alert_type="EDGE_DEVICE", lifecycle_state="RESOLVED",
                dedup_key=str(uuid.uuid4()), device_id=DEVICE_ID,
            )

            # Instance-keyed rule/alert -- the pre-existing, already-working case.
            iid = make_instance(cur)
            instance_ids.append(iid)
            activity_rule = make_rule(cur, alert_type="ACTIVITY", name="STEPD_TEST activity late", severity="WARNING")
            rule_ids.append(activity_rule)
            active_with_instance = make_alert_log(
                cur, rule_id=activity_rule, alert_type="ACTIVITY", lifecycle_state="ACTIVE",
                dedup_key=str(iid), activity_instance_id=iid, zone_id=ZONE_ID,
            )

        # --- Default call: lifecycle_state defaults to ACTIVE ---
        default_rows = {r["id"]: r for r in list_recent_alerts(FARM_ID, limit=200)}
        check("default call returns the ACTIVE device alert (no instance)",
              active_no_instance in default_rows)
        check("default call returns the ACTIVE activity alert (with instance)",
              active_with_instance in default_rows)
        check("default call does NOT return the RESOLVED device alert",
              resolved_no_instance not in default_rows)

        # --- Old-style positional call (backward compatibility) ---
        old_style_rows = {r["id"]: r for r in list_recent_alerts(FARM_ID)}
        check("old-style positional call (farm_id only) still works and defaults to ACTIVE",
              active_no_instance in old_style_rows and resolved_no_instance not in old_style_rows)

        # --- Explicit RESOLVED ---
        resolved_rows = {r["id"]: r for r in list_recent_alerts(FARM_ID, lifecycle_state="RESOLVED", limit=200)}
        check("explicit lifecycle_state='RESOLVED' returns the resolved device alert",
              resolved_no_instance in resolved_rows)
        check("explicit lifecycle_state='RESOLVED' excludes the ACTIVE alerts",
              active_no_instance not in resolved_rows and active_with_instance not in resolved_rows)

        # --- lifecycle_state=None returns both ---
        both_rows = {r["id"]: r for r in list_recent_alerts(FARM_ID, lifecycle_state=None, limit=200)}
        check("lifecycle_state=None returns both ACTIVE and RESOLVED",
              active_no_instance in both_rows and resolved_no_instance in both_rows
              and active_with_instance in both_rows)

        # --- Field correctness on the no-instance (LEFT JOIN) row ---
        row = default_rows[active_no_instance]
        check("no-instance row: activity_instance_id is None", row["activity_instance_id"] is None)
        check("no-instance row: activity_type_id is None (LEFT JOIN, not dropped)", row["activity_type_id"] is None)
        check("no-instance row: alert_type == 'EDGE_DEVICE'", row["alert_type"] == "EDGE_DEVICE")
        check("no-instance row: lifecycle_state == 'ACTIVE'", row["lifecycle_state"] == "ACTIVE")
        check("no-instance row: severity == 'CRITICAL'", row["severity"] == "CRITICAL")
        check("no-instance row: device_id matches", str(row["device_id"]) == DEVICE_ID)
        check("no-instance row: dedup_key is present", row["dedup_key"] is not None)
        check("no-instance row: resolved_at is None (still ACTIVE)", row["resolved_at"] is None)

        # --- Field correctness on the with-instance row ---
        row2 = default_rows[active_with_instance]
        check("with-instance row: activity_instance_id matches", str(row2["activity_instance_id"]) == iid)
        check("with-instance row: activity_type_id == 3", row2["activity_type_id"] == 3)
        check("with-instance row: activity_schedule_id matches", str(row2["activity_schedule_id"]) == SCRAP_MORNING_SCHEDULE)
        check("with-instance row: alert_type == 'ACTIVITY'", row2["alert_type"] == "ACTIVITY")
        check("with-instance row: zone_id matches", str(row2["zone_id"]) == ZONE_ID)

        # --- Field correctness on the resolved row ---
        row3 = resolved_rows[resolved_no_instance]
        check("resolved row: resolved_at is set", row3["resolved_at"] is not None)
        check("resolved row: lifecycle_state == 'RESOLVED'", row3["lifecycle_state"] == "RESOLVED")

    finally:
        cleanup(rule_ids, instance_ids)


def test_existing_test_step6_dashboard_call_shape_unbroken():
    """The only other existing caller (scripts/test_step6_dashboard.py) calls
    list_recent_alerts(FARM_ID) with no other args and just iterates the
    result -- confirm that shape still works with zero fixtures present."""
    rows = list_recent_alerts(FARM_ID)
    check("plain list_recent_alerts(FARM_ID) call does not raise, returns a list",
          isinstance(rows, list))


if __name__ == "__main__":
    test_full_matrix()
    test_existing_test_step6_dashboard_call_shape_unbroken()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
