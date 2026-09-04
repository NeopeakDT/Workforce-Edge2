"""
backend/scripts/test_activity_missed_rule_seed.py
Alert Rule Seed Completion -- ACTIVITY_MISSED.

Proves the newly-seeded production ACTIVITY_MISSED alert_rule rows
(ops/seed_alert_rules_activity_missed.sql) actually work end-to-end against
the real evaluator, using a synthetic activity_instance (never a real
production instance) so no production alert is manufactured.

This is deliberately narrower than scripts/test_stepB_alert_evaluator.py's
test_activity_missed(), which creates its OWN temporary alert_rule to prove
the evaluator logic in isolation. This test creates no rule at all -- it
uses the real seeded "ACTIVITY_MISSED: Morning Scrapping" rule as-is (read
only, never modified) and only fabricates the activity_instance side, to
prove the actual production configuration this task added is wired
correctly.

Fixture: one synthetic activity_instance at a fixed past date (2020-06-16,
distinct from the 2020-06-15 date already used by other Step D tests, to
avoid any accidental collision) for the real "Morning Scrapping" schedule,
with session_classification='MISSED'. Cleaned up in finally regardless of
outcome. The real seeded alert_rule row is read but never inserted,
updated, or deleted by this script.

Run: python scripts/test_activity_missed_rule_seed.py
"""

from pathlib import Path
import sys
import uuid

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import activity_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"
ACTIVITY_TYPE_SCRAPPING = 3
FIXTURE_DATE = "2020-06-16"  # distinct from other Step D tests' 2020-06-15

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_missed_instance(cur):
    iid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, actual_end_at, status, source, zone_id, session_classification)
        VALUES (%s, %s, %s, %s, DATE %s, %s, %s, 'ENDED', 'SYSTEM', %s, 'MISSED')
        """,
        (iid, FARM_ID, ACTIVITY_TYPE_SCRAPPING, SCRAP_MORNING_SCHEDULE, FIXTURE_DATE,
         utc_now(), utc_now(), ZONE_ID),
    )
    return iid


def cleanup(instance_id):
    with get_cursor() as cur:
        if instance_id:
            cur.execute("DELETE FROM alert_log WHERE dedup_key = %s", (instance_id,))
            cur.execute("DELETE FROM activity_instance WHERE id = %s", (instance_id,))


def test_real_seeded_rule_fires_on_missed_instance():
    instance_id = None
    try:
        # Confirm the real seeded rule exists and read its id, without touching it.
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT id, severity, is_active FROM alert_rule
                WHERE farm_id = %s AND activity_schedule_id = %s
                  AND name = 'ACTIVITY_MISSED: Morning Scrapping'
                """,
                (FARM_ID, SCRAP_MORNING_SCHEDULE),
            )
            rule = cur.fetchone()
        check("real ACTIVITY_MISSED rule exists for Morning Scrapping", rule is not None)
        check("real rule severity is CRITICAL", rule and rule["severity"] == "CRITICAL")
        check("real rule is_active", rule and rule["is_active"] is True)
        if not rule:
            return

        with get_cursor() as cur:
            instance_id = make_missed_instance(cur)

        result = activity_matcher.evaluate_finalized_instance(instance_id)
        check("evaluator finds the synthetic instance", result.get("instance_found"))
        check("evaluator created an alert for the real rule", rule["id"] in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute(
                "SELECT al.lifecycle_state, al.alert_type, ar.severity FROM alert_log al "
                "JOIN alert_rule ar ON ar.id = al.alert_rule_id "
                "WHERE al.dedup_key = %s",
                (instance_id,),
            )
            rows = cur.fetchall()
        check("exactly one alert_log row created", len(rows) == 1, f"got {len(rows)}")
        if rows:
            check("alert lifecycle_state is ACTIVE", rows[0]["lifecycle_state"] == "ACTIVE")
            check("alert alert_type is ACTIVITY", rows[0]["alert_type"] == "ACTIVITY")
            check("alert severity is CRITICAL (from the real rule)", rows[0]["severity"] == "CRITICAL")

        # Re-evaluation must not duplicate (partial unique index on ACTIVE dedup).
        activity_matcher.evaluate_finalized_instance(instance_id)
        with get_cursor() as cur:
            cur.execute("SELECT count(*) AS c FROM alert_log WHERE dedup_key = %s", (instance_id,))
            count_after = cur.fetchone()["c"]
        check("re-evaluation does not duplicate the alert", count_after == 1, f"got {count_after}")

    finally:
        cleanup(instance_id)


if __name__ == "__main__":
    test_real_seeded_rule_fires_on_missed_instance()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
