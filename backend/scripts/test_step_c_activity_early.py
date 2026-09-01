"""
backend/scripts/test_step_c_activity_early.py
STEP C — Standalone integration test for activity_matcher.evaluate_early_start().

Run: python scripts/test_step_c_activity_early.py

Creates/deletes only its own test fixtures (alert_rule rows + synthetic
activity_instance rows); no production data is mutated.

All "minutes early" offsets below are computed from the REAL
activity_schedule.ideal_start_time row (queried at run time, not hardcoded
IST-offset arithmetic) via pytz, per the Step C review correction: computing
ideal_start_local from the real schedule row is less fragile than assuming a
fixed UTC offset for the farm's timezone.
"""

from pathlib import Path
import sys
import uuid
from datetime import date, datetime, timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import pytz
from psycopg2.extras import Json
from common.db import get_cursor
from alerts.matchers import activity_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
# Morning Scrapping: ideal_start_time 05:00 IST
SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"
# activity_instance carries a uq_missed_schedule_per_day unique constraint on
# (farm_id, activity_schedule_id, activity_date). This farm's real
# missed_activity_cron pipeline is live in this environment and creates a
# genuine session_classification='MISSED' row for this exact schedule for
# "today" every day shortly after its ideal_start_time passes -- so
# date.today() collides with real production data. A fixed, far-past date
# with no real activity_instance rows keeps this test isolated from that
# production data without ever touching it. evaluate_early_start uses the
# instance's own activity_date column directly (Correction 3), never
# "today", so any date works correctly here.
TEST_ACTIVITY_DATE = date(2020, 6, 15)

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def get_ideal_start_local(cur, activity_date):
    """Real schedule row + real farm timezone -> tz-aware ideal_start_local for activity_date."""
    cur.execute("SELECT timezone FROM farm WHERE id = %s", (FARM_ID,))
    farm_tz = pytz.timezone(cur.fetchone()["timezone"])
    cur.execute("SELECT ideal_start_time FROM activity_schedule WHERE id = %s", (SCRAP_MORNING_SCHEDULE,))
    ideal_start_time = cur.fetchone()["ideal_start_time"]
    naive = datetime.combine(activity_date, ideal_start_time)
    return farm_tz.localize(naive)


def make_rule(cur, threshold_min=30):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 3, %s, 'STEPC_TEST Early Scrapping',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, SCRAP_MORNING_SCHEDULE,
         Json({"metric": "minutes_before_ideal_start", "operator": ">", "value": threshold_min})),
    )
    return rule_id


def make_late_rule(cur):
    """A LATE-metric rule scoped to the same schedule, used to prove EARLY never
    creates a stray ACTIVE LATE alert (Test H)."""
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 3, %s, 'STEPC_TEST Late Scrapping (early-test sentinel)',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, SCRAP_MORNING_SCHEDULE,
         Json({"metric": "minutes_since_ideal_start", "operator": ">", "value": 0})),
    )
    return rule_id


def make_instance(cur, actual_start_at, activity_date):
    iid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, status, source, zone_id)
        VALUES (%s, %s, 3, %s, %s, %s, 'IN_PROGRESS', 'SYSTEM', %s)
        """,
        (iid, FARM_ID, SCRAP_MORNING_SCHEDULE, activity_date, actual_start_at, ZONE_ID),
    )
    return iid


def cleanup(rule_ids, instance_ids):
    # NOTE: this farm already has real (non-STEPC_TEST) alert_rule rows,
    # seeded by Task 2 (ops/seed_alert_rules_step_c.sql), for
    # minutes_before_ideal_start / minutes_since_ideal_start on this exact
    # schedule ("ACTIVITY_EARLY: Morning Scrapping" etc.). Those real rules
    # legitimately also match our synthetic activity_instance rows (they
    # are scoped by farm/type/schedule, not by rule name), so they create
    # their own alert_log rows referencing our synthetic instance via its
    # FK. Deleting alert_log by instance_id (in addition to by our own
    # rule_ids) is required to cleanly delete the synthetic instance --
    # this only removes alert_log rows tied to OUR synthetic (fictitious)
    # instance, never any alert_log row tied to real activity data.
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if instance_ids:
            cur.execute("DELETE FROM alert_log WHERE activity_instance_id = ANY(%s::uuid[])", (instance_ids,))
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


def test_early_start_triggers():
    rule_ids, instance_ids = [], []
    try:
        today = TEST_ACTIVITY_DATE
        with get_cursor() as cur:
            rule_id = make_rule(cur)
            ideal_start_local = get_ideal_start_local(cur, today)
        rule_ids.append(rule_id)

        # 40 minutes early -> above the 30-minute threshold, must fire.
        start_local = ideal_start_local - timedelta(minutes=40)
        early_start_utc = start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid = make_instance(cur, early_start_utc, today)
        instance_ids.append(iid)

        result = activity_matcher.evaluate_early_start(iid)
        check("ACTIVITY_EARLY: instance found", result["instance_found"])
        check("ACTIVITY_EARLY: alert created", rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_EARLY: exactly one row", len(rows) == 1, f"got {len(rows)}")
        if rows:
            check("ACTIVITY_EARLY: immediately RESOLVED", rows[0]["lifecycle_state"] == "RESOLVED")
            check("ACTIVITY_EARLY: resolved_at set", rows[0]["resolved_at"] is not None)
            check("ACTIVITY_EARLY: dedup_key is instance id", rows[0]["dedup_key"] == iid)
            # severity lives on alert_rule, not alert_log -- confirm the created
            # row's alert_rule_id matches the rule we made (which carries
            # severity='WARNING'), which is the only cross-check available here.
            check("ACTIVITY_EARLY: alert_log.alert_rule_id matches the firing rule",
                  rows[0]["alert_rule_id"] == uuid.UUID(rule_id) or str(rows[0]["alert_rule_id"]) == rule_id)
    finally:
        cleanup(rule_ids, instance_ids)


def test_within_threshold_no_alert():
    rule_ids, instance_ids = [], []
    try:
        today = TEST_ACTIVITY_DATE
        with get_cursor() as cur:
            rule_id = make_rule(cur)
            ideal_start_local = get_ideal_start_local(cur, today)
        rule_ids.append(rule_id)

        # Only 10 minutes early -> below the 30-minute threshold, must not fire.
        start_local = ideal_start_local - timedelta(minutes=10)
        near_ideal_start_utc = start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid = make_instance(cur, near_ideal_start_utc, today)
        instance_ids.append(iid)

        result = activity_matcher.evaluate_early_start(iid)
        check(
            "ACTIVITY_EARLY: within-threshold start does not fire",
            rule_id not in result.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids, instance_ids)


def test_repeated_evaluation_does_not_duplicate():
    """Test C: calling evaluate_early_start twice on the same instance/rule must
    result in exactly one alert_log row -- proves the local dedup guard added
    in activity_matcher.evaluate_early_start (Correction 2) works, since
    upsert_active_alert's own ACTIVE-only guard cannot dedupe a RESOLVED
    point-in-time insert."""
    rule_ids, instance_ids = [], []
    try:
        today = TEST_ACTIVITY_DATE
        with get_cursor() as cur:
            rule_id = make_rule(cur)
            ideal_start_local = get_ideal_start_local(cur, today)
        rule_ids.append(rule_id)

        start_local = ideal_start_local - timedelta(minutes=40)
        early_start_utc = start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid = make_instance(cur, early_start_utc, today)
        instance_ids.append(iid)

        result1 = activity_matcher.evaluate_early_start(iid)
        result2 = activity_matcher.evaluate_early_start(iid)
        check("ACTIVITY_EARLY (repeat): first call creates alert", rule_id in result1.get("alerts_created", []))
        check("ACTIVITY_EARLY (repeat): second call does not re-create", rule_id not in result2.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_EARLY (repeat): exactly one alert_log row after two evaluations", len(rows) == 1, f"got {len(rows)}")
    finally:
        cleanup(rule_ids, instance_ids)


def test_early_does_not_create_active_late():
    """Test H: an EARLY-firing instance must never produce an ACTIVE row for a
    LATE-metric rule on the same schedule. evaluate_early_start only ever
    touches minutes_before_ideal_start rules and never calls
    evaluate_late_start_sweep, so a sentinel LATE rule must show zero rows."""
    early_rule_ids, late_rule_ids, instance_ids = [], [], []
    try:
        today = TEST_ACTIVITY_DATE
        with get_cursor() as cur:
            early_rule_id = make_rule(cur)
            late_rule_id = make_late_rule(cur)
            ideal_start_local = get_ideal_start_local(cur, today)
        early_rule_ids.append(early_rule_id)
        late_rule_ids.append(late_rule_id)

        start_local = ideal_start_local - timedelta(minutes=40)
        early_start_utc = start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid = make_instance(cur, early_start_utc, today)
        instance_ids.append(iid)

        result = activity_matcher.evaluate_early_start(iid)
        check("ACTIVITY_EARLY (H): EARLY alert created", early_rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute(
                "SELECT * FROM alert_log WHERE alert_rule_id = %s AND lifecycle_state = 'ACTIVE'",
                (late_rule_id,),
            )
            late_active_rows = cur.fetchall()
        check("ACTIVITY_EARLY (H): no ACTIVE alert_log row for the LATE rule", len(late_active_rows) == 0)

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (late_rule_id,))
            late_any_rows = cur.fetchall()
        check("ACTIVITY_EARLY (H): no alert_log row of any kind for the LATE rule", len(late_any_rows) == 0)
    finally:
        cleanup(early_rule_ids + late_rule_ids, instance_ids)


def test_boundary_exactly_at_threshold_does_not_fire():
    """Test I: strict '>' semantics (per alert_conditions.condition_matches,
    which uses observed > value). Exactly 30 minutes early (the configured
    threshold) must NOT fire; 31 minutes early must fire.

    activity_instance has a uq_missed_schedule_per_day unique constraint on
    (farm_id, activity_schedule_id, activity_date), so the two instances in
    this test cannot coexist -- the first is created, evaluated, and deleted
    before the second is created."""
    rule_ids, instance_ids = [], []
    try:
        today = TEST_ACTIVITY_DATE
        with get_cursor() as cur:
            rule_id = make_rule(cur, threshold_min=30)
            ideal_start_local = get_ideal_start_local(cur, today)
        rule_ids.append(rule_id)

        # Exactly at the threshold -> must not fire.
        boundary_start_local = ideal_start_local - timedelta(minutes=30)
        boundary_start_utc = boundary_start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid_boundary = make_instance(cur, boundary_start_utc, today)

        result_boundary = activity_matcher.evaluate_early_start(iid_boundary)
        check(
            "ACTIVITY_EARLY (I): exactly 30 minutes early does not fire (strict >)",
            rule_id not in result_boundary.get("alerts_created", []),
        )
        with get_cursor() as cur:
            cur.execute("DELETE FROM activity_instance WHERE id = %s", (iid_boundary,))

        # One minute past the threshold -> must fire.
        over_start_local = ideal_start_local - timedelta(minutes=31)
        over_start_utc = over_start_local.astimezone(pytz.utc)
        with get_cursor() as cur:
            iid_over = make_instance(cur, over_start_utc, today)
        instance_ids.append(iid_over)

        result_over = activity_matcher.evaluate_early_start(iid_over)
        check(
            "ACTIVITY_EARLY (I): 31 minutes early fires",
            rule_id in result_over.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids, instance_ids)


if __name__ == "__main__":
    test_early_start_triggers()
    test_within_threshold_no_alert()
    test_repeated_evaluation_does_not_duplicate()
    test_early_does_not_create_active_late()
    test_boundary_exactly_at_threshold_does_not_fire()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
