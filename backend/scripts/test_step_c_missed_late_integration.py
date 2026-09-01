"""
backend/scripts/test_step_c_missed_late_integration.py
STEP C / Task 4 — Integration tests for the ACTIVITY_MISSED <-> ACTIVITY_LATE
wiring added in missed_activity_cron.py and activity_matcher.py:

  - missed_activity_cron.py:detect_missed_activities() now captures newly
    created MISSED rows and, AFTER the outer commit, calls
    activity_matcher.resolve_late_start_alerts_for_occurrence() (new) then
    activity_matcher.evaluate_finalized_instance() (existing) for each.
  - activity_matcher.py:resolve_late_start_alerts_for_occurrence() (new).
  - activity_matcher.py:evaluate_late_start_sweep()'s create branch now
    re-checks for a just-appeared activity_instance immediately after its
    own INSERT and self-corrects (Part F(b)).

Follows this repo's existing test convention (standalone script, no pytest,
check()/PASS-FAIL, real DB, fixtures cleaned up in `finally`). Uses fully
synthetic activity_schedule/activity_instance/alert_rule/alert_log fixtures
(own uuids, own schedule rows) rather than picking a real live schedule --
this keeps every test deterministic and independent of wall-clock timing
against production schedules, and avoids any collision with the live
missed_activity_cron / alerts_cron processes that are running in this
environment (see test_step_c_activity_late.py's docstring for why that
matters). The one exception is test_end_to_end_cron_wiring(), which
deliberately calls the real detect_missed_activities() against a
synthetic schedule rigged so its MISSED cutoff has already passed --
that function scans ALL active schedules farm-wide with no farm_id
filter, so it will also process real production schedules exactly as the
live cron already does; this test only asserts on its own synthetic
schedule's outcome and touches no one else's data.

Run: python scripts/test_step_c_missed_late_integration.py
"""

from pathlib import Path
import sys
import uuid
from datetime import date, time, timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import pytz
from psycopg2.extras import Json

from common.db import get_cursor
from common.time_utils import utc_now
from alerts.alert_conditions import upsert_active_alert, resolve_active_occurrence
from alerts.matchers import activity_matcher
from aggregation.missed_activity_cron import detect_missed_activities

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
ACTIVITY_TYPE_ID = 3  # Scrapping -- reused elsewhere in Step C tests

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


# -------------------------------------------------------------------------
# Fixture helpers
# -------------------------------------------------------------------------
def get_farm_today_local():
    with get_cursor() as cur:
        cur.execute("SELECT timezone FROM farm WHERE id = %s", (FARM_ID,))
        farm_tz = pytz.timezone(cur.fetchone()["timezone"])
    now_local = utc_now().astimezone(farm_tz)
    return farm_tz, now_local.date()


def make_schedule(cur, ideal_start_time=time(9, 0), ideal_end_time=time(9, 30), tolerance_late_min=15):
    schedule_id = str(uuid.uuid4())
    # activity_schedule has a UNIQUE(farm_id, activity_type_id, label) constraint
    # -- give every fixture a unique label (schedule_id embedded) so parallel
    # fixtures within one test (schedule A/B) and across test functions never
    # collide on label alone.
    label = f"STEPC_TEST4 synthetic schedule {schedule_id}"
    cur.execute(
        """
        INSERT INTO activity_schedule
            (id, farm_id, activity_type_id, label, ideal_start_time, ideal_end_time,
             tolerance_early_min, tolerance_late_min, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, 15, %s, true)
        """,
        (schedule_id, FARM_ID, ACTIVITY_TYPE_ID, label, ideal_start_time, ideal_end_time, tolerance_late_min),
    )
    return schedule_id


def make_late_rule(cur, schedule_id, value=0, operator=">"):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPC_TEST4 Late Sweep',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id,
         Json({"metric": "minutes_since_ideal_start", "operator": operator, "value": value})),
    )
    return rule_id


def make_classification_rule(cur, schedule_id, value):
    """value in {'LATE','MISSED'} -- finalization-style rule used by Test 5
    to exercise the EXISTING (unmodified) schedule-wide recovery path."""
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPC_TEST4 Classification',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id,
         Json({"metric": "session_classification", "operator": "=", "value": value})),
    )
    return rule_id


def seed_active_alert(rule_id, dedup_key, activity_instance_id=None):
    """Seeds an ACTIVE alert_log row directly via the shared helper (not raw
    SQL) so it goes through the exact same idempotency/shape logic as
    production code. activity_instance_id=None is legal per
    STEP7_RELAX_ACTIVITY_ALERT_INSTANCE_CHECK.sql as long as dedup_key is
    set (verified live per the controller's pre-implementation findings)."""
    upsert_active_alert(
        farm_id=FARM_ID,
        rule={"id": rule_id, "alert_type": "ACTIVITY"},
        dedup_key=dedup_key,
        message="STEPC_TEST4 seeded ACTIVE alert",
        details={},
        activity_instance_id=activity_instance_id,
        zone_id=ZONE_ID,
    )


def get_alert_state(rule_id, dedup_key):
    with get_cursor() as cur:
        cur.execute(
            "SELECT lifecycle_state FROM alert_log WHERE alert_rule_id = %s AND dedup_key = %s",
            (rule_id, dedup_key),
        )
        rows = cur.fetchall()
    return rows


def insert_missed_instance(cur, schedule_id, activity_date):
    """Direct insert shaped exactly like missed_activity_cron.py's own
    synthetic MISSED row (status='ENDED', session_classification='MISSED',
    source='SYSTEM') -- used where the test needs full control over which
    occurrence gets a MISSED row without depending on wall-clock cutoffs."""
    instance_id = str(uuid.uuid4())
    now = utc_now()
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, actual_end_at, actual_duration_sec,
             started_offset_min, ended_offset_min, within_ideal_window,
             status, session_classification, source, created_at, updated_at)
        VALUES (%s, %s, %s, %s, %s, NULL, NULL, 0, NULL, NULL, FALSE,
                'ENDED', 'MISSED', 'SYSTEM', %s, %s)
        """,
        (instance_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id, activity_date, now, now),
    )
    return instance_id


def cleanup(schedule_ids=(), rule_ids=(), instance_ids=()):
    """Deletes fixtures. Also deletes ANY activity_instance row tied to one
    of our synthetic schedule_ids, not just the tracked instance_ids -- the
    live production missed_activity_cron/aggregation pipeline in this
    environment (see test_step_c_activity_late.py's docstring) can pick up
    our synthetic is_active=true schedules between fixture creation and
    cleanup and insert its own row for them, which would otherwise violate
    the activity_instance_activity_schedule_id_fkey FK when the schedule
    itself is deleted below."""
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (list(rule_ids),))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (list(rule_ids),))
        if instance_ids:
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (list(instance_ids),))
        if schedule_ids:
            cur.execute(
                "DELETE FROM activity_instance WHERE activity_schedule_id = ANY(%s::uuid[])",
                (list(schedule_ids),),
            )
            cur.execute("DELETE FROM activity_schedule WHERE id = ANY(%s::uuid[])", (list(schedule_ids),))


# -------------------------------------------------------------------------
# Test 1: LATE resolved by MISSED (same occurrence)
# -------------------------------------------------------------------------
def test_late_resolved_by_missed():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        _, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(cur)
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id)
        rule_ids.append(rule_id)

        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        seed_active_alert(rule_id, dedup_key)
        rows = get_alert_state(rule_id, dedup_key)
        check("Test1: ACTIVE LATE alert seeded", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        with get_cursor() as cur:
            instance_id = insert_missed_instance(cur, schedule_id, today_local)
        instance_ids.append(instance_id)

        resolved = activity_matcher.resolve_late_start_alerts_for_occurrence(
            FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local
        )
        check("Test1: resolve_late_start_alerts_for_occurrence resolved 1", resolved == 1, detail=f"got {resolved}")

        rows_after = get_alert_state(rule_id, dedup_key)
        check(
            "Test1: LATE alert now RESOLVED",
            len(rows_after) == 1 and rows_after[0]["lifecycle_state"] == "RESOLVED",
        )
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 2: MISSED for one occurrence must not resolve a different occurrence
# -------------------------------------------------------------------------
def test_same_occurrence_only():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        _, today_local = get_farm_today_local()
        yesterday_local = today_local - timedelta(days=1)

        with get_cursor() as cur:
            schedule_a = make_schedule(cur)
            schedule_b = make_schedule(cur)
        schedule_ids.extend([schedule_a, schedule_b])

        with get_cursor() as cur:
            rule_a = make_late_rule(cur, schedule_a)
            rule_b = make_late_rule(cur, schedule_b)
        rule_ids.extend([rule_a, rule_b])

        dedup_a_today = f"{schedule_a}:{today_local.isoformat()}"
        dedup_a_yesterday = f"{schedule_a}:{yesterday_local.isoformat()}"
        dedup_b_today = f"{schedule_b}:{today_local.isoformat()}"

        seed_active_alert(rule_a, dedup_a_today)
        seed_active_alert(rule_a, dedup_a_yesterday)
        seed_active_alert(rule_b, dedup_b_today)

        with get_cursor() as cur:
            instance_id = insert_missed_instance(cur, schedule_a, today_local)
        instance_ids.append(instance_id)

        activity_matcher.resolve_late_start_alerts_for_occurrence(
            FARM_ID, ACTIVITY_TYPE_ID, schedule_a, today_local
        )

        rows_a_today = get_alert_state(rule_a, dedup_a_today)
        rows_a_yesterday = get_alert_state(rule_a, dedup_a_yesterday)
        rows_b_today = get_alert_state(rule_b, dedup_b_today)

        check("Test2: schedule A/today RESOLVED", rows_a_today[0]["lifecycle_state"] == "RESOLVED")
        check("Test2: schedule A/yesterday still ACTIVE (different date)", rows_a_yesterday[0]["lifecycle_state"] == "ACTIVE")
        check("Test2: schedule B/today still ACTIVE (different schedule)", rows_b_today[0]["lifecycle_state"] == "ACTIVE")
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 3: LATE does not return after MISSED (repeated sweep)
# -------------------------------------------------------------------------
def test_late_does_not_return_after_missed():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        farm_tz, today_local = get_farm_today_local()
        # ideal_start_time comfortably in the past today so minutes_since > 0.
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, ideal_start_time=time(0, 1))
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id, value=0)
        rule_ids.append(rule_id)

        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        with get_cursor() as cur:
            instance_id = insert_missed_instance(cur, schedule_id, today_local)
        instance_ids.append(instance_id)

        result = activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        check("Test3: sweep did not create alert for occupied occurrence", rule_id not in result["alerts_created"])

        rows = get_alert_state(rule_id, dedup_key)
        check("Test3: no ACTIVE row exists for the occurrence", not any(r["lifecycle_state"] == "ACTIVE" for r in rows))

        # Run again to be sure repeated sweeps stay stable.
        activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        rows_again = get_alert_state(rule_id, dedup_key)
        check(
            "Test3: repeated sweep still creates no ACTIVE row",
            not any(r["lifecycle_state"] == "ACTIVE" for r in rows_again),
        )
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 4: activity starts before MISSED -- already covered by Task 3's own
# tests (test_step_c_activity_late.py::test_late_resolves_when_instance_appears).
# Included here only as a light confirmatory check using our own fixtures.
# -------------------------------------------------------------------------
def test_late_resolves_when_real_instance_appears():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        farm_tz, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, ideal_start_time=time(0, 1))
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id, value=0)
        rule_ids.append(rule_id)

        result = activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        check("Test4: sweep fires before instance exists", rule_id in result["alerts_created"])

        with get_cursor() as cur:
            iid = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, %s, %s, %s, %s, 'IN_PROGRESS', 'SYSTEM', %s)
                """,
                (iid, FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local, utc_now(), ZONE_ID),
            )
        instance_ids.append(iid)

        activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        rows = get_alert_state(rule_id, dedup_key)
        check("Test4: LATE resolves once a real instance exists", rows[0]["lifecycle_state"] == "RESOLVED")
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 5: activity starts after MISSED -- EXISTING (Step B, unmodified)
# schedule-wide recovery via evaluate_finalized_instance(). Not new Task 4
# logic; verified here only to confirm it still functions with a MISSED
# row's classification later corrected in place (as activity_aggregator.py
# would eventually do), since uq_missed_schedule_per_day forbids a second
# INSERT for the same occurrence.
# -------------------------------------------------------------------------
def test_existing_missed_recovery_still_works():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        _, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(cur)
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            instance_id = insert_missed_instance(cur, schedule_id, today_local)
        instance_ids.append(instance_id)

        with get_cursor() as cur:
            class_rule_id = make_classification_rule(cur, schedule_id, "MISSED")
        rule_ids.append(class_rule_id)

        # Schedule-wide recovery is keyed by instance id as dedup_key for the
        # classification alert itself (per evaluate_finalized_instance).
        seed_active_alert(class_rule_id, str(instance_id), activity_instance_id=instance_id)
        rows = get_alert_state(class_rule_id, str(instance_id))
        check("Test5: MISSED classification alert seeded ACTIVE", rows[0]["lifecycle_state"] == "ACTIVE")

        # Simulate activity_aggregator.py's eventual backfill correcting the
        # existing MISSED row's classification once real footage is found.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE activity_instance SET session_classification = 'ON_TIME', status='ENDED' WHERE id = %s",
                (instance_id,),
            )

        activity_matcher.evaluate_finalized_instance(instance_id)

        rows_after = get_alert_state(class_rule_id, str(instance_id))
        check(
            "Test5: existing schedule-wide recovery resolves the MISSED alert",
            rows_after[0]["lifecycle_state"] == "RESOLVED",
        )
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 6: repeated execution never duplicates ACTIVE rows
# -------------------------------------------------------------------------
def test_repeated_execution_no_duplicates():
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        _, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(cur)
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id)
        rule_ids.append(rule_id)

        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        seed_active_alert(rule_id, dedup_key)

        with get_cursor() as cur:
            instance_id = insert_missed_instance(cur, schedule_id, today_local)
        instance_ids.append(instance_id)

        for _ in range(5):
            activity_matcher.resolve_late_start_alerts_for_occurrence(
                FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local
            )

        rows = get_alert_state(rule_id, dedup_key)
        check("Test6: exactly one alert_log row after repeated resolve calls", len(rows) == 1)
        check("Test6: it is RESOLVED", rows[0]["lifecycle_state"] == "RESOLVED")

        for _ in range(5):
            activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        rows_after_sweeps = get_alert_state(rule_id, dedup_key)
        check("Test6: repeated sweeps do not add rows or resurrect ACTIVE", len(rows_after_sweeps) == 1 and rows_after_sweeps[0]["lifecycle_state"] == "RESOLVED")
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# Test 7: Part F(b) race guard
# -------------------------------------------------------------------------
def test_race_guard_post_insert_recheck():
    """Simulates the exact race Part F(b) closes: evaluate_late_start_sweep()
    reads instance_exists=False, then (between its own INSERT and its
    post-insert recheck) a MISSED row lands for the same occurrence -- e.g.
    missed_activity_cron.py running concurrently. We cannot literally split
    two DB round-trips inside one function call without mocking internal
    timing, so instead we instrument the exact call this function makes at
    that point (alerts.matchers.activity_matcher.upsert_active_alert) with a
    wrapper that performs the real insert and then, as its side effect,
    inserts the competing activity_instance row immediately afterward --
    landing exactly inside the window between "INSERT succeeded" and "the
    new post-insert recheck query runs". This documents precisely what is
    and isn't simulated: it is not a true concurrent process, but it does
    exercise the literal code path (the recheck query and its self-correct
    branch) added for Part F(b) under the exact ordering it exists to guard.
    """
    schedule_ids, rule_ids, instance_ids = [], [], []
    original_upsert = activity_matcher.upsert_active_alert
    try:
        _, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, ideal_start_time=time(0, 1))
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id, value=0)
        rule_ids.append(rule_id)

        planted_instance_id = str(uuid.uuid4())

        def racing_upsert(**kwargs):
            inserted = original_upsert(**kwargs)
            # Land the competing MISSED-shaped row right after the LATE
            # alert's own INSERT commits, before evaluate_late_start_sweep's
            # post-insert recheck query runs.
            if inserted:
                with get_cursor() as cur:
                    now = utc_now()
                    cur.execute(
                        """
                        INSERT INTO activity_instance
                            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                             actual_start_at, actual_end_at, actual_duration_sec,
                             started_offset_min, ended_offset_min, within_ideal_window,
                             status, session_classification, source, created_at, updated_at)
                        VALUES (%s, %s, %s, %s, %s, NULL, NULL, 0, NULL, NULL, FALSE,
                                'ENDED', 'MISSED', 'SYSTEM', %s, %s)
                        """,
                        (planted_instance_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local, now, now),
                    )
                instance_ids.append(planted_instance_id)
            return inserted

        activity_matcher.upsert_active_alert = racing_upsert
        try:
            result = activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        finally:
            activity_matcher.upsert_active_alert = original_upsert

        check(
            "Test7: race guard -- rule not reported as created (self-corrected)",
            rule_id not in result["alerts_created"],
        )
        check("Test7: race guard -- alerts_resolved counted the self-correction", result["alerts_resolved"] >= 1)

        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        rows = get_alert_state(rule_id, dedup_key)
        check(
            "Test7: race guard -- alert_log row ends RESOLVED, not left ACTIVE",
            len(rows) == 1 and rows[0]["lifecycle_state"] == "RESOLVED",
        )
    finally:
        activity_matcher.upsert_active_alert = original_upsert
        cleanup(schedule_ids, rule_ids, instance_ids)


# -------------------------------------------------------------------------
# End-to-end: real detect_missed_activities() wiring
# -------------------------------------------------------------------------
def test_end_to_end_cron_wiring():
    """Calls the real, unmocked detect_missed_activities() against a
    synthetic schedule rigged so its MISSED cutoff has already passed for
    both 'today' and 'yesterday' (ideal_start_time/ideal_end_time both very
    early local time, tolerance_late_min=0) -- deterministic except in the
    literal first minute after local midnight. detect_missed_activities()
    has no farm_id filter and will also process real production schedules
    exactly as the live cron does; this test asserts only on its own
    synthetic schedule's occurrence and touches nothing else."""
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        _, today_local = get_farm_today_local()
        with get_cursor() as cur:
            schedule_id = make_schedule(
                cur, ideal_start_time=time(0, 0), ideal_end_time=time(0, 1), tolerance_late_min=0
            )
        schedule_ids.append(schedule_id)
        with get_cursor() as cur:
            rule_id = make_late_rule(cur, schedule_id, value=0)
        rule_ids.append(rule_id)

        dedup_key = f"{schedule_id}:{today_local.isoformat()}"
        seed_active_alert(rule_id, dedup_key)

        detect_missed_activities()

        with get_cursor() as cur:
            cur.execute(
                "SELECT id, session_classification FROM activity_instance "
                "WHERE farm_id = %s AND activity_schedule_id = %s AND activity_date = %s",
                (FARM_ID, schedule_id, today_local),
            )
            created_row = cur.fetchone()
            # detect_missed_activities also processes 'yesterday' for this
            # schedule -- collect that row too so cleanup removes it.
            cur.execute(
                "SELECT id FROM activity_instance WHERE farm_id = %s AND activity_schedule_id = %s",
                (FARM_ID, schedule_id),
            )
            for r in cur.fetchall():
                instance_ids.append(r["id"])

        check(
            "E2E: detect_missed_activities() created a MISSED row for today's occurrence",
            created_row is not None and created_row["session_classification"] == "MISSED",
        )

        rows = get_alert_state(rule_id, dedup_key)
        check(
            "E2E: the seeded ACTIVE LATE alert was resolved by the real cron wiring",
            len(rows) == 1 and rows[0]["lifecycle_state"] == "RESOLVED",
        )
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


if __name__ == "__main__":
    test_late_resolved_by_missed()
    test_same_occurrence_only()
    test_late_does_not_return_after_missed()
    test_late_resolves_when_real_instance_appears()
    test_existing_missed_recovery_still_works()
    test_repeated_execution_no_duplicates()
    test_race_guard_post_insert_recheck()
    test_end_to_end_cron_wiring()

    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
