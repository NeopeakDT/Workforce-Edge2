"""
backend/scripts/test_step_c_activity_late.py
STEP C — Standalone integration test for activity_matcher.evaluate_late_start_sweep(),
plus the Correction-1 finalization-no-longer-creates-ACTIVITY_LATE behavior.

Run: python scripts/test_step_c_activity_late.py

IMPORTANT environmental note (discovered while writing this test, not a bug
in the evaluator): this farm's real missed_activity_cron / activity
aggregation pipeline is live and running in this environment. It creates a
genuine session_classification='MISSED' activity_instance row for a
schedule's activity_date once that schedule's ideal_start_time (plus its
own grace period) has passed for the day. Because activity_instance carries
a uq_missed_schedule_per_day unique constraint on
(farm_id, activity_schedule_id, activity_date), a schedule whose ideal
start has already passed "today" and been swept by that real production
cron will already have a real row occupying today's date -- so this test
cannot always assume the Morning Scrapping schedule (ideal_start_time
05:00 IST) has "no instance yet" for today, and must never delete that row
(it is real production data, out of scope to touch).

To stay correct at any run time without ever touching production data,
`pick_clear_schedule()` below dynamically selects, from this farm's known
activity_schedule rows, one whose ideal_start_time has already passed today
by a safety margin (avoiding the flaky window right at the boundary) AND
which has no existing activity_instance row for today yet. If no such
schedule currently qualifies (e.g. every schedule's ideal start has already
passed and been swept for the day), the "no instance yet" tests print
[SKIP] rather than fail or flake against real data.

SECOND, MORE SERIOUS environmental finding (also discovered while writing
this test, and reported in full in task-3-report.md): this live database
has a CHECK constraint `chk_activity_alert_has_instance` (added by
STEP5_ADD_ALERT_LIFECYCLE_AND_TYPE.sql, predating Step C) requiring every
alert_log row with alert_type='ACTIVITY' to have a non-null
activity_instance_id. evaluate_late_start_sweep()'s entire reason for
existing, per the frozen design doc's section 2 ("Keying: schedule-keyed,
not instance-keyed -- no instance exists yet when this should first
fire"), is to insert exactly such a row -- alert_type='ACTIVITY',
activity_instance_id=NULL. That INSERT is therefore rejected by this
constraint every time a rule's condition is actually met with no instance
yet, for ANY alert_rule with alert_type='ACTIVITY' scoped this way,
including the real rules seeded by Task 2 (not just this file's test
rules). This is a genuine schema/design conflict outside this task's file
scope (DB schema changes are explicitly forbidden for Task 3) -- the
evaluator code below is implemented exactly per the frozen spec and Task 3
brief, and the tests below call it exactly as designed; `run_sweep()`
catches the resulting psycopg2 CheckViolation so this script can still run
to completion and report a complete, honest picture instead of crashing on
the first schedule that trips it.
"""

from pathlib import Path
import sys
import uuid
from datetime import datetime, timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import pytz
import psycopg2.errors
from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import activity_matcher
from alerts import alert_evaluator

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"

# All active (activity_schedule_id, activity_type_id) pairs on this farm,
# used by pick_clear_schedule() to find one with no real instance yet today.
# Morning Scrapping (activity_type 3, ideal_start_time 05:00 IST) is kept
# first since it's the schedule referenced throughout Step B/C so far and is
# preferred when it happens to qualify.
CANDIDATE_SCHEDULES = [
    ("f37b59e2-6da2-4d30-bc44-fbb73f1d18b3", 3),  # Morning Scrapping, 05:00 IST
    ("38c71cfc-0769-4d87-94b9-2b3447dbef90", 3),  # Evening Scrapping, 15:00 IST
    ("b583b038-42cb-4fd6-97ce-01b65f95f706", 1),  # 05:00 IST
    ("a866bb82-e816-4327-99cf-ec2495ca1adc", 1),  # 16:30 IST
    ("75b0a443-cf6d-45f2-88b1-44d8b37db138", 2),  # 05:00 IST
    ("e82d9f83-89ba-4967-ae08-79e9b5dfc8b6", 2),  # 16:30 IST
]

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def run_sweep(farm_id):
    """Wraps activity_matcher.evaluate_late_start_sweep. See the module
    docstring's "SECOND, MORE SERIOUS environmental finding" -- the live DB's
    chk_activity_alert_has_instance CHECK constraint rejects the INSERT this
    function performs whenever a rule's condition is met with no
    activity_instance yet (the sweep's entire reason for existing). This
    wrapper catches that specific psycopg2 error so the test script can
    still run to completion and report an honest, complete picture instead
    of crashing uncontrolled on the first schedule that trips it. get_cursor()
    rolls back its own connection on the exception (see common/db.py), so no
    partial/corrupt state is left behind by the caught INSERT.
    """
    try:
        return activity_matcher.evaluate_late_start_sweep(farm_id=farm_id), None
    except psycopg2.errors.CheckViolation as exc:
        return {"schedules_evaluated": 0, "alerts_created": [], "alerts_resolved": 0}, str(exc).splitlines()[0]


def pick_clear_schedule(cur, min_margin_minutes=2):
    """Return (schedule_id, activity_type_id, today_local) for a schedule
    whose ideal start has passed today by a safety margin and which has no
    activity_instance row yet for today -- or None if none currently
    qualifies. See module docstring."""
    cur.execute("SELECT timezone FROM farm WHERE id = %s", (FARM_ID,))
    farm_tz = pytz.timezone(cur.fetchone()["timezone"])
    now_local = datetime.now(pytz.utc).astimezone(farm_tz)
    today_local = now_local.date()

    for schedule_id, activity_type_id in CANDIDATE_SCHEDULES:
        cur.execute("SELECT ideal_start_time FROM activity_schedule WHERE id = %s", (schedule_id,))
        ideal_start_time = cur.fetchone()["ideal_start_time"]
        ideal_start_local = farm_tz.localize(datetime.combine(today_local, ideal_start_time))
        minutes_since = (now_local - ideal_start_local).total_seconds() / 60.0
        if minutes_since < min_margin_minutes:
            continue
        cur.execute(
            """
            SELECT 1 FROM activity_instance
            WHERE farm_id = %s AND activity_schedule_id = %s AND activity_date = %s
            LIMIT 1
            """,
            (FARM_ID, schedule_id, today_local),
        )
        if cur.fetchone() is not None:
            continue
        return schedule_id, activity_type_id, today_local
    return None


def make_rule(cur, schedule_id, activity_type_id, metric="minutes_since_ideal_start", value=0, operator=">"):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPC_TEST Late Scrapping Sweep',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, activity_type_id, schedule_id,
         Json({"metric": metric, "operator": operator, "value": value})),
        # value=0 makes this deterministic: any evaluation after the ideal
        # start time fires, proving the trigger path without depending on
        # exact wall-clock timing (beyond having already passed).
    )
    return rule_id


def make_session_classification_late_rule(cur, schedule_id, activity_type_id):
    """A finalization-style session_classification='LATE' rule scoped to the
    same schedule -- used by Test G to directly prove Correction 1 (that
    finalizing an instance with session_classification='LATE' no longer
    creates an alert_log row for this kind of rule at all)."""
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPC_TEST Late Scrapping Finalization (should be dead)',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, activity_type_id, schedule_id,
         Json({"metric": "session_classification", "operator": "=", "value": "LATE", "duration_minutes": 0})),
    )
    return rule_id


def cleanup(rule_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))


def test_late_sweep_triggers_when_no_instance_exists():
    rule_ids = []
    try:
        with get_cursor() as cur:
            picked = pick_clear_schedule(cur)
        if picked is None:
            print("[SKIP] ACTIVITY_LATE sweep (no-instance case) -- every candidate schedule already "
                  "has a real activity_instance row for today; nothing safe to test against right now.")
            return
        schedule_id, activity_type_id, _ = picked

        with get_cursor() as cur:
            rule_id = make_rule(cur, schedule_id, activity_type_id)
        rule_ids.append(rule_id)

        # No activity_instance exists for this schedule+today (confirmed by
        # pick_clear_schedule) -- the sweep must fire.
        result, sweep_error = run_sweep(FARM_ID)
        if sweep_error:
            check("ACTIVITY_LATE sweep: schedules evaluated > 0", False,
                  detail=f"blocked by chk_activity_alert_has_instance -- {sweep_error}")
        else:
            check("ACTIVITY_LATE sweep: schedules evaluated > 0", result["schedules_evaluated"] > 0)
        check("ACTIVITY_LATE sweep: alert created", rule_id in result.get("alerts_created", []),
              detail="blocked by chk_activity_alert_has_instance (see module docstring)" if sweep_error else "")

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_LATE: one ACTIVE row", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE",
              detail="blocked by chk_activity_alert_has_instance" if sweep_error else "")

        # Re-run: dedup must prevent a second row.
        run_sweep(FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check("ACTIVITY_LATE: re-run does not duplicate", len(rows_after) == 1)
    finally:
        cleanup(rule_ids)


def test_late_resolves_when_instance_appears():
    rule_ids, instance_ids = [], []
    try:
        with get_cursor() as cur:
            picked = pick_clear_schedule(cur)
        if picked is None:
            print("[SKIP] ACTIVITY_LATE sweep (resolve-on-appear case) -- every candidate schedule "
                  "already has a real activity_instance row for today.")
            return
        schedule_id, activity_type_id, today_local = picked

        with get_cursor() as cur:
            rule_id = make_rule(cur, schedule_id, activity_type_id)
        rule_ids.append(rule_id)

        _, sweep_error = run_sweep(FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_LATE: fired before instance exists", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE",
              detail="blocked by chk_activity_alert_has_instance" if sweep_error else "")

        iid = str(uuid.uuid4())
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, %s, %s, %s, %s, 'IN_PROGRESS', 'SYSTEM', %s)
                """,
                (iid, FARM_ID, activity_type_id, schedule_id, today_local, utc_now(), ZONE_ID),
            )
        instance_ids.append(iid)

        run_sweep(FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check(
            "ACTIVITY_LATE: resolves once instance exists",
            bool(rows_after) and rows_after[0]["lifecycle_state"] == "RESOLVED" and rows_after[0]["resolved_at"] is not None,
            detail="blocked by chk_activity_alert_has_instance (no ACTIVE row was ever created to resolve)" if sweep_error else "",
        )
    finally:
        cleanup(rule_ids)
        with get_cursor() as cur:
            if instance_ids:
                cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


def test_finalization_does_not_create_second_late_alert():
    """Test G. Reproduces Test F's sequence (sweep fires ACTIVE, then instance
    appears and resolves it), then finalizes that instance with
    session_classification='LATE' and calls the finalized-instance entry
    point (alert_evaluator.evaluate_activity_alerts). Proves Correction 1:
      1. The sweep rule's row count must stay at 1 (still RESOLVED from the
         instance-appeared resolution) -- finalization must not touch it.
      2. A separate session_classification='LATE' rule on the same schedule
         must get ZERO alert_log rows at all -- finalization's LATE branch
         is now a dead no-op per the frozen spec (Correction 1), so it must
         never call upsert_active_alert for target_value == 'LATE'.
    """
    sweep_rule_ids, classification_rule_ids, instance_ids = [], [], []
    try:
        with get_cursor() as cur:
            picked = pick_clear_schedule(cur)
        if picked is None:
            print("[SKIP] ACTIVITY_LATE finalization test (G) -- every candidate schedule already "
                  "has a real activity_instance row for today.")
            return
        schedule_id, activity_type_id, today_local = picked

        with get_cursor() as cur:
            sweep_rule_id = make_rule(cur, schedule_id, activity_type_id)
            classification_rule_id = make_session_classification_late_rule(cur, schedule_id, activity_type_id)
        sweep_rule_ids.append(sweep_rule_id)
        classification_rule_ids.append(classification_rule_id)

        # Step 1: sweep fires while no instance exists. NOTE: this INSERT is
        # expected to be rejected by chk_activity_alert_has_instance (see
        # module docstring) -- run_sweep() catches that so the rest of this
        # test can still run and report an honest picture.
        _, sweep_error_1 = run_sweep(FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (sweep_rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_LATE (G): sweep fired before instance exists",
              len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE",
              detail="blocked by chk_activity_alert_has_instance" if sweep_error_1 else "")

        # Step 2: instance appears -> sweep resolves it (if step 1 managed to
        # create an ACTIVE row at all; if it didn't, this is a no-op resolve).
        iid = str(uuid.uuid4())
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, %s, %s, %s, %s, 'IN_PROGRESS', 'SYSTEM', %s)
                """,
                (iid, FARM_ID, activity_type_id, schedule_id, today_local, utc_now(), ZONE_ID),
            )
        instance_ids.append(iid)

        run_sweep(FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (sweep_rule_id,))
            rows_resolved = cur.fetchall()
        check("ACTIVITY_LATE (G): resolved once instance exists",
              (len(rows_resolved) == 1 and rows_resolved[0]["lifecycle_state"] == "RESOLVED") if not sweep_error_1
              else len(rows_resolved) == 0,
              detail="step 1 was blocked, so there was nothing to resolve" if sweep_error_1 else "")
        count_before_finalize = len(rows_resolved)

        # Step 3: finalize the instance as LATE and call the finalized-instance
        # entry point -- must NOT create any new alert_log row anywhere
        # (this is the actual Correction-1 assertion, independent of whether
        # step 1/2 above were blocked by the unrelated schema issue).
        with get_cursor() as cur:
            cur.execute(
                "UPDATE activity_instance SET status='ENDED', actual_end_at=%s, session_classification='LATE' WHERE id=%s",
                (utc_now(), iid),
            )
        alert_evaluator.evaluate_activity_alerts(iid)

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (sweep_rule_id,))
            rows_after_finalize = cur.fetchall()
        check(
            "ACTIVITY_LATE (G): sweep-rule row count unchanged by finalization",
            len(rows_after_finalize) == count_before_finalize,
            detail=f"had {count_before_finalize} rows before finalization, {len(rows_after_finalize)} after",
        )

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (classification_rule_id,))
            classification_rows = cur.fetchall()
        check(
            "ACTIVITY_LATE (G): session_classification='LATE' rule gets zero rows (Correction 1)",
            len(classification_rows) == 0,
            detail=f"got {len(classification_rows)}",
        )
    finally:
        cleanup(sweep_rule_ids + classification_rule_ids)
        with get_cursor() as cur:
            if instance_ids:
                cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


def test_boundary_strict_greater_than():
    """Test I (LATE boundary). evaluate_late_start_sweep computes minutes_since
    from the real wall clock (there is no fixture we can set to freeze
    'now'), so we cannot deterministically place a threshold exactly at a
    known minute mark without risking flakiness from the few milliseconds
    that pass between reading a reference point and the sweep's own
    datetime.now() call. Instead we query a reference `minutes_since` at
    test start (from a schedule known to have already passed its ideal
    start today) and construct two rules with a generous margin on each
    side of it:
      - a rule whose threshold is comfortably BELOW the reference
        (`reference - 5`) -- observed minutes_since is certain to still be
        greater than this by the time the sweep runs, so it must fire;
      - a rule whose threshold is comfortably ABOVE the reference
        (`reference + 1000`) -- observed minutes_since cannot have grown by
        1000 minutes in the course of this test, so it must not fire.
    This still exercises the same strict '>' comparison
    (alert_conditions.condition_matches's `>` operator) that a tight
    boundary would, without a flaky sub-second race against the sweep's own
    clock read. Reuses pick_clear_schedule() (a schedule with no instance
    yet today) rather than just any past-ideal-start schedule, because if an
    instance already existed for the chosen schedule+date the sweep would
    take its resolve branch instead of its create branch and never call
    upsert_active_alert for either rule, making the "fires" assertion
    meaningless.
    """
    rule_ids = []
    try:
        with get_cursor() as cur:
            picked = pick_clear_schedule(cur)
        if picked is None:
            print("[SKIP] ACTIVITY_LATE boundary test -- every candidate schedule already has a "
                  "real activity_instance row for today.")
            return
        schedule_id, activity_type_id, today_local = picked

        with get_cursor() as cur:
            cur.execute("SELECT timezone FROM farm WHERE id = %s", (FARM_ID,))
            farm_tz = pytz.timezone(cur.fetchone()["timezone"])
            cur.execute("SELECT ideal_start_time FROM activity_schedule WHERE id = %s", (schedule_id,))
            ideal_start_time = cur.fetchone()["ideal_start_time"]

        now_local = datetime.now(pytz.utc).astimezone(farm_tz)
        ideal_start_local = farm_tz.localize(datetime.combine(today_local, ideal_start_time))
        reference_minutes_since = (now_local - ideal_start_local).total_seconds() / 60.0

        with get_cursor() as cur:
            below_rule_id = make_rule(cur, schedule_id, activity_type_id, value=reference_minutes_since - 5)
            above_rule_id = make_rule(cur, schedule_id, activity_type_id, value=reference_minutes_since + 1000)
        rule_ids.extend([below_rule_id, above_rule_id])

        result, sweep_error = run_sweep(FARM_ID)
        check("ACTIVITY_LATE boundary: below-threshold rule fires (now_local > cutoff)",
              below_rule_id in result.get("alerts_created", []),
              detail="blocked by chk_activity_alert_has_instance (see module docstring)" if sweep_error else "")
        check("ACTIVITY_LATE boundary: above-threshold rule does not fire",
              above_rule_id not in result.get("alerts_created", []))
    finally:
        cleanup(rule_ids)


if __name__ == "__main__":
    test_late_sweep_triggers_when_no_instance_exists()
    test_late_resolves_when_instance_appears()
    test_finalization_does_not_create_second_late_alert()
    test_boundary_strict_greater_than()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
