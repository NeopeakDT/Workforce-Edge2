"""
backend/scripts/test_activity_late_third_state_fix.py
Fix verification: evaluate_late_start_sweep()'s three-occurrence-state logic.

Background (see the 2026-09-04 ACTIVITY_LATE/ACTIVITY_MISSED lifecycle
audit): the first fix (requiring actual_start_at IS NOT NULL) correctly
stopped a SYSTEM/MISSED placeholder from being treated as "the activity
started," but on its own it let the sweep re-CREATE a new ACTIVE
ACTIVITY_LATE row for an occurrence that missed_activity_cron.py's
resolve_late_start_alerts_for_occurrence() had already, correctly, closed
out. This test proves the second fix: the sweep now recognizes a third
state -- session_classification='MISSED' AND source='SYSTEM' -- as CLOSED,
and takes no action (neither create nor resolve) for it.

TEST-HARNESS HARDENING (v2): an earlier version of this file created its
fixture activity_schedule row is_active=true (later: briefly toggled true
only for the duration of a single evaluate_late_start_sweep() call). Both
were caught, on separate occasions, by the real, independently-running
workforce-phase5.timer / workforce-alerts.timer -- which fire every 60s
against this same database regardless of what this test does -- creating
real (if fully synthetic-labeled) activity_instance/alert_log residue that
had to be identified and cleaned up after the fact. Toggling is_active
only shrinks that race; it does not eliminate it.

This version eliminates it entirely instead of shrinking it: the fixture
activity_schedule row is created is_active=FALSE and is NEVER flipped to
true, so it is unconditionally invisible to every real production cron's
own schedule-discovery query (every one of which filters
`WHERE activity_schedule.is_active = true` -- confirmed against
evaluate_late_start_sweep(), missed_activity_cron.py, and alerts_cron.py).
To still exercise evaluate_late_start_sweep()'s real, unmodified logic
against this schedule, only its own internal schedule-discovery query
(the one `SELECT ... FROM activity_schedule s JOIN farm f ...` at the top
of the function) is intercepted via a thin cursor wrapper and answered
with a single in-memory row describing this test's fixture -- every other
query the function issues (the per-occurrence activity_instance check,
_load_activity_rules(), the post-insert race-window recheck) passes
through untouched to the real database, against this test's real synthetic
alert_rule/activity_instance fixtures. No production code is modified --
this is a test-file-only technique (patches
alerts.matchers.activity_matcher.get_cursor, a name in this test process
only; it has no effect on the separately-running systemd services).

The fixture activity_schedule row still physically exists (is_active=false,
permanently) only because activity_instance.activity_schedule_id carries a
real foreign key to activity_schedule(id) -- it is never queried by
production code's own discovery path, by construction.

Uses a fully synthetic activity_schedule (own uuid/label) purely as an FK
target -- never a real production schedule or activity_instance. Fixtures
cleaned up in `finally`; production alert_log is snapshotted before/after
as a tripwire.

Run: python scripts/test_activity_late_third_state_fix.py
"""

from contextlib import contextmanager
from pathlib import Path
import sys
import uuid
from datetime import time, timedelta
from unittest.mock import patch

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

import pytz
from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import activity_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
ACTIVITY_TYPE_ID = 3  # Scrapping, reused elsewhere in Step C tests

# Distinguishing substring of evaluate_late_start_sweep()'s own
# schedule-discovery query, used only to decide which SELECT to intercept.
_SCHEDULE_QUERY_MARKER = "FROM activity_schedule s"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def snapshot_alert_log():
    with get_cursor() as cur:
        cur.execute("SELECT id FROM alert_log ORDER BY id")
        return sorted(r["id"] for r in cur.fetchall())


def farm_today_local():
    with get_cursor() as cur:
        cur.execute("SELECT timezone FROM farm WHERE id = %s", (FARM_ID,))
        farm_tz = pytz.timezone(cur.fetchone()["timezone"])
    now_local = utc_now().astimezone(farm_tz)
    return now_local.date(), (now_local - timedelta(hours=3)).time(), farm_tz.zone


class _ScheduleInjectingCursor:
    """Wraps a real cursor. Answers evaluate_late_start_sweep()'s own
    schedule-discovery query with a single in-memory fake row instead of
    touching the real activity_schedule table for that one query; every
    other query passes straight through to the real cursor untouched."""

    def __init__(self, real_cursor, fake_schedule_row):
        self._real = real_cursor
        self._fake_schedule_row = fake_schedule_row
        self._intercepted = False

    def execute(self, query, params=None):
        if _SCHEDULE_QUERY_MARKER in query:
            self._intercepted = True
            return None
        self._intercepted = False
        return self._real.execute(query, params)

    def fetchall(self):
        if self._intercepted:
            return [self._fake_schedule_row]
        return self._real.fetchall()

    def fetchone(self):
        if self._intercepted:
            return self._fake_schedule_row
        return self._real.fetchone()

    def __getattr__(self, item):
        return getattr(self._real, item)


def run_sweep_isolated(fake_schedule_row):
    """Runs the real, unmodified evaluate_late_start_sweep() with its own
    schedule-discovery query answered by fake_schedule_row only -- the real
    activity_schedule table is never read for schedule enumeration, so no
    row's is_active value (real or fixture) is ever relevant to whether
    this call discovers anything. Only this test process is affected
    (unittest.mock.patch is process-local); the live systemd services are
    untouched."""

    @contextmanager
    def _spy_get_cursor():
        with get_cursor() as cur:
            yield _ScheduleInjectingCursor(cur, fake_schedule_row)

    with patch("alerts.matchers.activity_matcher.get_cursor", _spy_get_cursor):
        return activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)


def make_schedule(cur, label_suffix, ideal_start_time):
    """is_active=false, permanently -- never flipped. Exists only as the
    real, physical FK target activity_instance.activity_schedule_id
    requires; never discovered by any production cron's own query, and (per
    run_sweep_isolated) never even queried by this test's own sweep call
    either."""
    schedule_id = str(uuid.uuid4())
    label = f"STEPFIX3_TEST {label_suffix} {schedule_id}"
    cur.execute(
        """
        INSERT INTO activity_schedule
            (id, farm_id, activity_type_id, label, ideal_start_time, ideal_end_time,
             tolerance_early_min, tolerance_late_min, is_active)
        VALUES (%s, %s, %s, %s, %s, %s, 15, 15, false)
        """,
        (schedule_id, FARM_ID, ACTIVITY_TYPE_ID, label, ideal_start_time, ideal_start_time),
    )
    return schedule_id


def make_fake_schedule_row(schedule_id, ideal_start_time, timezone_name):
    """Matches exactly the columns evaluate_late_start_sweep()'s own query
    selects: schedule_id, farm_id, activity_type_id, ideal_start_time, timezone."""
    return {
        "schedule_id": schedule_id,
        "farm_id": FARM_ID,
        "activity_type_id": ACTIVITY_TYPE_ID,
        "ideal_start_time": ideal_start_time,
        "timezone": timezone_name,
    }


def make_late_rule(cur, schedule_id):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPFIX3_TEST Late Sweep',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id,
         Json({"metric": "minutes_since_ideal_start", "operator": ">", "value": 0})),
    )
    return rule_id


def make_missed_rule(cur, schedule_id):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, %s, %s, 'STEPFIX3_TEST Missed',
                %s, 'CRITICAL', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, ACTIVITY_TYPE_ID, schedule_id,
         Json({"metric": "session_classification", "operator": "=", "value": "MISSED"})),
    )
    return rule_id


def insert_instance(cur, schedule_id, activity_date, *, source, session_classification, status,
                     actual_start_at, actual_end_at=None):
    iid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, actual_end_at, status, source, zone_id, session_classification)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """,
        (iid, FARM_ID, ACTIVITY_TYPE_ID, schedule_id, activity_date,
         actual_start_at, actual_end_at, status, source, ZONE_ID, session_classification),
    )
    return iid


def cleanup(schedule_ids=(), rule_ids=(), instance_ids=()):
    with get_cursor() as cur:
        if instance_ids:
            cur.execute("DELETE FROM alert_log WHERE activity_instance_id = ANY(%s::uuid[])", (instance_ids,))
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if instance_ids:
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))
        if schedule_ids:
            cur.execute("DELETE FROM activity_schedule WHERE id = ANY(%s::uuid[])", (schedule_ids,))


# TEST 1 + TEST 6: no instance -> LATE creates, dedup on re-run
def test_no_instance_creates_and_dedups():
    today_local, ideal_start, tz_name = farm_today_local()
    schedule_ids, rule_ids = [], []
    try:
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, "no-instance", ideal_start)
            rule_id = make_late_rule(cur, schedule_id)
        schedule_ids.append(schedule_id)
        rule_ids.append(rule_id)
        fake_row = make_fake_schedule_row(schedule_id, ideal_start, tz_name)

        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("1. STATE 1 (no instance): LATE created ACTIVE",
              len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE", detail=str(rows))

        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check("6. dedup: repeated sweep on undecided occurrence does not duplicate",
              len(rows_after) == 1)
    finally:
        cleanup(schedule_ids, rule_ids)


# TEST 2: real occurrence -> LATE resolves
def test_real_occurrence_resolves():
    today_local, ideal_start, tz_name = farm_today_local()
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, "real-occurrence", ideal_start)
            rule_id = make_late_rule(cur, schedule_id)
        schedule_ids.append(schedule_id)
        rule_ids.append(rule_id)
        fake_row = make_fake_schedule_row(schedule_id, ideal_start, tz_name)

        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            iid = insert_instance(
                cur, schedule_id, today_local,
                source="AI", session_classification=None, status="IN_PROGRESS",
                actual_start_at=utc_now(),
            )
        instance_ids.append(iid)

        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("2. STATE 2 (real occurrence, actual_start_at set): LATE resolves",
              len(rows) == 1 and rows[0]["lifecycle_state"] == "RESOLVED", detail=str(rows))
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# TEST 3 + TEST 7: SYSTEM/MISSED placeholder -> sweep takes no action, ever
def test_closed_occurrence_no_action_and_stays_closed():
    today_local, ideal_start, tz_name = farm_today_local()
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, "closed", ideal_start)
            rule_id = make_late_rule(cur, schedule_id)
        schedule_ids.append(schedule_id)
        rule_ids.append(rule_id)
        fake_row = make_fake_schedule_row(schedule_id, ideal_start, tz_name)

        # First sweep fires LATE (undecided occurrence).
        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("pre-condition: LATE fires before any instance exists",
              len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        # missed_activity_cron.py's real path: it would call
        # resolve_late_start_alerts_for_occurrence() right after inserting
        # the placeholder. Call it directly here too, exactly like the real
        # cron does, before ever running the sweep again.
        with get_cursor() as cur:
            iid = insert_instance(
                cur, schedule_id, today_local,
                source="SYSTEM", session_classification="MISSED", status="ENDED",
                actual_start_at=None, actual_end_at=None,
            )
        instance_ids.append(iid)
        activity_matcher.resolve_late_start_alerts_for_occurrence(
            FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local
        )
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_resolved = cur.fetchall()
        check("4. existing MISSED path: resolve_late_start_alerts_for_occurrence() resolves LATE",
              len(rows_resolved) == 1 and rows_resolved[0]["lifecycle_state"] == "RESOLVED",
              detail=str(rows_resolved))

        # THE FIX: now run the sweep (possibly many times, as the real 60s
        # timer would) -- it must never re-create ACTIVE LATE for this
        # already-closed occurrence, and must not need to do anything.
        run_sweep_isolated(fake_row)
        run_sweep_isolated(fake_row)
        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s ORDER BY triggered_at", (rule_id,))
            rows_final = cur.fetchall()
        check(
            "3+7. STATE 3 (closed): repeated sweeps do not recreate ACTIVE LATE "
            "(still exactly one row, still RESOLVED)",
            len(rows_final) == 1 and rows_final[0]["lifecycle_state"] == "RESOLVED",
            detail=str(rows_final),
        )
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


# TEST 5: evaluate_finalized_instance() still creates the separate
# ACTIVITY_MISSED alert, unaffected by this fix.
def test_missed_alert_still_created():
    today_local, ideal_start, tz_name = farm_today_local()
    schedule_ids, rule_ids, instance_ids = [], [], []
    try:
        with get_cursor() as cur:
            schedule_id = make_schedule(cur, "missed-alert", ideal_start)
            late_rule_id = make_late_rule(cur, schedule_id)
            missed_rule_id = make_missed_rule(cur, schedule_id)
        schedule_ids.append(schedule_id)
        rule_ids.extend([late_rule_id, missed_rule_id])
        fake_row = make_fake_schedule_row(schedule_id, ideal_start, tz_name)

        run_sweep_isolated(fake_row)
        with get_cursor() as cur:
            iid = insert_instance(
                cur, schedule_id, today_local,
                source="SYSTEM", session_classification="MISSED", status="ENDED",
                actual_start_at=None, actual_end_at=None,
            )
        instance_ids.append(iid)

        # Exactly the real missed_activity_cron.py call sequence.
        activity_matcher.resolve_late_start_alerts_for_occurrence(
            FARM_ID, ACTIVITY_TYPE_ID, schedule_id, today_local
        )
        result = activity_matcher.evaluate_finalized_instance(iid)
        check("5a. evaluate_finalized_instance finds the instance", result.get("instance_found"))
        check("5b. evaluate_finalized_instance creates the separate ACTIVITY_MISSED alert",
              missed_rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (missed_rule_id,))
            missed_rows = cur.fetchall()
        check("5c. exactly one ACTIVE ACTIVITY_MISSED row", len(missed_rows) == 1 and missed_rows[0]["lifecycle_state"] == "ACTIVE")

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (late_rule_id,))
            late_rows = cur.fetchall()
        check("5d. LATE row remains separate, RESOLVED, not converted into MISSED",
              len(late_rows) == 1 and late_rows[0]["lifecycle_state"] == "RESOLVED"
              and late_rows[0]["id"] != missed_rows[0]["id"])
    finally:
        cleanup(schedule_ids, rule_ids, instance_ids)


def test_production_alert_log_unaffected(before_snapshot):
    after = snapshot_alert_log()
    check("production alert_log identical before/after (no contamination)",
          before_snapshot == after, detail=f"before={before_snapshot} after={after}")


if __name__ == "__main__":
    before = snapshot_alert_log()

    test_no_instance_creates_and_dedups()
    test_real_occurrence_resolves()
    test_closed_occurrence_no_action_and_stays_closed()
    test_missed_alert_still_created()
    test_production_alert_log_unaffected(before)

    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
