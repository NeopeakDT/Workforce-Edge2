"""
backend/scripts/test_posture_milking_message.py
Task 2 -- POSTURE_DATA_STALE MILKING-aware message.

Proves, against the real database, real evaluator (posture_matcher.py),
and the real POSTURE_DATA_STALE production rule -- but ONLY synthetic
farm_zone + posture_observation rows, never real ones -- that:

  1. A continuous run of MILKING-mode observations produces a duration
     message measured from the START of that run, not from any
     activity_instance.actual_start_at (no activity_instance is ever
     created in this file -- proving the message cannot possibly be
     derived from it).
  2. A MILKING run preceded by a NORMAL-mode row uses the MILKING run's
     own start, not the earlier NORMAL row's timestamp.
  3. A NORMAL-mode latest observation preserves the exact pre-Task-2
     message format.
  4. A row with no 'mode' key in metadata (legacy/malformed data) does not
     fabricate a MILKING duration -- falls back to the generic message.
  5. Nothing about WHEN the alert fires/resolves, its severity, or its
     dedup key changes -- same rule, same condition, same lifecycle
     mechanics as the pre-existing POSTURE_DATA_STALE tests.
  6. A gap between two MILKING rows greater than
     _MAX_MILKING_CONTINUITY_GAP_SECONDS (600s / 10 min) breaks
     continuity -- the current continuous MILKING period starts at the
     MILKING row AFTER the gap, not the one before it. A gap at or under
     that threshold does not break continuity (tolerates one
     missed/delayed 300s flush).

All fixtures (farm_zone, posture_observation, alert_rule, alert_log) are
synthetic and removed in a finally block regardless of outcome. No real
zone/device/rule is ever touched.

Run: python scripts/test_posture_milking_message.py
"""

from pathlib import Path
import sys
import uuid
from datetime import timedelta, datetime, timezone

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json

from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import posture_matcher
from alerts.matchers.posture_matcher import _find_current_milking_start

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_zone(cur):
    zone_id = str(uuid.uuid4())
    cur.execute(
        "INSERT INTO farm_zone (id, farm_id, name) VALUES (%s, %s, 'STEPTASK2_TEST synthetic zone')",
        (zone_id, FARM_ID),
    )
    return zone_id


def insert_observation(cur, zone_id, observed_at, mode, include_mode_key=True):
    metadata = {"herd_size": 10} if not include_mode_key else {"mode": mode, "herd_size": 10}
    cur.execute(
        """
        INSERT INTO posture_observation
            (farm_id, zone_id, observed_at, standing_count, feeding_count,
             laying_count, standing_percentage, laying_percentage, metadata)
        VALUES (%s, %s, %s, 0, 0, 0, 0.0, 0.0, %s)
        """,
        (FARM_ID, zone_id, observed_at, Json(metadata)),
    )


def make_forced_trigger_rule(cur):
    """Same 'value=-1' forced-trigger convention used by the existing
    test_stepB_alert_evaluator.py::test_posture_data_stale()."""
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 'STEPTASK2_TEST Posture Stale (forced trigger)', %s, 'WARNING', 'POSTURE', true)
        """,
        (rule_id, FARM_ID, Json({"metric": "observation_age_minutes", "operator": ">", "value": -1, "duration_minutes": 0})),
    )
    return rule_id


def fetch_alert_message(rule_id, dedup_key):
    with get_cursor() as cur:
        cur.execute(
            "SELECT message, lifecycle_state FROM alert_log WHERE alert_rule_id = %s AND dedup_key = %s "
            "ORDER BY triggered_at DESC LIMIT 1",
            (rule_id, dedup_key),
        )
        return cur.fetchone()


def cleanup(rule_ids, zone_id):
    with get_cursor() as cur:
        if zone_id:
            cur.execute("DELETE FROM alert_log WHERE dedup_key = %s", (zone_id,))
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if zone_id:
            cur.execute("DELETE FROM posture_observation WHERE zone_id = %s", (zone_id,))
            cur.execute("DELETE FROM farm_zone WHERE id = %s", (zone_id,))


def test_continuous_milking_history():
    """5 consecutive MILKING rows, 5 min apart, ending ~now. No
    activity_instance is ever created in this test -- the duration cannot
    possibly come from actual_start_at."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            now = utc_now()
            milking_start = now - timedelta(minutes=20)
            for offset in (20, 15, 10, 5, 0):
                insert_observation(cur, zone_id, now - timedelta(minutes=offset), "MILKING")
            rule_id = make_forced_trigger_rule(cur)
            rule_ids.append(rule_id)

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("continuous MILKING: observation found", result.get("observation_found") is True)
        check("continuous MILKING: rule fires", rule_id in result.get("alerts_created", []))

        row = fetch_alert_message(rule_id, zone_id)
        check("continuous MILKING: alert row exists", row is not None)
        if row:
            check(
                "continuous MILKING: message reflects ~20 min duration from earliest row, not a generic message",
                row["message"] == "Cows are in milking for 20 minutes.",
                detail=row["message"],
            )
            check(
                "continuous MILKING: message does not mention 'no posture data received' (old generic wording)",
                "no posture data received" not in row["message"],
            )
    finally:
        cleanup(rule_ids, zone_id)


def test_milking_transition_from_normal():
    """4:50 NORMAL, 5:00/5:05/5:10 MILKING, 'now' ~= 5:12 -- duration must
    start at 5:00 (the MILKING transition), not 4:50."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            now = utc_now()
            t_5_00 = now - timedelta(minutes=12)
            t_4_50 = t_5_00 - timedelta(minutes=10)
            insert_observation(cur, zone_id, t_4_50, "NORMAL")
            insert_observation(cur, zone_id, t_5_00, "MILKING")
            insert_observation(cur, zone_id, t_5_00 + timedelta(minutes=5), "MILKING")
            insert_observation(cur, zone_id, t_5_00 + timedelta(minutes=10), "MILKING")
            rule_id = make_forced_trigger_rule(cur)
            rule_ids.append(rule_id)

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("transition: rule fires", rule_id in result.get("alerts_created", []))

        row = fetch_alert_message(rule_id, zone_id)
        check("transition: alert row exists", row is not None)
        if row:
            # Elapsed since t_5_00 is ~12 minutes; elapsed since t_4_50 would be ~22.
            check(
                "transition: duration measured from the MILKING transition (~12 min), not the earlier NORMAL row (~22 min)",
                row["message"] == "Cows are in milking for 12 minutes.",
                detail=row["message"],
            )
    finally:
        cleanup(rule_ids, zone_id)


def test_non_milking_message_unchanged():
    """Latest row is NORMAL mode -- message must be byte-identical to the
    pre-Task-2 format."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            insert_observation(cur, zone_id, utc_now() - timedelta(minutes=15), "NORMAL")
            rule_id = make_forced_trigger_rule(cur)
            rule_ids.append(rule_id)

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("non-MILKING: rule fires", rule_id in result.get("alerts_created", []))

        row = fetch_alert_message(rule_id, zone_id)
        check("non-MILKING: alert row exists", row is not None)
        if row:
            check(
                "non-MILKING: message keeps exact pre-Task-2 format (rule name + 'no posture data received for N minutes.')",
                row["message"].startswith("STEPTASK2_TEST Posture Stale (forced trigger): no posture data received for"),
                detail=row["message"],
            )
            check("non-MILKING: message does not say 'Cows are in milking'", "Cows are in milking" not in row["message"])
    finally:
        cleanup(rule_ids, zone_id)


def test_missing_mode_key_no_fabricated_duration():
    """metadata present but with no 'mode' key at all (legacy/malformed
    row) -- must NOT be treated as MILKING, must NOT fabricate a
    duration, falls back to the generic message."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            insert_observation(cur, zone_id, utc_now() - timedelta(minutes=15), mode=None, include_mode_key=False)
            rule_id = make_forced_trigger_rule(cur)
            rule_ids.append(rule_id)

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("missing mode key: rule fires", rule_id in result.get("alerts_created", []))

        row = fetch_alert_message(rule_id, zone_id)
        check("missing mode key: alert row exists", row is not None)
        if row:
            check(
                "missing mode key: falls back to generic staleness message, no fabricated MILKING duration",
                "Cows are in milking" not in row["message"] and "no posture data received" in row["message"],
                detail=row["message"],
            )
    finally:
        cleanup(rule_ids, zone_id)


def test_lifecycle_unchanged_dedup_and_resolve():
    """Same dedup/resolve mechanics as before -- re-evaluation of an
    unchanged MILKING condition does not duplicate, and once the
    condition clears the alert still resolves via the existing
    resolve_active_occurrence path (proves Task 2 touched only message
    text, not lifecycle)."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            insert_observation(cur, zone_id, utc_now() - timedelta(minutes=10), "MILKING")
            trigger_rule_id = make_forced_trigger_rule(cur)
            quiet_rule_id = str(uuid.uuid4())
            cur.execute(
                """
                INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
                VALUES (%s, %s, 'STEPTASK2_TEST Posture Stale (quiet)', %s, 'WARNING', 'POSTURE', true)
                """,
                (quiet_rule_id, FARM_ID, Json({"metric": "observation_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0})),
            )
            rule_ids.extend([trigger_rule_id, quiet_rule_id])

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("lifecycle: trigger rule fires, quiet rule does not",
              trigger_rule_id in result.get("alerts_created", []) and quiet_rule_id not in result.get("alerts_created", []))

        # Re-evaluate: dedup, no duplicate ACTIVE row.
        posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        with get_cursor() as cur:
            cur.execute(
                "SELECT lifecycle_state FROM alert_log WHERE alert_rule_id = %s AND dedup_key = %s",
                (trigger_rule_id, zone_id),
            )
            rows_after = cur.fetchall()
        check("lifecycle: re-evaluation does not duplicate the ACTIVE row", len(rows_after) == 1, detail=str(rows_after))

        # Raise the threshold so the condition stops matching -> must resolve.
        with get_cursor() as cur:
            cur.execute(
                "UPDATE alert_rule SET condition = %s WHERE id = %s",
                (Json({"metric": "observation_age_minutes", "operator": ">", "value": 10_000_000, "duration_minutes": 0}), trigger_rule_id),
            )
        posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        row_final = fetch_alert_message(trigger_rule_id, zone_id)
        check("lifecycle: alert resolves once condition no longer holds (unchanged resolve mechanics)",
              row_final and row_final["lifecycle_state"] == "RESOLVED", detail=str(row_final))
    finally:
        cleanup(rule_ids, zone_id)


# ---------------------------------------------------------------------
# Continuity-gap fix (posture_matcher._find_current_milking_start).
# The unit-level tests below call the function directly against an
# in-memory fake cursor -- no DB access at all -- so the exact gap
# boundaries can be tested precisely without relying on real query
# timing. The final test in this section proves the same fix end-to-end
# through the real DB and the real evaluate_zone_staleness() message.
# ---------------------------------------------------------------------

class _FakeCursor:
    """Minimal stand-in for a DB cursor: execute() is a no-op, fetchall()
    returns pre-built rows. Used only to test _find_current_milking_start's
    own walk/gap logic in isolation, with no database involved."""

    def __init__(self, rows):
        self._rows = rows

    def execute(self, *args, **kwargs):
        pass

    def fetchall(self):
        return self._rows


def _t(minute, hour=5):
    return datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)


def _rows_desc(pairs):
    """pairs: list of (minute, mode) in chronological order -- returned
    DESC (newest first), matching the real SQL's ORDER BY observed_at DESC."""
    return [{"observed_at": _t(m), "mode": mode} for m, mode in reversed(pairs)]


def test_gap_continuity_unit_normal_cadence():
    """Normal 5-minute cadence, no gaps -- whole run is one continuous period."""
    rows = _rows_desc([(0, "MILKING"), (5, "MILKING"), (10, "MILKING"), (15, "MILKING")])
    start = _find_current_milking_start(_FakeCursor(rows), FARM_ID, "zone")
    check("gap unit: normal 5-min cadence stays fully continuous (start=05:00)", start == _t(0), detail=str(start))


def test_gap_continuity_unit_gap_within_threshold():
    """A 10-minute (600s) gap is AT the threshold -- continuity preserved
    (tolerates exactly one missed 300s flush)."""
    rows = _rows_desc([(0, "MILKING"), (10, "MILKING")])
    start = _find_current_milking_start(_FakeCursor(rows), FARM_ID, "zone")
    check("gap unit: 10-minute gap (== 600s threshold) does not break continuity", start == _t(0), detail=str(start))


def test_gap_continuity_unit_gap_exceeds_threshold():
    """An 11-minute (660s) gap exceeds the threshold -- continuity breaks;
    the period after the gap is the current one."""
    rows = _rows_desc([(0, "MILKING"), (11, "MILKING")])
    start = _find_current_milking_start(_FakeCursor(rows), FARM_ID, "zone")
    check("gap unit: 11-minute gap (> 600s threshold) breaks continuity (start=05:11, not 05:00)",
          start == _t(11), detail=str(start))


def test_gap_continuity_unit_exact_reported_scenario():
    """The exact scenario from the review: 05:00/05:05/05:10 MILKING, a
    30-minute gap, then 05:40 MILKING. Continuous start must be 05:40."""
    rows = _rows_desc([(0, "MILKING"), (5, "MILKING"), (10, "MILKING"), (40, "MILKING")])
    start = _find_current_milking_start(_FakeCursor(rows), FARM_ID, "zone")
    check("gap unit: 05:00/05:05/05:10 + 30-min gap + 05:40 -> start=05:40, not 05:00",
          start == _t(40), detail=str(start))


def test_gap_scenario_end_to_end_db():
    """Same exact scenario as above, through the real DB and the real
    evaluate_zone_staleness() message -- proves the fix end-to-end, not
    just at the unit level. 'Now' is pinned just after 05:40 so the
    reported duration is small and unambiguous (~1 minute), not the ~41
    minutes a gap-naive implementation would report from 05:00."""
    zone_id = None
    rule_ids = []
    try:
        with get_cursor() as cur:
            zone_id = make_zone(cur)
            now = utc_now()
            # Anchor the whole scenario relative to "now" so the test
            # doesn't depend on wall-clock date/time.
            t_05_00 = now - timedelta(minutes=41)
            insert_observation(cur, zone_id, t_05_00, "MILKING")
            insert_observation(cur, zone_id, t_05_00 + timedelta(minutes=5), "MILKING")
            insert_observation(cur, zone_id, t_05_00 + timedelta(minutes=10), "MILKING")
            insert_observation(cur, zone_id, t_05_00 + timedelta(minutes=40), "MILKING")  # 30-min gap before this row
            rule_id = make_forced_trigger_rule(cur)
            rule_ids.append(rule_id)

        result = posture_matcher.evaluate_zone_staleness(FARM_ID, zone_id)
        check("gap e2e: rule fires", rule_id in result.get("alerts_created", []))

        row = fetch_alert_message(rule_id, zone_id)
        check("gap e2e: alert row exists", row is not None)
        if row:
            # ~1 minute since the post-gap 05:40 row, NOT ~41 minutes since 05:00.
            check(
                "gap e2e: message reflects only the post-gap duration (~1 min), not the full 05:00-to-now span (~41 min)",
                row["message"] == "Cows are in milking for 1 minute.",
                detail=row["message"],
            )
    finally:
        cleanup(rule_ids, zone_id)


if __name__ == "__main__":
    test_continuous_milking_history()
    test_milking_transition_from_normal()
    test_non_milking_message_unchanged()
    test_missing_mode_key_no_fabricated_duration()
    test_lifecycle_unchanged_dedup_and_resolve()
    test_gap_continuity_unit_normal_cadence()
    test_gap_continuity_unit_gap_within_threshold()
    test_gap_continuity_unit_gap_exceeds_threshold()
    test_gap_continuity_unit_exact_reported_scenario()
    test_gap_scenario_end_to_end_db()

    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
