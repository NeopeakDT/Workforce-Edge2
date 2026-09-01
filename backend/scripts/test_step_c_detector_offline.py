"""
backend/scripts/test_step_c_detector_offline.py
STEP C — Standalone integration test for
edge_device_matcher.evaluate_detector_offline().

Run: python scripts/test_step_c_detector_offline.py

IMPORTANT fixture note: this repo's Task 5 dispatch explicitly forbids
writing to the real production edge_device row (id
f0d5c399-6939-4b26-bf5a-fe24c2ed5738, name Rahuri-Jetson-01) -- not even
temporarily -- because workforce-phase5.timer and other systemd services run
continuously against this live database, and a prior task's is_active=true
fixture writes to a scanned table caused live collision incidents. This test
therefore never touches that row. Instead, every test case INSERTs its own
fully synthetic edge_device fixture (own uuid, own unique name/code, a dummy
non-null api_key_hash, is_active=false so it never surfaces in device
listings, and detector_last_seen_at set directly at INSERT time to whatever
value that case needs) and deletes it -- along with any alert_log/alert_rule
rows tied to it -- in a `finally` block. The evaluator itself does not gate
on edge_device.is_active, so is_active=false does not affect what's being
tested here.
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
from alerts.matchers import edge_device_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"

REAL_DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"

results = []
_seq = [0]


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_device(cur, farm_id, detector_last_seen_at):
    """INSERT a fully synthetic edge_device fixture. detector_last_seen_at is
    set directly at INSERT time (never via a separate UPDATE)."""
    _seq[0] += 1
    device_id = str(uuid.uuid4())
    code = f"STEPC_TEST_DETECTOR_{_seq[0]}_{device_id[:8]}"
    cur.execute(
        """
        INSERT INTO edge_device (id, farm_id, name, code, api_key_hash, is_active, detector_last_seen_at)
        VALUES (%s, %s, %s, %s, 'stepc_test_fake_hash', false, %s)
        """,
        (device_id, farm_id, f"STEPC_TEST Detector {_seq[0]}", code, detector_last_seen_at),
    )
    return device_id


def make_rule(cur, farm_id, threshold_min=5):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 'STEPC_TEST Detector Offline', %s, 'CRITICAL', 'EDGE_DEVICE', true)
        """,
        (rule_id, farm_id,
         Json({"metric": "detector_heartbeat_age_minutes", "operator": ">", "value": threshold_min})),
    )
    return rule_id


def cleanup(rule_ids=None, device_ids=None):
    """Deletes alert_log rows keyed by BOTH our own test rule_ids AND our own
    synthetic device_ids -- since our synthetic devices live on the real
    farm, real pre-existing production EDGE_DEVICE rules (e.g. the live
    WORKFORCE_DETECTOR_OFFLINE rule seeded by an earlier task) also evaluate
    against them and can create their own alert_log rows tied to our device_id
    but a production alert_rule_id we didn't create and must not delete.
    Deleting by device_id as well as by rule_id catches those too, without
    ever touching a real device's alert_log rows (device_id is always ours)."""
    rule_ids = rule_ids or []
    device_ids = device_ids or []
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
        if device_ids:
            cur.execute("DELETE FROM alert_log WHERE device_id = ANY(%s::uuid[])", (device_ids,))
        if rule_ids:
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if device_ids:
            cur.execute("DELETE FROM edge_device WHERE id = ANY(%s::uuid[])", (device_ids,))


def test_a_fresh_timestamp_no_alert():
    """A. Fresh detector timestamp -> no alert."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID)
            device_id = make_device(cur, FARM_ID, utc_now())
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        result = edge_device_matcher.evaluate_detector_offline(device_id)
        check("A: device found", result["device_found"])
        check("A: no alert created for fresh timestamp", rule_id not in result.get("alerts_created", []))
    finally:
        cleanup(rule_ids, device_ids)


def test_b_just_below_threshold_no_alert():
    """B. Timestamp just below threshold (4 min for a 5-min rule) -> no alert."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
            device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=4))
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        result = edge_device_matcher.evaluate_detector_offline(device_id)
        check("B: no alert just below threshold", rule_id not in result.get("alerts_created", []))
    finally:
        cleanup(rule_ids, device_ids)


def test_c_beyond_threshold_creates_active_alert():
    """C. Timestamp beyond threshold (10 min for a 5-min rule) -> ACTIVE alert created."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
            device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=10))
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        result = edge_device_matcher.evaluate_detector_offline(device_id)
        check("C: device found", result["device_found"])
        check("C: alert created", rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("C: exactly one ACTIVE row", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")
    finally:
        cleanup(rule_ids, device_ids)


def test_d_repeated_stale_no_duplicate():
    """D. Repeated stale evaluation -> no duplicate ACTIVE row."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
            device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=10))
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        edge_device_matcher.evaluate_detector_offline(device_id)
        edge_device_matcher.evaluate_detector_offline(device_id)

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("D: exactly one row after repeated stale evaluation", len(rows) == 1,
              detail=f"got {len(rows)} rows")
    finally:
        cleanup(rule_ids, device_ids)


def test_e_fresh_after_active_resolves():
    """E. Fresh timestamp after an ACTIVE alert exists -> resolves it."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
            device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=10))
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        edge_device_matcher.evaluate_detector_offline(device_id)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("E: ACTIVE alert exists before recovery", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        with get_cursor() as cur:
            cur.execute("UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s", (utc_now(), device_id))

        edge_device_matcher.evaluate_detector_offline(device_id)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check(
            "E: alert resolves once timestamp is fresh again",
            len(rows_after) == 1 and rows_after[0]["lifecycle_state"] == "RESOLVED" and rows_after[0]["resolved_at"] is not None,
        )
    finally:
        cleanup(rule_ids, device_ids)


def test_f_non_default_threshold_read_from_condition():
    """F. Non-default threshold value (20 min, not 5) proves the evaluator
    reads condition['value'] rather than a hardcoded number."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=20)
            # 15 minutes: beyond a hardcoded-5 threshold, but below this rule's real 20.
            device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=15))
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        result = edge_device_matcher.evaluate_detector_offline(device_id)
        check(
            "F: 15min stale does not fire a 20min-threshold rule",
            rule_id not in result.get("alerts_created", []),
        )

        with get_cursor() as cur:
            cur.execute("UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s",
                        (utc_now() - timedelta(minutes=25), device_id))
        result2 = edge_device_matcher.evaluate_detector_offline(device_id)
        check(
            "F: 25min stale fires a 20min-threshold rule",
            rule_id in result2.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids, device_ids)


def test_g_device_isolation():
    """G. Two synthetic devices, only one stale; the other's rule must be
    untouched (no alert)."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
            stale_device_id = make_device(cur, FARM_ID, utc_now() - timedelta(minutes=10))
            fresh_device_id = make_device(cur, FARM_ID, utc_now())
        rule_ids.append(rule_id)
        device_ids.extend([stale_device_id, fresh_device_id])

        stale_result = edge_device_matcher.evaluate_detector_offline(stale_device_id)
        fresh_result = edge_device_matcher.evaluate_detector_offline(fresh_device_id)

        check("G: stale device triggers alert", rule_id in stale_result.get("alerts_created", []))
        check("G: fresh device does not trigger alert", rule_id not in fresh_result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("G: exactly one alert_log row (only from the stale device)", len(rows) == 1)
        check(
            "G: the one alert_log row's details reference the stale device, not the fresh one",
            rows[0]["details"] is not None and rows[0]["details"].get("device_id") == stale_device_id,
        )
    finally:
        cleanup(rule_ids, device_ids)


def test_h_rule_scoping_by_farm():
    """H. Confirm rule scoping via _load_edge_device_rules (farm_id +
    alert_type='EDGE_DEVICE' + is_active=true) is what's actually used -- a
    rule scoped to a different farm_id must never be matched.

    NOTE on approach: this DB currently has exactly one real farm row
    (Harmony Dairy), and INSERTing a second synthetic farm row was tried and
    rejected -- `farm` carries a live `grant_creator_farm_owner()` AFTER
    INSERT trigger that unconditionally writes a user_farm_access row keyed
    off auth.uid(), which is NULL outside of a real Supabase-authenticated
    session, so a raw psycopg2 INSERT into `farm` throws a NotNullViolation
    from inside that trigger. Rather than touch triggers or fabricate an
    auth context (both out of scope and risky against a live DB), this test
    instead calls edge_device_matcher._load_edge_device_rules(cur, farm_id)
    directly -- the exact function evaluate_detector_offline uses for
    scoping -- with a random, definitely-nonexistent farm_id, and proves it
    returns none of the EDGE_DEVICE rules that are visibly present (via the
    same function) for the real farm_id. This exercises precisely the
    `WHERE farm_id = %s` scoping the brief asks Test H to confirm, without
    requiring a second real farm row."""
    rule_ids = []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID, threshold_min=5)
        rule_ids.append(rule_id)

        random_other_farm_id = str(uuid.uuid4())
        with get_cursor() as cur:
            real_farm_rules = edge_device_matcher._load_edge_device_rules(cur, FARM_ID)
            other_farm_rules = edge_device_matcher._load_edge_device_rules(cur, random_other_farm_id)

        real_farm_rule_ids = [r["id"] for r in real_farm_rules]
        other_farm_rule_ids = [r["id"] for r in other_farm_rules]

        check(
            "H: _load_edge_device_rules(FARM_ID) includes our rule",
            rule_id in real_farm_rule_ids,
        )
        check(
            "H: _load_edge_device_rules(other farm_id) returns none of FARM_ID's rules",
            len(other_farm_rule_ids) == 0 and rule_id not in other_farm_rule_ids,
        )
    finally:
        cleanup(rule_ids)


def test_i_null_last_seen_does_not_trigger():
    """I. NULL detector_last_seen_at at INSERT time -> no alert (adapted from
    the brief's test_null_pulse_does_not_trigger; no real-device UPDATE/
    restore needed at all with the synthetic fixture)."""
    rule_ids, device_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur, FARM_ID)
            device_id = make_device(cur, FARM_ID, None)
        rule_ids.append(rule_id)
        device_ids.append(device_id)

        result = edge_device_matcher.evaluate_detector_offline(device_id)
        check("I: device found", result["device_found"])
        check("I: age_minutes is None for NULL detector_last_seen_at", result.get("age_minutes") is None)
        check(
            "I: NULL (no pulse ever) does not immediately alert",
            rule_id not in result.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids, device_ids)


def test_real_device_row_never_touched():
    """Sanity check, not part of the A-I matrix: confirm this test file never
    wrote to the real production device row."""
    with get_cursor() as cur:
        cur.execute("SELECT detector_last_seen_at FROM edge_device WHERE id = %s", (REAL_DEVICE_ID,))
        row = cur.fetchone()
    check(
        "Real device row (Rahuri-Jetson-01) detector_last_seen_at is still NULL",
        row is not None and row["detector_last_seen_at"] is None,
    )


if __name__ == "__main__":
    test_a_fresh_timestamp_no_alert()
    test_b_just_below_threshold_no_alert()
    test_c_beyond_threshold_creates_active_alert()
    test_d_repeated_stale_no_duplicate()
    test_e_fresh_after_active_resolves()
    test_f_non_default_threshold_read_from_condition()
    test_g_device_isolation()
    test_h_rule_scoping_by_farm()
    test_i_null_last_seen_does_not_trigger()
    test_real_device_row_never_touched()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
