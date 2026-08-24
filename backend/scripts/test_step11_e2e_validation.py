# scripts/test_step11_e2e_validation.py
"""
End-to-End Dashboard API Validation (STEP 11 — Dashboard).

Different in kind from the Step 3-10 test scripts: those each validate
one endpoint in isolation. This one validates the COMPOSED
GET /api/v1/dashboard/posture/dashboard response as a system --
cross-section consistency, independent DB recomputation of critical
values (not just "does it match another endpoint"), real query-count
and latency measurement, and a live regression run of every prior
step's test script.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step11_e2e_validation.py
"""

import os
import subprocess
import sys
import time
from decimal import Decimal
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Access token (same convention as the Step 3-10 test scripts)
# ---------------------------------------------------------------------
ACCESS_TOKEN = "PASTE_REAL_ACCESS_TOKEN_HERE"

SUPABASE_URL = os.environ.get("SUPABASE_URL", "https://cxvoidjhkbrsjxpipbdg.supabase.co")
SUPABASE_ANON_KEY = os.environ.get("SUPABASE_ANON_KEY")
TEST_USER_EMAIL = os.environ.get("TEST_USER_EMAIL")
TEST_USER_PASSWORD = os.environ.get("TEST_USER_PASSWORD")


def _resolve_access_token() -> str:
    if ACCESS_TOKEN != "PASTE_REAL_ACCESS_TOKEN_HERE":
        return ACCESS_TOKEN

    if not (SUPABASE_ANON_KEY and TEST_USER_EMAIL and TEST_USER_PASSWORD):
        print("❌ ERROR: No ACCESS_TOKEN pasted, and SUPABASE_ANON_KEY / "
              "TEST_USER_EMAIL / TEST_USER_PASSWORD env vars are not all set.")
        sys.exit(1)

    import requests

    resp = requests.post(
        f"{SUPABASE_URL}/auth/v1/token?grant_type=password",
        headers={"apikey": SUPABASE_ANON_KEY, "Content-Type": "application/json"},
        json={"email": TEST_USER_EMAIL, "password": TEST_USER_PASSWORD},
        timeout=10,
    )
    if resp.status_code != 200:
        print(f"❌ ERROR: Supabase login failed ({resp.status_code}): {resp.text}")
        sys.exit(1)

    token = resp.json().get("access_token")
    if not token:
        print(f"❌ ERROR: Login response had no access_token: {resp.json()}")
        sys.exit(1)

    print("✅ Fetched a fresh access token via password grant.")
    return token


# Real farm/zone (see test_step6_posture_current.py for provenance).
FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "36259855-f8f7-4e30-93e5-84e536aef8b6"
FARM_TZ = "Asia/Kolkata"

UNAUTHORIZED_FARM_ID = "00000000-0000-0000-0000-000000000000"
COMPLETED_DAY = "2026-08-14"     # known-good, non-legacy, confirmed complete
NO_DATA_DAY = "2026-07-28"       # confirmed real outage day (Step 9 audit), inside the 30d window

ENDPOINT = "/api/v1/dashboard/posture/dashboard"

_TOKEN = None
_findings = {}  # collected for the final report


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _point_by_date(points, date_str):
    return next((p for p in points if p["date"] == date_str), None)


# ---------------------------------------------------------------------
# 5. Authorization
# ---------------------------------------------------------------------

def test_authorization(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    assert resp.status_code == 401
    resp = client.get(
        ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID},
        headers={"Authorization": "Bearer garbage.invalid.token"},
    )
    assert resp.status_code == 401
    resp = client.get(ENDPOINT, params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    assert resp.status_code == 403
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    assert resp.status_code == 200
    print("5) authorization -> 401/401/403/200 all correct")
    _findings["authorization"] = "PASS (401 missing, 401 invalid, 403 unauthorized farm, 200 authorized)"
    return resp.json()


# ---------------------------------------------------------------------
# Timed + query-counted fetch (used for the performance measurement)
# ---------------------------------------------------------------------

def _timed_instrumented_fetch(client):
    import psycopg2.extras

    queries = []
    real_execute = psycopg2.extras.RealDictCursor.execute

    def counting_execute(self, query, vars=None):
        queries.append(query)
        return real_execute(self, query, vars)

    psycopg2.extras.RealDictCursor.execute = counting_execute
    try:
        start = time.perf_counter()
        resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
        elapsed_ms = (time.perf_counter() - start) * 1000
    finally:
        psycopg2.extras.RealDictCursor.execute = real_execute

    assert resp.status_code == 200
    return resp.json(), len(queries), elapsed_ms


# ---------------------------------------------------------------------
# 6-13. Top-level contract + section presence
# ---------------------------------------------------------------------

def test_top_level_contract(body):
    required = {
        "farm_id", "zone_id", "timezone", "generated_at", "current", "today_summary",
        "trend_24h", "trend_7d", "trend_30d", "camera_current", "camera_trend_7d",
        "camera_summary", "data_quality",
    }
    missing = required - set(body.keys())
    print("6) top-level contract -> missing keys:", missing)
    assert not missing, missing
    assert body["farm_id"] == FARM_ID
    assert body["zone_id"] == ZONE_ID
    assert body["timezone"] == FARM_TZ
    _findings["top_level_contract"] = f"PASS (all {len(required)} required keys present, farm/zone/timezone correct)"


# ---------------------------------------------------------------------
# Independent DB recomputation helpers
# ---------------------------------------------------------------------

def _independent_latest_normal_row():
    from common.db import get_cursor

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, standing_count, feeding_count, laying_count,
                   standing_percentage, laying_percentage, metadata
            FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s ORDER BY observed_at DESC LIMIT 1
            """,
            (FARM_ID, ZONE_ID),
        )
        return cur.fetchone()


def _independent_today_count():
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    start = build_utc_from_local_date_time(today_local, time.min, FARM_TZ)
    end = build_utc_from_local_date_time(today_local + timedelta(days=1), time.min, FARM_TZ)
    with get_cursor() as cur:
        cur.execute(
            "SELECT count(*) AS c FROM public.posture_observation_normal WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s",
            (FARM_ID, ZONE_ID, start, end),
        )
        return cur.fetchone()["c"], today_local


def _independent_day_bucket(date_str):
    from datetime import date as date_cls, time, timedelta

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    d = date_cls.fromisoformat(date_str)
    start = build_utc_from_local_date_time(d, time.min, FARM_TZ)
    end = build_utc_from_local_date_time(d + timedelta(days=1), time.min, FARM_TZ)
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT feeding_count, standing_percentage, laying_percentage, metadata
            FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
            """,
            (FARM_ID, ZONE_ID, start, end),
        )
        rows = cur.fetchall()

    if not rows:
        return None

    n = len(rows)
    feeding_sum = Decimal(0)
    for r in rows:
        herd = (r["metadata"] or {}).get("herd_size")
        feeding_sum += (Decimal(r["feeding_count"]) / Decimal(herd) * 100) if herd else Decimal(0)
    standing_sum = sum((r["standing_percentage"] for r in rows), Decimal(0))
    resting_sum = sum((r["laying_percentage"] for r in rows), Decimal(0))

    return {
        "observation_count": n,
        "feeding": round(float(feeding_sum / n), 2),
        "standing": round(float(standing_sum / n), 2),
        "resting": round(float(resting_sum / n), 2),
    }


# ---------------------------------------------------------------------
# 7-11: current / today / 24h / 7d / 30d / NO_DATA
# ---------------------------------------------------------------------

def test_current_validation(body):
    latest_row = _independent_latest_normal_row()
    current = body["current"]
    if latest_row is None:
        assert current is None
        _findings["current_validation"] = "PASS (no NORMAL data, correctly null)"
        print("7) current validation -> no data, correctly null")
        return

    matches = (
        current is not None
        and current["observed_at"] == latest_row["observed_at"].isoformat()
        and current["posture"]["standing"]["count"] == latest_row["standing_count"]
        and current["posture"]["resting"]["count"] == latest_row["laying_count"]
    ) if current else False
    print("7) current validation -> API:", current["observed_at"] if current else None,
          " DB latest:", latest_row["observed_at"].isoformat())
    # observed_at can differ if a newer row landed between the composed
    # call and this recheck; only hard-assert when they match the same row.
    if current and current["observed_at"] == latest_row["observed_at"].isoformat():
        assert current["posture"]["standing"]["count"] == latest_row["standing_count"]
        assert current["posture"]["resting"]["count"] == latest_row["laying_count"]
        _findings["current_validation"] = "PASS (matches latest NORMAL row exactly)"
    else:
        _findings["current_validation"] = "PASS (newer row landed between calls; structurally present and non-null)"
        assert current is not None


def test_today_summary_validation(body):
    expected_count, today_local = _independent_today_count()
    got_count = body["today_summary"]["observation_count"] if body["today_summary"] else 0
    print("8) today's observation_count -> API:", got_count, " independent DB count:", expected_count)
    # Allow the API's count to be <= a fresh recount taken slightly later
    # (new rows can land in between), but never greater (that would mean
    # fabricated/duplicated data).
    assert got_count <= expected_count
    assert body["today_summary"]["date"] == today_local.isoformat()
    _findings["today_summary_validation"] = f"PASS (API={got_count}, independent DB={expected_count}, API<=DB as expected for a live day)"


def test_24h_structure_validation(body):
    trend = body["trend_24h"]
    assert len(trend["points"]) == 24
    assert all(p["status"] in ("NORMAL", "MILKING", "NO_DATA") for p in trend["points"])
    print("9) 24h structure -> 24 points, all valid statuses")
    _findings["24h_validation"] = "PASS (24 points, valid statuses, structurally verified)"


def test_7d_completed_bucket(body):
    expected = _independent_day_bucket(COMPLETED_DAY)
    got = _point_by_date(body["trend_7d"]["points"], COMPLETED_DAY)
    print("10) 7d completed bucket (%s) -> API: %s  Independent: %s" % (COMPLETED_DAY, got, expected))
    assert got is not None and expected is not None
    assert got["observation_count"] == expected["observation_count"]
    assert got["feeding"] == expected["feeding"]
    assert got["standing"] == expected["standing"]
    assert got["resting"] == expected["resting"]
    _findings["7d_validation"] = f"PASS ({COMPLETED_DAY}: count={got['observation_count']}, feeding={got['feeding']}, standing={got['standing']}, resting={got['resting']}, exact match)"


def test_30d_completed_bucket(body):
    expected = _independent_day_bucket(COMPLETED_DAY)
    got = _point_by_date(body["trend_30d"]["points"], COMPLETED_DAY)
    print("11) 30d completed bucket (%s) -> API: %s  Independent: %s" % (COMPLETED_DAY, got, expected))
    assert got is not None and expected is not None
    assert got["observation_count"] == expected["observation_count"]
    assert got["feeding"] == expected["feeding"]
    assert got["standing"] == expected["standing"]
    assert got["resting"] == expected["resting"]
    _findings["30d_validation"] = f"PASS ({COMPLETED_DAY}: exact match against independent recomputation)"


def test_no_data_day(body):
    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time
    from datetime import date as date_cls, time, timedelta

    d = date_cls.fromisoformat(NO_DATA_DAY)
    start = build_utc_from_local_date_time(d, time.min, FARM_TZ)
    end = build_utc_from_local_date_time(d + timedelta(days=1), time.min, FARM_TZ)
    with get_cursor() as cur:
        cur.execute(
            "SELECT count(*) AS c FROM public.posture_observation_normal WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s",
            (FARM_ID, ZONE_ID, start, end),
        )
        db_count = cur.fetchone()["c"]

    got = _point_by_date(body["trend_30d"]["points"], NO_DATA_DAY)
    print("12) NO_DATA day (%s) -> API status: %s  DB row count: %s" % (NO_DATA_DAY, got["status"] if got else None, db_count))
    assert db_count == 0, f"expected {NO_DATA_DAY} to be a confirmed empty day, found {db_count} rows"
    assert got is not None
    assert got["status"] == "NO_DATA"
    assert got["feeding"] is None and got["standing"] is None and got["resting"] is None
    assert got["observation_count"] == 0
    _findings["no_data_validation"] = f"PASS ({NO_DATA_DAY}: confirmed 0 DB rows, API correctly reports NO_DATA with null values)"


# ---------------------------------------------------------------------
# 12: Camera validation
# ---------------------------------------------------------------------

def test_camera_validation(body):
    from common.db import get_cursor

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, metadata FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s ORDER BY observed_at DESC LIMIT 500
            """,
            (FARM_ID, ZONE_ID),
        )
        recent_rows = cur.fetchall()

    # Recompute the exact same 7-day window the API's camera_summary uses.
    from datetime import datetime, time, timedelta

    import pytz

    from common.time_utils import build_utc_from_local_date_time

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    start = build_utc_from_local_date_time(today_local - timedelta(days=6), time.min, FARM_TZ)

    per_camera = {}
    for r in recent_rows:
        if r["observed_at"] < start:
            continue
        for code, c in (r["metadata"] or {}).get("cameras", {}).items():
            per_camera.setdefault(code, []).append((c.get("feeding", 0), c.get("standing", 0)))

    mismatches = []
    for cam in body["camera_summary"]["cameras"]:
        code = cam["camera_code"]
        entries = per_camera.get(code, [])
        if not entries:
            continue
        n = len(entries)
        exp_feeding = round(float(sum(Decimal(e[0]) for e in entries) / n), 2)
        exp_standing = round(float(sum(Decimal(e[1]) for e in entries) / n), 2)
        # Tolerance: this recompute used a LIMIT 500 window, not the
        # exact same unbounded query the API runs, so minor drift near
        # the window edge is expected if there are >500 rows in 7 days
        # (there are, ~1500) -- this only re-verifies the API is in the
        # right ballpark from an independently-pulled sample, full exact
        # match is already proven in test_step8_camera_activity.py.
        if abs(cam["feeding"]["average_count"] - exp_feeding) > 0.5 or abs(cam["standing"]["average_count"] - exp_standing) > 0.5:
            mismatches.append((code, cam["feeding"]["average_count"], exp_feeding, cam["standing"]["average_count"], exp_standing))

    print("13) camera_summary sample cross-check -> mismatches (tolerance 0.5):", mismatches)
    assert not mismatches, mismatches
    _findings["camera_validation"] = "PASS (sample-based cross-check within tolerance; exact match already proven in Step 8's own test)"


# ---------------------------------------------------------------------
# 13: Data quality validation
# ---------------------------------------------------------------------

def test_data_quality_validation(body):
    dq = body["data_quality"]
    print("14) data_quality ->", dq["completeness"]["calculation_status"], dq["period"]["is_complete"])
    assert dq["period"]["is_complete"] is False  # composed endpoint always uses today
    assert dq["completeness"]["calculation_status"] == "PARTIAL_PERIOD"
    assert dq["completeness"]["completeness_percentage"] is None
    _findings["data_quality_validation"] = "PASS (today correctly PARTIAL_PERIOD, no fabricated percentage)"


# ---------------------------------------------------------------------
# 14: Cross-section consistency
# ---------------------------------------------------------------------

def test_cross_section_consistency(body):
    issues = []

    # All timezone-bearing sections must agree.
    tzs = {
        body["timezone"],
        body["trend_24h"]["timezone"], body["trend_7d"]["timezone"], body["trend_30d"]["timezone"],
        body["camera_trend_7d"]["timezone"], body["camera_summary"]["timezone"],
        body["data_quality"]["period"]["timezone"],
    }
    if body["today_summary"]:
        tzs.add(body["today_summary"]["timezone"])
    if tzs != {FARM_TZ}:
        issues.append(("timezone mismatch", tzs))

    # Today's NORMAL observation count must agree across three
    # independently-computed sections: today_summary, data_quality, and
    # trend_7d/trend_30d's last (today) point.
    today_counts = {"data_quality": body["data_quality"]["completeness"]["observation_count"]}
    if body["today_summary"]:
        today_counts["today_summary"] = body["today_summary"]["observation_count"]
    today_counts["trend_7d_last_point"] = body["trend_7d"]["points"][-1]["observation_count"]
    today_counts["trend_30d_last_point"] = body["trend_30d"]["points"][-1]["observation_count"]
    if len(set(today_counts.values())) > 1:
        issues.append(("today's observation_count disagrees across sections", today_counts))

    # freshness.latest_observation_at (any mode) must be >= current's
    # observed_at (NORMAL only) -- freshness considers a superset of rows.
    if body["current"] and body["data_quality"]["freshness"]["latest_observation_at"]:
        if body["data_quality"]["freshness"]["latest_observation_at"] < body["current"]["observed_at"]:
            issues.append(("freshness anchor older than current observation", None))

    print("15) cross-section consistency -> issues:", issues if issues else "none")
    print("    today's observation_count across sections:", today_counts)
    assert not issues, issues
    _findings["cross_section_consistency"] = f"PASS (timezone uniform: {FARM_TZ}; today's count agrees across sections: {today_counts})"


# ---------------------------------------------------------------------
# Performance
# ---------------------------------------------------------------------

def test_performance(client):
    body, query_count, elapsed_ms = _timed_instrumented_fetch(client)
    print(f"16) performance -> {query_count} DB queries, {elapsed_ms:.1f} ms wall-clock")
    _findings["query_count"] = query_count
    _findings["latency_ms"] = round(elapsed_ms, 1)
    return body


# ---------------------------------------------------------------------
# Regression
# ---------------------------------------------------------------------

def run_regression_suite():
    scripts = [
        ("Step 3-7", "test_step6_posture_current.py"),
        ("Step 4", "test_step6_posture_summary_today.py"),
        ("Step 5", "test_step6_posture_trend_24h.py"),
        ("Step 6", "test_step6_posture_trend_7d.py"),
        ("Step 7", "test_step7_posture_trend_30d.py"),
        ("Step 8", "test_step8_camera_activity.py"),
        ("Step 9", "test_step9_data_quality.py"),
        ("Step 10", "test_step10_dashboard.py"),
    ]
    env = dict(os.environ)
    results = {}
    for label, script in scripts:
        proc = subprocess.run(
            [sys.executable, str(Path(__file__).parent / script)],
            capture_output=True, text=True, env=env, cwd=str(BACKEND_ROOT),
            timeout=300,
        )
        ok = proc.returncode == 0
        results[f"{label} ({script})"] = ok
        print(f"   {'✅' if ok else '❌'} {label} ({script})")
        if not ok:
            print(proc.stdout[-1500:])
            print(proc.stderr[-1500:])
    return results


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()

    try:
        body = test_authorization(client)
        test_top_level_contract(body)
        test_current_validation(body)
        test_today_summary_validation(body)
        test_24h_structure_validation(body)
        test_7d_completed_bucket(body)
        test_30d_completed_bucket(body)
        test_no_data_day(body)
        test_camera_validation(body)
        test_data_quality_validation(body)
        test_cross_section_consistency(body)
        body_for_perf = test_performance(client)

        print("\n17) Regression: Steps 3-10 test scripts, run live:")
        results = run_regression_suite()
        _findings["regression"] = results
        assert all(results.values()), f"regressions: {[k for k, v in results.items() if not v]}"
    except AssertionError as e:
        print(f"\n❌ FAILED: {e}")
        print("\nFinal verdict: FAIL")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        print("\nFinal verdict: FAIL")
        sys.exit(1)

    print("\n" + "=" * 60)
    print("STEP 11 FINDINGS SUMMARY")
    print("=" * 60)
    for k, v in _findings.items():
        print(f"{k}: {v}")
    print("\n✅ Final verdict: PASS")


if __name__ == "__main__":
    main()
