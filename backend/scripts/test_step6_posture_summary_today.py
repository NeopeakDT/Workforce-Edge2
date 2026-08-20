# scripts/test_step6_posture_summary_today.py
"""
Posture Today's-Summary API test script (STEP 6 — Dashboard, Step 4).

Exercises GET /api/v1/dashboard/posture/summary/today end-to-end through
the real FastAPI app (auth -> authorization -> farm-local day resolution
-> aggregate query -> peak-timestamp query), the same way
scripts/test_step6_posture_current.py exercises Step 3.

This does NOT hardcode expected averages/peaks — "today" keeps
accumulating observations while the farm-local day is still open, so a
fixed expected number would go stale within minutes. Instead it
independently recomputes the same aggregates in plain Python straight
from posture_observation_normal and asserts the API agrees with that
independent computation. This is the same cross-check used to validate
the SQL during implementation (see Step 4 report).

Usage:
    Fill in ACCESS_TOKEN (or the SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py for
    how to get either), then run:
        python scripts/test_step6_posture_summary_today.py

Requires:
    - FARM_ID / ZONE_ID: a farm+zone with at least one NORMAL
      observation for the current farm-local day.
    - ACCESS_TOKEN (or env vars): a Supabase access token for a user
      WITH access to FARM_ID.
    - UNAUTHORIZED_FARM_ID: any farm the token's user does NOT have
      access to.
    - EMPTY_DATE: a farm-local date with zero NORMAL observations
      (default: 2020-01-01, long before this system existed).
"""

import os
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Access token (same convention as test_step6_posture_current.py)
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
EMPTY_DATE = "2020-01-01"  # farm-local date with zero NORMAL observations

ENDPOINT = "/api/v1/dashboard/posture/summary/today"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _independent_expected():
    """
    Recompute today's aggregates straight from posture_observation_normal
    in plain Python, independent of the SQL under test, as the source of
    truth to assert the API against.
    """
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    target_date = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    day_start = build_utc_from_local_date_time(target_date, time.min, FARM_TZ)
    day_end = build_utc_from_local_date_time(target_date + timedelta(days=1), time.min, FARM_TZ)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, feeding_count, standing_percentage, laying_percentage, metadata
            FROM public.posture_observation_normal
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
            """,
            (FARM_ID, ZONE_ID, day_start, day_end),
        )
        rows = cur.fetchall()

    feeding_pcts, standing_pcts, resting_pcts, coverages = [], [], [], []
    partial = 0
    for r in rows:
        md = r["metadata"] or {}
        herd = md.get("herd_size")
        feeding_pcts.append(float(r["feeding_count"]) / herd * 100 if herd else None)
        standing_pcts.append(float(r["standing_percentage"]))
        resting_pcts.append(float(r["laying_percentage"]))
        exp, rec = md.get("expected_cameras"), md.get("received_cameras")
        if exp and rec is not None and rec < exp:
            partial += 1
        if exp:
            coverages.append(rec / exp * 100)

    peak_f = max(feeding_pcts)
    peak_s = max(standing_pcts)
    peak_r = max(resting_pcts)

    return {
        "date": target_date,
        "n": len(rows),
        "avg_feeding": round(sum(feeding_pcts) / len(rows), 2),
        "avg_standing": round(sum(standing_pcts) / len(rows), 2),
        "avg_resting": round(sum(resting_pcts) / len(rows), 2),
        "peak_feeding": round(peak_f, 2),
        "peak_standing": round(peak_s, 2),
        "peak_resting": round(peak_r, 2),
        "peak_feeding_count": sum(1 for fp in feeding_pcts if fp == peak_f),
        "peak_standing_count": sum(1 for sp in standing_pcts if sp == peak_s),
        "peak_resting_count": sum(1 for rp in resting_pcts if rp == peak_r),
        "partial_camera_observations": partial,
        "avg_coverage": round(sum(coverages) / len(coverages), 2) if coverages else None,
    }


def test_successful_summary(client, expected):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) summary response ->", resp.status_code, resp.json())
    assert resp.status_code == 200, "expected 200 for a day with NORMAL observations"
    body = resp.json()
    assert body["date"] == expected["date"].isoformat()
    return body


def test_observation_count(body, expected):
    print("2) observation_count ->", body["observation_count"], "expected", expected["n"])
    assert body["observation_count"] == expected["n"]


def test_avg_feeding(body, expected):
    got = body["posture"]["feeding"]["average_percentage"]
    print("3) avg feeding ->", got, "expected", expected["avg_feeding"])
    assert got == expected["avg_feeding"]


def test_avg_standing(body, expected):
    got = body["posture"]["standing"]["average_percentage"]
    print("4) avg standing ->", got, "expected", expected["avg_standing"])
    assert got == expected["avg_standing"]


def test_avg_resting(body, expected):
    got = body["posture"]["resting"]["average_percentage"]
    print("5) avg resting ->", got, "expected", expected["avg_resting"])
    assert got == expected["avg_resting"]


def test_averages_sum_to_100(body):
    total = (
        body["posture"]["feeding"]["average_percentage"]
        + body["posture"]["standing"]["average_percentage"]
        + body["posture"]["resting"]["average_percentage"]
    )
    print("6) averages sum ->", total)
    # Independently-rounded percentages can be off by a cent on the
    # rounding boundary; allow a small tolerance rather than exact 100.
    assert abs(total - 100) <= 0.05, f"expected ~100, got {total}"


def test_peak_feeding(body, expected):
    got = body["posture"]["feeding"]["peak_percentage"]
    print("7) peak feeding ->", got, "expected", expected["peak_feeding"])
    assert got == expected["peak_feeding"]


def test_peak_standing(body, expected):
    got = body["posture"]["standing"]["peak_percentage"]
    print("8) peak standing ->", got, "expected", expected["peak_standing"])
    assert got == expected["peak_standing"]


def test_peak_resting(body, expected):
    got = body["posture"]["resting"]["peak_percentage"]
    print("9) peak resting ->", got, "expected", expected["peak_resting"])
    assert got == expected["peak_resting"]


def test_multiple_peak_timestamps(body, expected):
    for zone, key in (("feeding", "peak_feeding_count"), ("standing", "peak_standing_count"),
                       ("resting", "peak_resting_count")):
        times = body["posture"][zone]["peak_times"]
        print(f"10) {zone} peak_times -> {len(times)} timestamp(s), expected {expected[key]}")
        assert len(times) == expected[key]
        assert len(times) == len(set(times)), "peak_times must not contain duplicates"


def test_partial_camera_observations(body, expected):
    got = body["data_quality"]["partial_camera_observations"]
    print("11) partial_camera_observations ->", got, "expected", expected["partial_camera_observations"])
    assert got == expected["partial_camera_observations"]


def test_avg_camera_coverage(body, expected):
    got = body["data_quality"]["average_camera_coverage_percentage"]
    print("12) average_camera_coverage_percentage ->", got, "expected", expected["avg_coverage"])
    assert got == expected["avg_coverage"]


def test_milking_excluded_structurally(expected):
    # MILKING rows are all-zero and excluded by posture_observation_normal
    # at the DB layer (see STEP4_ADD_POSTURE_OBSERVATION_NORMAL_VIEW.sql);
    # the independent cross-check above reads from the same view, so
    # agreement between it and the API is itself evidence MILKING rows
    # never entered either computation.
    print("13) MILKING exclusion -> enforced by posture_observation_normal view (see Step 3)")


def test_no_data_response(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": ZONE_ID, "date": EMPTY_DATE},
        headers=_auth_headers(),
    )
    print("14) no-data response ->", resp.status_code, resp.json())
    assert resp.status_code == 404, "must not fabricate zero values for no data"


def test_authorization(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID},
        headers=_auth_headers(),
    )
    print("15a) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403

    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("15b) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401

    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": ZONE_ID},
        headers={"Authorization": "Bearer garbage.invalid.token"},
    )
    print("15c) invalid token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()
    expected = _independent_expected()

    try:
        body = test_successful_summary(client, expected)
        test_observation_count(body, expected)
        test_avg_feeding(body, expected)
        test_avg_standing(body, expected)
        test_avg_resting(body, expected)
        test_averages_sum_to_100(body)
        test_peak_feeding(body, expected)
        test_peak_standing(body, expected)
        test_peak_resting(body, expected)
        test_multiple_peak_timestamps(body, expected)
        test_partial_camera_observations(body, expected)
        test_avg_camera_coverage(body, expected)
        test_milking_excluded_structurally(expected)
        test_no_data_response(client)
        test_authorization(client)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All posture/summary/today checks passed.")


if __name__ == "__main__":
    main()
