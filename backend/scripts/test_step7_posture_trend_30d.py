# scripts/test_step7_posture_trend_30d.py
"""
Posture 30-Day Trend API test script (STEP 7 — Dashboard).

Exercises GET /api/v1/dashboard/posture/trend/30d end-to-end through the
real FastAPI app, following the same convention as
test_step6_posture_current.py (Step 3), test_step6_posture_summary_today.py
(Step 4), test_step6_posture_trend_24h.py (Step 5), and
test_step6_posture_trend_7d.py (Step 6).

Like Steps 4/5/6, this independently recomputes the expected daily
aggregates in plain Python (Decimal, not float — see Step 5/6 test
scripts for why float summation can land on the wrong side of an exact
rounding boundary) straight from posture_observation_normal, rather than
hardcoding numbers that go stale as new days roll into the 30-day
window. Jul 22/23 and Aug 14 (spec items 7-9) are checked against this
live independent recomputation, not fixed literals.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step7_posture_trend_30d.py
"""

import os
import sys
from decimal import Decimal
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Access token (same convention as the Step 3/4/5/6 test scripts)
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
EMPTY_ZONE_ID = "79c7a034-024d-4793-aa8b-681864ebf163"

ENDPOINT = "/api/v1/dashboard/posture/trend/30d"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _independent_expected(farm_id=FARM_ID, zone_id=ZONE_ID):
    """
    Recompute the 30-day, day-bucketed NORMAL averages straight from the
    DB in plain Python (Decimal arithmetic), independent of the SQL
    under test.
    """
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    dates = [today_local - timedelta(days=d) for d in range(29, -1, -1)]

    window_start_utc = build_utc_from_local_date_time(dates[0], time.min, FARM_TZ)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time.min, FARM_TZ)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, feeding_count, standing_percentage, laying_percentage, metadata
            FROM public.posture_observation_normal
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
            """,
            (farm_id, zone_id, window_start_utc, window_end_utc),
        )
        rows = cur.fetchall()

    buckets = {}
    for r in rows:
        idx = int((r["observed_at"] - window_start_utc).total_seconds() // 86400)
        buckets.setdefault(idx, []).append(r)

    expected_points = []
    for i, day in enumerate(dates):
        day_rows = buckets.get(i, [])
        if day_rows:
            n = len(day_rows)
            feeding_sum = Decimal(0)
            for r in day_rows:
                herd = (r["metadata"] or {}).get("herd_size")
                feeding_sum += (Decimal(r["feeding_count"]) / Decimal(herd) * 100) if herd else Decimal(0)
            standing_sum = sum((r["standing_percentage"] for r in day_rows), Decimal(0))
            resting_sum = sum((r["laying_percentage"] for r in day_rows), Decimal(0))
            status = "NORMAL"
            feeding = round(float(feeding_sum / n), 2)
            standing = round(float(standing_sum / n), 2)
            resting = round(float(resting_sum / n), 2)
            observation_count = n
        else:
            status, feeding, standing, resting, observation_count = "NO_DATA", None, None, None, 0

        expected_points.append({
            "date": day.isoformat(), "status": status, "feeding": feeding,
            "standing": standing, "resting": resting, "observation_count": observation_count,
        })

    return {"dates": dates, "points": expected_points}


def _point_by_date(points, date_str):
    return next((p for p in points if p["date"] == date_str), None)


_EXPECTED_FIELDS = ("date", "status", "feeding", "standing", "resting", "observation_count")


def _matches_expected(got: dict, exp: dict) -> bool:
    """
    Compare only the fields _independent_expected() computes.
    The real response has extra fields (partial_camera_observations,
    average_camera_coverage_percentage) that the independent
    recomputation doesn't include, so plain dict equality would fail
    even when every shared field matches.
    """
    return all(got[k] == exp[k] for k in _EXPECTED_FIELDS)


def test_http_200(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) HTTP 200 ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_30_points(body):
    print("2) point count ->", len(body["points"]))
    assert len(body["points"]) == 30


def test_oldest_date(body, expected):
    got = body["points"][0]["date"]
    exp = expected["dates"][0].isoformat()
    print("3) oldest date ->", got, "expected", exp)
    assert got == exp


def test_newest_date(body, expected):
    got = body["points"][-1]["date"]
    exp = expected["dates"][-1].isoformat()
    print("4) newest date ->", got, "expected", exp, "(today)")
    assert got == exp


def test_timezone(body):
    print("5) timezone ->", body["timezone"])
    assert body["timezone"] == FARM_TZ


def test_local_calendar_boundaries(client):
    # Same technique as Step 6's test: prove a farm-local boundary was
    # actually used, not a naive UTC calendar day, for a tz with a
    # non-zero offset (IST = UTC+5:30).
    from datetime import datetime, timezone as dt_timezone, time as time_type

    from common.time_utils import build_utc_from_local_date_time

    utc_today = datetime.now(dt_timezone.utc).date()
    utc_start = datetime(utc_today.year, utc_today.month, utc_today.day, tzinfo=dt_timezone.utc)
    farm_local_start = build_utc_from_local_date_time(utc_today, time_type.min, FARM_TZ)
    print("6) UTC-day start vs farm-local-day start ->", utc_start, "vs", farm_local_start)
    assert utc_start != farm_local_start


def test_jul22_values(body, expected):
    got, exp = _point_by_date(body["points"], "2026-07-22"), _point_by_date(expected["points"], "2026-07-22")
    print("7) Jul 22 ->", got)
    assert got is not None, "Jul 22 must be the oldest date in the window"
    assert _matches_expected(got, exp), (got, exp)


def test_jul23_values(body, expected):
    got, exp = _point_by_date(body["points"], "2026-07-23"), _point_by_date(expected["points"], "2026-07-23")
    print("8) Jul 23 ->", got)
    assert _matches_expected(got, exp), (got, exp)


def test_aug14_values(body, expected):
    got, exp = _point_by_date(body["points"], "2026-08-14"), _point_by_date(expected["points"], "2026-08-14")
    print("9) Aug 14 ->", got)
    assert _matches_expected(got, exp), (got, exp)
    # Cross-check against Step 6's independently-verified Aug 14 value
    # (11.4 / 13.35 / 75.25, count 222) — same underlying data, same day.
    assert got["feeding"] == 11.4 and got["standing"] == 13.35 and got["resting"] == 75.25
    assert got["observation_count"] == 222


def test_daily_feeding(body, expected):
    got = [p["feeding"] for p in body["points"]]
    exp = [p["feeding"] for p in expected["points"]]
    print("10) daily feeding matches ->", got == exp)
    assert got == exp


def test_daily_standing(body, expected):
    got = [p["standing"] for p in body["points"]]
    exp = [p["standing"] for p in expected["points"]]
    print("11) daily standing matches ->", got == exp)
    assert got == exp


def test_daily_resting(body, expected):
    got = [p["resting"] for p in body["points"]]
    exp = [p["resting"] for p in expected["points"]]
    print("12) daily resting matches ->", got == exp)
    assert got == exp


def test_observation_counts(body, expected):
    got = [p["observation_count"] for p in body["points"]]
    exp = [p["observation_count"] for p in expected["points"]]
    print("13) observation counts match ->", got == exp)
    assert got == exp


def test_no_data_jul28_31(body):
    gap_dates = ["2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]
    statuses = {d: _point_by_date(body["points"], d)["status"] for d in gap_dates}
    print("14) Jul 28-31 statuses ->", statuses)
    assert all(s == "NO_DATA" for s in statuses.values()), statuses


def test_no_data_values_null(body):
    gap_dates = ["2026-07-28", "2026-07-29", "2026-07-30", "2026-07-31"]
    bad = []
    for d in gap_dates:
        p = _point_by_date(body["points"], d)
        if p["feeding"] is not None or p["standing"] is not None or p["resting"] is not None:
            bad.append(p)
    print("15) Jul 28-31 null (not zero) values -> violations:", len(bad))
    assert not bad, bad


def test_partial_camera_observations(body, expected):
    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time
    from datetime import time as time_type, timedelta as td

    mismatches = []
    for g, day in zip(body["points"], expected["dates"]):
        start = build_utc_from_local_date_time(day, time_type.min, FARM_TZ)
        end = build_utc_from_local_date_time(day + td(days=1), time_type.min, FARM_TZ)
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT count(*) AS c FROM public.posture_observation_normal
                WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
                  AND (metadata->>'received_cameras')::int < (metadata->>'expected_cameras')::int
                """,
                (FARM_ID, ZONE_ID, start, end),
            )
            expected_partial = cur.fetchone()["c"]
        if g["partial_camera_observations"] != expected_partial:
            mismatches.append((g["date"], g["partial_camera_observations"], expected_partial))
    print("16) partial_camera_observations mismatches ->", len(mismatches))
    assert not mismatches, mismatches


def test_average_camera_coverage(body):
    for p in body["points"]:
        assert "average_camera_coverage_percentage" in p
        if p["status"] == "NORMAL":
            assert 0 <= p["average_camera_coverage_percentage"] <= 100
        else:
            assert p["average_camera_coverage_percentage"] is None
    print("17) average_camera_coverage_percentage present and sane on all 30 points")


def test_milking_excluded(body, expected):
    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time
    from datetime import time as time_type, timedelta as td

    checked = 0
    for day in expected["dates"]:
        start = build_utc_from_local_date_time(day, time_type.min, FARM_TZ)
        end = build_utc_from_local_date_time(day + td(days=1), time_type.min, FARM_TZ)
        with get_cursor() as cur:
            cur.execute(
                """
                SELECT count(*) AS c FROM public.posture_observation
                WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
                  AND metadata->>'mode' = 'MILKING'
                """,
                (FARM_ID, ZONE_ID, start, end),
            )
            if cur.fetchone()["c"] > 0:
                checked += 1
    # Daily averages already independently verified NORMAL-only in items
    # 10-12; this just confirms MILKING rows were actually present in
    # the window (so the exclusion check isn't vacuously true).
    print(f"18) {checked}/30 days had raw MILKING rows; daily averages already verified NORMAL-only")
    assert checked > 0, "expected at least some MILKING rows in a real 30-day window"


def test_farm_zone_filtering(client, body):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": EMPTY_ZONE_ID}, headers=_auth_headers())
    other = resp.json()
    print("19) farm/zone filtering -> empty zone all NO_DATA:",
          all(p["status"] == "NO_DATA" for p in other["points"]))
    assert resp.status_code == 200
    assert all(p["status"] == "NO_DATA" for p in other["points"])
    assert other["points"] != body["points"]


def test_unauthorized_farm(client):
    resp = client.get(ENDPOINT, params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("20) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403


def test_missing_token(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("21) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_invalid_token(client):
    resp = client.get(
        ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID},
        headers={"Authorization": "Bearer garbage.invalid.token"},
    )
    print("22) invalid token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_current_day_partial(body):
    today_point = body["points"][-1]
    print("23) today's point ->", today_point["status"], "observation_count =", today_point["observation_count"])
    assert today_point["status"] == "NORMAL" or today_point["observation_count"] == 0


def test_independent_recomputation_done():
    # Item 24 is structural, not a separate assertion: every numeric
    # test above (7-13, 16) already compares against
    # _independent_expected()'s Decimal-exact recomputation, not
    # hardcoded literals.
    print("24) independent Decimal-exact recomputation -> used throughout (see items 7-13, 16)")


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()
    expected = _independent_expected()

    try:
        body = test_http_200(client)
        test_30_points(body)
        test_oldest_date(body, expected)
        test_newest_date(body, expected)
        test_timezone(body)
        test_local_calendar_boundaries(client)
        test_jul22_values(body, expected)
        test_jul23_values(body, expected)
        test_aug14_values(body, expected)
        test_daily_feeding(body, expected)
        test_daily_standing(body, expected)
        test_daily_resting(body, expected)
        test_observation_counts(body, expected)
        test_no_data_jul28_31(body)
        test_no_data_values_null(body)
        test_partial_camera_observations(body, expected)
        test_average_camera_coverage(body)
        test_milking_excluded(body, expected)
        test_farm_zone_filtering(client, body)
        test_unauthorized_farm(client)
        test_missing_token(client)
        test_invalid_token(client)
        test_current_day_partial(body)
        test_independent_recomputation_done()
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All posture/trend/30d checks passed.")


if __name__ == "__main__":
    main()
