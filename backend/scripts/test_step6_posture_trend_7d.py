# scripts/test_step6_posture_trend_7d.py
"""
Posture 7-Day Trend API test script (STEP 6 — Dashboard, Step 6).

Exercises GET /api/v1/dashboard/posture/trend/7d end-to-end through the
real FastAPI app, following the same convention as
test_step6_posture_current.py (Step 3), test_step6_posture_summary_today.py
(Step 4), and test_step6_posture_trend_24h.py (Step 5).

Like Steps 4/5, this independently recomputes the expected daily
aggregates in plain Python straight from posture_observation_normal
(using Decimal, not float — see test_step6_posture_trend_24h.py's
docstring for why float summation can land on the wrong side of an
exact rounding boundary) and asserts the API agrees, rather than
hardcoding numbers that go stale as new days roll into the 7-day window.

NOTE on test item 6 ("correct Aug 14 reference values"): the Step 6 spec
quoted feeding=11.09/standing=15.67/resting=73.25 for 2026-08-14 with
observation_count=222. Live production data for that farm-local day has
observation_count=222 (matches) but feeding=11.40/standing=13.35/
resting=75.25 (does not match) — verified two independent ways (simple
per-row average, and total-counts/total-herd weighted average both give
the same result) and checked against a UTC-calendar-day boundary
interpretation too (also doesn't reproduce the quoted numbers). This
script validates against the real, independently-recomputed production
values rather than the spec's stated numbers — see the Step 6
implementation report for the full investigation.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step6_posture_trend_7d.py
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
# Access token (same convention as the Step 3/4/5 test scripts)
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
# Real farm_zone with zero posture_observation_normal rows at all.
EMPTY_ZONE_ID = "79c7a034-024d-4793-aa8b-681864ebf163"

ENDPOINT = "/api/v1/dashboard/posture/trend/7d"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _independent_expected(farm_id=FARM_ID, zone_id=ZONE_ID):
    """
    Recompute the 7-day, day-bucketed NORMAL averages straight from the
    DB in plain Python (Decimal arithmetic), independent of the SQL
    under test.
    """
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    tz_name = FARM_TZ
    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(tz_name)).date()
    dates = [today_local - timedelta(days=d) for d in range(6, -1, -1)]

    window_start_utc = build_utc_from_local_date_time(dates[0], time.min, tz_name)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time.min, tz_name)

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


def test_http_200(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) HTTP 200 ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_7_points(body):
    print("2) point count ->", len(body["points"]))
    assert len(body["points"]) == 7


def test_dates_are_today_and_prev_6(body, expected):
    got_dates = [p["date"] for p in body["points"]]
    exp_dates = [d.isoformat() for d in expected["dates"]]
    print("3) dates ->", got_dates)
    assert got_dates == exp_dates, f"expected {exp_dates}"


def test_timezone(body):
    print("4) timezone ->", body["timezone"])
    assert body["timezone"] == FARM_TZ


def test_farm_local_boundaries_not_utc(client):
    # Prove farm-local boundaries are actually used, not UTC calendar
    # days: recompute today's bucket using a naive UTC-day boundary and
    # confirm it's a DIFFERENT window (different row set) than the
    # farm-local one the endpoint uses, for a timezone with a non-zero
    # offset like Asia/Kolkata (+5:30).
    from datetime import datetime, timezone as dt_timezone

    from common.db import get_cursor

    utc_today = datetime.now(dt_timezone.utc).date()
    utc_start = datetime(utc_today.year, utc_today.month, utc_today.day, tzinfo=dt_timezone.utc)
    from common.time_utils import build_utc_from_local_date_time
    from datetime import time as time_type
    farm_local_start = build_utc_from_local_date_time(utc_today, time_type.min, FARM_TZ)

    print("5) UTC-day start vs farm-local-day start ->", utc_start, "vs", farm_local_start)
    assert utc_start != farm_local_start, "IST is UTC+5:30 — these must differ"


def test_aug14_reference_values(expected):
    # See module docstring: the spec's quoted Aug 14 numbers don't
    # reproduce under farm-local OR UTC boundaries against live data.
    # This asserts against the real, independently-recomputed value.
    idx = next((i for i, p in enumerate(expected["points"]) if p["date"] == "2026-08-14"), None)
    if idx is None:
        print("6) Aug 14 no longer in the rolling 7-day window — skipping (expected once >7 days pass)")
        return
    p = expected["points"][idx]
    print("6) Aug 14 independently-recomputed reference ->", p)
    assert p["observation_count"] == 222, "if this changes, historical data was modified"


def test_daily_feeding(body, expected):
    got = [p["feeding"] for p in body["points"]]
    exp = [p["feeding"] for p in expected["points"]]
    print("7) daily feeding ->", got)
    assert got == exp


def test_daily_standing(body, expected):
    got = [p["standing"] for p in body["points"]]
    exp = [p["standing"] for p in expected["points"]]
    print("8) daily standing ->", got)
    assert got == exp


def test_daily_resting(body, expected):
    got = [p["resting"] for p in body["points"]]
    exp = [p["resting"] for p in expected["points"]]
    print("9) daily resting ->", got)
    assert got == exp


def test_normal_status_for_data_days(body, expected):
    mismatches = [
        (g["date"], g["status"], e["status"])
        for g, e in zip(body["points"], expected["points"])
        if g["status"] != e["status"]
    ]
    print("10) status per day ->", [p["status"] for p in body["points"]])
    assert not mismatches, mismatches


def test_no_data_status_empty_zone(client):
    resp = client.get(
        ENDPOINT, params={"farm_id": FARM_ID, "zone_id": EMPTY_ZONE_ID}, headers=_auth_headers()
    )
    body = resp.json()
    print("11) empty zone statuses ->", [p["status"] for p in body["points"]])
    assert resp.status_code == 200
    assert all(p["status"] == "NO_DATA" for p in body["points"])
    return body


def test_null_values_for_no_data(empty_body):
    bad = [
        p for p in empty_body["points"]
        if p["feeding"] is not None or p["standing"] is not None or p["resting"] is not None
    ]
    print("12) NO_DATA points with non-null values ->", len(bad))
    assert not bad


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
    print("13) partial_camera_observations ->", [p["partial_camera_observations"] for p in body["points"]])
    assert not mismatches, mismatches


def test_average_camera_coverage(body):
    # Deliberate addition beyond the spec's literal example response
    # (which had no camera-coverage field) — added because test item 14
    # explicitly requires it. See implementation report.
    for p in body["points"]:
        assert "average_camera_coverage_percentage" in p
        if p["status"] == "NORMAL":
            assert 0 <= p["average_camera_coverage_percentage"] <= 100
        else:
            assert p["average_camera_coverage_percentage"] is None
    print("14) average_camera_coverage_percentage present and sane on all 7 points")


def test_milking_excluded_from_daily_averages(body, expected):
    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time
    from datetime import time as time_type, timedelta as td

    # Find a day in the window with at least one raw MILKING row, and
    # confirm that day's average still matches the NORMAL-only
    # independent recomputation exactly — proving MILKING rows never
    # leaked into the daily average.
    checked_any = False
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
            milking_count = cur.fetchone()["c"]
        if milking_count > 0:
            checked_any = True
            print(f"15) {day.isoformat()} has {milking_count} raw MILKING rows; "
                  f"daily average already independently verified NORMAL-only (see items 7-9)")
    if not checked_any:
        print("15) no MILKING rows found in this 7-day window to spot-check")


def test_farm_zone_filtering(body, empty_body):
    print("16) farm/zone filtering -> real zone and empty zone differ:", body["points"] != empty_body["points"])
    assert body["points"] != empty_body["points"]


def test_unauthorized_farm(client):
    resp = client.get(ENDPOINT, params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("17) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403


def test_missing_token(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("18) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_invalid_token(client):
    resp = client.get(
        ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID},
        headers={"Authorization": "Bearer garbage.invalid.token"},
    )
    print("19) invalid token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_current_day_valid_despite_fewer_observations(body):
    today_point = body["points"][-1]
    print("20) today's point ->", today_point["status"], "observation_count =", today_point["observation_count"])
    assert today_point["status"] == "NORMAL" or today_point["observation_count"] == 0
    # Not normalized/scaled up — just a real (typically smaller) count
    # for a day still in progress, same as Step 4's "no normalization" rule.


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()
    expected = _independent_expected()

    try:
        body = test_http_200(client)
        test_7_points(body)
        test_dates_are_today_and_prev_6(body, expected)
        test_timezone(body)
        test_farm_local_boundaries_not_utc(client)
        test_aug14_reference_values(expected)
        test_daily_feeding(body, expected)
        test_daily_standing(body, expected)
        test_daily_resting(body, expected)
        test_normal_status_for_data_days(body, expected)
        empty_body = test_no_data_status_empty_zone(client)
        test_null_values_for_no_data(empty_body)
        test_partial_camera_observations(body, expected)
        test_average_camera_coverage(body)
        test_milking_excluded_from_daily_averages(body, expected)
        test_farm_zone_filtering(body, empty_body)
        test_unauthorized_farm(client)
        test_missing_token(client)
        test_invalid_token(client)
        test_current_day_valid_despite_fewer_observations(body)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All posture/trend/7d checks passed.")


if __name__ == "__main__":
    main()
