# scripts/test_step6_posture_trend_24h.py
"""
Posture 24-Hour Trend API test script (STEP 6 — Dashboard, Step 5).

Exercises GET /api/v1/dashboard/posture/trend/24h end-to-end through the
real FastAPI app, the same way test_step6_posture_current.py (Step 3)
and test_step6_posture_summary_today.py (Step 4) exercise their layers.

Like Step 4's test, this does NOT hardcode expected values — the rolling
24h window keeps moving and accumulating observations. Instead it
independently recomputes the same per-hour aggregates in plain Python
straight from posture_observation_normal + posture_observation (raw, for
MILKING detection), and asserts the API agrees.

IMPORTANT — use Decimal, not float, for the independent average:
standing_percentage/laying_percentage are stored as exact numeric(5,2).
Postgres's avg() computes an exact Decimal average, and the endpoint
rounds that once with Python's round() (banker's rounding). Summing as
float() before dividing accumulates binary-representation error that can
land on the wrong side of an exact .xx5 rounding boundary (discovered
during implementation: a bucket averaging to exactly 12.625 rounded to
12.62 via exact Decimal math, but 12.63 via float summation). Decimal
summation here avoids that false mismatch.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step6_posture_trend_24h.py
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
# Access token (same convention as the Step 3/4 test scripts)
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
# Real farm_zone with zero posture_observation rows at all (NORMAL or
# MILKING) — used for the "all 24 hours NO_DATA" case.
EMPTY_ZONE_ID = "79c7a034-024d-4793-aa8b-681864ebf163"

ENDPOINT = "/api/v1/dashboard/posture/trend/24h"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _independent_expected(farm_id=FARM_ID, zone_id=ZONE_ID):
    """
    Recompute the 24-hour, hour-bucketed NORMAL averages + MILKING
    presence straight from the DB in plain Python, independent of the
    SQL under test.
    """
    from datetime import datetime, timedelta

    import pytz

    from common.db import get_cursor

    tz = pytz.timezone(FARM_TZ)
    now_local = datetime.now(pytz.utc).astimezone(tz)
    current_hour_local = now_local.replace(minute=0, second=0, microsecond=0)
    window_start_local = current_hour_local - timedelta(hours=23)
    window_start_utc = window_start_local.astimezone(pytz.utc)
    window_end_utc = window_start_utc + timedelta(hours=24)

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
        normal_rows = cur.fetchall()

        cur.execute(
            """
            SELECT observed_at
            FROM public.posture_observation
            WHERE farm_id = %s AND zone_id = %s
              AND observed_at >= %s AND observed_at < %s
              AND metadata ->> 'mode' = 'MILKING'
            """,
            (farm_id, zone_id, window_start_utc, window_end_utc),
        )
        milking_rows = cur.fetchall()

    buckets = {}
    for r in normal_rows:
        idx = int((r["observed_at"] - window_start_utc).total_seconds() // 3600)
        buckets.setdefault(idx, []).append(r)

    milking_idx = set()
    for r in milking_rows:
        idx = int((r["observed_at"] - window_start_utc).total_seconds() // 3600)
        milking_idx.add(idx)

    expected_points = []
    for i in range(24):
        rows = buckets.get(i, [])
        if rows:
            n = len(rows)
            feeding_sum = Decimal(0)
            for r in rows:
                herd = (r["metadata"] or {}).get("herd_size")
                feeding_sum += (Decimal(r["feeding_count"]) / Decimal(herd) * 100) if herd else Decimal(0)
            standing_sum = sum((r["standing_percentage"] for r in rows), Decimal(0))
            resting_sum = sum((r["laying_percentage"] for r in rows), Decimal(0))
            status = "NORMAL"
            feeding = round(float(feeding_sum / n), 2)
            standing = round(float(standing_sum / n), 2)
            resting = round(float(resting_sum / n), 2)
            observation_count = n
        elif i in milking_idx:
            status, feeding, standing, resting, observation_count = "MILKING", None, None, None, 0
        else:
            status, feeding, standing, resting, observation_count = "NO_DATA", None, None, None, 0

        expected_points.append({
            "status": status, "feeding": feeding, "standing": standing,
            "resting": resting, "observation_count": observation_count,
        })

    return {"window_start_local": window_start_local, "points": expected_points}


def test_http_200(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) HTTP 200 ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_24_points(body):
    print("2) point count ->", len(body["points"]))
    assert len(body["points"]) == 24


def test_timezone(body):
    print("3) timezone ->", body["timezone"])
    assert body["timezone"] == FARM_TZ


def test_hourly_aggregation_matches(body, expected):
    """Covers items 4-7: hourly aggregation, feeding/standing/resting %."""
    mismatches = []
    for i, (got, exp) in enumerate(zip(body["points"], expected["points"])):
        for key in ("status", "feeding", "standing", "resting", "observation_count"):
            if got[key] != exp[key]:
                mismatches.append((i, got["time"], key, got[key], exp[key]))
    print(f"4-7) hourly aggregation -> {len(body['points']) - len(mismatches)}/24 fields-sets matched")
    if mismatches:
        for m in mismatches[:10]:
            print("   mismatch:", m)
    assert not mismatches, f"{len(mismatches)} bucket field mismatches"


def test_statuses_present(body):
    statuses = {p["status"] for p in body["points"]}
    print("8-10) statuses seen in this window ->", statuses)
    assert statuses <= {"NORMAL", "MILKING", "NO_DATA"}
    # Not asserting all three appear — depends on the current window's
    # real data. Cross-checked against the independent recomputation
    # above (test_hourly_aggregation_matches), which is the real proof.


def test_milking_and_no_data_are_null(body):
    bad = [
        p for p in body["points"]
        if p["status"] in ("MILKING", "NO_DATA")
        and (p["feeding"] is not None or p["standing"] is not None or p["resting"] is not None)
    ]
    print("11-12) MILKING/NO_DATA points with non-null values ->", len(bad))
    assert not bad, f"MILKING/NO_DATA points must never carry fabricated values: {bad}"


def test_partial_camera_observations_present(body):
    for p in body["points"]:
        assert "partial_camera_observations" in p
        assert isinstance(p["partial_camera_observations"], int)
    print("13) partial_camera_observations present on all 24 points")


def test_farm_zone_filtering(client, body):
    # A different real zone with its own data must not produce the same
    # timeline as ZONE_ID (unless coincidentally identical, vanishingly
    # unlikely with live sensor data) — proves the WHERE clause is
    # actually scoping by zone_id, not just farm_id.
    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": EMPTY_ZONE_ID},
        headers=_auth_headers(),
    )
    other = resp.json()
    print("14) farm/zone filtering -> empty zone has", len(other["points"]), "points, all NO_DATA:",
          all(p["status"] == "NO_DATA" for p in other["points"]))
    assert resp.status_code == 200
    assert all(p["status"] == "NO_DATA" for p in other["points"])
    assert other["points"] != body["points"]


def test_unauthorized_farm(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID},
        headers=_auth_headers(),
    )
    print("15) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403


def test_missing_token(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("16) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_invalid_token(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": ZONE_ID},
        headers={"Authorization": "Bearer garbage.invalid.token"},
    )
    print("17) invalid token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_all_no_data_still_24_points(client):
    # Item 18: an all-NO_DATA window still returns exactly 24 points,
    # never 404. EMPTY_ZONE_ID already covers this — re-asserted here
    # explicitly against the endpoint's own contract, not just filtering.
    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": EMPTY_ZONE_ID},
        headers=_auth_headers(),
    )
    body = resp.json()
    print("18) all-NO_DATA window -> status", resp.status_code, "points", len(body["points"]))
    assert resp.status_code == 200
    assert len(body["points"]) == 24
    assert all(p["status"] == "NO_DATA" for p in body["points"])


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()
    expected = _independent_expected()

    try:
        body = test_http_200(client)
        test_24_points(body)
        test_timezone(body)
        test_hourly_aggregation_matches(body, expected)
        test_statuses_present(body)
        test_milking_and_no_data_are_null(body)
        test_partial_camera_observations_present(body)
        test_farm_zone_filtering(client, body)
        test_unauthorized_farm(client)
        test_missing_token(client)
        test_invalid_token(client)
        test_all_no_data_still_24_points(client)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All posture/trend/24h checks passed.")


if __name__ == "__main__":
    main()
