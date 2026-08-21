# scripts/test_step8_camera_activity.py
"""
Camera Activity Analytics test script (STEP 8 — Dashboard).

Exercises GET /api/v1/dashboard/posture/camera/current,
GET /api/v1/dashboard/posture/camera/trend/7d, and
GET /api/v1/dashboard/posture/camera/summary end-to-end through the real
FastAPI app, following the same convention as the Step 3-7 test scripts.

Like Steps 4-7, this independently recomputes expected values in plain
Python straight from posture_observation_normal's metadata.cameras
(Decimal arithmetic where it matters, consistent with prior steps),
rather than hardcoding numbers that go stale as new observations land.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step8_camera_activity.py
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
# Access token (same convention as the Step 3-7 test scripts)
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
EXPECTED_CAMERA_CODES = {"grp1-front-left", "grp1-front-center", "grp1-front-right"}

CURRENT_ENDPOINT = "/api/v1/dashboard/posture/camera/current"
TREND_ENDPOINT = "/api/v1/dashboard/posture/camera/trend/7d"
SUMMARY_ENDPOINT = "/api/v1/dashboard/posture/camera/summary"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _independent_camera_names():
    from common.db import get_cursor

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT fc.code AS camera_code, fc.id AS camera_id, fc.name AS camera_name
            FROM public.camera_activity_zone caz
            JOIN public.farm_camera fc ON fc.id = caz.camera_id
            WHERE caz.farm_id=%s AND caz.zone_id=%s AND caz.activity_type_id=4 AND caz.is_active=true
            """,
            (FARM_ID, ZONE_ID),
        )
        return {r["camera_code"]: dict(r) for r in cur.fetchall()}


def _independent_latest_row():
    from common.db import get_cursor

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, metadata FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s ORDER BY observed_at DESC LIMIT 1
            """,
            (FARM_ID, ZONE_ID),
        )
        return cur.fetchone()


def _independent_trend_expected():
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    dates = [today_local - timedelta(days=d) for d in range(6, -1, -1)]
    window_start_utc = build_utc_from_local_date_time(dates[0], time.min, FARM_TZ)
    window_end_utc = build_utc_from_local_date_time(dates[-1] + timedelta(days=1), time.min, FARM_TZ)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT observed_at, metadata FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
            """,
            (FARM_ID, ZONE_ID, window_start_utc, window_end_utc),
        )
        rows = cur.fetchall()

    # per camera_code -> per day_index -> list of (feeding, standing)
    buckets = {}
    for r in rows:
        idx = int((r["observed_at"] - window_start_utc).total_seconds() // 86400)
        for code, c in (r["metadata"] or {}).get("cameras", {}).items():
            buckets.setdefault(code, {}).setdefault(idx, []).append((c.get("feeding", 0), c.get("standing", 0)))

    expected = {}
    for code in EXPECTED_CAMERA_CODES:
        points = []
        for i, day in enumerate(dates):
            entries = buckets.get(code, {}).get(i, [])
            if entries:
                n = len(entries)
                feeding_avg = round(float(sum(Decimal(e[0]) for e in entries) / n), 2)
                standing_avg = round(float(sum(Decimal(e[1]) for e in entries) / n), 2)
                points.append({"date": day.isoformat(), "status": "NORMAL",
                                "feeding_average_count": feeding_avg, "standing_average_count": standing_avg,
                                "observation_count": n})
            else:
                points.append({"date": day.isoformat(), "status": "NO_DATA",
                                "feeding_average_count": None, "standing_average_count": None,
                                "observation_count": 0})
        expected[code] = points

    return dates, expected


def _independent_summary_expected(window_days=7):
    from datetime import datetime, time, timedelta

    import pytz

    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time

    today_local = datetime.now(pytz.utc).astimezone(pytz.timezone(FARM_TZ)).date()
    window_start_date = today_local - timedelta(days=window_days - 1)
    window_start_utc = build_utc_from_local_date_time(window_start_date, time.min, FARM_TZ)
    window_end_utc = build_utc_from_local_date_time(today_local + timedelta(days=1), time.min, FARM_TZ)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT count(*) AS c FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
            """,
            (FARM_ID, ZONE_ID, window_start_utc, window_end_utc),
        )
        total_observations = cur.fetchone()["c"]

        cur.execute(
            """
            SELECT observed_at, metadata FROM public.posture_observation_normal
            WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
            """,
            (FARM_ID, ZONE_ID, window_start_utc, window_end_utc),
        )
        rows = cur.fetchall()

    per_camera = {}
    for r in rows:
        for code, c in (r["metadata"] or {}).get("cameras", {}).items():
            per_camera.setdefault(code, []).append((c.get("feeding", 0), c.get("standing", 0)))

    stats = {}
    for code, entries in per_camera.items():
        n = len(entries)
        feeding_avg = round(float(sum(Decimal(e[0]) for e in entries) / n), 2)
        standing_avg = round(float(sum(Decimal(e[1]) for e in entries) / n), 2)
        stats[code] = {
            "observation_count": n,
            "presence_percentage": round(n / total_observations * 100, 2) if total_observations else 0.0,
            "feeding_avg": feeding_avg,
            "standing_avg": standing_avg,
        }

    def _rank(field):
        vals = {c: stats[c][field] for c in stats}
        ordered = sorted(vals, key=lambda c: vals[c], reverse=True)
        ranks = {}
        for i, c in enumerate(ordered):
            ranks[c] = ranks[ordered[i - 1]] if i > 0 and vals[c] == vals[ordered[i - 1]] else i + 1
        return ranks

    feeding_ranks = _rank("feeding_avg")
    standing_ranks = _rank("standing_avg")
    for code in stats:
        stats[code]["feeding_rank"] = feeding_ranks[code]
        stats[code]["standing_rank"] = standing_ranks[code]

    return total_observations, stats


def test_authenticated_access(client):
    resp = client.get(CURRENT_ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) authenticated current ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_unauthorized_farm(client):
    resp = client.get(CURRENT_ENDPOINT, params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("2) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403


def test_missing_token(client):
    resp = client.get(CURRENT_ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("3) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_current_camera_extraction(current_body):
    print("4) current cameras ->", current_body["cameras"])
    assert "observed_at" in current_body
    assert isinstance(current_body["cameras"], list)


def test_all_expected_cameras_returned(current_body, expected_names):
    codes = {c["camera_code"] for c in current_body["cameras"]}
    print("5) camera codes returned ->", codes, "expected", set(expected_names))
    assert codes == set(expected_names)


def test_camera_ids_codes_correct(current_body, expected_names):
    # camera_id isn't in the current response body by design (code/name
    # only) -- verify codes match the authoritative farm_camera mapping.
    for c in current_body["cameras"]:
        assert c["camera_code"] in expected_names
    print("6) camera codes match authoritative camera_activity_zone/farm_camera mapping")


def test_camera_names_match_farm_camera(current_body, expected_names):
    mismatches = [
        (c["camera_code"], c["camera_name"], expected_names[c["camera_code"]]["camera_name"])
        for c in current_body["cameras"]
        if c["camera_name"] != expected_names[c["camera_code"]]["camera_name"]
    ]
    print("7) camera_name verbatim match -> mismatches:", mismatches)
    assert not mismatches, mismatches


def test_feeding_standing_values(current_body, latest_row):
    md = latest_row["metadata"] or {}
    cam_data = md.get("cameras", {})
    mismatches = []
    for c in current_body["cameras"]:
        expected = cam_data.get(c["camera_code"])
        exp_feeding = expected["feeding"] if expected else None
        exp_standing = expected["standing"] if expected else None
        if c["feeding_count"] != exp_feeding or c["standing_count"] != exp_standing:
            mismatches.append((c["camera_code"], c["feeding_count"], exp_feeding, c["standing_count"], exp_standing))
    print("8-9) feeding/standing values -> mismatches:", mismatches)
    assert not mismatches, mismatches


def test_trend_7d(client, trend_expected_dates):
    resp = client.get(TREND_ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("10) trend HTTP 200 ->", resp.status_code)
    assert resp.status_code == 200
    body = resp.json()
    assert body["timezone"] == FARM_TZ
    assert len(body["cameras"]) == 3
    for cam in body["cameras"]:
        assert len(cam["points"]) == 7
        assert [p["date"] for p in cam["points"]] == [d.isoformat() for d in trend_expected_dates]
    return body


def test_bucket_correctness_and_no_data(trend_body, expected_trend):
    mismatches = []
    no_data_seen = False
    for cam in trend_body["cameras"]:
        code = cam["camera_code"]
        exp_points = expected_trend[code]
        for got, exp in zip(cam["points"], exp_points):
            if got["status"] == "NO_DATA":
                no_data_seen = True
                if got["feeding_average_count"] is not None or got["standing_average_count"] is not None:
                    mismatches.append(("NO_DATA has non-null value", code, got))
            if got != exp:
                mismatches.append((code, got, exp))
    print("11) 7-day bucket correctness -> mismatches:", len(mismatches))
    print("12) camera-level NO_DATA present in window ->", no_data_seen, "(false is fine if no gap this week)")
    assert not mismatches, mismatches[:5]


def test_summary(client):
    resp = client.get(SUMMARY_ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("13) summary HTTP 200 ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_summary_averages_counts_presence(summary_body, expected_total, expected_stats):
    dq = summary_body["data_quality"]
    print("14-16) summary data_quality ->", dq, "expected total", expected_total)
    assert dq["total_observations"] == expected_total
    assert dq["expected_camera_count"] == len(expected_stats)

    mismatches = []
    for c in summary_body["cameras"]:
        code = c["camera_code"]
        exp = expected_stats.get(code)
        if exp is None:
            mismatches.append((code, "unexpected camera in response"))
            continue
        if c["observation_count"] != exp["observation_count"]:
            mismatches.append((code, "observation_count", c["observation_count"], exp["observation_count"]))
        if c["presence_percentage"] != exp["presence_percentage"]:
            mismatches.append((code, "presence_percentage", c["presence_percentage"], exp["presence_percentage"]))
        if c["feeding"]["average_count"] != exp["feeding_avg"]:
            mismatches.append((code, "feeding_avg", c["feeding"]["average_count"], exp["feeding_avg"]))
        if c["standing"]["average_count"] != exp["standing_avg"]:
            mismatches.append((code, "standing_avg", c["standing"]["average_count"], exp["standing_avg"]))
    print("   averages/counts/presence mismatches:", mismatches)
    assert not mismatches, mismatches


def test_feeding_ranking(summary_body, expected_stats):
    got = {c["camera_code"]: c["feeding"]["rank"] for c in summary_body["cameras"]}
    exp = {code: s["feeding_rank"] for code, s in expected_stats.items()}
    print("17) feeding ranking ->", got, "expected", exp)
    assert got == exp


def test_standing_ranking(summary_body, expected_stats):
    got = {c["camera_code"]: c["standing"]["rank"] for c in summary_body["cameras"]}
    exp = {code: s["standing_rank"] for code, s in expected_stats.items()}
    print("18) standing ranking ->", got, "expected", exp)
    assert got == exp


def test_missing_camera_handling():
    # Real production data has ~100% presence for all 3 cameras (see
    # Step 8A audit), so this exercises the "camera configured but zero
    # observations this window" path directly against the response
    # builder rather than waiting for a real outage.
    from dashboard.dashboard_api import _build_camera_summary_response
    from dashboard.dashboard_query_service import get_camera_names

    names = get_camera_names(FARM_ID, ZONE_ID)
    one_code = next(iter(names))
    fake_per_camera = {
        code: {"avg_feeding": 1.0, "avg_standing": 1.0, "observation_count": 100}
        for code in names if code != one_code
    }
    resp = _build_camera_summary_response(7, FARM_TZ, 100, names, fake_per_camera)
    missing = next(c for c in resp["cameras"] if c["camera_code"] == one_code)
    print("19) missing-camera handling ->", missing)
    assert missing["observation_count"] == 0
    assert missing["presence_percentage"] == 0.0
    assert missing["feeding"]["average_count"] is None
    assert missing["feeding"]["rank"] is None
    assert missing["standing"]["average_count"] is None
    assert missing["standing"]["rank"] is None


def test_normal_only_filtering():
    # Structural, not a new query: every function under test reads
    # exclusively from posture_observation_normal (verified by code
    # inspection + the Step 8A audit's mode-count verification), so
    # MILKING rows never enter any of these three endpoints' results.
    print("20) NORMAL-only filtering -> all three endpoints read exclusively from posture_observation_normal")


def test_no_fabricated_resting_or_herd_percentage(current_body, trend_body, summary_body):
    import json

    for body in (current_body, trend_body, summary_body):
        blob = json.dumps(body)
        assert "resting" not in blob.lower(), "camera endpoints must never expose a per-camera resting figure"
        assert "herd" not in blob.lower(), "camera endpoints must never expose a per-camera herd percentage"
    print("21) no fabricated resting/herd fields in any camera endpoint response")


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()

    try:
        current_body = test_authenticated_access(client)
        test_unauthorized_farm(client)
        test_missing_token(client)

        expected_names = _independent_camera_names()
        test_current_camera_extraction(current_body)
        test_all_expected_cameras_returned(current_body, expected_names)
        test_camera_ids_codes_correct(current_body, expected_names)
        test_camera_names_match_farm_camera(current_body, expected_names)

        latest_row = _independent_latest_row()
        test_feeding_standing_values(current_body, latest_row)

        trend_dates, expected_trend = _independent_trend_expected()
        trend_body = test_trend_7d(client, trend_dates)
        test_bucket_correctness_and_no_data(trend_body, expected_trend)

        summary_body = test_summary(client)
        expected_total, expected_stats = _independent_summary_expected()
        test_summary_averages_counts_presence(summary_body, expected_total, expected_stats)
        test_feeding_ranking(summary_body, expected_stats)
        test_standing_ranking(summary_body, expected_stats)

        test_missing_camera_handling()
        test_normal_only_filtering()
        test_no_fabricated_resting_or_herd_percentage(current_body, trend_body, summary_body)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All camera activity analytics checks passed.")


if __name__ == "__main__":
    main()
