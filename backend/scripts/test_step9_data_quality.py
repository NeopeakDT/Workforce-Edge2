# scripts/test_step9_data_quality.py
"""
Data Quality Metrics API test script (STEP 9 — Dashboard).

Exercises GET /api/v1/dashboard/posture/data-quality end-to-end through
the real FastAPI app, following the same convention as the Step 3-8
test scripts.

Tests against three real, distinct farm-local days rather than one:
- today (partial, in-progress)
- a full past day (2026-08-14)
- a day inside the pre-mode-field legacy window (2026-07-10)
plus the real Aug 3->4 cross-midnight anomaly found during Step 9
implementation, to prove the MILKING_ADJACENT_ANOMALY classification
and the boundary-row gap-detection fix both work against live data.

Usage:
    Fill in ACCESS_TOKEN (or SUPABASE_ANON_KEY / TEST_USER_EMAIL /
    TEST_USER_PASSWORD env vars — see test_step6_posture_current.py),
    then run:
        python scripts/test_step9_data_quality.py
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
# Access token (same convention as the Step 3-8 test scripts)
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
FULL_PAST_DAY = "2026-08-14"           # confirmed complete, non-legacy, has data
LEGACY_DAY = "2026-07-10"              # inside the pre-mode-field window
ANOMALY_DAY_1 = "2026-08-03"           # real ~20h cross-midnight anomaly (Step 9 audit)
ANOMALY_DAY_2 = "2026-08-04"           # same anomaly, seen from the other side

ENDPOINT = "/api/v1/dashboard/posture/data-quality"

_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def _get(client, farm_id=FARM_ID, zone_id=ZONE_ID, date=None):
    params = {"farm_id": farm_id, "zone_id": zone_id}
    if date:
        params["date"] = date
    return client.get(ENDPOINT, params=params, headers=_auth_headers())


def test_authenticated_access(client):
    resp = _get(client, date=FULL_PAST_DAY)
    print("1) authenticated access ->", resp.status_code)
    assert resp.status_code == 200
    return resp.json()


def test_unauthorized_farm(client):
    resp = _get(client, farm_id=UNAUTHORIZED_FARM_ID)
    print("2) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403


def test_missing_token(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("3) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def test_full_day_structure(full_day_body):
    for section in ("period", "coverage", "cadence", "completeness", "freshness", "mode_distribution"):
        assert section in full_day_body, f"missing section: {section}"
    print("4) full-day response has all required sections")


def test_full_day_is_complete_and_valid(full_day_body):
    period = full_day_body["period"]
    completeness = full_day_body["completeness"]
    print("5) full day period/completeness ->", period["is_complete"], completeness["calculation_status"])
    assert period["date"] == FULL_PAST_DAY
    assert period["is_complete"] is True
    assert completeness["calculation_status"] == "VALID"
    assert completeness["completeness_percentage"] is not None
    assert 0 <= completeness["completeness_percentage"] <= 100


def test_full_day_cadence_matches_audit(full_day_body):
    cadence = full_day_body["cadence"]
    print("6) full-day cadence ->", cadence)
    # Audit established ~5.03 min median cadence; a single real day
    # should land close to it, not exactly (real jitter).
    assert 4.5 <= cadence["median_interval_minutes"] <= 5.5
    assert cadence["largest_gap_classification"] in (
        "NORMAL_CADENCE", "MILKING_EXPECTED", "MILKING_ADJACENT_ANOMALY", "UNEXPLAINED",
    )


def test_completeness_independent_recomputation(full_day_body):
    """
    Independently recompute completeness for FULL_PAST_DAY straight from
    the DB and compare to the API — same cross-check discipline as
    Steps 4-8's tests.
    """
    from common.db import get_cursor
    from common.time_utils import build_utc_from_local_date_time
    from datetime import date as date_cls, time, timedelta

    d = date_cls.fromisoformat(FULL_PAST_DAY)
    start = build_utc_from_local_date_time(d, time.min, FARM_TZ)
    end = build_utc_from_local_date_time(d + timedelta(days=1), time.min, FARM_TZ)

    with get_cursor() as cur:
        cur.execute(
            """
            SELECT count(*) FILTER (WHERE metadata->>'mode'='NORMAL') AS normal_c,
                   count(*) FILTER (WHERE metadata->>'mode'='MILKING') AS milking_c,
                   count(*) FILTER (WHERE metadata->>'mode' IS NULL) AS legacy_c
            FROM public.posture_observation
            WHERE farm_id=%s AND zone_id=%s AND observed_at>=%s AND observed_at<%s
            """,
            (FARM_ID, ZONE_ID, start, end),
        )
        row = cur.fetchone()

    expected_milking_minutes = row["milking_c"] * 5.0
    expected_count = round((1440 - expected_milking_minutes) / 5.0)
    expected_pct = round(row["normal_c"] / expected_count * 100, 2) if expected_count else None

    got = full_day_body["completeness"]
    print("7) independent completeness recomputation -> expected", expected_pct, "got", got["completeness_percentage"])
    assert got["observation_count"] == row["normal_c"]
    assert got["expected_observation_count"] == expected_count
    assert got["completeness_percentage"] == expected_pct
    assert row["legacy_c"] == 0  # sanity: this day must not be legacy-affected


def test_today_partial_period(client):
    resp = _get(client)  # no date -> defaults to today
    print("8) today (no date param) ->", resp.status_code)
    assert resp.status_code == 200
    body = resp.json()
    print("   period.is_complete ->", body["period"]["is_complete"])
    print("   completeness.calculation_status ->", body["completeness"]["calculation_status"])
    assert body["period"]["is_complete"] is False
    assert body["completeness"]["calculation_status"] == "PARTIAL_PERIOD"
    assert body["completeness"]["completeness_percentage"] is None
    assert body["completeness"]["expected_observation_count"] is None
    # observation_count must still be real, not null, even when partial.
    assert isinstance(body["completeness"]["observation_count"], int)
    return body


def test_legacy_day(client):
    resp = _get(client, date=LEGACY_DAY)
    print("9) legacy day ->", resp.status_code)
    assert resp.status_code == 200
    body = resp.json()
    print("   calculation_status ->", body["completeness"]["calculation_status"])
    print("   mode_distribution ->", body["mode_distribution"])
    assert body["completeness"]["calculation_status"] == "LEGACY_DATA_PRESENT"
    assert body["completeness"]["completeness_percentage"] is None
    assert body["mode_distribution"]["legacy_unknown_count"] > 0
    assert body["mode_distribution"]["normal_count"] == 0
    return body


def test_no_fabricated_zero_on_legacy_or_partial(today_body, legacy_body):
    # The core "never fabricate a zero" rule: neither the partial day
    # nor the legacy day should report completeness_percentage: 0 (which
    # would misleadingly look like "0% healthy") -- both must be null.
    print("10) no fabricated zero -> today", today_body["completeness"]["completeness_percentage"],
          "legacy", legacy_body["completeness"]["completeness_percentage"])
    assert today_body["completeness"]["completeness_percentage"] is None
    assert legacy_body["completeness"]["completeness_percentage"] is None


def test_cross_midnight_anomaly_both_sides(client):
    """
    The real ~20h Aug 3->4 gap found during implementation must surface
    as MILKING_ADJACENT_ANOMALY on BOTH days it touches, with the true
    duration (not split into two smaller in-day gaps) -- this is exactly
    what the boundary-row fix added during this implementation exists
    to guarantee.
    """
    body1 = _get(client, date=ANOMALY_DAY_1).json()
    body2 = _get(client, date=ANOMALY_DAY_2).json()

    c1, c2 = body1["cadence"], body2["cadence"]
    print("11) Aug 3 cadence ->", c1)
    print("    Aug 4 cadence ->", c2)

    assert c1["largest_gap_classification"] == "MILKING_ADJACENT_ANOMALY"
    assert c2["largest_gap_classification"] == "MILKING_ADJACENT_ANOMALY"
    # Both days see the SAME real gap (it only happened once) -- same duration.
    assert c1["largest_gap_minutes"] == c2["largest_gap_minutes"]
    assert c1["largest_gap_minutes"] > 1000  # the true ~1208 min, not a split-down fragment


def test_freshness_cross_check(full_day_body):
    freshness = full_day_body["freshness"]
    print("12) freshness ->", freshness)
    assert freshness["device_status"] in ("ONLINE", "OFFLINE", "UNKNOWN")
    assert freshness["latest_observation_age_minutes"] is not None
    assert freshness["latest_device_heartbeat_age_minutes"] is not None
    # Freshness reflects "right now", not the historical period being
    # inspected -- so even a query for a past day should show a recent
    # (small) age given the pipeline is live in production right now.
    assert freshness["latest_observation_age_minutes"] < 60


def test_mode_distribution_sums(full_day_body):
    md = full_day_body["mode_distribution"]
    completeness = full_day_body["completeness"]
    print("13) mode_distribution ->", md)
    assert md["normal_count"] == completeness["observation_count"]
    assert md["normal_count"] + md["milking_count"] + md["legacy_unknown_count"] > 0


def test_no_aggregation_window_minutes_field(full_day_body, today_body, legacy_body):
    import json

    for body in (full_day_body, today_body, legacy_body):
        blob = json.dumps(body)
        assert "aggregation_window" not in blob.lower(), \
            "the mislabeled metadata field must not leak into this API"
    print("14) mislabeled aggregation_window_minutes field never exposed")


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()

    try:
        full_day_body = test_authenticated_access(client)
        test_unauthorized_farm(client)
        test_missing_token(client)
        test_full_day_structure(full_day_body)
        test_full_day_is_complete_and_valid(full_day_body)
        test_full_day_cadence_matches_audit(full_day_body)
        test_completeness_independent_recomputation(full_day_body)
        today_body = test_today_partial_period(client)
        legacy_body = test_legacy_day(client)
        test_no_fabricated_zero_on_legacy_or_partial(today_body, legacy_body)
        test_cross_midnight_anomaly_both_sides(client)
        test_freshness_cross_check(full_day_body)
        test_mode_distribution_sums(full_day_body)
        test_no_aggregation_window_minutes_field(full_day_body, today_body, legacy_body)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All data-quality checks passed.")


if __name__ == "__main__":
    main()
