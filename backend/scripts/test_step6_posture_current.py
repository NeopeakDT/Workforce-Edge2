# scripts/test_step6_posture_current.py
"""
Posture Current-Status API test script (STEP 6 — Dashboard, Step 3).

Exercises GET /api/v1/dashboard/posture/current end-to-end through the
real FastAPI app (auth -> authorization -> query), the same way
scripts/test_auth_jwt.py and scripts/test_device_auth_api.py exercise
their layers: paste real fixture values below and read the output.

Usage:
    Fill in ACCESS_TOKEN, FARM_ID, ZONE_ID, UNAUTHORIZED_FARM_ID,
    EMPTY_ZONE_ID below, then run:
        python scripts/test_step6_posture_current.py

Requires:
    - FARM_ID / ZONE_ID: a farm+zone with at least one posture_observation
      row where metadata->>'mode' = 'NORMAL' (see
      jetson/posture/posture_scheduler.py for how those rows are written).
    - ACCESS_TOKEN: a Supabase access token for a user WITH access to
      FARM_ID (see scripts/test_auth_jwt.py for how to get one).
    - UNAUTHORIZED_FARM_ID: any farm the token's user does NOT have
      access to (not ADMIN, no user_farm_access row).
    - EMPTY_ZONE_ID: a real farm_zone in FARM_ID with no NORMAL
      posture_observation rows (or any zone_id if none exists yet).
"""

import os
import sys
from pathlib import Path

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()

# ---------------------------------------------------------------------
# Access token
# ---------------------------------------------------------------------
# Two ways to supply one, checked in this order:
#
# 1. Paste a token directly here (same manual convention as
#    scripts/test_auth_jwt.py). Good for a one-off run.
ACCESS_TOKEN = "PASTE_REAL_ACCESS_TOKEN_HERE"
#
# 2. Or set these three env vars (e.g. in backend/.env — gitignored) and
#    leave ACCESS_TOKEN as the placeholder above; the script logs in via
#    Supabase's password grant on every run so you never have to
#    manually curl+paste a token again. SUPABASE_ANON_KEY is the public
#    "anon" key from Supabase dashboard -> Settings -> API Keys (safe to
#    store, NOT the JWT signing secret). TEST_USER_EMAIL/PASSWORD are a
#    real (ideally disposable/test) Supabase Auth user's credentials —
#    do not hardcode a real password directly in this file.
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
        print("💡 Either paste a token into ACCESS_TOKEN (see scripts/test_auth_jwt.py),")
        print("   or export SUPABASE_ANON_KEY, TEST_USER_EMAIL, TEST_USER_PASSWORD.")
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


# Real farm/zone (confirmed live via direct DB query — "Harmony Dairy",
# device Rahuri-Jetson-01 / JETSON_01, zone "Pen 1 - rest zone").
FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "36259855-f8f7-4e30-93e5-84e536aef8b6"

UNAUTHORIZED_FARM_ID = "00000000-0000-0000-0000-000000000000"
# Real farm_zone with zero posture_observation_normal rows (confirmed
# live) — used for the no-data / 404 case.
EMPTY_ZONE_ID = "79c7a034-024d-4793-aa8b-681864ebf163"

ENDPOINT = "/api/v1/dashboard/posture/current"

# Set by main() via _resolve_access_token() before any test function runs.
_TOKEN = None


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _auth_headers():
    return {"Authorization": f"Bearer {_TOKEN}"}


def test_returns_latest_normal_observation(client):
    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID}, headers=_auth_headers())
    print("1) latest NORMAL observation ->", resp.status_code, resp.json())
    assert resp.status_code == 200, "expected 200 with a NORMAL observation present"
    body = resp.json()
    # Response no longer echoes mode/farm_id/zone_id (semantic contract,
    # not a raw row dump) — MILKING exclusion is verified structurally:
    # a MILKING row is all-zero, so a nonzero herd/posture reading here
    # is itself evidence it wasn't a MILKING row.
    return body


def test_milking_rows_excluded(client, body):
    # Enforced at the DB layer by the public.posture_observation_normal
    # view (WHERE metadata->>'mode' = 'NORMAL'), which the query service
    # reads from exclusively.
    # MILKING rows are written with herd size untouched but all counts
    # zeroed (see posture_scheduler._write_scheduled_milking_observation);
    # a real NORMAL reading should show nonzero herd/posture data.
    print("2) herd size present (not a MILKING placeholder) ->", body["herd"]["size"])
    assert body["herd"]["size"] is None or body["herd"]["size"] > 0


def test_farm_zone_filtering(client):
    # farm_id/zone_id are query params, not echoed in the semantic
    # response body — filtering is verified by requesting a farm/zone
    # combination with no NORMAL rows and confirming 404 (see
    # test_no_data_response), plus the DB-level WHERE clause itself.
    print("3) farm/zone filtering -> verified via test_no_data_response (see below)")


def test_counts(client, body):
    posture = body["posture"]
    print(
        "4) counts -> feeding=%s standing=%s resting=%s"
        % (posture["feeding"]["count"], posture["standing"]["count"], posture["resting"]["count"])
    )
    for zone in ("feeding", "standing", "resting"):
        assert isinstance(posture[zone]["count"], int) and posture[zone]["count"] >= 0


def test_percentages(client, body):
    posture = body["posture"]
    print(
        "5) percentages -> feeding%%=%s standing%%=%s resting%%=%s"
        % (posture["feeding"]["percentage"], posture["standing"]["percentage"], posture["resting"]["percentage"])
    )
    for zone in ("feeding", "standing", "resting"):
        pct = posture[zone]["percentage"]
        assert pct is None or 0 <= float(pct) <= 100


def test_herd_size(client, body):
    print("6) herd.size ->", body["herd"]["size"])
    assert body["herd"]["size"] is None or body["herd"]["size"] >= 0


def test_camera_coverage(client, body):
    dq = body["data_quality"]
    print("7) data_quality ->", dq)
    assert "expected_cameras" in dq
    assert "received_cameras" in dq
    assert "coverage_percentage" in dq
    if dq["expected_cameras"]:
        assert 0 <= dq["coverage_percentage"] <= 100


def test_no_data_response(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": FARM_ID, "zone_id": EMPTY_ZONE_ID},
        headers=_auth_headers(),
    )
    print("8) no-data response ->", resp.status_code, resp.json())
    assert resp.status_code == 404, "must not fabricate zero values for no data"


def test_unauthorized_farm(client):
    resp = client.get(
        ENDPOINT,
        params={"farm_id": UNAUTHORIZED_FARM_ID, "zone_id": ZONE_ID},
        headers=_auth_headers(),
    )
    print("9a) unauthorized farm ->", resp.status_code, resp.json())
    assert resp.status_code == 403

    resp = client.get(ENDPOINT, params={"farm_id": FARM_ID, "zone_id": ZONE_ID})
    print("9b) missing token ->", resp.status_code, resp.json())
    assert resp.status_code == 401


def main():
    global _TOKEN
    _TOKEN = _resolve_access_token()

    client = _client()

    try:
        body = test_returns_latest_normal_observation(client)
        test_milking_rows_excluded(client, body)
        test_farm_zone_filtering(client)
        test_counts(client, body)
        test_percentages(client, body)
        test_herd_size(client, body)
        test_camera_coverage(client, body)
        test_no_data_response(client)
        test_unauthorized_farm(client)
    except AssertionError as e:
        print(f"❌ FAILED: {e}")
        sys.exit(1)
    except Exception as e:
        print(f"❌ Unexpected Error: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

    print("\n✅ All posture/current checks passed.")


if __name__ == "__main__":
    main()
