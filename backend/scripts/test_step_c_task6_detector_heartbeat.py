# scripts/test_step_c_task6_detector_heartbeat.py
"""
Detector-heartbeat ingest endpoint test script (Step C, Task 6).

Exercises POST /api/v1/ingest/detector-heartbeat end-to-end through the
real FastAPI app (fastapi.testclient.TestClient), the same way
scripts/test_step6_posture_trend_24h.py exercises the dashboard layer.

PRODUCTION SAFETY:
- Never touches the real device row (f0d5c399-6939-4b26-bf5a-fe24c2ed5738,
  "Rahuri-Jetson-01"). Every test uses a fully synthetic edge_device
  fixture (own uuid, unique code/name, a real generated api_key_hash,
  is_active=true, farm_id = the real farm 608e7a58-d46e-4f6c-bd19-b8c2a8d59050).
- Fixtures are created immediately before use and deleted in `finally`.
- The synthetic device's plaintext API key is never printed/logged.

Frozen design decision under test: this endpoint is deliberately dumb --
it records any authenticated pulse unconditionally, regardless of
payload.detector_healthy. Test F is the single most important check here:
detector_healthy=false must still update detector_last_seen_at.

Usage:
    python scripts/test_step_c_task6_detector_heartbeat.py
"""

import hashlib
import secrets
import sys
import uuid
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv(BACKEND_ROOT / ".env")

from common.db import get_cursor
from common.time_utils import utc_now

REAL_FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
REAL_DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"

ENDPOINT = "/api/v1/ingest/detector-heartbeat"


def _client():
    from fastapi.testclient import TestClient
    from main import app

    return TestClient(app)


def _make_synthetic_device(suffix: str):
    """
    Create a fully synthetic edge_device fixture row. Returns
    (device_id, plaintext_api_key). Caller MUST delete it in `finally`.
    """
    device_id = str(uuid.uuid4())
    plaintext_key = secrets.token_hex(32)
    api_key_hash = hashlib.sha256(plaintext_key.encode()).hexdigest()
    code = f"TEST_STEP_C_T6_{suffix}_{uuid.uuid4().hex[:8]}".upper()
    name = f"synthetic-test-device-step-c-task6-{suffix}"

    with get_cursor() as cur:
        cur.execute(
            """
            INSERT INTO edge_device (
                id, farm_id, name, code, api_key_hash, is_active, created_at
            )
            VALUES (%s, %s, %s, %s, %s, %s, %s)
            """,
            (device_id, REAL_FARM_ID, name, code, api_key_hash, True, utc_now()),
        )

    return device_id, plaintext_key


def _delete_device(device_id: str):
    with get_cursor() as cur:
        cur.execute("DELETE FROM edge_device WHERE id = %s", (device_id,))


def _get_detector_last_seen_at(device_id: str):
    with get_cursor() as cur:
        cur.execute(
            "SELECT detector_last_seen_at FROM edge_device WHERE id = %s",
            (device_id,),
        )
        row = cur.fetchone()
        return row["detector_last_seen_at"] if row else None


def _get_real_device_detector_last_seen_at():
    return _get_detector_last_seen_at(REAL_DEVICE_ID)


# --------------------------------------------------------------------
# A. Valid authenticated request -> HTTP success, timestamp updates
# --------------------------------------------------------------------
def test_a_valid_request_updates_timestamp(client):
    device_id, plaintext_key = _make_synthetic_device("A")
    try:
        before = _get_detector_last_seen_at(device_id)
        assert before is None, "fresh fixture should start with NULL detector_last_seen_at"

        resp = client.post(
            ENDPOINT,
            json={"detector_healthy": True, "total_frames": 100, "camera_count": 4},
            headers={"X-DEVICE-KEY": plaintext_key},
        )
        print("A) valid request ->", resp.status_code, resp.json())
        assert resp.status_code == 200
        assert resp.json() == {"status": "recorded", "detector_healthy": True}

        after = _get_detector_last_seen_at(device_id)
        assert after is not None
        print("A) detector_last_seen_at updated ->", after)
    finally:
        _delete_device(device_id)


# --------------------------------------------------------------------
# B. Invalid device key -> rejected, timestamp NOT modified
# --------------------------------------------------------------------
def test_b_invalid_device_key(client):
    device_id, plaintext_key = _make_synthetic_device("B")
    try:
        before = _get_detector_last_seen_at(device_id)

        resp = client.post(
            ENDPOINT,
            json={"detector_healthy": True},
            headers={"X-DEVICE-KEY": "wrong_" + plaintext_key},
        )
        print("B) invalid device key ->", resp.status_code, resp.json())
        assert resp.status_code == 401

        after = _get_detector_last_seen_at(device_id)
        assert after == before, "detector_last_seen_at must not change on failed auth"
    finally:
        _delete_device(device_id)


# --------------------------------------------------------------------
# C. Missing device key -> rejected (report actual status code)
# --------------------------------------------------------------------
def test_c_missing_device_key(client):
    resp = client.post(ENDPOINT, json={"detector_healthy": True})
    print("C) missing device key ->", resp.status_code, resp.json())
    # FastAPI rejects a missing required Header parameter itself, before the
    # endpoint body / resolve_device_from_headers() ever runs -> 422, not 401.
    assert resp.status_code == 422


# --------------------------------------------------------------------
# D. Unknown device (well-formed but never-provisioned key) -> rejected
# --------------------------------------------------------------------
def test_d_unknown_device(client):
    never_provisioned_key = secrets.token_hex(32)
    resp = client.post(
        ENDPOINT,
        json={"detector_healthy": True},
        headers={"X-DEVICE-KEY": never_provisioned_key},
    )
    print("D) unknown device ->", resp.status_code, resp.json())
    assert resp.status_code == 401


# --------------------------------------------------------------------
# E. detector_healthy=true -> timestamp updates
# --------------------------------------------------------------------
def test_e_healthy_true_updates(client):
    device_id, plaintext_key = _make_synthetic_device("E")
    try:
        resp = client.post(
            ENDPOINT,
            json={"detector_healthy": True},
            headers={"X-DEVICE-KEY": plaintext_key},
        )
        print("E) detector_healthy=true ->", resp.status_code, resp.json())
        assert resp.status_code == 200
        after = _get_detector_last_seen_at(device_id)
        assert after is not None
    finally:
        _delete_device(device_id)


# --------------------------------------------------------------------
# F. detector_healthy=false -> pulse STILL recorded (timestamp updates)
#    Most important test: proves this is NOT the wrong Option-A-style
#    behavior of gating the update on payload content.
# --------------------------------------------------------------------
def test_f_healthy_false_still_updates(client):
    device_id, plaintext_key = _make_synthetic_device("F")
    try:
        before = _get_detector_last_seen_at(device_id)
        assert before is None

        resp = client.post(
            ENDPOINT,
            json={"detector_healthy": False},
            headers={"X-DEVICE-KEY": plaintext_key},
        )
        print("F) detector_healthy=false ->", resp.status_code, resp.json())
        assert resp.status_code == 200
        assert resp.json()["detector_healthy"] is False

        after = _get_detector_last_seen_at(device_id)
        assert after is not None, (
            "detector_last_seen_at MUST update even when detector_healthy=false "
            "-- this endpoint must never gate the update on payload content"
        )
        print("F) detector_last_seen_at updated despite detector_healthy=false ->", after)
    finally:
        _delete_device(device_id)


# --------------------------------------------------------------------
# G. Device isolation -- device A's request must never update device B
# --------------------------------------------------------------------
def test_g_device_isolation(client):
    device_a_id, key_a = _make_synthetic_device("G_A")
    device_b_id, key_b = _make_synthetic_device("G_B")
    try:
        b_before = _get_detector_last_seen_at(device_b_id)
        assert b_before is None

        resp = client.post(
            ENDPOINT,
            json={"detector_healthy": True},
            headers={"X-DEVICE-KEY": key_a},
        )
        assert resp.status_code == 200

        a_after = _get_detector_last_seen_at(device_a_id)
        b_after = _get_detector_last_seen_at(device_b_id)
        print("G) device isolation -> A updated:", a_after is not None, "| B unchanged:", b_after == b_before)
        assert a_after is not None
        assert b_after == b_before, "device A's pulse must never update device B's timestamp"
    finally:
        _delete_device(device_a_id)
        _delete_device(device_b_id)


def main():
    client = _client()

    real_before = _get_real_device_detector_last_seen_at()
    print("Real device (Rahuri-Jetson-01) detector_last_seen_at BEFORE run ->", real_before)

    tests = [
        test_a_valid_request_updates_timestamp,
        test_b_invalid_device_key,
        test_c_missing_device_key,
        test_d_unknown_device,
        test_e_healthy_true_updates,
        test_f_healthy_false_still_updates,
        test_g_device_isolation,
    ]

    failures = []
    for t in tests:
        try:
            t(client)
        except AssertionError as e:
            failures.append((t.__name__, str(e)))
            print(f"❌ FAILED: {t.__name__}: {e}")
        except Exception as e:
            failures.append((t.__name__, str(e)))
            print(f"❌ ERROR: {t.__name__}: {e}")
            import traceback
            traceback.print_exc()

    real_after = _get_real_device_detector_last_seen_at()
    print("Real device (Rahuri-Jetson-01) detector_last_seen_at AFTER run ->", real_after)
    if real_after != real_before:
        failures.append(("production_safety_check", "REAL DEVICE ROW WAS MODIFIED"))
        print("❌ CRITICAL: real device row's detector_last_seen_at changed!")

    print()
    if failures:
        print(f"❌ {len(failures)}/{len(tests)} tests failed (plus safety checks).")
        sys.exit(1)

    print(f"✅ All {len(tests)} detector-heartbeat ingest checks passed.")
    print("✅ Real device row confirmed untouched.")


if __name__ == "__main__":
    main()
