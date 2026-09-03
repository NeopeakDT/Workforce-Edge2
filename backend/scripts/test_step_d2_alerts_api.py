"""
backend/scripts/test_step_d2_alerts_api.py
Step D2 — GET /api/v1/dashboard/alerts endpoint test.

Exercises the real FastAPI app end-to-end (real routing, real query
delegation to dashboard_query_service.list_recent_alerts(), real DB reads
against synthetic STEPD_TEST fixtures cleaned up in finally) with ONE
deliberate deviation from this repo's existing dashboard-test convention
(test_step6_posture_trend_24h.py etc.): those scripts require a real
Supabase login (SUPABASE_ANON_KEY/TEST_USER_EMAIL/TEST_USER_PASSWORD), none
of which are available in this environment (confirmed unset, and this
project's security rules forbid ever sourcing/printing real credentials
during automated work). Authentication itself is therefore mocked via
unittest.mock.patch on dashboard_api.parse_auth_header/user_can_access_farm
-- everything downstream (routing, query params, validation, DB query,
response shape) is exercised for real, unmocked. This is the same
isolation technique already used elsewhere in this session's test suites
for external dependencies unavailable in this sandbox.

Run: python scripts/test_step_d2_alerts_api.py
"""

from pathlib import Path
import sys
import uuid
from unittest.mock import patch

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from common.auth import AuthContext

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"
ENDPOINT = "/api/v1/dashboard/alerts"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def _client():
    from fastapi.testclient import TestClient
    from main import app
    return TestClient(app)


def make_rule(cur, *, alert_type, severity="WARNING"):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 'STEPD_TEST D2 rule', %s, %s, %s, true)
        """,
        (rule_id, FARM_ID, Json({"metric": "test", "operator": ">", "value": 0}), severity, alert_type),
    )
    return rule_id


def make_alert_log(cur, *, rule_id, alert_type, lifecycle_state, dedup_key, device_id=None):
    alert_id = str(uuid.uuid4())
    now = utc_now()
    resolved_at = now if lifecycle_state == "RESOLVED" else None
    cur.execute(
        """
        INSERT INTO alert_log
            (id, farm_id, alert_rule_id, alert_type, device_id,
             triggered_at, status, lifecycle_state, resolved_at, dedup_key, message, details)
        VALUES (%s, %s, %s, %s, %s, %s, 'SENT', %s, %s, %s, %s, %s)
        """,
        (alert_id, FARM_ID, rule_id, alert_type, device_id,
         now, lifecycle_state, resolved_at, dedup_key, "STEPD_TEST D2 alert", Json({})),
    )
    return alert_id


def cleanup(rule_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))


def test_full_matrix():
    client = _client()
    rule_ids = []
    try:
        with get_cursor() as cur:
            device_rule = make_rule(cur, alert_type="EDGE_DEVICE", severity="CRITICAL")
            rule_ids.append(device_rule)
            active_id = make_alert_log(
                cur, rule_id=device_rule, alert_type="EDGE_DEVICE", lifecycle_state="ACTIVE",
                dedup_key=str(uuid.uuid4()), device_id=DEVICE_ID,
            )
            resolved_id = make_alert_log(
                cur, rule_id=device_rule, alert_type="EDGE_DEVICE", lifecycle_state="RESOLVED",
                dedup_key=str(uuid.uuid4()), device_id=DEVICE_ID,
            )
            activity_rule = make_rule(cur, alert_type="ACTIVITY")
            rule_ids.append(activity_rule)
            activity_active_id = make_alert_log(
                cur, rule_id=activity_rule, alert_type="ACTIVITY", lifecycle_state="ACTIVE",
                dedup_key=str(uuid.uuid4()),
            )

        fake_ctx = AuthContext(user_id=str(uuid.uuid4()), role="USER")

        # --- 1. Missing authentication -> 401 ---
        resp = client.get(ENDPOINT, params={"farm_id": FARM_ID})
        check("missing Authorization header -> 401", resp.status_code == 401, detail=str(resp.status_code))

        # --- 2. Invalid authentication -> 401 ---
        with patch("dashboard.dashboard_api.parse_auth_header", side_effect=PermissionError("bad token")):
            resp = client.get(
                ENDPOINT, params={"farm_id": FARM_ID}, headers={"Authorization": "Bearer garbage"},
            )
        check("invalid token -> 401", resp.status_code == 401, detail=str(resp.status_code))

        # --- 3. Authenticated but unauthorized for this farm -> 403 ---
        with patch("dashboard.dashboard_api.parse_auth_header", return_value=fake_ctx), \
             patch("dashboard.dashboard_api.user_can_access_farm", return_value=False):
            resp = client.get(
                ENDPOINT, params={"farm_id": FARM_ID}, headers={"Authorization": "Bearer x"},
            )
        check("no farm access -> 403", resp.status_code == 403, detail=str(resp.status_code))

        # --- 4. Authenticated + authorized -> 200, default lifecycle_state=ACTIVE ---
        with patch("dashboard.dashboard_api.parse_auth_header", return_value=fake_ctx), \
             patch("dashboard.dashboard_api.user_can_access_farm", return_value=True):
            resp = client.get(
                ENDPOINT, params={"farm_id": FARM_ID}, headers={"Authorization": "Bearer x"},
            )
            check("authorized request -> 200", resp.status_code == 200, detail=str(resp.text[:200]))
            body = resp.json()
            check("response echoes lifecycle_state default ACTIVE", body.get("lifecycle_state") == "ACTIVE")
            check("response echoes farm_id", body.get("farm_id") == FARM_ID)
            check("response has 'alerts' list and 'count'", isinstance(body.get("alerts"), list) and "count" in body)
            ids = [a["id"] for a in body["alerts"]]
            check("default (ACTIVE) response includes the ACTIVE device alert", active_id in ids)
            check("default (ACTIVE) response includes the ACTIVE activity alert", activity_active_id in ids)
            check("default (ACTIVE) response excludes the RESOLVED alert", resolved_id not in ids)

            # --- 5. Explicit RESOLVED ---
            resp2 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "lifecycle_state": "RESOLVED"},
                headers={"Authorization": "Bearer x"},
            )
            check("explicit RESOLVED -> 200", resp2.status_code == 200)
            ids2 = [a["id"] for a in resp2.json()["alerts"]]
            check("RESOLVED response includes the resolved alert", resolved_id in ids2)
            check("RESOLVED response excludes ACTIVE alerts", active_id not in ids2)

            # --- 6. Valid alert_type filter ---
            resp3 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "alert_type": "EDGE_DEVICE"},
                headers={"Authorization": "Bearer x"},
            )
            check("alert_type=EDGE_DEVICE -> 200", resp3.status_code == 200)
            ids3 = [a["id"] for a in resp3.json()["alerts"]]
            check("alert_type filter includes the EDGE_DEVICE alert", active_id in ids3)
            check("alert_type filter excludes the ACTIVITY alert", activity_active_id not in ids3)

            # --- 7. Invalid lifecycle_state -> 422 (Literal validation, never reaches SQL) ---
            resp4 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "lifecycle_state": "DROP TABLE alert_log"},
                headers={"Authorization": "Bearer x"},
            )
            check("invalid lifecycle_state -> 422, not passed through to SQL", resp4.status_code == 422,
                  detail=str(resp4.status_code))

            # --- 8. Invalid alert_type -> 422 ---
            resp5 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "alert_type": "NOT_A_REAL_TYPE"},
                headers={"Authorization": "Bearer x"},
            )
            check("invalid alert_type -> 422", resp5.status_code == 422, detail=str(resp5.status_code))

            # --- 9. limit handling ---
            resp6 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "limit": 1},
                headers={"Authorization": "Bearer x"},
            )
            check("limit=1 -> 200, returns at most 1 row", resp6.status_code == 200 and len(resp6.json()["alerts"]) <= 1)

            resp7 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "limit": 0},
                headers={"Authorization": "Bearer x"},
            )
            check("limit=0 (below ge=1) -> 422", resp7.status_code == 422, detail=str(resp7.status_code))

            resp8 = client.get(
                ENDPOINT,
                params={"farm_id": FARM_ID, "limit": 999},
                headers={"Authorization": "Bearer x"},
            )
            check("limit=999 (above le=100) -> 422", resp8.status_code == 422, detail=str(resp8.status_code))

            # --- 10. query-service delegation: response fields match D1's contract ---
            row = next(a for a in body["alerts"] if a["id"] == active_id)
            check("delegated row has lifecycle_state", row.get("lifecycle_state") == "ACTIVE")
            check("delegated row has alert_type", row.get("alert_type") == "EDGE_DEVICE")
            check("delegated row has dedup_key", row.get("dedup_key") is not None)
            check("delegated row has device_id", str(row.get("device_id")) == DEVICE_ID)
            check("delegated row has severity from alert_rule (CRITICAL)", row.get("severity") == "CRITICAL")

    finally:
        cleanup(rule_ids)


if __name__ == "__main__":
    test_full_matrix()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
