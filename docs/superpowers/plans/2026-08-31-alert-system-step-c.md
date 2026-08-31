# Alert System Step C Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Wire the existing Step B alert evaluators (`activity_matcher.py`, `posture_matcher.py`, `edge_device_matcher.py`) into production, revise `ACTIVITY_LATE` to its final operational spec, add `ACTIVITY_EARLY`, and implement `WORKFORCE_DETECTOR_OFFLINE` end-to-end (Jetson watchdog pulse → backend endpoint → matcher → periodic evaluation).

**Architecture:** A new 60-second systemd timer (`workforce-alerts.timer` → `aggregation/alerts_cron.py`) becomes the single production entry point for every periodic/sweep-style alert evaluation (`ACTIVITY_LATE`, `ACTIVITY_RUNNING_LONG`, `POSTURE_DATA_STALE`, `EDGE_DEVICE_OFFLINE`, `WORKFORCE_DETECTOR_OFFLINE`). Point-in-time alerts (`ACTIVITY_EARLY`, `ACTIVITY_UNSCHEDULED`) are called inline from the exact aggregator code path that creates the fact they describe. `ACTIVITY_MISSED` continues to be created by `missed_activity_cron.py` (already wired via `workforce-phase5.timer`), which now also calls the existing finalized-instance matcher on the row it creates. On the Jetson, `edge_watchdog.py` gains a 60s POST of a `detector_healthy` boolean (derived from its existing `is_system_stuck()`-style progress check, never from raw file freshness) to a new backend endpoint.

**Tech Stack:** Python 3, FastAPI, psycopg2, systemd timers (existing pattern), pytest-free standalone test scripts (repo convention — see `backend/IMPORT_GUIDE.md`).

**Spec:** `docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md`

## Global Constraints

- Do NOT implement `CAMERA_NOT_CONTRIBUTING`, `CAMERA_OFFLINE`/`STREAM_LOST`, any device-resource alert, or `ACTIVITY_PIPELINE_STALE`.
- Do NOT change `session_classification` semantics or its producers (`activity_schedule_resolver.py`).
- `ACTIVITY_LATE` uses `alert_rule.condition` threshold (`alert_late_start_min`, default 30) — never `activity_schedule.tolerance_late_min`.
- `ACTIVITY_LATE`/`ACTIVITY_EARLY` are never derived from finalized `LATE`/`EARLY` classification.
- `WORKFORCE_DETECTOR_OFFLINE` fires on **pulse absence** (`detector_last_seen_at` older than 5 minutes), never on a received `detector_healthy: false` payload value.
- No new DB tables. Exactly one new column: `edge_device.detector_last_seen_at TIMESTAMPTZ NULL`.
- `notification_dispatcher.py`'s known JOIN→LEFT JOIN issue is **not** fixed here — confirmed not a Step C dependency (nothing in this plan reads through that join); left for Step D.
- Standalone scripts run via `python some_file.py` need the manual `sys.path` setup described in `backend/IMPORT_GUIDE.md` — one level of `.parent` per directory below `backend/`.

---

## File-by-file change plan

| File | Change |
|---|---|
| `backend/STEP6_ADD_DETECTOR_HEALTH.sql` | new — adds `edge_device.detector_last_seen_at` |
| `backend/ops/seed_alert_rules_step_c.sql` | new — idempotent seed rows for `ACTIVITY_LATE`, `ACTIVITY_EARLY`, `WORKFORCE_DETECTOR_OFFLINE` |
| `backend/alerts/matchers/activity_matcher.py` | add `evaluate_early_start()`, add `evaluate_late_start_sweep()`, revise/replace old `ACTIVITY_LATE` handling inside `evaluate_finalized_instance()` |
| `backend/alerts/matchers/edge_device_matcher.py` | add `evaluate_detector_offline()` |
| `backend/api/edge_detector_health_api.py` | new — `POST /ingest/detector-heartbeat` |
| `backend/main.py` | register the new router (guarded by existing `ENABLE_INGEST_APIS` flag) |
| `backend/aggregation/activity_aggregator.py` | call `evaluate_early_start()` at both `INSERT INTO activity_instance` sites (lines ~597, ~2829) |
| `backend/aggregation/missed_activity_cron.py` | after creating a MISSED instance, call `activity_matcher.evaluate_finalized_instance()` on it |
| `backend/aggregation/alerts_cron.py` | new — the Step C periodic sweep orchestrator |
| `jetson/edge_watchdog.py` | add 60s detector-health POST loop |
| `systemd/workforce-alerts.service`, `systemd/workforce-alerts.timer` | new — 60s timer running `alerts_cron.py` |
| `backend/scripts/test_step_c_activity_late.py` | new — standalone test script |
| `backend/scripts/test_step_c_activity_early.py` | new — standalone test script |
| `backend/scripts/test_step_c_detector_offline.py` | new — standalone test script |

---

### Task 1: Database migration — `detector_last_seen_at`

**Files:**
- Create: `backend/STEP6_ADD_DETECTOR_HEALTH.sql`

**Interfaces:**
- Produces: `edge_device.detector_last_seen_at` (nullable `TIMESTAMPTZ`), read by `edge_device_matcher.evaluate_detector_offline()` (Task 5) and written by `api/edge_detector_health_api.py` (Task 4).

- [ ] **Step 1: Write the migration file**

```sql
-- STEP-6 Add detector-process health tracking, separate from board heartbeat
-- 2026-08-31
--
-- Why: the 2026-08-27/28 incident showed edge_device.last_seen_at (board
-- telemetry, sent by edge_heartbeat_agent.py) staying healthy for ~27h
-- while the detection pipeline (workforce-edge.service) was fully down.
-- This column tracks a SEPARATE signal: the last time workforce-watchdog.py
-- confirmed real detection-pipeline progress (via is_system_stuck()-style
-- logic, not raw /tmp/workforce_edge_alive freshness) and reported it here.
-- See docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md.
--
-- Idempotent: safe to re-run.

ALTER TABLE public.edge_device
  ADD COLUMN IF NOT EXISTS detector_last_seen_at timestamptz;

COMMENT ON COLUMN public.edge_device.detector_last_seen_at IS
  'Last confirmed workforce-detector pipeline progress, reported by edge_watchdog.py. NULL = no pulse ever received (new device or pre-Step-C). Distinct from last_seen_at, which only proves the board/OS is online.';
```

- [ ] **Step 2: Apply it to the dev/staging database and verify**

Run: `psql "$DATABASE_URL" -f backend/STEP6_ADD_DETECTOR_HEALTH.sql`
Then: `psql "$DATABASE_URL" -c "\d edge_device"` — confirm `detector_last_seen_at | timestamp with time zone` appears, nullable, no default.

- [ ] **Step 3: Commit**

```bash
git add backend/STEP6_ADD_DETECTOR_HEALTH.sql
git commit -m "Add edge_device.detector_last_seen_at for WORKFORCE_DETECTOR_OFFLINE"
```

---

### Task 2: Seed the three new `alert_rule` thresholds

**Files:**
- Create: `backend/ops/seed_alert_rules_step_c.sql`

**Interfaces:**
- Consumes: `alert_rule` table (existing, `STEP1_DATABASE_BASELINE.sql`), `alert_type` enum (existing, `STEP5_ADD_ALERT_LIFECYCLE_AND_TYPE.sql`).
- Produces: three `alert_rule` rows the Task 3/5 matchers will look up by `condition->>'metric'`.

- [ ] **Step 1: Write the seed script**

Farm ID is the one production farm currently onboarded (`608e7a58-d46e-4f6c-bd19-b8c2a8d59050`, "Harmony Dairy" — confirm against `SELECT id, name FROM farm;` before running on a new environment). One `WORKFORCE_DETECTOR_OFFLINE` rule is farm-scoped but device-agnostic (`activity_type_id`/`activity_schedule_id` NULL, matched by `alert_type='EDGE_DEVICE'` the same way `EDGE_DEVICE_OFFLINE` already is). `ACTIVITY_LATE`/`ACTIVITY_EARLY` need one rule per active `activity_schedule` (6 schedules currently: 2 milking, 2 feeding, 2 scrapping).

```sql
-- backend/ops/seed_alert_rules_step_c.sql
-- Seeds alert_rule rows for ACTIVITY_LATE, ACTIVITY_EARLY, WORKFORCE_DETECTOR_OFFLINE.
-- Idempotent: ON CONFLICT DO NOTHING against (farm_id, name) — no unique index on
-- that pair exists yet, so this relies on name being distinct per run; re-running
-- with the same names is safe (harmless duplicate insert avoided by the NOT EXISTS
-- guard below, not by a DB constraint).

DO $$
DECLARE
  v_farm_id uuid := '608e7a58-d46e-4f6c-bd19-b8c2a8d59050';
  v_schedule record;
BEGIN
  -- ACTIVITY_LATE and ACTIVITY_EARLY: one rule per active schedule.
  FOR v_schedule IN
    SELECT id, activity_type_id, label
    FROM activity_schedule
    WHERE farm_id = v_farm_id AND is_active = true
  LOOP
    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_LATE: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_LATE: ' || v_schedule.label,
              '{"metric": "minutes_since_ideal_start", "operator": ">", "value": 30}'::jsonb,
              'WARNING', 'ACTIVITY', true);
    END IF;

    IF NOT EXISTS (
      SELECT 1 FROM alert_rule
      WHERE farm_id = v_farm_id AND activity_schedule_id = v_schedule.id
        AND name = 'ACTIVITY_EARLY: ' || v_schedule.label
    ) THEN
      INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                               name, condition, severity, alert_type, is_active)
      VALUES (gen_random_uuid(), v_farm_id, v_schedule.activity_type_id, v_schedule.id,
              'ACTIVITY_EARLY: ' || v_schedule.label,
              '{"metric": "minutes_before_ideal_start", "operator": ">", "value": 30}'::jsonb,
              'WARNING', 'ACTIVITY', true);
    END IF;
  END LOOP;

  -- WORKFORCE_DETECTOR_OFFLINE: one device-agnostic, farm-scoped rule.
  IF NOT EXISTS (
    SELECT 1 FROM alert_rule
    WHERE farm_id = v_farm_id AND name = 'WORKFORCE_DETECTOR_OFFLINE'
  ) THEN
    INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                             name, condition, severity, alert_type, is_active)
    VALUES (gen_random_uuid(), v_farm_id, NULL, NULL,
            'WORKFORCE_DETECTOR_OFFLINE',
            '{"metric": "detector_heartbeat_age_minutes", "operator": ">", "value": 5}'::jsonb,
            'CRITICAL', 'EDGE_DEVICE', true);
  END IF;
END $$;
```

- [ ] **Step 2: Run it and verify**

Run: `psql "$DATABASE_URL" -f backend/ops/seed_alert_rules_step_c.sql`
Then: `psql "$DATABASE_URL" -c "SELECT name, condition, alert_type FROM alert_rule WHERE name LIKE 'ACTIVITY_LATE%' OR name LIKE 'ACTIVITY_EARLY%' OR name = 'WORKFORCE_DETECTOR_OFFLINE';"` — expect 13 rows (6 LATE + 6 EARLY + 1 detector).

- [ ] **Step 3: Commit**

```bash
git add backend/ops/seed_alert_rules_step_c.sql
git commit -m "Seed alert_rule rows for ACTIVITY_LATE/EARLY and WORKFORCE_DETECTOR_OFFLINE"
```

---

### Task 3: `activity_matcher.py` — `evaluate_early_start()` and `evaluate_late_start_sweep()`

**Files:**
- Modify: `backend/alerts/matchers/activity_matcher.py`
- Test: `backend/scripts/test_step_c_activity_late.py`, `backend/scripts/test_step_c_activity_early.py`

**Interfaces:**
- Consumes: `alert_conditions.upsert_active_alert()`, `alert_conditions.resolve_active_occurrence()`, `common.db.get_cursor()`, `common.time_utils.utc_now()` (all existing).
- Produces:
  - `evaluate_early_start(activity_instance_id: str) -> dict` — `{"instance_found": bool, "alerts_created": list[str]}`
  - `evaluate_late_start_sweep(farm_id: str | None = None) -> dict` — `{"schedules_evaluated": int, "alerts_created": list[str], "alerts_resolved": int}`

- [ ] **Step 1: Write the failing test for `evaluate_early_start`**

```python
# backend/scripts/test_step_c_activity_early.py
"""
Standalone integration test for activity_matcher.evaluate_early_start().
Run: python scripts/test_step_c_activity_early.py
Creates/deletes only its own test fixtures (alert_rule + a synthetic
activity_instance); no production data is mutated.
"""
from pathlib import Path
import sys
import uuid
from datetime import date, datetime, timedelta, timezone

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json
from common.db import get_cursor
from alerts.matchers import activity_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
# Morning Scrapping: ideal_start_time 05:00 IST
SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_rule(cur, threshold_min=30):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 3, %s, 'STEPC_TEST Early Scrapping',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, SCRAP_MORNING_SCHEDULE,
         Json({"metric": "minutes_before_ideal_start", "operator": ">", "value": threshold_min})),
    )
    return rule_id


def make_instance(cur, actual_start_at, activity_date):
    iid = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO activity_instance
            (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
             actual_start_at, status, source, zone_id)
        VALUES (%s, %s, 3, %s, %s, %s, 'IN_PROGRESS', 'SYSTEM', %s)
        """,
        (iid, FARM_ID, SCRAP_MORNING_SCHEDULE, activity_date, actual_start_at, ZONE_ID),
    )
    return iid


def cleanup(rule_ids, instance_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))
        if instance_ids:
            cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


def test_early_start_triggers():
    rule_ids, instance_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur)
        rule_ids.append(rule_id)

        # Schedule ideal_start_time is 05:00 IST -> 40 min early = 04:20 IST.
        today = date.today()
        early_start_utc = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc) \
            .replace(hour=22, minute=50) - timedelta(days=1)  # ~04:20 IST previous UTC date
        with get_cursor() as cur:
            iid = make_instance(cur, early_start_utc, today)
        instance_ids.append(iid)

        result = activity_matcher.evaluate_early_start(iid)
        check("ACTIVITY_EARLY: instance found", result["instance_found"])
        check("ACTIVITY_EARLY: alert created", rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_EARLY: exactly one row", len(rows) == 1, f"got {len(rows)}")
        if rows:
            check("ACTIVITY_EARLY: immediately RESOLVED", rows[0]["lifecycle_state"] == "RESOLVED")
            check("ACTIVITY_EARLY: resolved_at set", rows[0]["resolved_at"] is not None)
            check("ACTIVITY_EARLY: dedup_key is instance id", rows[0]["dedup_key"] == iid)
    finally:
        cleanup(rule_ids, instance_ids)


def test_within_threshold_no_alert():
    rule_ids, instance_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur)
        rule_ids.append(rule_id)

        # Only 10 minutes early -> below the 30-minute threshold, must not fire.
        today = date.today()
        near_ideal_start_utc = datetime.combine(today, datetime.min.time(), tzinfo=timezone.utc) \
            .replace(hour=23, minute=20) - timedelta(days=1)  # ~04:50 IST
        with get_cursor() as cur:
            iid = make_instance(cur, near_ideal_start_utc, today)
        instance_ids.append(iid)

        result = activity_matcher.evaluate_early_start(iid)
        check(
            "ACTIVITY_EARLY: within-threshold start does not fire",
            rule_id not in result.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids, instance_ids)


if __name__ == "__main__":
    test_early_start_triggers()
    test_within_threshold_no_alert()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd backend && python scripts/test_step_c_activity_early.py`
Expected: `AttributeError: module 'alerts.matchers.activity_matcher' has no attribute 'evaluate_early_start'`

- [ ] **Step 3: Implement `evaluate_early_start()` and the shared timezone helper**

Add to `backend/alerts/matchers/activity_matcher.py` (near the top, alongside the existing metric constants):

```python
_EARLY_START_METRIC = "minutes_before_ideal_start"
_LATE_START_METRIC = "minutes_since_ideal_start"


def _ideal_start_local(schedule_row, activity_date, farm_tz):
    """schedule_row needs ideal_start_time; farm_tz is a pytz timezone."""
    naive = datetime.combine(activity_date, schedule_row["ideal_start_time"])
    return farm_tz.localize(naive)
```

Add the function itself (after `evaluate_finalized_instance`):

```python
def evaluate_early_start(activity_instance_id):
    """
    STEP C — ACTIVITY_EARLY. Point-in-time, instance-keyed: fires (already
    RESOLVED) the moment an instance's actual_start_at is more than
    alert_early_start_min before its schedule's ideal_start_time. Never
    derived from session_classification -- that stays historical/reporting.
    """
    with get_cursor() as cur:
        cur.execute(
            """
            SELECT ai.id, ai.farm_id, ai.activity_type_id, ai.activity_schedule_id,
                   ai.actual_start_at, ai.zone_id,
                   s.ideal_start_time, f.timezone
            FROM activity_instance ai
            JOIN activity_schedule s ON s.id = ai.activity_schedule_id
            JOIN farm f ON f.id = ai.farm_id
            WHERE ai.id = %s
            """,
            (activity_instance_id,),
        )
        row = cur.fetchone()
        if not row or row["actual_start_at"] is None or row["activity_schedule_id"] is None:
            return {"instance_found": bool(row), "alerts_created": []}
        rules = _load_activity_rules(cur, row["farm_id"], row["activity_type_id"], row["activity_schedule_id"])

    farm_tz = pytz.timezone(row["timezone"])
    start_local = row["actual_start_at"].astimezone(farm_tz)
    ideal_start_local = _ideal_start_local(row, start_local.date(), farm_tz)
    minutes_before = (ideal_start_local - start_local).total_seconds() / 60.0

    created = []
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _EARLY_START_METRIC:
            continue
        if not condition_matches(condition, minutes_before):
            continue

        inserted = upsert_active_alert(
            farm_id=row["farm_id"],
            rule=rule,
            dedup_key=str(row["id"]),
            message=f"{rule['name']}: activity started {minutes_before:.0f} minutes early.",
            details={
                "activity_type_id": row["activity_type_id"],
                "activity_schedule_id": str(row["activity_schedule_id"]),
                "minutes_before_ideal_start": round(minutes_before, 1),
            },
            activity_instance_id=row["id"],
            zone_id=row["zone_id"],
            immediately_resolve=True,
        )
        if inserted:
            created.append(rule["id"])

    return {"instance_found": True, "alerts_created": created}
```

- [ ] **Step 4: Run the early-start test to verify it passes**

Run: `cd backend && python scripts/test_step_c_activity_early.py`
Expected: `2/2 checks passed` (adjust the fixed UTC-offset arithmetic in the test if farm timezone data differs from the assumed Asia/Kolkata offset — verify with `SELECT timezone FROM farm WHERE id = '608e7a58-d46e-4f6c-bd19-b8c2a8d59050';` first).

- [ ] **Step 5: Write the failing test for `evaluate_late_start_sweep`**

```python
# backend/scripts/test_step_c_activity_late.py
"""
Standalone integration test for activity_matcher.evaluate_late_start_sweep().
Run: python scripts/test_step_c_activity_late.py
"""
from pathlib import Path
import sys
import uuid

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import activity_matcher

FARM_ID = "608e7a58-d46e-4f6c-bd19-b8c2a8d59050"
ZONE_ID = "69e66202-9c88-4da1-bcfe-ffd5d25daf08"
# Morning Scrapping: ideal_start_time 05:00 IST -- guaranteed > 30 min past
# ideal start for any run time after ~05:31 IST; this test only asserts the
# ACTIVE/RESOLVED transition, not real-time-of-day dependent absolute counts,
# so it is safe to run at any hour.
SCRAP_MORNING_SCHEDULE = "f37b59e2-6da2-4d30-bc44-fbb73f1d18b3"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_rule(cur):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, activity_type_id, activity_schedule_id,
                                 name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 3, %s, 'STEPC_TEST Late Scrapping Sweep',
                %s, 'WARNING', 'ACTIVITY', true)
        """,
        (rule_id, FARM_ID, SCRAP_MORNING_SCHEDULE,
         Json({"metric": "minutes_since_ideal_start", "operator": ">", "value": 0})),
        # value=0 makes this deterministic: any evaluation after the ideal
        # start time (00:00 local is always past for a same-day schedule
        # once the sweep runs) fires, proving the trigger path without
        # depending on wall-clock timing.
    )
    return rule_id


def cleanup(rule_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))


def test_late_sweep_triggers_when_no_instance_exists():
    rule_ids = []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur)
        rule_ids.append(rule_id)

        # No activity_instance exists for this schedule+today in this test's
        # isolated rule -- the sweep must fire.
        result = activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        check("ACTIVITY_LATE sweep: schedules evaluated > 0", result["schedules_evaluated"] > 0)
        check("ACTIVITY_LATE sweep: alert created", rule_id in result.get("alerts_created", []))

        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_LATE: one ACTIVE row", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        # Re-run: dedup must prevent a second row.
        activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check("ACTIVITY_LATE: re-run does not duplicate", len(rows_after) == 1)
    finally:
        cleanup(rule_ids)


def test_late_resolves_when_instance_appears():
    rule_ids, instance_ids = [], []
    try:
        with get_cursor() as cur:
            rule_id = make_rule(cur)
        rule_ids.append(rule_id)

        activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows = cur.fetchall()
        check("ACTIVITY_LATE: fired before instance exists", len(rows) == 1 and rows[0]["lifecycle_state"] == "ACTIVE")

        iid = str(uuid.uuid4())
        with get_cursor() as cur:
            cur.execute(
                """
                INSERT INTO activity_instance
                    (id, farm_id, activity_type_id, activity_schedule_id, activity_date,
                     actual_start_at, status, source, zone_id)
                VALUES (%s, %s, 3, %s, CURRENT_DATE, %s, 'IN_PROGRESS', 'SYSTEM', %s)
                """,
                (iid, FARM_ID, SCRAP_MORNING_SCHEDULE, utc_now(), ZONE_ID),
            )
        instance_ids.append(iid)

        activity_matcher.evaluate_late_start_sweep(farm_id=FARM_ID)
        with get_cursor() as cur:
            cur.execute("SELECT * FROM alert_log WHERE alert_rule_id = %s", (rule_id,))
            rows_after = cur.fetchall()
        check(
            "ACTIVITY_LATE: resolves once instance exists",
            rows_after[0]["lifecycle_state"] == "RESOLVED" and rows_after[0]["resolved_at"] is not None,
        )
    finally:
        cleanup(rule_ids)
        with get_cursor() as cur:
            if instance_ids:
                cur.execute("DELETE FROM activity_instance WHERE id = ANY(%s::uuid[])", (instance_ids,))


if __name__ == "__main__":
    test_late_sweep_triggers_when_no_instance_exists()
    test_late_resolves_when_instance_appears()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
```

- [ ] **Step 6: Run it to verify it fails**

Run: `cd backend && python scripts/test_step_c_activity_late.py`
Expected: `AttributeError: ... has no attribute 'evaluate_late_start_sweep'`

- [ ] **Step 7: Implement `evaluate_late_start_sweep()`**

```python
def evaluate_late_start_sweep(farm_id=None):
    """
    STEP C — ACTIVITY_LATE. Schedule-keyed periodic sweep (called from
    aggregation/alerts_cron.py every 60s), not instance-keyed -- by
    definition no activity_instance exists yet when this should first fire.
    Deliberately ignores activity_schedule.tolerance_late_min (that's
    ACTIVITY_MISSED's mechanism); uses alert_rule.condition's own
    minutes_since_ideal_start threshold instead.
    """
    with get_cursor() as cur:
        query = """
            SELECT s.id AS schedule_id, s.farm_id, s.activity_type_id,
                   s.ideal_start_time, f.timezone
            FROM activity_schedule s
            JOIN farm f ON f.id = s.farm_id
            WHERE s.is_active = true
        """
        params = ()
        if farm_id:
            query += " AND s.farm_id = %s"
            params = (farm_id,)
        cur.execute(query, params)
        schedules = cur.fetchall()

    created = []
    resolved = 0
    for sched in schedules:
        farm_tz = pytz.timezone(sched["timezone"])
        now_local = datetime.now(timezone.utc).astimezone(farm_tz)
        today_local = now_local.date()
        ideal_start_local = _ideal_start_local(sched, today_local, farm_tz)
        minutes_since = (now_local - ideal_start_local).total_seconds() / 60.0
        if minutes_since < 0:
            continue  # ideal start hasn't happened yet today

        with get_cursor() as cur:
            cur.execute(
                """
                SELECT 1 FROM activity_instance
                WHERE farm_id = %s AND activity_schedule_id = %s AND activity_date = %s
                LIMIT 1
                """,
                (sched["farm_id"], sched["schedule_id"], today_local),
            )
            instance_exists = cur.fetchone() is not None
            rules = _load_activity_rules(cur, sched["farm_id"], sched["activity_type_id"], sched["schedule_id"])

        dedup_key = f"{sched['schedule_id']}:{today_local.isoformat()}"
        for rule in rules:
            condition = rule["condition"] or {}
            if condition.get("metric") != _LATE_START_METRIC:
                continue

            if not instance_exists and condition_matches(condition, minutes_since):
                inserted = upsert_active_alert(
                    farm_id=sched["farm_id"],
                    rule=rule,
                    dedup_key=dedup_key,
                    message=f"{rule['name']}: no activity started {minutes_since:.0f} minutes after ideal start.",
                    details={
                        "activity_type_id": sched["activity_type_id"],
                        "activity_schedule_id": str(sched["schedule_id"]),
                        "activity_date": today_local.isoformat(),
                        "minutes_since_ideal_start": round(minutes_since, 1),
                    },
                )
                if inserted:
                    created.append(rule["id"])
            elif instance_exists:
                resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {"schedules_evaluated": len(schedules), "alerts_created": created, "alerts_resolved": resolved}
```

- [ ] **Step 8: Run both tests to verify they pass**

Run: `cd backend && python scripts/test_step_c_activity_late.py && python scripts/test_step_c_activity_early.py`
Expected: both print `N/N checks passed` and exit 0.

- [ ] **Step 9: Commit**

```bash
git add backend/alerts/matchers/activity_matcher.py backend/scripts/test_step_c_activity_late.py backend/scripts/test_step_c_activity_early.py
git commit -m "Add ACTIVITY_LATE sweep and ACTIVITY_EARLY evaluators (Step C)"
```

---

### Task 4: `ACTIVITY_MISSED` escalation — resolve `ACTIVITY_LATE` and fire the alert on the new MISSED row

**Files:**
- Modify: `backend/aggregation/missed_activity_cron.py`

**Interfaces:**
- Consumes: `alerts.matchers.activity_matcher.evaluate_finalized_instance()` (existing, unchanged), `alerts.alert_conditions.resolve_active_occurrence()` (existing).

- [ ] **Step 1: Locate the MISSED-row creation point**

Find where `missed_activity_cron.py` inserts the `status='ENDED', session_classification='MISSED'` row (the STEP-5B logic referenced in its module docstring). It already knows `farm_id`, `activity_schedule_id`, `activity_date` at that point.

- [ ] **Step 2: Add the two calls immediately after the INSERT, inside the same function, guarded so a matcher failure never blocks missed-activity detection**

```python
    # STEP C: resolve any still-ACTIVE ACTIVITY_LATE for this schedule/date
    # (MISSED supersedes it -- see spec section 2), then let the finalized-
    # instance matcher fire ACTIVITY_MISSED itself on the row we just made.
    # Never let an alert-layer failure block missed-activity detection.
    try:
        from alerts.alert_conditions import resolve_active_occurrence
        from alerts.matchers import activity_matcher as _activity_matcher

        late_dedup_key = f"{schedule_id}:{activity_date.isoformat()}"
        with get_cursor() as cur:
            cur.execute(
                "SELECT id FROM alert_rule WHERE farm_id = %s AND activity_schedule_id = %s "
                "AND alert_type = 'ACTIVITY' AND condition ->> 'metric' = 'minutes_since_ideal_start'",
                (farm_id, schedule_id),
            )
            late_rules = cur.fetchall()
        for rule in late_rules:
            resolve_active_occurrence(rule_id=rule["id"], dedup_key=late_dedup_key)

        _activity_matcher.evaluate_finalized_instance(missed_instance_id)
    except Exception as e:
        print(f"[MISSED][ALERT_WARN] evaluator failed for instance={missed_instance_id}: {e}")
```

(`missed_instance_id`, `farm_id`, `schedule_id`, `activity_date` are the existing local variables from the surrounding function — match exact names when integrating; if the function returns/computes the new instance's id under a different variable name, use that name instead of `missed_instance_id`.)

- [ ] **Step 3: Manual verification against a real MISSED row**

Run: `cd backend && python -c "
from aggregation.missed_activity_cron import detect_missed_activities
detect_missed_activities()
"`
Then: `psql "$DATABASE_URL" -c "SELECT al.message, al.lifecycle_state FROM alert_log al JOIN alert_rule ar ON ar.id = al.alert_rule_id WHERE ar.name LIKE 'STEPB_TEST%' OR ar.name LIKE 'ACTIVITY_LATE%' OR ar.name LIKE 'ACTIVITY_MISSED%' ORDER BY al.triggered_at DESC LIMIT 5;"` — confirm no unhandled exception in stdout and (if a MISSED row was actually created this run) a matching `alert_log` row appears.

- [ ] **Step 4: Commit**

```bash
git add backend/aggregation/missed_activity_cron.py
git commit -m "Wire ACTIVITY_MISSED alert + ACTIVITY_LATE resolution into missed_activity_cron.py"
```

---

### Task 5: `edge_device_matcher.py` — `evaluate_detector_offline()`

**Files:**
- Modify: `backend/alerts/matchers/edge_device_matcher.py`
- Test: `backend/scripts/test_step_c_detector_offline.py`

**Interfaces:**
- Consumes: `edge_device.detector_last_seen_at` (Task 1), `alert_conditions.upsert_active_alert()`/`resolve_active_occurrence()`.
- Produces: `evaluate_detector_offline(device_id: str) -> dict` — `{"device_found": bool, "age_minutes": float | None, "alerts_created": list[str]}`.

- [ ] **Step 1: Write the failing test**

```python
# backend/scripts/test_step_c_detector_offline.py
"""
Standalone integration test for edge_device_matcher.evaluate_detector_offline().
Run: python scripts/test_step_c_detector_offline.py
"""
from pathlib import Path
import sys
import uuid
from datetime import timedelta

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from psycopg2.extras import Json
from common.db import get_cursor
from common.time_utils import utc_now
from alerts.matchers import edge_device_matcher

DEVICE_ID = "f0d5c399-6939-4b26-bf5a-fe24c2ed5738"

results = []


def check(name, condition, detail=""):
    status = "PASS" if condition else "FAIL"
    results.append((name, status))
    print(f"[{status}] {name}" + (f" -- {detail}" if detail and status == "FAIL" else ""))


def make_rule(cur, farm_id, threshold_min=5):
    rule_id = str(uuid.uuid4())
    cur.execute(
        """
        INSERT INTO alert_rule (id, farm_id, name, condition, severity, alert_type, is_active)
        VALUES (%s, %s, 'STEPC_TEST Detector Offline', %s, 'CRITICAL', 'EDGE_DEVICE', true)
        """,
        (rule_id, farm_id,
         Json({"metric": "detector_heartbeat_age_minutes", "operator": ">", "value": threshold_min})),
    )
    return rule_id


def cleanup(rule_ids):
    with get_cursor() as cur:
        if rule_ids:
            cur.execute("DELETE FROM alert_log WHERE alert_rule_id = ANY(%s::uuid[])", (rule_ids,))
            cur.execute("DELETE FROM alert_rule WHERE id = ANY(%s::uuid[])", (rule_ids,))


def test_stale_pulse_triggers():
    rule_ids = []
    try:
        with get_cursor() as cur:
            cur.execute("SELECT farm_id FROM edge_device WHERE id = %s", (DEVICE_ID,))
            farm_id = cur.fetchone()["farm_id"]
            rule_id = make_rule(cur, farm_id)
            cur.execute(
                "UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s",
                (utc_now() - timedelta(minutes=10), DEVICE_ID),
            )
        rule_ids.append(rule_id)

        result = edge_device_matcher.evaluate_detector_offline(DEVICE_ID)
        check("DETECTOR_OFFLINE: device found", result["device_found"])
        check("DETECTOR_OFFLINE: alert created", rule_id in result.get("alerts_created", []))
    finally:
        cleanup(rule_ids)


def test_null_pulse_does_not_trigger():
    rule_ids = []
    try:
        with get_cursor() as cur:
            cur.execute("SELECT farm_id FROM edge_device WHERE id = %s", (DEVICE_ID,))
            farm_id = cur.fetchone()["farm_id"]
            rule_id = make_rule(cur, farm_id)
            cur.execute(
                "UPDATE edge_device SET detector_last_seen_at = NULL WHERE id = %s",
                (DEVICE_ID,),
            )
        rule_ids.append(rule_id)

        result = edge_device_matcher.evaluate_detector_offline(DEVICE_ID)
        check(
            "DETECTOR_OFFLINE: NULL (no pulse ever) does not immediately alert",
            rule_id not in result.get("alerts_created", []),
        )
    finally:
        cleanup(rule_ids)
        with get_cursor() as cur:
            cur.execute(
                "UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s",
                (None, DEVICE_ID),
            )


if __name__ == "__main__":
    test_stale_pulse_triggers()
    test_null_pulse_does_not_trigger()
    total = len(results)
    passed = sum(1 for _, s in results if s == "PASS")
    print(f"\n{passed}/{total} checks passed")
    if passed != total:
        sys.exit(1)
```

- [ ] **Step 2: Run it to verify it fails**

Run: `cd backend && python scripts/test_step_c_detector_offline.py`
Expected: `AttributeError: ... has no attribute 'evaluate_detector_offline'`

- [ ] **Step 3: Implement it — note the explicit NULL/first-pulse grace behavior**

```python
_DETECTOR_AGE_METRIC = "detector_heartbeat_age_minutes"


def evaluate_detector_offline(device_id):
    """
    STEP C — WORKFORCE_DETECTOR_OFFLINE. Driven purely by freshness/absence
    of edge_device.detector_last_seen_at (set by the new detector-heartbeat
    ingest endpoint, see api/edge_detector_health_api.py) -- never by a
    received payload's detector_healthy value. A NULL detector_last_seen_at
    (new device, or pre-Step-C rollout) is "not yet observed", not "stale":
    do not alert on it, matching evaluate_device_offline's identical
    handling of last_seen_at IS NULL.
    """
    with get_cursor() as cur:
        cur.execute(
            "SELECT id, farm_id, detector_last_seen_at FROM edge_device WHERE id = %s",
            (device_id,),
        )
        device = cur.fetchone()
        if not device:
            return {"device_found": False}
        rules = _load_edge_device_rules(cur, device["farm_id"])

    if device["detector_last_seen_at"] is None:
        return {"device_found": True, "age_minutes": None, "alerts_created": []}

    age_minutes = (utc_now() - device["detector_last_seen_at"]).total_seconds() / 60.0

    created = []
    resolved = 0
    dedup_key = str(device_id)
    for rule in rules:
        condition = rule["condition"] or {}
        if condition.get("metric") != _DETECTOR_AGE_METRIC:
            continue

        if condition_matches(condition, age_minutes):
            inserted = upsert_active_alert(
                farm_id=device["farm_id"],
                rule=rule,
                dedup_key=dedup_key,
                message=f"{rule['name']}: no detector-health pulse in {age_minutes:.1f} minutes.",
                details={"device_id": str(device_id), "detector_heartbeat_age_minutes": round(age_minutes, 2)},
                device_id=device_id,
            )
            if inserted:
                created.append(rule["id"])
        else:
            resolved += resolve_active_occurrence(rule_id=rule["id"], dedup_key=dedup_key)

    return {
        "device_found": True,
        "age_minutes": age_minutes,
        "alerts_created": created,
        "alerts_resolved": resolved,
    }
```

- [ ] **Step 4: Run the test to verify it passes**

Run: `cd backend && python scripts/test_step_c_detector_offline.py`
Expected: `2/2 checks passed`

- [ ] **Step 5: Commit**

```bash
git add backend/alerts/matchers/edge_device_matcher.py backend/scripts/test_step_c_detector_offline.py
git commit -m "Add WORKFORCE_DETECTOR_OFFLINE matcher (Step C)"
```

---

### Task 6: Backend ingest endpoint — `POST /ingest/detector-heartbeat`

**Files:**
- Create: `backend/api/edge_detector_health_api.py`
- Modify: `backend/main.py`

**Interfaces:**
- Consumes: `common.device_auth.resolve_device_from_headers()` (existing, same pattern as `heartbeat_ingest_api.py`).
- Produces: updates `edge_device.detector_last_seen_at` — read by Task 5's `evaluate_detector_offline()`.

- [ ] **Step 1: Write the endpoint, mirroring `heartbeat_ingest_api.py`'s structure exactly, deliberately as its own file/router**

```python
"""
backend/api/edge_detector_health_api.py
Detector-Process Health Ingest API (Step C)

Accepts a periodic pulse from jetson/edge_watchdog.py proving the workforce
detection pipeline (not just the board) is making progress. Deliberately a
separate endpoint/table-column from /ingest/heartbeat (edge_heartbeat_agent.py,
board telemetry) -- see docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md
for why these must stay distinct signals.

NO alerting here -- this endpoint only records the pulse. WORKFORCE_DETECTOR_OFFLINE
is evaluated separately by aggregation/alerts_cron.py based on staleness of
what gets written here, never on this request's payload content.
"""

from fastapi import APIRouter, Header, HTTPException
from pydantic import BaseModel

from common.db import get_cursor
from common.time_utils import utc_now
from common.device_auth import resolve_device_from_headers, DeviceAuthError

router = APIRouter(prefix="/ingest", tags=["detector-health"])


class DetectorHeartbeatIn(BaseModel):
    detector_healthy: bool
    total_frames: int | None = None
    camera_count: int | None = None


@router.post("/detector-heartbeat")
def ingest_detector_heartbeat(
    payload: DetectorHeartbeatIn,
    x_device_key: str = Header(..., alias="X-DEVICE-KEY"),
):
    """
    Ingest a detector-health pulse. Always accepted and always updates
    detector_last_seen_at regardless of payload.detector_healthy's value --
    the ALERT decision (WORKFORCE_DETECTOR_OFFLINE) is driven by whether
    pulses keep arriving at all, not by what any single pulse says. A
    detector_healthy=false pulse still proves the watchdog itself is alive
    and reporting; only silence (no pulse for 5 min) is the alert trigger.
    """
    try:
        device_ctx = resolve_device_from_headers({"X-DEVICE-KEY": x_device_key})
    except DeviceAuthError as e:
        raise HTTPException(status_code=401, detail=str(e))

    device_id = device_ctx["device_id"]
    now = utc_now()

    with get_cursor() as cur:
        cur.execute(
            "UPDATE edge_device SET detector_last_seen_at = %s WHERE id = %s",
            (now, str(device_id)),
        )

    return {"status": "recorded", "detector_healthy": payload.detector_healthy}
```

- [ ] **Step 2: Register the router in `main.py`, guarded by the existing ingest-APIs flag**

Find the block in `backend/main.py` that does `if ENABLE_INGEST_APIS: app.include_router(heartbeat_ingest_api.router)` (or equivalent) and add the sibling import/include next to it:

```python
from api import edge_detector_health_api
# ... inside the same ENABLE_INGEST_APIS-guarded block as heartbeat_ingest_api:
app.include_router(edge_detector_health_api.router, prefix="/api/v1")
```

(Match the exact prefix convention already used for `heartbeat_ingest_api.router` in that file — copy its `include_router` call's prefix/tags arguments exactly, don't guess a different shape.)

- [ ] **Step 3: Manual smoke test against a running dev server**

Run: `cd backend && uvicorn main:app --port 8000 &`
Then:
```bash
curl -s -X POST http://127.0.0.1:8000/api/v1/ingest/detector-heartbeat \
  -H "X-DEVICE-KEY: <real test device key from edge_device.api_key_hash fixture>" \
  -H "Content-Type: application/json" \
  -d '{"detector_healthy": true, "total_frames": 123, "camera_count": 7}'
```
Expected: `{"status":"recorded","detector_healthy":true}`, HTTP 200.
Then: `psql "$DATABASE_URL" -c "SELECT detector_last_seen_at FROM edge_device WHERE id = '<device id>';"` — confirm it's within the last few seconds.

- [ ] **Step 4: Commit**

```bash
git add backend/api/edge_detector_health_api.py backend/main.py
git commit -m "Add POST /ingest/detector-heartbeat endpoint (Step C)"
```

---

### Task 7: `edge_watchdog.py` — send the detector-health pulse

**Files:**
- Modify: `jetson/edge_watchdog.py`

**Interfaces:**
- Consumes: the existing local `WATCHDOG_FILE_PATH` payload (already read by `read_heartbeat_payload()`).
- Produces: an HTTP POST to the new endpoint from Task 6, on its own 60s cadence, independent of the existing 15s `CHECK_INTERVAL_SEC` local stuck-detection loop.

- [ ] **Step 1: Add config for the backend endpoint (mirrors `edge_heartbeat_agent.py`'s env-var pattern) and a `requests` dependency**

Add near the top of `jetson/edge_watchdog.py`, alongside the existing `WATCHDOG_FILE_PATH`/`WATCHDOG_TIMEOUT_SEC` constants:

```python
import requests

API_BASE = os.getenv("EDGE_API_BASE")
DEVICE_KEY = os.getenv("EDGE_DEVICE_KEY")
DETECTOR_PULSE_INTERVAL_SEC = int(os.getenv("EDGE_DETECTOR_PULSE_INTERVAL_SEC", "60"))
```

- [ ] **Step 2: Add the health-derivation function — grounded in `total_frames` progress, deliberately NOT raw file freshness**

```python
def compute_detector_healthy(payload):
    """
    Derive a detector_healthy boolean for the OUTWARD pulse, independent of
    is_system_stuck()'s own internal restart-decision counters (different
    cadence: this runs every DETECTOR_PULSE_INTERVAL_SEC=60s, is_system_stuck
    runs every CHECK_INTERVAL_SEC=15s -- keeping them separate avoids
    coupling the network-reporting cadence to the local restart algorithm's
    state machine).

    Deliberately does NOT just check payload age/freshness: the GPU
    inference-worker thread can refresh /tmp/workforce_edge_alive's
    timestamp on its own idle-poll loop even when every camera capture
    thread is stuck (see docs/superpowers/specs/2026-08-31-alert-system-step-c-design.md).
    Uses the same total_frames signal is_system_stuck() relies on instead.
    """
    if not payload:
        return False, 0, 0

    process_started_at = float(payload.get("process_started_at") or 0)
    if process_started_at > 0 and (time.time() - process_started_at) < STARTUP_GRACE_SEC:
        # Still starting up -- not yet meaningful to judge, report healthy
        # to avoid a false-negative pulse during normal boot.
        return True, payload.get("total_frames", 0), len(payload.get("camera_last_seen", {}))

    if not hasattr(compute_detector_healthy, "prev_total_frames"):
        compute_detector_healthy.prev_total_frames = payload.get("total_frames", 0)
        return True, payload.get("total_frames", 0), len(payload.get("camera_last_seen", {}))

    total_frames = payload.get("total_frames", 0)
    healthy = total_frames != compute_detector_healthy.prev_total_frames
    compute_detector_healthy.prev_total_frames = total_frames
    return healthy, total_frames, len(payload.get("camera_last_seen", {}))


def send_detector_pulse(detector_healthy, total_frames, camera_count):
    if not API_BASE or not DEVICE_KEY:
        return
    try:
        requests.post(
            f"{API_BASE.rstrip('/')}/ingest/detector-heartbeat",
            headers={"X-DEVICE-KEY": DEVICE_KEY},
            json={
                "detector_healthy": detector_healthy,
                "total_frames": total_frames,
                "camera_count": camera_count,
            },
            timeout=5,
        )
    except Exception as e:
        print(f"[WATCHDOG][PULSE] send failed: {str(e)[:150]}")
```

- [ ] **Step 3: Wire it into `main()` on its own timer, without disturbing the existing 15s check loop**

```python
def main():
    print("Edge Watchdog started")
    print(f"Watching  : {WATCHDOG_FILE_PATH}")
    print(f"Timeout   : {WATCHDOG_TIMEOUT_SEC}s")
    print(f"Service   : {DETECTOR_SERVICE_NAME}")
    print(f"Pulse     : every {DETECTOR_PULSE_INTERVAL_SEC}s to {API_BASE}")

    last_pulse_at = 0.0

    while True:
        payload = read_heartbeat_payload()
        age_seconds = read_heartbeat_age_seconds()

        if time.time() - last_pulse_at >= DETECTOR_PULSE_INTERVAL_SEC:
            healthy, total_frames, camera_count = compute_detector_healthy(payload)
            send_detector_pulse(healthy, total_frames, camera_count)
            last_pulse_at = time.time()

        if payload and is_system_stuck(payload):
            print(f"[WATCHDOG] System stuck -> restarting {DETECTOR_SERVICE_NAME}")
            restart_detector()
            time.sleep(WATCHDOG_TIMEOUT_SEC)
            continue

        if age_seconds is not None and age_seconds > WATCHDOG_TIMEOUT_SEC:
            print(f"[WATCHDOG] Heartbeat stale ({age_seconds:.1f}s). Restarting {DETECTOR_SERVICE_NAME}")
            restart_detector()
            time.sleep(WATCHDOG_TIMEOUT_SEC)
        else:
            time.sleep(CHECK_INTERVAL_SEC)
```

- [ ] **Step 4: Syntax-check**

Run: `cd jetson && python3 -m py_compile edge_watchdog.py`
Expected: no output, exit 0.

- [ ] **Step 5: Manual verification against a running `workforce-edge` + this updated watchdog**

Deploy per the same process used for the earlier reliability fixes (copy file, `sudo systemctl restart workforce-watchdog`), then watch `journalctl -u workforce-watchdog -f` for the new `Pulse : every 60s to ...` startup line, and confirm via `psql` that `edge_device.detector_last_seen_at` advances roughly every 60s while `workforce-edge` is healthy.

- [ ] **Step 6: Commit**

```bash
git add jetson/edge_watchdog.py
git commit -m "Send detector-health pulse from watchdog, grounded in total_frames not raw file freshness"
```

---

### Task 8: `aggregation/alerts_cron.py` — the Step C periodic sweep orchestrator

**Files:**
- Create: `backend/aggregation/alerts_cron.py`

**Interfaces:**
- Consumes: `activity_matcher.evaluate_late_start_sweep()` (Task 3), `activity_matcher.evaluate_in_progress_instance()` (existing, unwired until now), `posture_matcher.evaluate_zone_staleness()` (existing, unwired until now), `edge_device_matcher.evaluate_device_offline()` (existing, unwired until now), `edge_device_matcher.evaluate_detector_offline()` (Task 5).

- [ ] **Step 1: Write the orchestrator, following `run_phase5.py`'s existing structure/print-logging convention**

```python
#!/usr/bin/env python3
"""
backend/aggregation/alerts_cron.py
STEP C — periodic alert-evaluation orchestrator.

Runs every 60s via systemd/workforce-alerts.timer. This is the single
production entry point for every SWEEP-style alert (one that must be
re-checked on a timer rather than triggered by a specific event):
ACTIVITY_LATE, ACTIVITY_RUNNING_LONG, POSTURE_DATA_STALE, EDGE_DEVICE_OFFLINE,
WORKFORCE_DETECTOR_OFFLINE.

Point-in-time alerts (ACTIVITY_EARLY, ACTIVITY_UNSCHEDULED, ACTIVITY_MISSED)
are NOT run from here -- they're called inline from the aggregator/cron
code path that creates the fact they describe (see activity_aggregator.py
and missed_activity_cron.py).

Each evaluator call is independently guarded: one farm/device/zone/schedule
failing must never block the others in the same sweep.
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
from alerts.matchers import activity_matcher, posture_matcher, edge_device_matcher


def run():
    print("[ALERTS_CRON] ACTIVITY_LATE sweep...")
    try:
        result = activity_matcher.evaluate_late_start_sweep()
        print(f"[ALERTS_CRON]   {result}")
    except Exception as e:
        print(f"[ALERTS_CRON][ERROR] ACTIVITY_LATE sweep failed: {e}")

    print("[ALERTS_CRON] ACTIVITY_RUNNING_LONG sweep...")
    with get_cursor() as cur:
        cur.execute("SELECT id FROM activity_instance WHERE status = 'IN_PROGRESS'")
        in_progress_ids = [row["id"] for row in cur.fetchall()]
    for iid in in_progress_ids:
        try:
            activity_matcher.evaluate_in_progress_instance(iid)
        except Exception as e:
            print(f"[ALERTS_CRON][ERROR] RUNNING_LONG failed for instance={iid}: {e}")

    print("[ALERTS_CRON] POSTURE_DATA_STALE sweep...")
    with get_cursor() as cur:
        cur.execute("SELECT id, farm_id FROM farm_zone")
        zones = cur.fetchall()
    for zone in zones:
        try:
            posture_matcher.evaluate_zone_staleness(zone["farm_id"], zone["id"])
        except Exception as e:
            print(f"[ALERTS_CRON][ERROR] POSTURE_DATA_STALE failed for zone={zone['id']}: {e}")

    print("[ALERTS_CRON] EDGE_DEVICE_OFFLINE / WORKFORCE_DETECTOR_OFFLINE sweep...")
    with get_cursor() as cur:
        cur.execute("SELECT id FROM edge_device WHERE is_active = true")
        devices = cur.fetchall()
    for device in devices:
        try:
            edge_device_matcher.evaluate_device_offline(device["id"])
        except Exception as e:
            print(f"[ALERTS_CRON][ERROR] EDGE_DEVICE_OFFLINE failed for device={device['id']}: {e}")
        try:
            edge_device_matcher.evaluate_detector_offline(device["id"])
        except Exception as e:
            print(f"[ALERTS_CRON][ERROR] WORKFORCE_DETECTOR_OFFLINE failed for device={device['id']}: {e}")


if __name__ == "__main__":
    run()
```

- [ ] **Step 2: Run it manually against dev/staging and confirm no unhandled exception**

Run: `cd backend && python aggregation/alerts_cron.py`
Expected: prints each `[ALERTS_CRON] ... sweep...` line and any per-item `[ERROR]` lines (should be none on a healthy dev DB with fixture data), exits 0.

- [ ] **Step 3: Commit**

```bash
git add backend/aggregation/alerts_cron.py
git commit -m "Add alerts_cron.py: Step C periodic evaluator orchestrator"
```

---

### Task 9: Wire `ACTIVITY_EARLY` into the two aggregator instance-creation sites

**Files:**
- Modify: `backend/aggregation/activity_aggregator.py`

**Interfaces:**
- Consumes: `alerts.matchers.activity_matcher.evaluate_early_start()` (Task 3).

- [ ] **Step 1: Add the import at the top of `activity_aggregator.py`**

```python
from alerts.matchers import activity_matcher as _alert_activity_matcher
```

- [ ] **Step 2: At the first creation site (`resolve_fallback_instance_or_skip`, ~line 597), call it right after a successful insert**

Find:
```python
    row = cur.fetchone()
    if row:
        iid = row["id"]
        print(
            f"[FALLBACK_INSTANCE] created instance_id={iid} event_row_id={event_row_id} "
            f"reason={fatal_label}"
        )
        return iid
```
Change to:
```python
    row = cur.fetchone()
    if row:
        iid = row["id"]
        print(
            f"[FALLBACK_INSTANCE] created instance_id={iid} event_row_id={event_row_id} "
            f"reason={fatal_label}"
        )
        try:
            _alert_activity_matcher.evaluate_early_start(iid)
        except Exception as e:
            print(f"[ALERT_WARN] evaluate_early_start failed for instance={iid}: {e}")
        return iid
```

- [ ] **Step 3: At the second creation site (~line 2829), apply the identical pattern**

Find the matching `row = cur.fetchone()` / `if row:` block that follows the second `INSERT INTO activity_instance` (the one inside the deeply-nested session-processing loop), and add the same guarded call right after `instance_id` (or whatever the local variable is named at that site) is assigned from `row["id"]`.

- [ ] **Step 4: Manual verification**

Run the aggregator once against dev/staging data that includes a known early-arriving event: `cd backend && python aggregation/activity_aggregator.py --max-loops 1`, then check for `[ALERT_WARN]` lines (should be none) and spot-check `alert_log` for any new `ACTIVITY_EARLY`-named rows if the test data actually qualifies.

- [ ] **Step 5: Commit**

```bash
git add backend/aggregation/activity_aggregator.py
git commit -m "Wire ACTIVITY_EARLY evaluation into both activity_instance creation sites"
```

---

### Task 10: New systemd timer — `workforce-alerts`

**Files:**
- Create: `systemd/workforce-alerts.service`, `systemd/workforce-alerts.timer`

**Interfaces:** none (process wiring only).

- [ ] **Step 1: Write the unit files, matching `workforce-phase5.service`/`.timer`'s exact structure**

```ini
# systemd/workforce-alerts.service
#
# Workforce Alerts Service (Step C)
#
# What it does:
# - Runs aggregation/alerts_cron.py once per invocation.
# - Evaluates ACTIVITY_LATE, ACTIVITY_RUNNING_LONG, POSTURE_DATA_STALE,
#   EDGE_DEVICE_OFFLINE, WORKFORCE_DETECTOR_OFFLINE.
#
# How to use:
# 1) sudo cp systemd/workforce-alerts.service /etc/systemd/system/workforce-alerts.service
# 2) sudo cp systemd/workforce-alerts.timer /etc/systemd/system/workforce-alerts.timer
# 3) sudo systemctl daemon-reload
# 4) sudo systemctl enable --now workforce-alerts.timer
#    journalctl -u workforce-alerts.service -f (--log)
# -----------------------------------------------------------------------------

[Unit]
Description=Workforce Alerts Evaluator (Step C)
After=network.target

[Service]
Type=oneshot
WorkingDirectory=/home/neopeak/Desktop/workforce/Edge2/backend
ExecStart=/home/neopeak/edge2/bin/python aggregation/alerts_cron.py
Environment=PYTHONUNBUFFERED=1
User=neopeak
```

```ini
# systemd/workforce-alerts.timer
#
# Workforce Alerts Timer (Step C)
# - Triggers workforce-alerts.service every 60 seconds.
# -----------------------------------------------------------------------------

[Unit]
Description=Run Workforce Alerts every 1 minute

[Timer]
OnBootSec=1min
OnUnitActiveSec=60s
Unit=workforce-alerts.service

[Install]
WantedBy=timers.target
```

- [ ] **Step 2: Commit** (installation on the live Jetson/backend host is a deployment step, not part of this commit — matches how `workforce-edge.service`'s `StartLimitBurst` change was handled: repo file first, `sudo cp`+`daemon-reload` as a separate, deliberate deploy action)

```bash
git add systemd/workforce-alerts.service systemd/workforce-alerts.timer
git commit -m "Add workforce-alerts systemd timer (Step C, not yet installed)"
```

---

## Deployment/rollback considerations

- **Order matters:** Task 1 (DB column) and Task 2 (seed rules) must land before Task 5/6/7 are deployed — the matcher and endpoint both assume the column exists, and the sweep needs rules to evaluate against.
- **`workforce-alerts.timer` is installed last**, after every task above is deployed and manually smoke-tested — until it's installed, nothing in this plan actually alerts anyone (Step B's existing behavior: all of this is dormant-but-safe until wired).
- **Rollback is per-task and cheap:** every change here is additive (new column, new endpoint, new files, small guarded call-sites wrapped in `try/except`) — nothing modifies existing Step B evaluator behavior for `MISSED`/`UNSCHEDULED`/`RUNNING_LONG`/`POSTURE_DATA_STALE`/`EDGE_DEVICE_OFFLINE`. Reverting any single task's commit is safe in isolation; reverting Task 1 requires also reverting Task 5/6/7 (dependent on the column).
- **`edge_watchdog.py` (Task 7) deploys the same way as the earlier reliability fixes:** copy the file, `sudo systemctl restart workforce-watchdog` — does not touch or require restarting `workforce-edge.service`.

## Risks and edge cases

- **`ACTIVITY_EARLY` coverage gap:** only wired at the two known `INSERT INTO activity_instance` sites (Task 9). `reopen_missed_activity_instance()` (a third path that reactivates an existing MISSED row rather than creating a new one) is NOT covered — if a reopened MISSED slot's `actual_start_at` also qualifies as "early," no alert fires. Flagged, not fixed, per the frozen spec's "explicitly analyze the safest integration point" instruction — YAGNI until this is shown to matter in practice.
- **`alert_rule` seed script (Task 2) is farm-specific** (hardcodes the one production farm ID) — must be re-run (or generalized) before onboarding a second farm.
- **Timezone/DST:** all local-time math in `evaluate_late_start_sweep`/`evaluate_early_start` goes through `pytz.timezone(farm["timezone"])`, matching the existing pattern in `evaluate_in_progress_instance`. Farm timezone is `Asia/Kolkata` (no DST) for the current farm — DST correctness for a future farm in a DST-observing timezone is untested by this plan's test scripts.
- **`alerts_cron.py` and `missed_activity_cron.py`/`activity_aggregator.py` run on independent timers** (60s vs. 60s vs. on-demand) — there's no cross-process locking. A `ACTIVITY_LATE` sweep and a `missed_activity_cron.py` run resolving the same dedup key could theoretically race; `resolve_active_occurrence`'s `UPDATE ... WHERE lifecycle_state = 'ACTIVE'` is naturally idempotent under this race (worst case: a harmless redundant no-op UPDATE), so no additional locking is added here.
- **`notification_dispatcher.py`'s JOIN issue is out of scope** — confirmed nothing in this plan reads through it; left for Step D as originally decided.

## Explicit list of things intentionally NOT changing

- `CAMERA_NOT_CONTRIBUTING`, `CAMERA_OFFLINE`/`STREAM_LOST`, any device-resource alert, `ACTIVITY_PIPELINE_STALE` — not implemented.
- `session_classification` semantics and `activity_schedule_resolver.py` — untouched.
- `activity_schedule.tolerance_late_min` — still exclusively `ACTIVITY_MISSED`'s mechanism.
- `notification_dispatcher.py` — untouched, its known JOIN issue remains for Step D.
- `ACTIVITY_UNSCHEDULED`, `ACTIVITY_RUNNING_LONG`'s own trigger logic, `POSTURE_DATA_STALE`, `EDGE_DEVICE_OFFLINE` — matcher logic unchanged; this plan only adds the periodic call sites they were missing.

## Recommended implementation order

Task 1 → Task 2 → Task 3 → Task 4 → Task 5 → Task 6 → Task 7 → Task 8 → Task 9 → Task 10, then a final end-to-end manual verification pass (trigger each alert deliberately in dev/staging, confirm `alert_log` rows and correct RESOLVED transitions) before installing `workforce-alerts.timer` in production.
