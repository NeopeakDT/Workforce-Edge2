# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What this is

Workforce Edge2 — a cattle-farm activity monitoring system with two independently-run Python trees:

- **`backend/`** — FastAPI service + Postgres (Supabase) that receives events from edge devices, aggregates them into activity sessions, evaluates compliance, and serves a read-only dashboard.
- **`jetson/`** — Standalone edge application that runs on NVIDIA Jetson devices at each farm: pulls RTSP camera streams, runs YOLO inference (activity + posture detection), and pushes events/heartbeats to `backend/` over HTTP.

There is no shared Python package between the two trees — they only interact over the HTTP API defined in `backend/api/`. Treat them as separate deployables when reasoning about imports, environment variables, and running code.

## Commands

### Backend

```bash
cd backend
pip install -r requirements.txt

# Run the API server (dev, auto-reload)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Run a standalone script/tool (see Import Rules below)
python scripts/test_db_connect.py
python scripts/test_auth_jwt.py
python scripts/test_device_auth_api.py
python scripts/test_step6_alerts.py
python scripts/test_step6_dashboard.py
python scripts/test_step6_dispatcher.py

# Aggregation worker (STEP-4, long-lived; use --max-loops 1 for a single pass while debugging)
python aggregation/activity_aggregator.py
python aggregation/activity_aggregator.py --max-loops 1

# Phase-5 batch (schedule/missed-activity/compliance), wrapper at backend root
python run_phase5.py
```

There is **no pytest/unittest suite** — "tests" are standalone scripts under `backend/scripts/` and `jetson/test_*.py` that you run directly and read output from. `backend/.env` holds `DATABASE_URL` and friends (copy from `.env.example`); it's read via `python-dotenv` and is gitignored.

### Jetson edge device

```bash
cd jetson
pip install -r jetson_requirements.txt   # Jetson target
# pip install -r requirements_cpu_pc.txt # CPU dev machine variant

# Mandatory boot sequence (config sync MUST succeed before inference starts)
python edge_config_sync.py
python edge_heartbeat_agent.py &
python edge_detector.py
```

`edge_detector.py` requires `EDGE_API_BASE` (must end in `/api/v1`) and `EDGE_DEVICE_KEY`, loaded from `backend/.env` (it reaches up to the sibling `backend/` dir for the dotenv file — see `PROJECT_ROOT`/`BACKEND_DIR` in `edge_detector.py`). `edge_config_sync.py` needs `BACKEND_API_URL`, settable via env var, `config/bootstrap_config.json` (gitignored), or CLI arg — see `jetson/SETUP_API_URL.md`.

Config comes from `jetson/config/local_cache.json` (gitignored, fetched at boot by `edge_config_sync.py`), validated by `config/local_cache.py::load_config()`, which hard-exits if required keys (`device_id`, `farm_id`, `farm_timezone`, `cameras`, `ml_model_version`) are missing.

### Import rules (backend) — read `backend/IMPORT_GUIDE.md` before adding a script

- `uvicorn main:app` already puts `backend/` on `sys.path`, so `main.py`, `api/*`, `dashboard/*`, `alerts/*` use plain `from common.db import ...` with **no path setup**.
- Anything run directly via `python some_file.py` needs manual `sys.path` setup at the top, because Python only adds the script's own folder:
  - One level below `backend/` (root scripts like `run_phase5.py`, `check_devices2.py`): `Path(__file__).resolve().parent`
  - Two levels below `backend/` (`scripts/`, `ops/`, `aggregation/`): `Path(__file__).resolve().parent.parent`
- Inside `common/`, use relative imports between siblings (`from .db import ...`); never add path setup there.
- `jetson/` is a fully separate tree with its own import root — the backend guide does not apply to it.

## Architecture

### Backend: phased, event-sourced pipeline

`backend/main.py` wires routers behind explicit phase flags (`ENABLE_EDGE_BOOTSTRAP_APIS`, `ENABLE_INGEST_APIS`, `ENABLE_DASHBOARD_APIS`, `ENABLE_ADMIN_APIS`) — it intentionally contains **no business logic**, only app assembly. The pipeline is phase-numbered end to end, and phase numbers show up throughout code comments, filenames (`STEP*_DATABASE_MIGRATION.sql`), and systemd unit names:

1. **Phase 3 — Edge bootstrap** (`api/edge_runtime_config_api.py`, `api/edge_ping_api.py`): device fetches its config, pings health.
2. **Phase 4 — Ingestion** (`api/event_ingest_api.py`, `api/heartbeat_ingest_api.py`): devices push `activity_detection_event` rows and heartbeats. Append-only — **no instance/session creation happens here**. Per-camera rate limiting and DB-level idempotency are enforced in this layer.
3. **Phase 5 — Aggregation** (`aggregation/`): deliberately **not exposed over HTTP** — runs via cron/systemd/CLI only (see comment block in `main.py`). `activity_aggregator.py` (STEP-4) consumes unlinked events and creates/updates `activity_instance` rows using schedule-aware, gap-tolerant merge logic (different merge/reopen windows per `activity_type_id` — milking vs. feeding vs. scrapping). `run_phase5.py` chains STEP-5A/5B (schedule resolution, missed-activity detection, compliance building) on top. Read the module docstring in `aggregation/activity_aggregator.py` before touching merge/attach logic — it documents live-attach vs. historical-replay semantics, stale-instance cleanup, and several `AGG_*` env overrides that change behavior non-obviously.
4. **Phase 6 — Dashboard** (`dashboard/dashboard_query_service.py`): read-only queries; currently disabled by default in `main.py` (no `dashboard_api.py` router exists yet — the flag is a placeholder).
5. **Alerts** (`alerts/`): evaluates compliance results and dispatches notifications; not phase-numbered but sits downstream of aggregation.

Cross-cutting pieces live in `common/`: `db.py` (lazy-initialized `psycopg2` connection pool over `DATABASE_URL`, `get_cursor()` context manager returning dict rows), `auth.py` (Supabase ES256 JWT verification via JWKS, for human/dashboard auth), `device_auth.py` (separate **machine** auth — `X-DEVICE-KEY` header hashed with SHA-256 and matched against `edge_device.api_key_hash`; this is distinct from `auth.py`'s user JWT flow), `time_utils.py`, `constants.py` (frozen timing parameters — don't casually retune), `audit_logger.py`, `idempotency.py`.

Database schema evolves via sequential `backend/STEP{N}_*.sql` migration files, applied by hand/ops process — there's no migration tool/ORM. `STEP1_DATABASE_BASELINE.sql` is a full non-incremental snapshot of the live schema (reconstructed via `pg_dump --schema-only` in Aug 2026, since the original incremental history predating it was never committed); later STEP files are additive `ALTER TABLE` migrations on top of it. Read the relevant `STEP*.sql` before assuming a column/table exists.

### Jetson: capture → detect → emit, no lifecycle decisions on-device

`jetson/edge_detector.py` is the main loop. Its own docstring states the boundary explicitly: the edge device **detects and emits** `START_CANDIDATE` / `FRAME_AGGREGATE` / `END_CANDIDATE` events — it **never decides activity lifecycle** (that's the backend aggregator's job). Structure:

- `runtime/model_loader.py` — loads YOLO models (`.pt`/`.onnx`/`.engine`) per pipeline key (`WORKFORCE`, `MILKING`, `POSTURE`), each with its own static TensorRT batch size.
- `runtime/video_stream.py`, `runtime/motion_detector.py`, `runtime/temporal_smoother.py`, `runtime/roi_utils.py` — stream handling, motion-based signals, detection smoothing, and ROI polygon filtering (ROI import is optional/best-effort — falls back to "no filtering" if `shapely` or the module isn't available, guarded in `edge_detector.py`).
- `utils/camera_routing.py::is_milking_camera()` — routes a camera to the milking pipeline purely from its config `code` field (local decision, not DB-driven).
- `posture/` — a separate detection concern layered on top of the main activity pipeline: `posture_detector.py` (per-frame inference, zone assignment — no scheduling, no DB access), `posture_scheduler.py` (buffers per-camera samples into minute snapshots, flushes aggregated pen observations to DB on its own interval; milking windows are schedule-only, no inference), `posture_db.py`, `posture_models.py`, `posture_utils.py`. Each module's docstring states what it explicitly does *not* do — respect those boundaries when extending.
- `roi_selection/` — interactive tooling (`roi_selector.py`) and per-camera ROI coordinate files (`.txt`/`.json`) checked into the repo; these are per-farm calibration data, not code.
- `edge_config_sync.py` / `edge_heartbeat_agent.py` / `edge_watchdog.py` — boot-time config fetch, periodic heartbeat, and a watchdog for restart-on-failure. `jetson/EXECUTION_TIMELINE.md` documents the required boot ordering and failure semantics (config sync must succeed and must fail loudly/fast — no silent fallback to stale config, no infinite retry).

`jetson/systemd/` and top-level `systemd/` hold the unit files for both trees (edge detector, watchdog, heartbeat, plus backend/aggregator/phase5 services) — check these when changing process lifecycle, env vars, or restart behavior, since expected `WorkingDirectory`/`Environment` values live there.

### Models

YOLO weight files (`WF_V1.2_best.pt`, `WF_V1.3_best.pt`, etc.) are checked in at `backend/` and `models/` at repo root — `backend/ops/model_registry_sync.py` handles registry sync. `models/` and `runs/` (training artifacts) are otherwise gitignored; don't assume every model path referenced in scripts exists on disk in this checkout (some paths in `jetson/test_*.py` are hardcoded to a specific dev machine, e.g. `/home/neopeak/...`).
