# Backend import guide

This document explains **how Python imports work in this backend** and **when you must add path setup** to a file.

It is for developers writing or running code under `backend/` — not for the Jetson edge app (`jetson/`), which is a separate tree with its own imports.

---

## What problem does this solve?

The backend is laid out like an application package:

```text
backend/
├── main.py              ← FastAPI entry (run with uvicorn here)
├── common/              ← DB, auth, time helpers
├── api/                 ← HTTP routers (ingest, edge bootstrap, dashboard)
├── aggregation/         ← Activity aggregator, phase-5 cron jobs
├── alerts/
├── dashboard/
├── scripts/             ← One-off test / dev scripts
└── ops/                 ← Admin / provisioning tools
```

When you run **`uvicorn main:app`** from `backend/`, Python’s working directory and import path already include `backend/`, so imports like `from common.db import get_cursor` work.

When you run a file **directly** with `python some_script.py`, Python only adds the script’s folder to the path — **not** `backend/`. Then `from common.db import ...` fails with `ModuleNotFoundError`.

**Path setup** adds the `backend/` directory to `sys.path` once at the top of standalone scripts so shared modules import correctly.

---

## Quick rules

| You are writing… | Run how? | Path setup? | Typical imports |
|------------------|----------|---------------|-----------------|
| `scripts/*.py` | `python scripts/...` | **Yes** | `from common.db import ...` |
| `ops/*.py` | `python ops/...` | **Yes** | `from common.db import ...` |
| `aggregation/*.py` (cron / batch) | `python aggregation/...` | **Yes** | `from common...`, `from aggregation...` |
| `backend/run_phase5.py` | `python run_phase5.py` | **Yes** (one level up) | delegates to `aggregation.run_phase5` |
| Loose `*.py` in `backend/` root | `python check_devices2.py` | **Yes** (`.parent` only) | `from common...` |
| `api/*.py`, `dashboard/*.py`, `alerts/*.py` | Imported by `main.py` | **No** | `from common.db import ...` |
| `common/*.py` | Imported by others | **No** | `from .db import ...` (relative) |
| `main.py` | `uvicorn main:app` | **No** | `from api.... import ...` |

**Rule of thumb:** path setup only in files you execute with `python path/to/file.py`. Never in modules that only exist to be imported.

---

## Standard path setup (copy-paste)

### Scripts in a subfolder (`scripts/`, `ops/`, `aggregation/`)

File is **two levels** below `backend/` → use `.parent.parent`:

```python
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()  # optional but recommended for DB credentials

from common.db import get_cursor
```

**Examples in this repo:** `scripts/test_db_connect.py`, `ops/device_provisioning.py`, `aggregation/missed_activity_cron.py`, `aggregation/activity_aggregator.py`.

### Scripts at `backend/` root

File is **one level** below `backend/` → use `.parent` only:

```python
import sys
from pathlib import Path

BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor
```

**Examples:** `run_phase5.py`, `check_devices2.py`.

---

## Running things the right way

Always activate your venv and run from the backend directory when possible:

```bash
cd /path/to/Workforce-Detection/backend
source ../torch_env/bin/activate   # or your venv
pip install -r requirements.txt

# API server (no path setup in main.py)
uvicorn main:app --host 0.0.0.0 --port 8000 --reload

# Standalone script (path setup inside the script)
python scripts/test_db_connect.py

# Phase-5 batch (wrapper at backend root)
python run_phase5.py
# same as: python aggregation/run_phase5.py
```

`.env` lives in `backend/`. Scripts that use the database should call `load_dotenv()` after path setup (see `scripts/test_db_connect.py`).

---

## Import styles by file type

### FastAPI modules (`api/`, `dashboard/`, `alerts/`)

Loaded when `main.py` starts. **Do not** add path setup.

This project uses **absolute imports from `backend/` as root**:

```python
# api/event_ingest_api.py
from common.db import get_cursor
from common.device_auth import resolve_device_from_headers
```

`main.py` imports routers the same way:

```python
from api.event_ingest_api import router as event_ingest_router
```

Relative imports like `from ..common.db` also work in theory, but **prefer `from common...`** here to match existing code.

### `common/` package

Other code imports `common` as a top-level package. Inside `common/`, use **relative imports** between siblings:

```python
# common/audit_logger.py
from .db import get_cursor
from .time_utils import utc_now
```

Do **not** add path setup inside `common/*.py`.

### Aggregation batch jobs

Files such as `activity_aggregator.py` and `missed_activity_cron.py` include path setup because they are often run as:

```bash
python aggregation/activity_aggregator.py
```

They may import both `common` and `aggregation`:

```python
from common.db import get_cursor
from aggregation.activity_schedule_resolver import resolve
```

When the aggregator is imported from `run_phase5.py`, path setup running twice is harmless (the guard `if str(BACKEND_ROOT) not in sys.path` prevents duplicates).

---

## `common/path_setup.py`

There is a small helper at `common/path_setup.py`, but **standalone scripts should use the inline block above**, not import `path_setup` first (that would require path setup before you can import `path_setup`).

Treat `path_setup.py` as documentation backup only; new scripts should copy the pattern from this guide or from `scripts/test_db_connect.py`.

---

## Backend layout (reference)

| Folder | Role |
|--------|------|
| `api/` | Edge bootstrap, event/heartbeat ingest |
| `aggregation/` | STEP-4 aggregator, STEP-5 schedule/missed, STEP-6 compliance |
| `alerts/` | Alert evaluation and notifications |
| `dashboard/` | Read-only dashboard APIs |
| `common/` | DB pool, device auth, JWT, time utils, audit |
| `scripts/` | Dev tests (DB, auth, alerts, dashboard) |
| `ops/` | Device provisioning, model registry sync |

SQL migrations and phase docs (`STEP*.sql`) are separate from Python imports.

---

## Troubleshooting

**`ModuleNotFoundError: No module named 'common'`**

- You ran a script without path setup, or used `.parent.parent` when the file is at `backend/` root (should be `.parent` only).
- Fix: add the correct block from [Standard path setup](#standard-path-setup-copy-paste), or `cd backend` and run via a module that already bootstraps path.

**`ModuleNotFoundError: No module named 'aggregation'`**

- Same as above — `backend/` must be on `sys.path`.

**Imports work in IDE but fail on device/cron**

- Cron/systemd may run from another cwd. Path setup in the script fixes that; relying on `cd backend` alone does not if the script lives in `scripts/`.

**Database connection errors after import fix**

- Imports are fine; check `backend/.env` and `load_dotenv()`.

---

## Checklist for a new standalone script

1. Create it under `scripts/` or `ops/` (or `aggregation/` if it’s a batch job).
2. Paste path setup with `.parent.parent` (or `.parent` if at `backend/` root).
3. Call `load_dotenv()` if using `common.db`.
4. Use `from common....` and `from aggregation....` as needed.
5. Do **not** copy path setup into `api/` or `common/` modules.
6. Run from anywhere: `python /full/path/to/backend/scripts/my_script.py` — path setup makes that work.

---

## Summary

- **Purpose of this file:** avoid `ModuleNotFoundError` and keep one consistent import style across the backend.
- **Add path setup:** only in executable scripts (`scripts/`, `ops/`, `aggregation/`, root wrappers).
- **Skip path setup:** `main.py`, `api/`, `common/`, `dashboard/`, `alerts/`.
- **Prefer:** inline `BACKEND_ROOT` block + `from common...` (matches current codebase).
- **Jetson / edge:** see `jetson/` code and docs; not covered here.
