"""
Main Application Entry Point
Workforce Management Backend

Phase-aware wiring:
- Phase 4: Ingestion APIs (ENABLED)
- Phase 5: Aggregation jobs (DISABLED by default)
- Phase 6: Alerts & Dashboard (DISABLED by default)

This file must NEVER contain business logic.

"""

# -------------------------------------------------
# Environment bootstrap (MUST be first)
# -------------------------------------------------
from dotenv import load_dotenv
load_dotenv()

import os
from fastapi import FastAPI

# -------------------------------------------------
# Phase Flags (explicit & safe)
# -------------------------------------------------
ENABLE_INGEST_APIS = True          # Phase 4
ENABLE_AGGREGATION_JOBS = False    # Phase 5
ENABLE_ALERT_APIS = False          # Phase 6
ENABLE_DASHBOARD_APIS = False      # Phase 6
ENABLE_ADMIN_APIS = False          # Optional

# -------------------------------------------------
# App Initialization
# -------------------------------------------------
app = FastAPI(
    title="Workforce Management Backend",
    version="1.0.0",
    docs_url="/docs",        # can be disabled later
    redoc_url="/redoc",
)

# -------------------------------------------------
# Health Check (ALWAYS ENABLED)
# -------------------------------------------------
@app.get("/health", tags=["system"])
def health():
    return {
        "status": "healthy",
        "service": "workforce-backend",
    }

# -------------------------------------------------
# Phase 4 — Ingestion APIs (Jetson → Backend)
# -------------------------------------------------
if ENABLE_INGEST_APIS:
    from api.event_ingest_api import router as event_ingest_router
    from api.heartbeat_ingest_api import router as heartbeat_ingest_router

    app.include_router(
        event_ingest_router,
        prefix="/api/v1",
        tags=["ingestion"],
    )

    app.include_router(
        heartbeat_ingest_router,
        prefix="/api/v1",
        tags=["ingestion"],
    )

# -------------------------------------------------
# Phase 5 — Aggregation (Background / Cron Jobs)
# -------------------------------------------------
if ENABLE_AGGREGATION_JOBS:
    """
    IMPORTANT:
    Aggregation is NOT an API.
    It should be triggered via:
    - cron
    - background worker
    - CLI
    DO NOT expose as HTTP unless explicitly needed.
    """

    # Example (NOT enabled yet):
    # from aggregation.activity_aggregator import run_aggregator
    # from aggregation.missed_activity_cron import run_missed_cron
    pass

# -------------------------------------------------
# Phase 6 — Alerts APIs
# -------------------------------------------------
if ENABLE_ALERT_APIS:
    from alerts.alert_evaluator import router as alert_router

    app.include_router(
        alert_router,
        prefix="/api/v1/alerts",
        tags=["alerts"],
    )

# -------------------------------------------------
# Phase 6 — Dashboard APIs (Read-only)
# -------------------------------------------------
if ENABLE_DASHBOARD_APIS:
    from dashboard.dashboard_query_service import router as dashboard_router

    app.include_router(
        dashboard_router,
        prefix="/api/v1/dashboard",
        tags=["dashboard"],
    )

# -------------------------------------------------
# Optional — Admin / Ops APIs
# -------------------------------------------------
if ENABLE_ADMIN_APIS:
    from api.admin_endpoints import router as admin_router

    app.include_router(
        admin_router,
        prefix="/api/v1/admin",
        tags=["admin"],
    )

# -------------------------------------------------
# Entry Point (Development Only)
# -------------------------------------------------
if __name__ == "__main__":
    import uvicorn

    uvicorn.run(
        "main:app",
        host="0.0.0.0",
        port=int(os.getenv("PORT", 8000)),
        reload=True,
    )
