"""
#Main Application Entry Point-

#Purpose: Main FastAPI application entry point and router orchestration
#Function: Wires together all API modules based on phase flags, includes both edge ping and runtime config routers under 
"edge-bootstrap" tags
#Use: Controls which features are enabled (currently Phase 3 bootstrap APIs + Phase 4 ingestion + Phase 6 dashboard) 
and serves as the central application hub

This file must NEVER contain business logic.

# Terminal command to run the backend server-
 uvicorn main:app --host 0.0.0.0 --port 8000 --reload

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
ENABLE_EDGE_BOOTSTRAP_APIS = True   # Phase 3 (MANDATORY)
ENABLE_INGEST_APIS = True          # Phase 4
ENABLE_DASHBOARD_APIS = True       # Phase 6 (read-only)
ENABLE_ADMIN_APIS = False          # Optional

# -------------------------------------------------
# App Initialization
# -------------------------------------------------
app = FastAPI(
    title="Workforce Management Backend",
    version="1.0.0",
    docs_url="/docs",
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
# Phase 3 — Edge Bootstrap APIs (Jetson → Backend)
# -------------------------------------------------
if ENABLE_EDGE_BOOTSTRAP_APIS:
    from api.edge_runtime_config_api import router as runtime_config_router
    from api.edge_ping_api import router as edge_ping_router

    app.include_router(
        runtime_config_router,
        prefix="/api/v1",
        tags=["edge-bootstrap"],
    )

    app.include_router(
        edge_ping_router,
        prefix="/api/v1",
        tags=["edge-bootstrap"],
    )



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
# Phase 5 — Aggregation (INTENTIONALLY NOT HTTP)
# -------------------------------------------------
"""
Aggregation is triggered via:
- cron
- background worker
- CLI
NEVER exposed via HTTP.
"""

# -------------------------------------------------
# Phase 6 — Dashboard APIs (READ-ONLY)
# -------------------------------------------------
if ENABLE_DASHBOARD_APIS:
    # NOTE:
    # dashboard_query_service.py is NOT a router.
    from dashboard.dashboard_api import router as dashboard_router

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
