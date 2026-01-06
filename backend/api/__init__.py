# API module for Workforce Management System

# backend/api/__init__.py

from fastapi import APIRouter

from api.event_ingest_api import router as event_router
from api.heartbeat_ingest_api import router as heartbeat_router

api_router = APIRouter(prefix="/api/v1")

# =========================
# Phase 4 – Runtime Ingestion (ENABLED)
# =========================
api_router.include_router(event_router)
api_router.include_router(heartbeat_router)

# =========================
# Phase 6 – Admin APIs (DISABLED)
# =========================
# from api.admin_endpoints import router as admin_router
# api_router.include_router(admin_router)
