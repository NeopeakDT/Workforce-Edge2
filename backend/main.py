"""
Main Application Entry Point
FastAPI application with all routes and background tasks.
"""

# CRITICAL: Load environment variables BEFORE any imports that use them
from dotenv import load_dotenv
load_dotenv()

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
import asyncio
from contextlib import asynccontextmanager

from .api import event_ingest_api, heartbeat_ingest_api, admin_endpoints
from .aggregation import missed_activity_cron, device_health_monitor
from .ops import event_retention_cleanup


@asynccontextmanager
async def lifespan(app: FastAPI):
    """Manage application lifespan and background tasks"""
    # Startup
    # Start background tasks
    missed_activity_task = asyncio.create_task(
        missed_activity_cron.run_missed_activity_check()
    )
    
    # TODO: Start other background tasks
    # cleanup_task = asyncio.create_task(...)
    
    yield
    
    # Shutdown
    missed_activity_task.cancel()
    # TODO: Cancel other background tasks


app = FastAPI(
    title="Workforce Management System API",
    description="Multi-farm AI activity monitoring backend",
    version="1.0.0",
    lifespan=lifespan
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],  # Configure appropriately for production
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(event_ingest_api.router)
app.include_router(heartbeat_ingest_api.router)
app.include_router(admin_endpoints.router)


@app.get("/")
async def root():
    """Root endpoint"""
    return {
        "message": "Workforce Management System API",
        "version": "1.0.0"
    }


@app.get("/health")
async def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}


if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)
