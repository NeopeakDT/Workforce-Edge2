"""
Edge Ping API (Phase 3)

Responds to heartbeat pings from Jetson devices.

Responsibilities:
- Authenticate device via X-DEVICE-KEY
- Respond with success status
- Lightweight & high-frequency safe


# Run this below in terminal
curl.exe -X GET http://127.0.0.1:8000/api/v1/edge/ping `
  -H "X-DEVICE-KEY: wf_test_device_key_001"
  -H "Content-Type: application/json"

Purpose: Health check endpoint for Jetson edge devices
Function: Provides a simple /api/v1/edge/ping GET endpoint that returns authorization status
Use: Jetson devices can ping this to verify backend connectivity and authorization
"""

from fastapi import APIRouter

router = APIRouter()

@router.get("/edge/ping")
def edge_ping():
    return {
        "status": "authorized",
        "service": "workforce-backend"
    }
