

#!/usr/bin/env python3
"""
Edge Heartbeat Agent (FINAL, TELEMETRY-AWARE)

Responsibilities:
- Periodically send device health heartbeat to backend
- Collect real Jetson telemetry automatically
- Fire-and-forget (non-blocking, fault tolerant)

Backend endpoint:
POST /api/v1/ingest/heartbeat
Auth: X-DEVICE-KEY

Stored in:
- edge_device_heartbeat (append-only)
- edge_device.last_seen_at (updated)
"""

import time
import os
import requests
from dotenv import load_dotenv

# Import Jetson-specific telemetry module
try:
    from jetson_telemetry import collect_telemetry
except ImportError:
    # Fallback if module not available (for testing on non-Jetson systems)
    def collect_telemetry():
        return {
            "cpu_temp_c": None,
            "gpu_temp_c": None,
            "disk_usage_pct": None,
            "memory_usage_pct": None,
            "notes": "telemetry module not available",
        }

# ------------------------------------------------------------------
# ENV / CONFIG
# ------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")
DOTENV_PATH = os.path.join(BACKEND_DIR, ".env")
load_dotenv(DOTENV_PATH)

API_BASE = os.getenv("EDGE_API_BASE")
DEVICE_KEY = os.getenv("EDGE_DEVICE_KEY")

if not API_BASE or not DEVICE_KEY:
    raise RuntimeError("EDGE_API_BASE or EDGE_DEVICE_KEY not set")

# API must be /api/v1
assert API_BASE.endswith("/api/v1"), (
    f"EDGE_API_BASE must end with '/api/v1', got {API_BASE}"
)

HEADERS = {"X-DEVICE-KEY": DEVICE_KEY}

# Interval (seconds)
INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "120"))

# ------------------------------------------------------------------
# TELEMETRY COLLECTION
# ------------------------------------------------------------------
# Telemetry is collected via jetson_telemetry module
# This provides production-grade, non-blocking telemetry for Jetson Orin devices

# ------------------------------------------------------------------
# HEARTBEAT SENDER
# ------------------------------------------------------------------
def send_heartbeat():
    """
    Send heartbeat to backend with telemetry data.
    
    Collects real Jetson device metrics (CPU temp, GPU temp, disk, memory)
    and sends them to the backend for health monitoring.
    """
    # Collect telemetry using Jetson-specific module
    payload = collect_telemetry()
    
    # Normalize API_BASE (remove trailing slash if present)
    base_url = API_BASE.rstrip("/")
    endpoint = f"{base_url}/ingest/heartbeat"

    try:
        resp = requests.post(
            endpoint,
            headers=HEADERS,
            json=payload,
            timeout=5,
        )

        if resp.status_code != 200:
            print(f"[HEARTBEAT][ERROR] HTTP {resp.status_code}: {resp.text[:200]}")
            return False

        return True

    except Exception as e:
        print(f"[HEARTBEAT][ERROR] {str(e)[:200]}")
        return False

# ------------------------------------------------------------------
# MAIN LOOP
# ------------------------------------------------------------------
def main():
    print("Edge Heartbeat Agent started")
    print(f"Backend   : {API_BASE}")
    print(f"Interval  : {INTERVAL}s")

    while True:
        send_heartbeat()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
