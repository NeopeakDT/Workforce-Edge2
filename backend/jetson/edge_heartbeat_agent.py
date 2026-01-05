"""
Edge Heartbeat Agent

Sends periodic heartbeat messages to backend for device health monitoring.
Runs continuously in the background to indicate device liveness.

Key Features:
    - Periodic heartbeat emission (default: 120 seconds)
    - Fire-and-forget design (non-blocking)
    - Automatic retry on next interval
    - Device authentication via EDGE_TOKEN

Architecture:
    - Edge device: Sends heartbeat signals only
    - Backend: Updates edge_device.last_seen_at timestamp
    - Separation: Edge reports, backend monitors

Usage:
    export EDGE_API_BASE=https://api.example.com
    export EDGE_TOKEN=your_device_token
    python edge_heartbeat_agent.py

Note: This should run as a background service (systemd) on Jetson device.
"""

import time
import os
import requests

# =========================
# ENV / CONFIG
# =========================
API_BASE = os.getenv("EDGE_API_BASE")
EDGE_TOKEN = os.getenv("EDGE_TOKEN")

if not API_BASE or not EDGE_TOKEN:
    raise RuntimeError("EDGE_API_BASE or EDGE_TOKEN not set")

HEADERS = {"Authorization": f"Bearer {EDGE_TOKEN}"}

# Heartbeat interval in seconds (default: 120 seconds = 2 minutes)
INTERVAL = int(os.getenv("HEARTBEAT_INTERVAL", "120"))


def send_heartbeat():
    """
    Send heartbeat signal to backend.
    
    Returns:
        bool: True if successful, False otherwise
    """
    try:
        response = requests.post(
            f"{API_BASE}/edge/heartbeat",
            headers=HEADERS,
            timeout=5,
        )
        # Log success (optional - can be removed for production)
        if response.status_code == 200:
            return True
        return False
    except Exception:
        # Fire-and-forget by design - failures are expected during network issues
        return False


def main():
    """
    Main heartbeat loop.
    
    Continuously sends heartbeat signals at configured interval.
    Runs forever until interrupted.
    """
    print(f"Heartbeat agent started. Interval: {INTERVAL}s")
    print(f"Backend: {API_BASE}")
    
    while True:
        send_heartbeat()
        time.sleep(INTERVAL)


if __name__ == "__main__":
    main()
