#!/usr/bin/env python3
"""
Restart the edge detector if its inference heartbeat goes stale.

This watchdog is intentionally external to the detector process so it can
recover hangs that a normal in-process monitor would miss.
"""

import json
import os
import subprocess
import time

from dotenv import load_dotenv


PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")
DOTENV_PATH = os.path.join(BACKEND_DIR, ".env")
load_dotenv(DOTENV_PATH)

WATCHDOG_FILE_PATH = os.getenv("EDGE_WATCHDOG_FILE", "/tmp/workforce_edge_alive")
WATCHDOG_TIMEOUT_SEC = int(os.getenv("EDGE_WATCHDOG_TIMEOUT_SEC", "60"))
CHECK_INTERVAL_SEC = int(os.getenv("EDGE_WATCHDOG_CHECK_INTERVAL_SEC", "15"))
DETECTOR_SERVICE_NAME = os.getenv(
    "EDGE_DETECTOR_SERVICE_NAME",
    "workforce-edge-detector.service",
)


def read_heartbeat_age_seconds():
    try:
        with open(WATCHDOG_FILE_PATH, "r", encoding="utf-8") as handle:
            payload = json.load(handle)
        heartbeat_ts = float(payload.get("ts", 0))
        if heartbeat_ts <= 0:
            return None
        return time.time() - heartbeat_ts
    except FileNotFoundError:
        return None
    except Exception:
        try:
            stat_result = os.stat(WATCHDOG_FILE_PATH)
            return time.time() - stat_result.st_mtime
        except Exception:
            return None


def restart_detector():
    subprocess.run(
        ["systemctl", "restart", DETECTOR_SERVICE_NAME],
        check=False,
    )


def main():
    print("Edge Watchdog started")
    print(f"Watching  : {WATCHDOG_FILE_PATH}")
    print(f"Timeout   : {WATCHDOG_TIMEOUT_SEC}s")
    print(f"Service   : {DETECTOR_SERVICE_NAME}")

    while True:
        age_seconds = read_heartbeat_age_seconds()

        if age_seconds is not None and age_seconds > WATCHDOG_TIMEOUT_SEC:
            print(
                f"[WATCHDOG] Heartbeat stale ({age_seconds:.1f}s). "
                f"Restarting {DETECTOR_SERVICE_NAME}"
            )
            restart_detector()
            time.sleep(WATCHDOG_TIMEOUT_SEC)
        else:
            time.sleep(CHECK_INTERVAL_SEC)


if __name__ == "__main__":
    main()