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
STARTUP_GRACE_SEC = int(os.getenv("EDGE_WATCHDOG_STARTUP_GRACE_SEC", "90"))
NO_PROGRESS_CHECKS = int(os.getenv("EDGE_WATCHDOG_NO_PROGRESS_CHECKS", "3"))
DETECTOR_SERVICE_NAME = os.getenv(
    "EDGE_DETECTOR_SERVICE_NAME",
    "workforce-edge.service",
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


def read_heartbeat_payload():
    try:
        with open(WATCHDOG_FILE_PATH, "r", encoding="utf-8") as handle:
            return json.load(handle)
    except Exception:
        return None


def is_system_stuck(payload):
    now = time.time()

    process_started_at = float(payload.get("process_started_at") or 0)
    if process_started_at > 0 and (now - process_started_at) < STARTUP_GRACE_SEC:
        if not getattr(is_system_stuck, "startup_logged", False):
            print("[WATCHDOG] In startup grace period -> skip checks")
            is_system_stuck.startup_logged = True
        is_system_stuck.prev_frames = payload.get("total_frames")
        is_system_stuck.no_progress_count = 0
        return False
    is_system_stuck.startup_logged = False

    total_frames = payload.get("total_frames", 0)

    if not hasattr(is_system_stuck, "prev_frames"):
        is_system_stuck.prev_frames = total_frames
        is_system_stuck.no_progress_count = 0
        return False

    if total_frames == is_system_stuck.prev_frames:
        is_system_stuck.no_progress_count += 1
        print(
            f"[WATCHDOG] No progress "
            f"({is_system_stuck.no_progress_count}/{NO_PROGRESS_CHECKS})"
        )
    else:
        is_system_stuck.no_progress_count = 0

    is_system_stuck.prev_frames = total_frames

    if is_system_stuck.no_progress_count < NO_PROGRESS_CHECKS:
        return False

    camera_last_seen = payload.get("camera_last_seen", {})
    for cam_id, seen_ts in camera_last_seen.items():
        if seen_ts and (now - float(seen_ts) > WATCHDOG_TIMEOUT_SEC):
            print(f"[WATCHDOG] Camera stuck: {cam_id}")
            return True

    print("[WATCHDOG] Confirmed no frame progress")
    return True


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
        payload = read_heartbeat_payload()
        age_seconds = read_heartbeat_age_seconds()

        if payload and is_system_stuck(payload):
            print(f"[WATCHDOG] System stuck -> restarting {DETECTOR_SERVICE_NAME}")
            restart_detector()
            time.sleep(WATCHDOG_TIMEOUT_SEC)
            continue

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
