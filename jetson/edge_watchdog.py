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

import requests
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

# Detector-health pulse config. Unlike edge_heartbeat_agent.py, this
# process's primary job is local restart-on-hang, so a missing
# API_BASE/DEVICE_KEY must NOT prevent the watchdog from running --
# it just means the pulse is never sent (see send_detector_health_pulse).
API_BASE = os.getenv("EDGE_API_BASE")
DEVICE_KEY = os.getenv("EDGE_DEVICE_KEY")
DETECTOR_PULSE_INTERVAL_SEC = int(os.getenv("EDGE_DETECTOR_PULSE_INTERVAL_SEC", "60"))

# Persists across main() loop iterations; last time send_detector_health_pulse
# returned True (not last attempt).
_last_pulse_sent_at = 0.0


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


def send_detector_health_pulse(payload):
    """
    Send a detector-health pulse to the backend.

    Must only ever be called from the watchdog's already-confirmed-healthy
    branch (Branch 3: is_system_stuck() evaluated and returned False for
    this payload) -- so detector_healthy is always True here; there is
    nothing left to compute or derive. The backend endpoint itself is
    dumb and records whatever it's told, so all health-decision
    responsibility lives here, in whether this function is called at all.

    Returns True only on a genuine HTTP 200. Never raises.
    """
    if not API_BASE or not DEVICE_KEY:
        return False

    body = {
        "detector_healthy": True,
        "total_frames": payload.get("total_frames", 0),
        "camera_count": len(payload.get("camera_last_seen") or {}),
    }

    base_url = API_BASE.rstrip("/")
    endpoint = f"{base_url}/ingest/detector-heartbeat"

    try:
        resp = requests.post(
            endpoint,
            headers={"X-DEVICE-KEY": DEVICE_KEY},
            json=body,
            timeout=5,
        )

        if resp.status_code != 200:
            print(f"[WATCHDOG][PULSE][ERROR] HTTP {resp.status_code}: {resp.text[:200]}")
            return False

        return True

    except Exception as e:
        print(f"[WATCHDOG][PULSE][ERROR] {str(e)[:200]}")
        return False


def restart_detector():
    # If the unit already hit its StartLimitBurst/StartLimitIntervalSec
    # window (crash-looped too many times, e.g. from persistent RTSP
    # flakiness), systemd parks it in a "failed" state and refuses ANY
    # further start/restart -- including this one -- until reset-failed
    # is called. Without this, `systemctl restart` here silently no-ops
    # forever: the watchdog keeps "detecting stuck" and "restarting" on
    # every cycle but the detector never actually comes back until a
    # human runs `systemctl reset-failed` by hand (this is what happened
    # in the 2026-08-27/28 ~27h outage -- restart attempts were logged
    # but none of them could have succeeded once the unit was parked).
    reset = subprocess.run(
        ["systemctl", "reset-failed", DETECTOR_SERVICE_NAME],
        check=False,
        capture_output=True,
        text=True,
    )
    if reset.returncode != 0:
        print(f"[WATCHDOG] reset-failed exited {reset.returncode}: {reset.stderr.strip()}")

    result = subprocess.run(
        ["systemctl", "restart", DETECTOR_SERVICE_NAME],
        check=False,
        capture_output=True,
        text=True,
    )
    if result.returncode == 0:
        print(f"[WATCHDOG] restart of {DETECTOR_SERVICE_NAME} succeeded")
    else:
        print(
            f"[WATCHDOG] restart of {DETECTOR_SERVICE_NAME} FAILED "
            f"(exit {result.returncode}): {result.stderr.strip()}"
        )


def run_one_watchdog_cycle(payload, age_seconds):
    """
    Perform exactly one iteration of the watchdog loop's decision logic:
    Branch 1 (system stuck -> restart), Branch 2 (heartbeat file stale ->
    restart), Branch 3 (healthy -> maybe send a detector-health pulse).

    This is a mechanical extraction of main()'s former inline loop body --
    same branches, same guards, same sleeps, same side effects on
    _last_pulse_sent_at -- so both main() and the test suite exercise this
    single code path instead of a parallel copy of it.
    """
    global _last_pulse_sent_at

    if payload and is_system_stuck(payload):
        print(f"[WATCHDOG] System stuck -> restarting {DETECTOR_SERVICE_NAME}")
        restart_detector()
        time.sleep(WATCHDOG_TIMEOUT_SEC)
        return

    if age_seconds is not None and age_seconds > WATCHDOG_TIMEOUT_SEC:
        print(
            f"[WATCHDOG] Heartbeat stale ({age_seconds:.1f}s). "
            f"Restarting {DETECTOR_SERVICE_NAME}"
        )
        restart_detector()
        time.sleep(WATCHDOG_TIMEOUT_SEC)
    else:
        if payload is not None and time.time() - _last_pulse_sent_at >= DETECTOR_PULSE_INTERVAL_SEC:
            if send_detector_health_pulse(payload):
                _last_pulse_sent_at = time.time()
        time.sleep(CHECK_INTERVAL_SEC)


def main():
    print("Edge Watchdog started")
    print(f"Watching  : {WATCHDOG_FILE_PATH}")
    print(f"Timeout   : {WATCHDOG_TIMEOUT_SEC}s")
    print(f"Service   : {DETECTOR_SERVICE_NAME}")
    if not API_BASE or not DEVICE_KEY:
        print(
            "[WATCHDOG] EDGE_API_BASE/EDGE_DEVICE_KEY not set -- "
            "detector-health pulses disabled (restart logic unaffected)"
        )

    while True:
        payload = read_heartbeat_payload()
        age_seconds = read_heartbeat_age_seconds()
        run_one_watchdog_cycle(payload, age_seconds)


if __name__ == "__main__":
    main()
