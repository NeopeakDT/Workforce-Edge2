#!/usr/bin/env python3
"""
backend/aggregation/alerts_cron.py
STEP C (Task 8) -- 60s SWEEP-style alert orchestrator.

This script is the periodic ("every 60s") entry point that re-evaluates
the SWEEP-style alerts, i.e. the alerts whose ACTIVE/RESOLVED state can
change purely with the passage of time and therefore need to be
re-checked on a timer rather than only at the moment some DB row is
written:

    ACTIVITY_LATE              (alerts.matchers.activity_matcher.evaluate_late_start_sweep)
    ACTIVITY_RUNNING_LONG      (alerts.matchers.activity_matcher.evaluate_in_progress_instance,
                                 called once per IN_PROGRESS activity_instance)
    POSTURE_DATA_STALE         (alerts.matchers.posture_matcher.evaluate_zone_staleness,
                                 called once per farm_zone)
    EDGE_DEVICE_OFFLINE        (alerts.matchers.edge_device_matcher.evaluate_device_offline,
                                 called once per active edge_device)
    WORKFORCE_DETECTOR_OFFLINE (alerts.matchers.edge_device_matcher.evaluate_detector_offline,
                                 called once per active edge_device)

Point-in-time alerts are deliberately NOT evaluated here:

    ACTIVITY_EARLY       -- fires at the moment an activity_instance is created.
                             Wiring evaluate_early_start() into
                             activity_aggregator.py's instance-creation call
                             sites is Task 9's job, not this script's.
    ACTIVITY_UNSCHEDULED -- likewise point-in-time, evaluated at finalization
                             (evaluate_finalized_instance), not on a sweep.
    ACTIVITY_MISSED       -- already wired into missed_activity_cron.py
                             (Task 4) as part of the STEP-5B batch; it has no
                             business being duplicated here.

Overlap protection
-------------------
main_with_lock() takes a non-blocking exclusive flock() on a fixed local
lock file before calling run(). This lock is intentionally HOST-LOCAL: it
only prevents two instances of this script racing each other on the SAME
machine (e.g. a slow tick overlapping the next systemd timer firing). This
deployment currently runs its backend + aggregation + alerts cron on a
single Edge/backend host, so a local flock is sufficient. If this scheduler
is ever moved to run from more than one host concurrently, a host-local
flock no longer provides mutual exclusion and would need to be reconsidered
-- e.g. replaced with a Postgres advisory lock (pg_try_advisory_lock) that
is visible across hosts. That is NOT implemented here; do not add it
speculatively.

Logging follows aggregation/run_phase5.py's print-based convention.
"""

from pathlib import Path
import sys
import os
import fcntl

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from common.db import get_cursor

from alerts.matchers.activity_matcher import (
    evaluate_late_start_sweep,
    evaluate_in_progress_instance,
)
from alerts.matchers.posture_matcher import evaluate_zone_staleness
from alerts.matchers.edge_device_matcher import (
    evaluate_device_offline,
    evaluate_detector_offline,
)

DEFAULT_LOCK_FILE = "/tmp/edge2_alerts_cron.lock"
LOCK_FILE = os.environ.get("ALERTS_CRON_LOCK_FILE", DEFAULT_LOCK_FILE)


def run():
    """
    Run one full sweep across all SWEEP-style alert evaluators. Each
    evaluator/entity invocation is individually wrapped in its own
    try/except so that one failure never prevents the rest of the sweep
    from running.
    """

    # -- ACTIVITY_LATE: self-sweeps every farm internally. No extra
    # transaction/get_cursor() wrapping here -- it manages its own. --
    print("[ALERTS_CRON] evaluate_late_start_sweep: starting")
    try:
        evaluate_late_start_sweep()
        print("[ALERTS_CRON] evaluate_late_start_sweep: done")
    except Exception as exc:
        print(f"[ALERTS_CRON] evaluate_late_start_sweep: FAILED -- {exc}")

    # -- ACTIVITY_RUNNING_LONG: once per IN_PROGRESS activity_instance. --
    with get_cursor() as cur:
        cur.execute("SELECT id FROM activity_instance WHERE status = 'IN_PROGRESS'")
        in_progress_ids = [row["id"] for row in cur.fetchall()]

    print(f"[ALERTS_CRON] evaluate_in_progress_instance: {len(in_progress_ids)} instance(s)")
    for instance_id in in_progress_ids:
        try:
            evaluate_in_progress_instance(instance_id)
        except Exception as exc:
            print(
                f"[ALERTS_CRON] evaluate_in_progress_instance: FAILED "
                f"instance_id={instance_id} -- {exc}"
            )

    # -- POSTURE_DATA_STALE: once per farm_zone. --
    with get_cursor() as cur:
        cur.execute("SELECT id, farm_id FROM farm_zone")
        zones = [(row["id"], row["farm_id"]) for row in cur.fetchall()]

    print(f"[ALERTS_CRON] evaluate_zone_staleness: {len(zones)} zone(s)")
    for zone_id, farm_id in zones:
        try:
            evaluate_zone_staleness(farm_id, zone_id)
        except Exception as exc:
            print(
                f"[ALERTS_CRON] evaluate_zone_staleness: FAILED "
                f"zone_id={zone_id} -- {exc}"
            )

    # -- EDGE_DEVICE_OFFLINE / WORKFORCE_DETECTOR_OFFLINE: once per active device. --
    with get_cursor() as cur:
        cur.execute("SELECT id FROM edge_device WHERE is_active = true")
        device_ids = [row["id"] for row in cur.fetchall()]

    print(f"[ALERTS_CRON] evaluate_device_offline/evaluate_detector_offline: {len(device_ids)} device(s)")
    for device_id in device_ids:
        try:
            evaluate_device_offline(device_id)
        except Exception as exc:
            print(
                f"[ALERTS_CRON] evaluate_device_offline: FAILED "
                f"device_id={device_id} -- {exc}"
            )

        try:
            evaluate_detector_offline(device_id)
        except Exception as exc:
            print(
                f"[ALERTS_CRON] evaluate_detector_offline: FAILED "
                f"device_id={device_id} -- {exc}"
            )

    print("[ALERTS_CRON] sweep complete")


def main_with_lock():
    """
    Acquire a non-blocking, host-local exclusive lock before running the
    sweep, so an overrunning tick can never overlap the next scheduled
    tick. If the lock is already held, skip this tick entirely rather
    than block or queue.
    """
    lock_fd = open(LOCK_FILE, "w")
    try:
        try:
            fcntl.flock(lock_fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError:
            print(
                "[ALERTS_CRON] another alerts cron is already running -- "
                "skipping this tick"
            )
            return

        try:
            run()
        finally:
            fcntl.flock(lock_fd, fcntl.LOCK_UN)
    finally:
        lock_fd.close()


if __name__ == "__main__":
    main_with_lock()
