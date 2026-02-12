#!/usr/bin/env python3
"""
PHASE-5 ORCHESTRATOR (AUTHORITATIVE)

Order (MANDATORY):
1. STEP-4 — Aggregate & close instances
2. STEP-5A — Resolve schedules & finalize status
3. STEP-5B — Detect MISSED activities
------------------------------------------------------------------------------------------------------------
run_phase5.py
    ↓
STEP-4 activity_aggregator
    - build instance
    - merge
    - close
    - compute duration
    ↓
STEP-5A activity_schedule_resolver
    - bind schedule
    - compute offsets
    - finalize status (EARLY/ON_TIME/LATE)
    ↓
STEP-5B detect_missed_activities
    - create MISSED if none exists

------------------------------------------------------------------------------------------------------------
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.activity_aggregator import run as step4_aggregator
from aggregation.activity_schedule_resolver import resolve as step5a_resolver
from aggregation.missed_activity_cron import detect_missed_activities


def run():
    print("[PHASE-5] STEP-4: aggregating activities…")
    step4_aggregator()

    print("[PHASE-5] STEP-5A: resolving schedules & status…")
    step5a_resolver()

    print("[PHASE-5] STEP-5B: detecting missed activities…")
    detect_missed_activities()


if __name__ == "__main__":
    run()
