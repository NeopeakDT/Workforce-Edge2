#!/usr/bin/env python3
"""
PHASE-5 ORCHESTRATOR (AUTHORITATIVE)

Order (MANDATORY):
1. STEP-5A — Resolve schedules & session classification
2. STEP-5B — Emit pending missed signals (no activity_instance insert)
3. STEP-6 — Build per-schedule/day compliance snapshot
------------------------------------------------------------------------------------------------------------
run_phase5.py
    ↓
STEP-5A activity_schedule_resolver
    - bind schedule
    - compute offsets
    - write session_classification (EARLY/ON_TIME/LATE/UNSCHEDULED)
    ↓
STEP-5B detect_missed_activities
    - print MISSED_PENDING if none exists
    ↓
STEP-6 build_activity_compliance
    - one decision row per (farm, schedule, date)

------------------------------------------------------------------------------------------------------------
"""

from pathlib import Path
import sys
import argparse

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.activity_schedule_resolver import resolve as step5a_resolver
from aggregation.missed_activity_cron import detect_missed_activities
from aggregation.activity_compliance_builder import build_activity_compliance


def run(include_step4=False):
    if include_step4:
        from aggregation.activity_aggregator import run as step4_aggregator

        print("[PHASE-5] STEP-4: aggregating activities…")
        step4_aggregator(max_loops=1)

    print("[PHASE-5] STEP-5A: resolving schedules & status…")
    step5a_resolver()

    print("[PHASE-5] STEP-5B: detecting missed activities…")
    detect_missed_activities()

    print("[PHASE-5] STEP-6: building compliance snapshots…")
    build_activity_compliance()


def main():
    parser = argparse.ArgumentParser(
        description="Run Phase-5/6 batch jobs (resolver + missed-pending + compliance)."
    )
    parser.add_argument(
        "--include-step4",
        action="store_true",
        help="Also run STEP-4 aggregator once before STEP-5.",
    )
    args = parser.parse_args()
    run(include_step4=args.include_step4)


if __name__ == "__main__":
    main()
