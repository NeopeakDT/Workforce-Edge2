#!/usr/bin/env python3
"""
Edge2 device

PHASE-5 ORCHESTRATOR (AUTHORITATIVE)

Order (MANDATORY)

1. STEP-5A
    Resolve schedules
    Compute offsets
    Write session_classification
        EARLY
        ON_TIME
        LATE
        UNSCHEDULED

2. STEP-5B
    Create MISSED activity_instance rows
    (status='ENDED', session_classification='MISSED')

No STEP-6.

activity_compliance has been removed.
activity_instance is now the single source of truth.
"""

from pathlib import Path
import sys
import argparse

BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.activity_schedule_resolver import resolve as step5a_resolver
from aggregation.missed_activity_cron import detect_missed_activities


def run(include_step4=False):
    if include_step4:
        from aggregation.activity_aggregator import run as step4_aggregator

        print("[PHASE-5] STEP-4: aggregating activities…")
        step4_aggregator(max_loops=1)

    print("[PHASE-5] STEP-5A: resolving schedules & status…")
    step5a_resolver()

    print("[PHASE-5] STEP-5B: detecting missed activities…")
    detect_missed_activities()


def main():
    parser = argparse.ArgumentParser(
        description="Run Phase-5 batch jobs (resolver + missed activity creation)."
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
