#!/usr/bin/env python3
"""
PHASE-5 ORCHESTRATOR (SAFE, FINAL)

This script exists ONLY for:
- local testing
- manual backfills
- controlled execution

It MUST NOT:
- create activity_instance rows
- link detection events
- infer lifecycle
- call legacy builder logic

Authoritative lifecycle ownership:
- STEP-1: Edge
- STEP-2: Ingest API
- STEP-3: activity_aggregator (resolver)
- STEP-5: missed_activity_cron (finalizer)
------------------------------------------------------------------------------------

EDGE (real-time)
   ↓
INGEST API (real-time, transactional)
   ↓
STEP-3 resolver (periodic / manual)
   ↓
STEP-5 cron (periodic)
------------------------------------------------------------------------------------------------
"""

from pathlib import Path
import sys

# -------------------------------------------------
# Bootstrap backend path
# -------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

# STEP-3 — Resolver (zone, merge, schedule binding)
from aggregation.activity_aggregator import run as step3_resolver

# STEP-5 — Finalization + MISSED (authoritative)
from aggregation.missed_activity_cron import (
    finalize_completed_activities,
    detect_missed_activities,
)


def run():
    """
    Manual / local execution order.

    Safe to run multiple times.
    Idempotent by design.
    """
    print("[PHASE-5] STEP-3 resolver starting…")
    step3_resolver()

    print("[PHASE-5] STEP-5 finalization starting…")
    finalize_completed_activities()
    detect_missed_activities()

    print("[PHASE-5] DONE")


if __name__ == "__main__":
    run()
