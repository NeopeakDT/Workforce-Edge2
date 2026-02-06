#!/usr/bin/env python3
"""
STEP-4 — ACTIVITY INSTANCE BUILDER (DISABLED / NEUTERED)

⚠️ IMPORTANT ⚠️
This file is intentionally DISABLED.

All lifecycle logic is handled by:
- STEP-1: Edge state machine (START / FRAME / END)
- STEP-2: Backend ingest (transactional, idempotent)
- STEP-3: activity_aggregator (zone, merge, schedule binding)
- STEP-5: finalization cron (EARLY / ON_TIME / LATE, MISSED)

This builder MUST NOT:
- create activity_instance rows
- attach detection events
- infer lifecycle from time gaps
- modify IN_PROGRESS instances
- finalize status

If this file ever starts creating instances again,
THE PIPELINE IS BROKEN.
"""

from pathlib import Path
import sys

# -------------------------------------------------------------------
# Bootstrap
# -------------------------------------------------------------------
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))


# -------------------------------------------------------------------
# STEP-4 — NO-OP BUILDER
# -------------------------------------------------------------------
def run():
    """
    Intentionally no-op.

    This function exists only to satisfy:
    - legacy cron hooks
    - deployment expectations

    DO NOT ADD LOGIC HERE.
    """
    return


# -------------------------------------------------------------------
if __name__ == "__main__":
    run()
