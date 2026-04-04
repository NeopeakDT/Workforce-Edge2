#!/usr/bin/env python3
"""
Compatibility entrypoint for Phase-5 batch orchestration.

Why this file exists:
- systemd/unit docs commonly point to `backend/run_phase5.py`
- the implementation lives in `backend/aggregation/run_phase5.py`
"""

from pathlib import Path
import sys

BACKEND_ROOT = Path(__file__).resolve().parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.run_phase5 import main


if __name__ == "__main__":
    main()

