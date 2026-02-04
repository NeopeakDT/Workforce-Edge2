"""
aggregation/
├── activity_instance_builder.py      # STEP-3
├── activity_aggregator.py             # STEP-4 + STEP-5a
├── missed_activity_cron.py        # STEP-5b
└── run_phase5.py                      # orchestrator (optional)

This script comments out the production aggregator and uses the test aggregator only.

"""
from pathlib import Path
import sys

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.activity_instance_builder import run as step3

# NOTE: Using production activity_aggregator for STEP-4 + STEP-5a
from aggregation.activity_aggregator import run as step4_5a
# from aggregation.test_activity_aggregator import run as step4_5a  # old test implementation

def run():
    step3()        # link ALL events + create instance if needed
    step4_5a()     # finalize + classify

if __name__ == "__main__":
    run()


