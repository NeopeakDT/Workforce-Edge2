"""
aggregation/
├── activity_instance_builder.py      # STEP-3
├── activity_aggregator.py             # STEP-4 + STEP-5a
├── missed_activity_cron.py        # STEP-5b
└── run_phase5.py                      # orchestrator (optional)

"""

from pathlib import Path
import sys

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from aggregation.activity_instance_builder import run as step3
from aggregation.activity_aggregator import run as step4_5a
from aggregation.missed_activity_cron import detect_missed_activities

def run():
    step3()
    step4_5a()
    # detect_missed_activities()

if __name__ == "__main__":
    run()
