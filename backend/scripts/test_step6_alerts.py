from pathlib import Path
import sys

# Add backend root
BACKEND_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BACKEND_ROOT))

from alerts.alert_evaluator import evaluate_activity_alerts

# MUST be a real activity_instance.id
ACTIVITY_INSTANCE_ID = "19bd87ba-2987-4a14-a998-fc25b8fe606c"

evaluate_activity_alerts(ACTIVITY_INSTANCE_ID)

print("STEP 6.1 executed for activity:", ACTIVITY_INSTANCE_ID)
