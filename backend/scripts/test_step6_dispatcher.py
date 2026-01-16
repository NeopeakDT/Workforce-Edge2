from pathlib import Path
import sys

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from alerts.notification_dispatcher import dispatch_notifications

dispatch_notifications()
print("STEP 6.2 dispatcher executed")
