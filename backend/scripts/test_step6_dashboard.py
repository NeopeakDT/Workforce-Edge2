from pathlib import Path
import sys

# Add backend root to Python path
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dashboard.dashboard_query_service import (
    get_farm_overview,
    list_today_activities,
    list_in_progress_activities,
    list_missed_activities,
    list_recent_alerts,
)

FARM_ID = "9fa991d5-23ee-429f-8c7f-09b4acc3a415"
ACTIVITY_DATE = "2026-01-15"

print("\n--- FARM OVERVIEW ---")
print(get_farm_overview(FARM_ID))

print("\n--- TODAY ACTIVITIES ---")
print(list_today_activities(FARM_ID, ACTIVITY_DATE))

print("\n--- IN PROGRESS ACTIVITIES ---")
print(list_in_progress_activities(FARM_ID))

print("\n--- MISSED ACTIVITIES ---")
print(list_missed_activities(FARM_ID))

print("\n--- RECENT ALERTS ---")
print(list_recent_alerts(FARM_ID))
