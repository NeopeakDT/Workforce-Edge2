# scripts/test_db_connect.py

import sys
from pathlib import Path

# Setup path for imports (allows script to run from any directory)
BACKEND_ROOT = Path(__file__).resolve().parent.parent
if str(BACKEND_ROOT) not in sys.path:
    sys.path.insert(0, str(BACKEND_ROOT))

from dotenv import load_dotenv
load_dotenv()  # remove if you export env vars manually

from common.db import get_cursor

def main():
    print("Testing database connection...")

    with get_cursor() as cur:
        cur.execute("SELECT now() as server_time;")
        row = cur.fetchone()

    print("Connection OK")
    print("Database server time:", row["server_time"])
    
    # Get local UTC time (async function, so we'll use datetime directly for sync script)
    from datetime import datetime, timezone
    local_utc = datetime.now(timezone.utc)
    print("Local UTC time:", local_utc)

if __name__ == "__main__":
    main()
