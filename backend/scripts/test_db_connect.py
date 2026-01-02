# scripts/test_db_connect.py

import sys
from pathlib import Path

# Add parent directory (backend) to Python path so we can import common module
backend_dir = Path(__file__).parent.parent
sys.path.insert(0, str(backend_dir))

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
