"""
Edge Config Sync

MANDATORY bootstrap script that fetches and validates configuration from backend.
Only script that pulls backend config. Atomic cache write prevents corruption.

Key Features:
    - Fetches configuration from backend API
    - Validates all required configuration keys
    - Atomic write to local_cache.json (prevents corruption)
    - Hard-fails on any error (prevents partial configuration)

Usage:
    # Method 1: Environment variable (recommended)
    export BACKEND_API_URL=https://api.example.com
    python edge_config_sync.py
    
    # Method 2: Bootstrap config file
    # Create config/bootstrap_config.json with {"backend_api_url": "https://api.example.com"}
    python edge_config_sync.py
    
    # Method 3: Command line argument
    python edge_config_sync.py https://api.example.com
    
    It runs exactly once per boot — before any inference starts.
    Must run before device starts inference. No inference allowed here.
"""

import json
import os
import sys
from pathlib import Path
import requests

# Get API_BASE from environment variable, config file, or command line
# Priority: 1. Environment variable, 2. bootstrap_config.json, 3. Command line arg
API_BASE = os.getenv("BACKEND_API_URL")

BOOTSTRAP_CONFIG_PATH = Path("config/bootstrap_config.json")
CACHE_PATH = Path("config/local_cache.json")


def fatal(msg: str):
    """Print fatal error and exit"""
    print(f"FATAL: {msg}", file=sys.stderr)
    sys.exit(1)


def get_api_base() -> str:
    """
    Get backend API URL from multiple sources.
    
    Priority order:
        1. BACKEND_API_URL environment variable
        2. bootstrap_config.json file
        3. Command line argument (if provided)
    
    Returns:
        str: Backend API base URL
        
    Raises:
        SystemExit: If API_BASE cannot be determined
    """
    global API_BASE
    
    # 1. Check environment variable
    if API_BASE:
        return API_BASE.rstrip('/')
    
    # 2. Check bootstrap config file
    if BOOTSTRAP_CONFIG_PATH.exists():
        try:
            bootstrap = json.loads(BOOTSTRAP_CONFIG_PATH.read_text())
            if "backend_api_url" in bootstrap:
                return bootstrap["backend_api_url"].rstrip('/')
        except Exception:
            pass  # Continue to next method
    
    # 3. Check command line argument (if provided)
    if len(sys.argv) > 1:
        return sys.argv[1].rstrip('/')
    
    # No valid source found
    fatal(
        "BACKEND_API_URL not set. Provide via:\n"
        "  1. Environment variable: export BACKEND_API_URL=https://api.example.com\n"
        "  2. Bootstrap config: config/bootstrap_config.json with 'backend_api_url' key\n"
        "  3. Command line: python edge_config_sync.py https://api.example.com"
    )


def main():
    """
    Fetch configuration from backend and write to local cache.
    
    Process:
        1. Get API_BASE from environment/config/command line
        2. Fetch config from backend API
        3. Validate all required keys are present
        4. Atomically write to local_cache.json
        5. Exit on any error
    """
    api_base = get_api_base()
    
    try:
        r = requests.get(
            f"{api_base}/edge/runtime-config",
            timeout=10,
            headers={"Authorization": "Bearer EDGE_TOKEN"},
        )
    except Exception as e:
        fatal(str(e))
    
    if r.status_code != 200:
        fatal(f"config fetch failed: {r.status_code}")
    
    cfg = r.json()
    
    for k in [
        "farm_camera",
        "camera_stream_config",
        "device_model_assignment",
        "ml_model_version",
    ]:
        if k not in cfg:
            fatal(f"missing key {k}")
    
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(cfg, indent=2))
    tmp.replace(CACHE_PATH)
    
    print("Config synced successfully")


if __name__ == "__main__":
    main()
