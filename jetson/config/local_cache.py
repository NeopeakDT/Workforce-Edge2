"""
Local Cache Configuration Manager

Single trusted access point for loading and validating cached configuration.
Hard-fails on any inconsistency to prevent partial boot.

Required Keys:
    - farm_camera: Farm and camera identification
    - camera_stream_config: Camera stream settings
    - device_model_assignment: Model assignment for this device
    - ml_model_version: Machine learning model version

Usage:
    from config.local_cache import load_config
    config = load_config()  # Exits if validation fails
"""

import json
import sys
from pathlib import Path
from typing import Dict, Any

CACHE_PATH = Path(__file__).parent / "local_cache.json"

REQUIRED_KEYS = {
    "device_id",
    "farm_id",
    "farm_timezone",
    "cameras",
    "ml_model_version",
}


def load_config() -> Dict[str, Any]:
   
    if not CACHE_PATH.exists():
        print("FATAL: local_cache.json missing", file=sys.stderr)
        sys.exit(1)    
    try:
        cfg = json.loads(CACHE_PATH.read_text())
    except Exception:
        print("FATAL: invalid JSON in local_cache.json", file=sys.stderr)
        sys.exit(1)
    
    missing = REQUIRED_KEYS - cfg.keys()
    if missing:
        print(f"FATAL: missing config sections {missing}", file=sys.stderr)
        sys.exit(1)
    
    return cfg
