"""
Edge Config Sync (Phase 3)

MANDATORY bootstrap script.
Fetches and validates runtime configuration from backend.
NO inference is allowed unless this succeeds.
"""

import json
import os
import sys
from pathlib import Path
import requests

# -------------------------------------------------
# Paths
# -------------------------------------------------
BASE_DIR = Path(__file__).parent
BOOTSTRAP_CONFIG_PATH = BASE_DIR / "config" / "bootstrap_config.json"
CACHE_PATH = BASE_DIR / "config" / "local_cache.json"

# -------------------------------------------------
# Fatal helper
# -------------------------------------------------
def fatal(msg: str):
    print(f"FATAL: {msg}", file=sys.stderr)
    sys.exit(1)

# -------------------------------------------------
# Resolve backend API base
# -------------------------------------------------
def get_api_base() -> str:
    api_base = os.getenv("BACKEND_API_URL")

    if api_base:
        return api_base.rstrip("/")

    if BOOTSTRAP_CONFIG_PATH.exists():
        try:
            data = json.loads(BOOTSTRAP_CONFIG_PATH.read_text())
            if "backend_api_url" in data:
                return data["backend_api_url"].rstrip("/")
        except Exception:
            fatal("Invalid bootstrap_config.json")

    if len(sys.argv) > 1:
        return sys.argv[1].rstrip("/")

    fatal("BACKEND_API_URL not set")

# -------------------------------------------------
# Main
# -------------------------------------------------
def main():
    api_base = get_api_base()

    device_code = os.getenv("DEVICE_CODE")
    edge_token = os.getenv("EDGE_TOKEN")

    if not device_code:
        fatal("DEVICE_CODE not set")

    if not edge_token:
        fatal("EDGE_TOKEN not set")

    url = f"{api_base}/api/v1/edge/runtime-config"

    try:
        r = requests.get(
            url,
            timeout=10,
            headers={
                "Authorization": f"Bearer {edge_token}",
                "X-DEVICE-CODE": device_code,
            },
        )
    except Exception as e:
        fatal(f"Connection error: {e}")

    if r.status_code != 200:
        fatal(f"Config fetch failed ({r.status_code}): {r.text}")

    cfg = r.json()

    # -------------------------------------------------
    # HARD VALIDATION (Phase-3 contract)
    # -------------------------------------------------
    required_keys = [
        "device_id",
        "farm_id",
        "farm_timezone",
        "cameras",
        "device_model_assignment",
        "ml_model_version",
    ]


    for key in required_keys:
        if key not in cfg:
            fatal(f"Missing required config key: {key}")

    # Per-camera validation
    for cam in cfg["cameras"]:
        if not cam.get("rtsp_url"):
            fatal("Camera missing rtsp_url")

        if not cam.get("roi_polygon"):
            fatal(f"Camera {cam['camera_id']} missing ROI")

        if not cam.get("fps"):
            fatal(f"Camera {cam['camera_id']} missing FPS")

    if not cfg["ml_model_version"].get("model_path"):
        fatal("Model path missing in ml_model_version")

    # -------------------------------------------------
    # Atomic cache write
    # -------------------------------------------------
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    tmp = CACHE_PATH.with_suffix(".tmp")
    tmp.write_text(json.dumps(cfg, indent=2))
    tmp.replace(CACHE_PATH)

    print("✅ Config sync successful")
    print(f"📄 Cached at: {CACHE_PATH}")

# -------------------------------------------------
if __name__ == "__main__":
    main()
