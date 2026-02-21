"""
jetson/edge_config_sync.py
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
from dotenv import load_dotenv

load_dotenv()

# -------------------------------------------------
# Paths
# -------------------------------------------------
BASE_DIR = Path(__file__).parent
BOOTSTRAP_CONFIG_PATH = BASE_DIR / "config" / "bootstrap_config.json"
CACHE_PATH = BASE_DIR / "config" / "local_cache.json"
load_dotenv(Path(__file__).parent.parent / "backend" / ".env")

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

    if not device_code:
        fatal("DEVICE_CODE not set")

    url = f"{api_base}/api/v1/edge/runtime-config"

    try:
        r = requests.get(
            url,
            timeout=10,
            headers={
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
    # SMART FALLBACK: Camera must have EITHER rtsp_url OR (nvr_rtsp_base + nvr_channel)
    for cam in cfg["cameras"]:
        st = cam.get("stream_type", "AUTO").upper()
        has_rtsp_url = bool(cam.get("rtsp_url"))
        has_nvr_config = bool(cam.get("nvr_rtsp_base") and cam.get("nvr_channel"))

        # FILE streams (testing)
        if st == "FILE":
            pass  # Handled locally, injected later

        # EXPLICIT RTSP
        elif st == "RTSP":
            if not has_rtsp_url:
                fatal(f"RTSP camera {cam['camera_id']} missing rtsp_url")

        # EXPLICIT NVR_CHANNEL
        elif st == "NVR_CHANNEL":
            if not has_nvr_config:
                fatal(f"NVR camera {cam['camera_id']} missing nvr_rtsp_base or nvr_channel")

        # AUTO or unspecified: Smart fallback
        elif st in ["AUTO", "UNKNOWN", None]:
            if not has_rtsp_url and not has_nvr_config:
                fatal(
                    f"Camera {cam['camera_id']}: Must provide either "
                    "rtsp_url OR (nvr_rtsp_base + nvr_channel)"
                )

        if not cam.get("fps"):
            fatal(f"Camera {cam['camera_id']} missing FPS")

    if not cfg["ml_model_version"].get("model_path"):
        fatal("Model path missing in ml_model_version")

    # -------------------------------------------------
    # Inject test video path for FILE streams
    # EDIT VIDEO PATH HERE ONLY ↓
    # -------------------------------------------------
    PROJECT_ROOT = BASE_DIR.parent
    TEST_VIDEO_PATH = PROJECT_ROOT / "test_data" / "Full_scrapping_video_2.mp4"  # ← EDIT THIS

    for cam in cfg["cameras"]:
        if cam["stream_type"] == "FILE":
            cam["video_file_path"] = str(TEST_VIDEO_PATH)

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
