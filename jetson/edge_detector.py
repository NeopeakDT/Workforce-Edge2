"""
Edge Detector

Main computer vision inference pipeline for Jetson devices. Detects specific
farm activities (SCRAPING, FEEDING) using AI model inference, spatial filtering,
and temporal smoothing. Emits detection signals to backend API.

Activity Detection Logic:
    - SCRAPING: Requires person + scrapping_tool within distance threshold
    - FEEDING: Requires tmr_machine OR tractor (person optional, no distance)

Key Features:
    - Multi-activity detection with independent temporal smoothing
    - Spatial filtering by Region of Interest (ROI)
    - Distance-based activity validation (SCRAPING only)
    - Fire-and-forget event emission to backend
    - Never touches database or activity lifecycle

Architecture:
    - Edge device: Inference + signal emission only
    - Backend: Activity lifecycle management
    - Separation of concerns: Edge detects, backend manages

Usage:
    export EDGE_API_BASE=https://api.example.com
    export EDGE_TOKEN=your_device_token
    python edge_detector.py

Dependencies:
    - config.local_cache: Load cached configuration
    - runtime.model_loader: YOLO model inference
    - runtime.roi_utils: Spatial filtering
    - runtime.temporal_smoother: Signal debouncing
"""

import cv2
import time
import math
import os
import requests

from config.local_cache import load_config
from runtime.model_loader import ModelRunner
from runtime.roi_utils import filter_by_roi
from runtime.temporal_smoother import TemporalSmoother

import argparse

# =========================
# ARGUMENT PARSING (MUST BE FIRST)
# =========================
parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()

DRY_RUN = args.dry_run

# =========================
# ENV LOADING & VALIDATION
# =========================
from dotenv import load_dotenv
load_dotenv()


API_BASE = os.getenv("EDGE_API_BASE")
EDGE_TOKEN = os.getenv("EDGE_TOKEN")

if not DRY_RUN:
    if not API_BASE or not EDGE_TOKEN:
        raise RuntimeError("EDGE_API_BASE or EDGE_TOKEN not set")

HEADERS = {"Authorization": f"Bearer {EDGE_TOKEN}"} if not DRY_RUN else {}


# =========================
# ACTIVITY SMOOTHERS
# =========================
SMOOTHERS = {
    "SCRAPING": TemporalSmoother(),
    "FEEDING": TemporalSmoother(),
}

# Class maps loaded from config (set in main())
PERSON_CLASSES = set()
SCRAPING_TOOL_CLASSES = set()
TMR_CLASSES = set()
TRACTOR_CLASSES = set()
SCRAPING_MAX_DISTANCE_PX = 120  # Default, overridden from config


# =========================
# UTILS - Checks distance between bounding boxes
# =========================
def bbox_center(bbox):
    x1, y1, x2, y2 = bbox
    return ((x1 + x2) / 2, (y1 + y2) / 2)


def euclidean(p1, p2):
    return math.sqrt((p1[0] - p2[0]) ** 2 + (p1[1] - p2[1]) ** 2)


def emit_event(payload):
    if DRY_RUN:
        print("[DRY-RUN]", payload)
        return

    try:
        requests.post(
            f"{API_BASE}/api/v1/edge/detection-event",
            json=payload,
            headers=HEADERS,
            timeout=2,
        )
    except Exception:
        # Fire-and-forget by design
        pass


# =========================
# ACTIVITY LOGIC (EDGE-ONLY)
# =========================
def detect_scraping(detections):
    """
    Scraping Logic:
    - person + scrapping_tool present (using semantic class_map)
    - distance between them <= threshold
    """
    persons = [d for d in detections if d["class"] in PERSON_CLASSES]
    tools = [d for d in detections if d["class"] in SCRAPING_TOOL_CLASSES]

    for p in persons:
        pc = bbox_center(p["bbox"])
        for t in tools:
            tc = bbox_center(t["bbox"])
            if euclidean(pc, tc) <= SCRAPING_MAX_DISTANCE_PX:
                return True

    return False


def detect_feeding(detections):
    """
    Feeding Logic:
    - tmr_machine OR tractor present (using semantic class_map)
    - person optional
    """
    for d in detections:
        if d["class"] in TMR_CLASSES or d["class"] in TRACTOR_CLASSES:
            return True
    return False


# =========================
# MAIN PIPELINE
# =========================
def main():
    cfg = load_config()

    # Load class_map from config (MANDATORY for model evolution)
    class_map = cfg["ml_model_version"]["class_map"]
    global PERSON_CLASSES, SCRAPING_TOOL_CLASSES, TMR_CLASSES, TRACTOR_CLASSES, SCRAPING_MAX_DISTANCE_PX
    
    PERSON_CLASSES = set(class_map["PERSON"])
    SCRAPING_TOOL_CLASSES = set(class_map["SCRAPING_TOOL"])
    TMR_CLASSES = set(class_map["TMR"])
    TRACTOR_CLASSES = set(class_map["TRACTOR"])

    # Load activity parameters from config (camera-dependent thresholds)
    activity_params = cfg.get("activity_params", {})
    SCRAPING_MAX_DISTANCE_PX = activity_params.get("SCRAPING_MAX_DISTANCE_PX", 120)

    # Use first camera for now (Phase 4 - single camera)
    camera = cfg["cameras"][0]

    camera_id = camera["camera_id"]
    roi_polygon = camera["roi_polygon"]

    # Safe guard: Validate ROI exists (fail fast > silent wrong inference)
    if not roi_polygon:
        raise RuntimeError(f"ROI missing for camera {camera_id}")

    # FPS handling (optional - for future frame skipping)
    fps = camera.get("fps", 5)
    
    # Model path resolution (backend decides which, Jetson decides where)
    model_rel_path = cfg["ml_model_version"]["model_path"]
    model_path = os.path.join(os.getcwd(), model_rel_path)

    if not os.path.exists(model_path):
        raise RuntimeError(f"Model file not found: {model_path}")

    # Use recorded video for testing (not RTSP)
    cap = cv2.VideoCapture("test_data/scraping_defrag 2.mp4")
    runner = ModelRunner(model_path)

    while True:
        ret, frame = cap.read()
        if not ret:
            time.sleep(1)
            continue

        detections = runner.infer(frame)
        detections = filter_by_roi(detections, roi_polygon)

        # ---------- SCRAPING ----------
        scraping_present = detect_scraping(detections)
        signal = SMOOTHERS["SCRAPING"].update(
            detections if scraping_present else []
        )

        if signal:
            emit_event({
                "activity_type": "SCRAPING",
                "event_type": signal["type"],
                "camera_id": camera_id,
                "ts": time.time(),
            })

        # ---------- FEEDING ----------
        feeding_present = detect_feeding(detections)
        signal = SMOOTHERS["FEEDING"].update(
            detections if feeding_present else []
        )

        if signal:
            emit_event({
                "activity_type": "FEEDING",
                "event_type": signal["type"],
                "camera_id": camera_id,
                "ts": time.time(),
            })


if __name__ == "__main__":
    main()
