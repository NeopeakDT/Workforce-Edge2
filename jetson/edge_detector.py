#!/usr/bin/env python3
"""
EDGE DETECTOR — FINAL (FRAME_AGGREGATE ENABLED)

Edge responsibility:
- Detect activities
- Emit START_CANDIDATE / FRAME_AGGREGATE / END_CANDIDATE
- NEVER decide lifecycle
"""

import cv2
import time
import math
import os
import requests
import argparse

from dotenv import load_dotenv
from config.local_cache import load_config
from runtime.model_loader import ModelRunner
from runtime.temporal_smoother import TemporalSmoother

try:
    from runtime.roi_utils import filter_by_roi
    ROI_ENABLED = True
except ImportError:
    ROI_ENABLED = False

# ------------------------------------------------------------------
# Args
# ------------------------------------------------------------------
parser = argparse.ArgumentParser()
parser.add_argument("--dry-run", action="store_true")
args = parser.parse_args()
DRY_RUN = args.dry_run

if DRY_RUN:
    ROI_ENABLED = False

# ------------------------------------------------------------------
# Env
# ------------------------------------------------------------------
load_dotenv()
API_BASE = os.getenv("EDGE_API_BASE")
EDGE_TOKEN = os.getenv("EDGE_TOKEN")

if not DRY_RUN and (not API_BASE or not EDGE_TOKEN):
    raise RuntimeError("EDGE_API_BASE or EDGE_TOKEN not set")

HEADERS = {"Authorization": f"Bearer {EDGE_TOKEN}"} if not DRY_RUN else {}

# ------------------------------------------------------------------
# Activity smoothing + emission control
# ------------------------------------------------------------------
SMOOTHERS = {
    "SCRAPPING": TemporalSmoother(),
    "FEEDING": TemporalSmoother(),
    "MILKING": TemporalSmoother(),
}

FRAME_EMIT_INTERVAL = {
    "FEEDING": 3,
    "SCRAPPING": 2,
    "MILKING": 5,
}

LAST_FRAME_EMIT = {
    "FEEDING": 0,
    "SCRAPPING": 0,
    "MILKING": 0,
}

# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
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
        pass  # fire-and-forget by design

def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

# ------------------------------------------------------------------
# Activity logic
# ------------------------------------------------------------------
def detect_scrapping(detections, person_classes, tool_classes, max_dist):
    persons = [d for d in detections if d["class"] in person_classes]
    tools = [d for d in detections if d["class"] in tool_classes]
    for p in persons:
        pc = bbox_center(p["bbox"])
        for t in tools:
            tc = bbox_center(t["bbox"])
            if euclidean(pc, tc) <= max_dist:
                return True
    return False

def detect_feeding(detections, tmr_classes, tractor_classes):
    return any(
        d["class"] in tmr_classes or d["class"] in tractor_classes
        for d in detections
    )

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    cfg = load_config()

    class_map = cfg["ml_model_version"]["class_map"]
    PERSON = set(class_map["PERSON"])
    TOOLS = set(class_map["SCRAPPING_TOOL"])  # Correct spelling: scrapping with double p
    TMR = set(class_map["TMR"])
    TRACTOR = set(class_map["TRACTOR"])

    camera = cfg["cameras"][0]
    camera_id = camera["camera_id"]
    roi = camera.get("roi_polygon")
    max_dist = cfg.get("activity_params", {}).get("SCRAPING_MAX_DISTANCE_PX", 120)

    model_rel_path = cfg["ml_model_version"]["model_path"]
    # From jetson/ directory, go to parent (Workforce-Detection/) then to models/
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    model_path = os.path.join(project_root, model_rel_path)
    if not os.path.exists(model_path):
        raise RuntimeError(f"Model not found: {model_path}")

    runner = ModelRunner(model_path)

    cap = cv2.VideoCapture("test_data/scraping_video_13.mp4")
    if not cap.isOpened():
        raise RuntimeError("Video open failed")

    while True:
        ret, frame = cap.read()
        if not ret or frame is None:
            break

        detections = runner.infer(frame)
        if ROI_ENABLED and roi:
            detections = filter_by_roi(detections, roi)

        now = time.time()

        # ---------------- SCRAPPING ----------------
        scrapping = detect_scrapping(detections, PERSON, TOOLS, max_dist)
        sig = SMOOTHERS["SCRAPPING"].update(detections if scrapping else [])

        if sig:
            emit_event({
                "activity_type": "SCRAPPING",
                "event_type": sig["type"],
                "camera_id": camera_id,
                "ts": now,
            })

        if scrapping and now - LAST_FRAME_EMIT["SCRAPPING"] >= FRAME_EMIT_INTERVAL["SCRAPPING"]:
            emit_event({
                "activity_type": "SCRAPPING",
                "event_type": "FRAME_AGGREGATE",
                "camera_id": camera_id,
                "ts": now,
            })
            LAST_FRAME_EMIT["SCRAPPING"] = now

        # ---------------- FEEDING ----------------
        feeding = detect_feeding(detections, TMR, TRACTOR)
        sig = SMOOTHERS["FEEDING"].update(detections if feeding else [])

        if sig:
            emit_event({
                "activity_type": "FEEDING",
                "event_type": sig["type"],
                "camera_id": camera_id,
                "ts": now,
            })

        if feeding and now - LAST_FRAME_EMIT["FEEDING"] >= FRAME_EMIT_INTERVAL["FEEDING"]:
            emit_event({
                "activity_type": "FEEDING",
                "event_type": "FRAME_AGGREGATE",
                "camera_id": camera_id,
                "ts": now,
            })
            LAST_FRAME_EMIT["FEEDING"] = now

        # ---------------- MILKING ----------------
        milking = any(d["class"] == "milking" for d in detections)
        sig = SMOOTHERS["MILKING"].update(detections if milking else [])

        if sig:
            emit_event({
                "activity_type": "MILKING",
                "event_type": sig["type"],
                "camera_id": camera_id,
                "ts": now,
            })

        if milking and now - LAST_FRAME_EMIT["MILKING"] >= FRAME_EMIT_INTERVAL["MILKING"]:
            emit_event({
                "activity_type": "MILKING",
                "event_type": "FRAME_AGGREGATE",
                "camera_id": camera_id,
                "ts": now,
            })
            LAST_FRAME_EMIT["MILKING"] = now

if __name__ == "__main__":
    main()
