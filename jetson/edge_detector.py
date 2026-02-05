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
from datetime import datetime, timezone
from queue import Queue
from threading import Thread

from dotenv import load_dotenv
from config.local_cache import load_config
from runtime.model_loader import ModelRunner
from runtime.temporal_smoother import TemporalSmoother

# ------------------------------------------------------------------
# Env
# ------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")
DOTENV_PATH = os.path.join(BACKEND_DIR, ".env")
load_dotenv(DOTENV_PATH)

API_BASE = os.getenv("EDGE_API_BASE")
DEVICE_KEY = os.getenv("EDGE_DEVICE_KEY")

if not API_BASE or not DEVICE_KEY:
    raise RuntimeError(
        f"EDGE_API_BASE or EDGE_DEVICE_KEY not set "
        f"(loaded .env from: {DOTENV_PATH}, "
        f"EDGE_API_BASE={API_BASE!r}, EDGE_DEVICE_KEY set={bool(DEVICE_KEY)})"
    )

assert API_BASE.endswith("/api/v1"), (
    f"EDGE_API_BASE must end with '/api/v1'. "
    f"Current value: {API_BASE!r}. "
    f"Expected format: http://host:port/api/v1"
)

HEADERS = {"X-DEVICE-KEY": DEVICE_KEY}

# ------------------------------------------------------------------
# Async event queue (CRITICAL for performance)
# ------------------------------------------------------------------
EVENT_QUEUE = Queue(maxsize=2000)

# ------------------------------------------------------------------
# Video Configuration
# ------------------------------------------------------------------
VIDEO_FILE_PATH = os.path.join(
    PROJECT_ROOT,
    "test_data",
    "Full_scrapping_video.mp4"
)

PROCESSING_FPS = float(os.getenv("EDGE_PROCESSING_FPS", "10.0"))

# ------------------------------------------------------------------
# Activity smoothing + emission control
# ------------------------------------------------------------------
SMOOTHERS = {
    "SCRAPPING": TemporalSmoother(),
    "FEEDING": TemporalSmoother(),
    "MILKING": TemporalSmoother(),
}
# Send FRAME_AGGREGATE at intervals (production-safe, reduced spam)
FRAME_EMIT_INTERVAL = {
    "FEEDING": 5,
    "SCRAPPING": 5,
    "MILKING": 8,
}

LAST_FRAME_EMIT = {
    "FEEDING": 0,
    "SCRAPPING": 0,
    "MILKING": 0,
}

ACTIVE = {
    "SCRAPPING": False,
    "FEEDING": False,
    "MILKING": False,
}

# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
def build_event_payload(activity, event_type, camera_id, detections):
    objects_dict = {}
    confidences = []

    for idx, det in enumerate(detections or []):
        cls = det.get("class", "unknown")
        conf = det.get("confidence", 0.0)
        confidences.append(conf)
        objects_dict.setdefault(cls, []).append({"id": f"{cls}_{idx}"})

    if event_type == "FRAME_AGGREGATE":
        confidence = 1.0
    elif event_type == "END_CANDIDATE":
        confidence = 0.7
    elif event_type == "START_CANDIDATE":
        confidence = sum(confidences) / len(confidences) if confidences else 0.8
    else:
        confidence = 0.7

    now_utc = datetime.now(timezone.utc).replace(microsecond=0)
    event_time = now_utc.isoformat().replace("+00:00", "Z")

    idempotency_key = f"{camera_id}-{activity}-{event_type}-{int(time.time() // 5)}"

    return {
        "camera_id": str(camera_id),
        "activity_type": activity,
        "event_type": event_type,
        "event_time": event_time,
        "confidence": confidence,
        "objects": objects_dict,
        "idempotency_key": idempotency_key,
    }

def emit_event(payload):
    try:
        resp = requests.post(
            f"{API_BASE.rstrip('/')}/ingest/event",
            json=payload,
            headers=HEADERS,
            timeout=5,
        )
        if resp.status_code != 200:
            print(f"[EDGE][ERROR] HTTP {resp.status_code}: {resp.text[:200]}")
    except Exception as e:
        print(f"[EDGE][ERROR] Request failed: {str(e)[:200]}")

def event_sender():
    """
    Background thread that sends events to backend.
    This MUST NEVER block inference.
    """
    while True:
        payload = EVENT_QUEUE.get()
        try:
            requests.post(
                f"{API_BASE.rstrip('/')}/ingest/event",
                json=payload,
                headers=HEADERS,
                timeout=3,
            )
        except Exception as e:
            print(f"[EDGE][EVENT-SENDER] failed: {str(e)[:200]}")
        finally:
            EVENT_QUEUE.task_done()

def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

# ------------------------------------------------------------------
# Activity logic
# ------------------------------------------------------------------
def detect_scrapping(detections, person_classes, tool_classes, max_dist):
    persons = [d for d in detections if d["class"].lower() in person_classes]
    tools = [d for d in detections if d["class"].lower() in tool_classes]

    for p in persons:
        pc = bbox_center(p["bbox"])
        for t in tools:
            if euclidean(pc, bbox_center(t["bbox"])) <= max_dist:
                return True
    return False

def detect_feeding(detections, tmr_classes, tractor_classes):
    return any(
        d["class"].lower() in tmr_classes or d["class"].lower() in tractor_classes
        for d in detections
    )

# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    cfg = load_config()

    class_map = cfg["ml_model_version"]["class_map"]
    PERSON = {c.lower() for c in class_map["PERSON"]}
    TOOLS = {c.lower() for c in class_map["SCRAPPING_TOOL"]}
    TMR = {c.lower() for c in class_map["TMR"]}
    TRACTOR = {c.lower() for c in class_map["TRACTOR"]}

    camera = cfg["cameras"][0]
    camera_id = camera["camera_id"]
    max_dist = cfg.get("activity_params", {}).get("SCRAPING_MAX_DISTANCE_PX", 120)

    model_path = os.path.join(PROJECT_ROOT, cfg["ml_model_version"]["model_path"])
    runner = ModelRunner(model_path)

    # Start async event sender (daemon thread)
    Thread(target=event_sender, daemon=True).start()

    cap = cv2.VideoCapture(VIDEO_FILE_PATH)
    if not cap.isOpened():
        raise RuntimeError("Failed to open video")

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    video_fps = cap.get(cv2.CAP_PROP_FPS)

    frame_skip_ratio = (
        max(1, round(video_fps / PROCESSING_FPS))
        if PROCESSING_FPS and video_fps > PROCESSING_FPS
        else 1
    )
    processing_fps = PROCESSING_FPS if frame_skip_ratio > 1 else video_fps

    print(f"Starting video processing: {total_frames} frames @ {video_fps:.2f} FPS")
    print(f"Processing FPS: {processing_fps:.2f}, Frame skip: {frame_skip_ratio}")
    print("=" * 80)

    frame_count = 0
    processed_frame_count = 0
    start_time = time.time()
    total_processing_time = 0.0

    def video_timestamp(frame_idx):
        """Calculate actual video timestamp based on source frame index."""
        return frame_idx / video_fps if video_fps > 0 else 0.0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_count += 1
        if frame_count % frame_skip_ratio != 0:
            continue

        processed_frame_count += 1
        t0 = time.time()

        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB).copy()
        detections = runner.infer(frame)

        frame_processing_time = time.time() - t0
        total_processing_time += frame_processing_time
        ts = video_timestamp(frame_count)  # Use actual video frame index for correct timestamp

        if detections and processed_frame_count % 50 == 0:
            classes = sorted({d["class"] for d in detections})
            print(
                f"Frame {processed_frame_count:5d} | "
                f"Time: {ts:6.2f}s | "
                f"Classes: [{', '.join(classes)}] | "
                f"Process: {frame_processing_time*1000:.1f}ms"
            )

        now = time.time()

        # SCRAPPING
        scrapping = detect_scrapping(detections, PERSON, TOOLS, max_dist)
        # Note: When frames are skipped, smoothing windows are effectively longer
        # This is acceptable for scrapping/feeding activities
        sig = SMOOTHERS["SCRAPPING"].update(detections if scrapping else [])

        if sig:
            # Prevent duplicate END_CANDIDATE events
            if sig["type"] == "END_CANDIDATE" and not ACTIVE["SCRAPPING"]:
                pass  # Skip duplicate END
            else:
                payload = build_event_payload("SCRAPPING", sig["type"], camera_id, detections)
                try:
                    EVENT_QUEUE.put(payload, block=False)
                except:
                    pass  # drop event if queue is full (backpressure safety)
                ACTIVE["SCRAPPING"] = sig["type"] != "END_CANDIDATE"

        if scrapping and ACTIVE["SCRAPPING"] and now - LAST_FRAME_EMIT["SCRAPPING"] >= FRAME_EMIT_INTERVAL["SCRAPPING"]:
            payload = build_event_payload("SCRAPPING", "FRAME_AGGREGATE", camera_id, detections)
            try:
                EVENT_QUEUE.put(payload, block=False)
            except:
                pass  # drop event if queue is full (backpressure safety)
            LAST_FRAME_EMIT["SCRAPPING"] = now

        # FEEDING
        feeding = detect_feeding(detections, TMR, TRACTOR)
        # Note: When frames are skipped, smoothing windows are effectively longer
        # This is acceptable for scrapping/feeding activities
        sig = SMOOTHERS["FEEDING"].update(detections if feeding else [])

        if sig:
            # Prevent duplicate END_CANDIDATE events
            if sig["type"] == "END_CANDIDATE" and not ACTIVE["FEEDING"]:
                pass  # Skip duplicate END
            else:
                payload = build_event_payload("FEEDING", sig["type"], camera_id, detections)
                try:
                    EVENT_QUEUE.put(payload, block=False)
                except:
                    pass  # drop event if queue is full (backpressure safety)
                ACTIVE["FEEDING"] = sig["type"] != "END_CANDIDATE"

        if feeding and ACTIVE["FEEDING"] and now - LAST_FRAME_EMIT["FEEDING"] >= FRAME_EMIT_INTERVAL["FEEDING"]:
            payload = build_event_payload("FEEDING", "FRAME_AGGREGATE", camera_id, detections)
            try:
                EVENT_QUEUE.put(payload, block=False)
            except:
                pass  # drop event if queue is full (backpressure safety)
            LAST_FRAME_EMIT["FEEDING"] = now

        # MILKING (disabled - model doesn't support it yet)
        milking = False
        # Note: When frames are skipped, smoothing windows are effectively longer
        # This is acceptable for scrapping/feeding activities
        sig = SMOOTHERS["MILKING"].update(detections if milking else [])

        if sig:
            # Prevent duplicate END_CANDIDATE events
            if sig["type"] == "END_CANDIDATE" and not ACTIVE["MILKING"]:
                pass  # Skip duplicate END
            else:
                payload = build_event_payload("MILKING", sig["type"], camera_id, detections)
                try:
                    EVENT_QUEUE.put(payload, block=False)
                except:
                    pass  # drop event if queue is full (backpressure safety)
                ACTIVE["MILKING"] = sig["type"] != "END_CANDIDATE"

        if milking and ACTIVE["MILKING"] and now - LAST_FRAME_EMIT["MILKING"] >= FRAME_EMIT_INTERVAL["MILKING"]:
            payload = build_event_payload("MILKING", "FRAME_AGGREGATE", camera_id, detections)
            try:
                EVENT_QUEUE.put(payload, block=False)
            except:
                pass  # drop event if queue is full (backpressure safety)
            LAST_FRAME_EMIT["MILKING"] = now

    cap.release()

    # Summary
    video_duration = video_timestamp(frame_count)  # Use actual video frame count for duration
    total_elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"[COMPLETE] Video Processing Finished")
    print(f"{'='*80}")
    print(f"Total Video Frames: {frame_count}")
    print(f"Processed Frames: {processed_frame_count}")
    print(f"Video Duration: {video_duration:.2f}s (at {video_fps:.2f} FPS)")
    print(f"Total Processing Time: {total_processing_time:.2f}s")
    print(f"Average Processing Speed: {processed_frame_count / total_processing_time:.2f} FPS" if total_processing_time > 0 else "N/A")
    print(f"{'='*80}\n")

if __name__ == "__main__":
    main()
