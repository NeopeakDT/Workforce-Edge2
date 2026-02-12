#!/usr/bin/env python3
"""
jetson/edge_detector.py
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
from uuid import uuid4

from dotenv import load_dotenv
from config.local_cache import load_config
from runtime.model_loader import ModelRunner
from runtime.temporal_smoother import TemporalSmoother
from runtime.video_stream import open_stream

# =========================
# ROI OPTIONAL IMPORT
# =========================
try:
    from runtime.roi_utils import filter_by_roi
    ROI_ENABLED = True
except ImportError:
    ROI_ENABLED = False
    print("[WARNING] ROI filtering disabled - shapely not available")
    def filter_by_roi(detections, polygon):
        """Fallback: return all detections if ROI not available"""
        return detections


# ------------------------------------------------------------------
# Env
# ------------------------------------------------------------------
PROJECT_ROOT = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
BACKEND_DIR = os.path.join(PROJECT_ROOT, "backend")
DOTENV_PATH = os.path.join(BACKEND_DIR, ".env")
load_dotenv(DOTENV_PATH)

# ------------------------------------------------------------------
# Edge-side constants (standalone, no backend dependency)
# ------------------------------------------------------------------
FRAME_AGGREGATE_INTERVAL_SEC = 10  # seconds

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
PROCESSING_FPS = float(os.getenv("EDGE_PROCESSING_FPS", "10.0"))

# ------------------------------------------------------------------
# Activity smoothing
# ------------------------------------------------------------------
SMOOTHERS = {
    "SCRAPPING": TemporalSmoother(),
    "FEEDING": TemporalSmoother(),
    # MILKING disabled - model doesn't support it yet
}

# ------------------------------------------------------------------
# Activity State Machine (per activity)
# ------------------------------------------------------------------
ACTIVITY_STATE = {
    "SCRAPPING": {
        "state": "INACTIVE",
        "session_id": None,
        "last_frame_emit": 0.0,
    },
    "FEEDING": {
        "state": "INACTIVE",
        "session_id": None,
        "last_frame_emit": 0.0,
    },
    # MILKING disabled - model doesn't support it yet
}
# ------------------------------------------------------------------
# Motion Tracking Memory (for feeding)
# ------------------------------------------------------------------
MOTION_MEMORY = {
    # "tmr_machine": {
    #     "prev_centroid": (x, y),
    #     "moving_since": float or None
    # }
}

# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
def build_event_payload(activity, event_type, camera_id, zone_id, detections, session_id):
    """
    Build event payload with mandatory event_id and session_id.
    
    Args:
        activity: Activity type (SCRAPPING, FEEDING, MILKING)
        event_type: Event type (START_CANDIDATE, FRAME_AGGREGATE, END_CANDIDATE)
        camera_id: Camera identifier
        detections: List of detections
        session_id: UUID session identifier (must be provided)
    
    Returns:
        Event payload dictionary
    """
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

    # Generate unique event_id and use it as idempotency_key
    event_id = str(uuid4())

    return {
        "event_id": event_id,
        "session_id": session_id,
        "camera_id": str(camera_id),
        "activity_type": activity,
        "event_type": event_type,
        "event_time": event_time,
        "confidence": confidence,
        "objects": objects_dict,
        "zones": {
            "primary": zone_id
        } if zone_id else None,
        "idempotency_key": event_id,
}


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


def build_pixel_roi(normalized_roi, frame_w, frame_h):
    """
    Convert normalized ROI coordinates to pixel coordinates.
    
    Args:
        normalized_roi: List of dicts with "x" and "y" keys (normalized 0-1)
        frame_w: Frame width in pixels
        frame_h: Frame height in pixels
        
    Returns:
        List of (x, y) tuples in pixel coordinates
    """
    return [
        (int(p["x"] * frame_w), int(p["y"] * frame_h))
        for p in normalized_roi
    ]


def resolve_zone_id(camera_cfg: dict, activity: str):
    """
    Resolve zone_id for a given activity from camera config.
    Returns None if not mapped.
    """
    zones = camera_cfg.get("activity_zones", {})
    zone_cfg = zones.get(activity)
    return zone_cfg["zone_id"] if zone_cfg else None

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

def detect_feeding_motion(
    detections,
    tmr_classes,
    tractor_classes,
    motion_threshold_px,
    min_motion_duration_sec,
    current_ts
):
    global MOTION_MEMORY

    feeding_signal = False

    for det in detections:
        cls = det["class"].lower()

        if cls not in tmr_classes and cls not in tractor_classes:
            continue

        cx, cy = bbox_center(det["bbox"])

        mem = MOTION_MEMORY.get(cls)

        if mem is None:
            MOTION_MEMORY[cls] = {
                "prev_centroid": (cx, cy),
                "moving_since": None
            }
            continue

        prev_centroid = mem["prev_centroid"]
        displacement = euclidean(prev_centroid, (cx, cy))

        # Update centroid
        mem["prev_centroid"] = (cx, cy)

        if displacement >= motion_threshold_px:
            if mem["moving_since"] is None:
                mem["moving_since"] = current_ts
            else:
                duration = current_ts - mem["moving_since"]
                if duration >= min_motion_duration_sec:
                    feeding_signal = True
        else:
            mem["moving_since"] = None

    return feeding_signal


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

    # TODO: support multi-camera processing (currently uses first camera)
    camera = cfg["cameras"][0]
    camera_id = camera["camera_id"]

    # Motion tracking configuration
    motion_threshold_px = camera.get("motion_sensitivity", 8)
    min_motion_duration_sec = 5.0  # start conservative


    # Resolve zone IDs for activities (may be None)
    ZONE_SCRAPPING = resolve_zone_id(camera, "SCRAPPING")
    ZONE_FEEDING = resolve_zone_id(camera, "FEEDING")

    # Do NOT hard-fail here; allow partial mappings during rollout.
    # Events for activities without zones will be skipped below.

    max_dist = cfg.get("activity_params", {}).get("SCRAPPING_MAX_DISTANCE_PX", 120)

    # Auto-detect device: Use CPU if CUDA not available (for laptop testing)
    device = os.getenv("EDGE_DEVICE", None)  # Allow override via env var
    if device is None:
        try:
            import torch
            device = "cuda" if torch.cuda.is_available() else "cpu"
        except ImportError:
            device = "cpu"
        except Exception:
            device = "cpu"
    
    print(f"🔧 Edge Detector: Using device: {device}")
    
    model_path = os.path.join(PROJECT_ROOT, cfg["ml_model_version"]["model_path"])
    runner = ModelRunner(model_path, device=device)

    # Start async event sender (daemon thread)
    Thread(target=event_sender, daemon=True).start()

    # --------------------------------------------------
    # Open video stream (FILE / RTSP / NVR_CHANNEL)
    # --------------------------------------------------
    stream = open_stream(camera)

    cap = stream.cap  # required for FPS / metadata only

    video_fps = cap.get(cv2.CAP_PROP_FPS) or PROCESSING_FPS
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)

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
        ret, frame = stream.read()
        if not ret:
            break

        frame_count += 1
        if frame_count % frame_skip_ratio != 0:
            continue

        processed_frame_count += 1
        t0 = time.time()

        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB).copy()
        detections = runner.infer(frame)

        # Apply ROI filtering if enabled and ROI polygons are configured
        frame_h, frame_w = frame.shape[:2]

        # SCRAPPING ROI filtering
        scrap_roi_cfg = camera.get("activity_zones", {}).get("SCRAPPING", {}).get("roi")
        if ROI_ENABLED and scrap_roi_cfg:
            scrap_polygon = build_pixel_roi(scrap_roi_cfg, frame_w, frame_h)
            detections_scrap = filter_by_roi(detections, scrap_polygon)
        else:
            detections_scrap = detections

        # FEEDING ROI filtering
        feed_roi_cfg = camera.get("activity_zones", {}).get("FEEDING", {}).get("roi")
        if ROI_ENABLED and feed_roi_cfg:
            feed_polygon = build_pixel_roi(feed_roi_cfg, frame_w, frame_h)
            detections_feed = filter_by_roi(detections, feed_polygon)
        else:
            detections_feed = detections

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

        # ------------------------------------------------------------------
        # SCRAPPING - State Machine (using ROI-filtered detections)
        # ------------------------------------------------------------------
        scrapping = detect_scrapping(detections_scrap, PERSON, TOOLS, max_dist)
        sig = SMOOTHERS["SCRAPPING"].update(detections_scrap if scrapping else [])
        state = ACTIVITY_STATE["SCRAPPING"]

        # 1. START transition: INACTIVE -> ACTIVE
        if ZONE_SCRAPPING and sig == "START" and state["state"] == "INACTIVE":
            state["state"] = "ACTIVE"
            state["session_id"] = str(uuid4())
            state["last_frame_emit"] = 0.0

            payload = build_event_payload("SCRAPPING", "START_CANDIDATE", camera_id, ZONE_SCRAPPING, detections_scrap, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[SCRAPPING] START_CANDIDATE emitted | session_id={state['session_id'][:8]}...")
            except:
                pass  # drop event if queue is full (backpressure safety)

        # 2. END transition: ACTIVE -> INACTIVE
        elif ZONE_SCRAPPING and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("SCRAPPING", "END_CANDIDATE", camera_id, ZONE_SCRAPPING, detections_scrap, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[SCRAPPING] END_CANDIDATE emitted | session_id={session_id_short}...")
            except:
                pass  # drop event if queue is full (backpressure safety)

            state["state"] = "INACTIVE"
            state["session_id"] = None
            state["last_frame_emit"] = 0.0  # Reset for next session

        # 3. FRAME_AGGREGATE: Only when ACTIVE and interval elapsed (AFTER START/END checks)
        elif state["state"] == "ACTIVE":
            if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                if ZONE_SCRAPPING:
                    payload = build_event_payload("SCRAPPING", "FRAME_AGGREGATE", camera_id, ZONE_SCRAPPING, detections_scrap, state["session_id"])
                    try:
                        EVENT_QUEUE.put(payload, block=False)
                        if processed_frame_count % 50 == 0:  # Print every 50 frames to avoid spam
                            print(f"[SCRAPPING] FRAME_AGGREGATE emitted | session_id={state['session_id'][:8]}...")
                    except:
                        pass  # drop event if queue is full (backpressure safety)
                state["last_frame_emit"] = now

        # ------------------------------------------------------------------
        # FEEDING - State Machine (using ROI-filtered detections)
        # ------------------------------------------------------------------
        feeding = detect_feeding_motion(
            detections_feed,
            TMR,
            TRACTOR,
            motion_threshold_px,
            min_motion_duration_sec,
            ts
        )

        sig = SMOOTHERS["FEEDING"].update(detections_feed if feeding else [])
        state = ACTIVITY_STATE["FEEDING"]

        # 1. START transition: INACTIVE -> ACTIVE
        if ZONE_FEEDING and sig == "START" and state["state"] == "INACTIVE":
            state["state"] = "ACTIVE"
            state["session_id"] = str(uuid4())
            state["last_frame_emit"] = 0.0

            payload = build_event_payload("FEEDING", "START_CANDIDATE", camera_id, ZONE_FEEDING, detections_feed, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[FEEDING] START_CANDIDATE emitted | session_id={state['session_id'][:8]}...")
            except:
                pass  # drop event if queue is full (backpressure safety)

        # 2. END transition: ACTIVE -> INACTIVE
        elif ZONE_FEEDING and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("FEEDING", "END_CANDIDATE", camera_id, ZONE_FEEDING, detections_feed, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[FEEDING] END_CANDIDATE emitted | session_id={session_id_short}...")
            except:
                pass  # drop event if queue is full (backpressure safety)

            state["state"] = "INACTIVE"
            state["session_id"] = None
            state["last_frame_emit"] = 0.0  # Reset for next session

        # 3. FRAME_AGGREGATE: Only when ACTIVE and interval elapsed (AFTER START/END checks)
        elif state["state"] == "ACTIVE":
            if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                if ZONE_FEEDING:
                    payload = build_event_payload("FEEDING", "FRAME_AGGREGATE", camera_id, ZONE_FEEDING, detections_feed, state["session_id"])
                    try:
                        EVENT_QUEUE.put(payload, block=False)
                        if processed_frame_count % 50 == 0:  # Print every 50 frames to avoid spam
                            print(f"[FEEDING] FRAME_AGGREGATE emitted | session_id={state['session_id'][:8]}...")
                    except:
                        pass  # drop event if queue is full (backpressure safety)
                state["last_frame_emit"] = now

        # Clean motion memory if no feeding-class detections present
        active_classes = {
            d["class"].lower()
            for d in detections_feed
            if d["class"].lower() in TMR or d["class"].lower() in TRACTOR
        }

        for cls in list(MOTION_MEMORY.keys()):
            if cls not in active_classes:
                MOTION_MEMORY.pop(cls)

        # MILKING disabled - model doesn't support it yet

    stream.release()

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
