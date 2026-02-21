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
import json
import sqlite3
import requests
import numpy as np
from datetime import datetime, timezone
from queue import Queue
from threading import Thread
import threading
from uuid import uuid4

from dotenv import load_dotenv
from config.local_cache import load_config
from runtime.model_loader import ModelRunner
from runtime.temporal_smoother import TemporalSmoother
from runtime.video_stream import open_stream
from runtime.motion_detector import MotionDetector

# Thermal protection
try:
    from jetson_telemetry import collect_telemetry
except ImportError:
    collect_telemetry = None

# =========================
# ROI OPTIONAL IMPORT
# =========================
try:
    from runtime.roi_utils import filter_by_roi
    ROI_ENABLED = True
except ImportError:
    ROI_ENABLED = False
    print("[WARNING] ROI filtering disabled - roi_utils not available")
    def filter_by_roi(detections, polygon, frame_shape, min_overlap_ratio=0.05, roi_mask=None):
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
# Thread-safe inference lock (GPU/TensorRT)
# ------------------------------------------------------------------
INFER_LOCK = threading.Lock()

# ------------------------------------------------------------------
# Async event queue (CRITICAL for performance)
# ------------------------------------------------------------------
EVENT_QUEUE = Queue(maxsize=10000)

# ------------------------------------------------------------------
# Video Configuration
# ------------------------------------------------------------------
PROCESSING_FPS = float(os.getenv("EDGE_PROCESSING_FPS", "10.0"))

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
        confidence = sum(confidences) / len(confidences) if confidences else 0.5
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


# ------------------------------------------------------------------
# Event Retry Mechanism (Prevent Event Loss)
# ------------------------------------------------------------------
RETRY_DB = os.path.join(PROJECT_ROOT, "event_retry.db")
RETRY_LOCK = threading.Lock()


def init_retry_db():
    """Initialize SQLite database for storing failed events."""
    with sqlite3.connect(RETRY_DB) as conn:
        conn.execute("""
            CREATE TABLE IF NOT EXISTS retry_queue (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                payload TEXT NOT NULL
            )
        """)
        conn.commit()


def save_failed_event(payload):
    """Save failed event to database for retry."""
    with RETRY_LOCK:
        with sqlite3.connect(RETRY_DB) as conn:
            conn.execute(
                "INSERT INTO retry_queue (payload) VALUES (?)",
                (json.dumps(payload),)
            )
            conn.commit()


def resend_failed_events():
    """Attempt to resend stored failed events."""
    with RETRY_LOCK:
        with sqlite3.connect(RETRY_DB) as conn:
            rows = conn.execute("SELECT id, payload FROM retry_queue").fetchall()
            for row_id, payload_json in rows:
                payload = json.loads(payload_json)
                try:
                    r = requests.post(
                        f"{API_BASE.rstrip('/')}/ingest/event",
                        json=payload,
                        headers=HEADERS,
                        timeout=3,
                    )
                    if r.status_code == 200:
                        conn.execute("DELETE FROM retry_queue WHERE id=?", (row_id,))
                        conn.commit()
                except:
                    break  # stop on first failure



# ------------------------------------------------------------------
# FIX 3: Thread Supervisor with Auto-Restart (Production Grade)
# ------------------------------------------------------------------
def camera_supervisor(camera, runner, person_classes, tool_classes, tmr_classes, tractor_classes, max_dist):
    """Supervisor wrapper: restarts camera on crash (self-healing system)."""
    camera_code = camera.get("code", "UNKNOWN")
    restart_count = 0
    
    while True:
        try:
            print(f"[SUPERVISOR] Starting camera: {camera_code}")
            process_camera(
                camera,
                runner,
                person_classes,
                tool_classes,
                tmr_classes,
                tractor_classes,
                max_dist,
            )
            # If process_camera returns normally (EOF reached), exit supervisor
            print(f"[SUPERVISOR] Camera {camera_code} finished normally")
            break
        except Exception as e:
            restart_count += 1
            print(f"[SUPERVISOR] ⚠️  Camera {camera_code} crashed (attempt #{restart_count}): {str(e)[:150]}")
            print(f"[SUPERVISOR] Restarting in 5 seconds...")
            time.sleep(5)


def event_sender():
    """
    Background thread that sends events to backend with retry mechanism.
    Failed events are saved to local database and retried periodically.
    This MUST NEVER block inference.
    """
    init_retry_db()

    while True:
        try:
            resend_failed_events()

            payload = EVENT_QUEUE.get()
            try:
                r = requests.post(
                    f"{API_BASE.rstrip('/')}/ingest/event",
                    json=payload,
                    headers=HEADERS,
                    timeout=3,
                )

                if r.status_code != 200:
                    raise Exception(f"Non-200: {r.status_code}")

            except Exception as e:
                print(f"[EDGE][EVENT-SENDER] failed → saving locally: {str(e)[:200]}")
                save_failed_event(payload)

            finally:
                EVENT_QUEUE.task_done()

        except Exception as e:
            print(f"[EDGE][EVENT-SENDER] CRITICAL LOOP ERROR: {e}")
            time.sleep(2)

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




# ------------------------------------------------------------------
# Per-camera processing
# ------------------------------------------------------------------
def process_camera(camera, runner, person_classes, tool_classes, tmr_classes, tractor_classes, max_dist):
    # FIX 2: Wrap entire camera loop in crash guard (auto-restart safe)
    try:
        _process_camera_impl(camera, runner, person_classes, tool_classes, tmr_classes, tractor_classes, max_dist)
    except Exception as e:
        print(f"[CAMERA {camera['camera_id']}] CRITICAL ERROR in camera thread: {str(e)[:200]}")
        import traceback
        traceback.print_exc()
        # Let supervisor handle restart
        raise


def _process_camera_impl(camera, runner, person_classes, tool_classes, tmr_classes, tractor_classes, max_dist):
    camera_id = camera["camera_id"]

    # Motion tracking configuration
    motion_threshold_px = camera.get("motion_sensitivity", 8)
    min_motion_duration_sec = 5.0  # start conservative

    motion_detector = MotionDetector(
        velocity_threshold_px=motion_threshold_px,
        area_velocity_threshold=1500,
        min_motion_duration_sec=min_motion_duration_sec,
    )

    smoothers = {
        "SCRAPPING": TemporalSmoother(),
        "FEEDING": TemporalSmoother(),
    }

    activity_state = {
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
    }

    # Resolve zone IDs for activities (may be None)
    zone_scrapping = resolve_zone_id(camera, "SCRAPPING")
    zone_feeding = resolve_zone_id(camera, "FEEDING")

    # --------------------------------------------------
    # Open video stream (FILE / RTSP / NVR_CHANNEL)
    # --------------------------------------------------
    stream = open_stream(camera)
    cap = stream.cap  # required for FPS / metadata only

    # Log video source information
    stream_type = camera.get("stream_type", "AUTO").upper()
    video_source = camera.get("video_file_path") or camera.get("rtsp_url") or "Unknown"
    print(f"\n[CAMERA {camera_id}] Opening video stream...")
    print(f"  Stream Type: {stream_type}")
    print(f"  Source: {video_source}")
    
    # Get actual frame resolution from video
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    actual_resolution = f"{frame_w}x{frame_h}" if frame_w > 0 and frame_h > 0 else "Unknown"
    config_resolution = camera.get("resolution", "N/A")
    print(f"  Config Resolution: {config_resolution}")
    print(f"  Actual Resolution: {actual_resolution}")

    # Determine if stream is live (RTSP/NVR) or recorded (FILE)
    # Smart detection: explicit stream_type OR infer from config
    stream_type = camera.get("stream_type", "AUTO").upper()
    
    if stream_type == "FILE":
        is_live = False
    elif stream_type == "RTSP":
        is_live = True
    elif stream_type == "NVR_CHANNEL":
        is_live = True
    else:
        # AUTO: Detect from config - live if has rtsp_url or nvr_channel
        is_live = bool(camera.get("rtsp_url") or camera.get("nvr_channel"))

    video_fps = cap.get(cv2.CAP_PROP_FPS) or PROCESSING_FPS
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)

    frame_skip_ratio = (
        max(1, round(video_fps / PROCESSING_FPS))
        if PROCESSING_FPS and video_fps > PROCESSING_FPS
        else 1
    )
    processing_fps = PROCESSING_FPS if frame_skip_ratio > 1 else video_fps

    print(f"[CAMERA {camera_id}] Starting video processing: {total_frames} frames @ {video_fps:.2f} FPS")
    print(f"[CAMERA {camera_id}] Processing FPS: {processing_fps:.2f}, Frame skip: {frame_skip_ratio}")
    print("=" * 80)

    frame_count = 0
    processed_frame_count = 0
    start_time = time.time()
    total_processing_time = 0.0

    scrap_roi_cfg = camera.get("activity_zones", {}).get("SCRAPPING", {}).get("roi")
    feed_roi_cfg = camera.get("activity_zones", {}).get("FEEDING", {}).get("roi")
    scrap_polygon = None
    feed_polygon = None
    scrap_mask = None
    feed_mask = None
    last_frame_size = None

    def video_timestamp(frame_idx):
        """Calculate actual video timestamp based on source frame index."""
        return frame_idx / video_fps if video_fps > 0 else 0.0

    # Hybrid throttling variables
    last_processed_time = 0.0
    frame_interval = 1.0 / PROCESSING_FPS if PROCESSING_FPS > 0 else 0

    # Reconnect backoff control
    reconnect_attempt = 0
    MAX_BACKOFF_SEC = 60

    while True:
        ret, frame = stream.read()

        if not ret:
            reconnect_attempt += 1
            backoff_delay = min(MAX_BACKOFF_SEC, 2 ** reconnect_attempt)

            print(
                f"[CAMERA {camera_id}] Stream lost. "
                f"Reconnect attempt #{reconnect_attempt} "
                f"(waiting {backoff_delay}s)..."
            )

            try:
                stream.release()
            except:
                pass

            time.sleep(backoff_delay)

            try:
                stream = open_stream(camera)
                print(f"[CAMERA {camera_id}] Reconnected successfully.")
                reconnect_attempt = 0  # reset after success
                continue
            except Exception as e:
                print(f"[CAMERA {camera_id}] Reconnect failed: {str(e)[:200]}")
                continue

        frame_count += 1

        # Hybrid frame throttling: time-based for live, frame-based for recorded
        if is_live:
            now_wall = time.time()
            if now_wall - last_processed_time < frame_interval:
                continue
            last_processed_time = now_wall
        else:
            if frame_count % frame_skip_ratio != 0:
                continue

        processed_frame_count += 1
        t0 = time.time()

        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB).copy()
        
        # -------------------------------
        # Thermal Protection
        # -------------------------------
        if collect_telemetry:
            telemetry = collect_telemetry()
            gpu_temp = telemetry.get("gpu_temp_c")

            if gpu_temp and gpu_temp > 90:
                print(f"[THERMAL] CRITICAL GPU TEMP {gpu_temp}C — throttling 2s")
                time.sleep(2)
            elif gpu_temp and gpu_temp > 85:
                print(f"[THERMAL] High GPU TEMP {gpu_temp}C — slowing down")
                time.sleep(0.5)

        # -------------------------------
        # Inference
        # -------------------------------
        # Thread-safe inference with shared model
        with INFER_LOCK:
            detections = runner.infer(frame)

        # Apply ROI filtering if enabled and ROI polygons are configured
        frame_h, frame_w = frame.shape[:2]
        frame_size = (frame_h, frame_w)

        if last_frame_size != frame_size:
            last_frame_size = frame_size
            scrap_polygon = None
            feed_polygon = None
            scrap_mask = None
            feed_mask = None

        # SCRAPPING ROI filtering (precompute polygon/mask on size change)
        if ROI_ENABLED and scrap_roi_cfg:
            if scrap_polygon is None:
                scrap_polygon = build_pixel_roi(scrap_roi_cfg, frame_w, frame_h)
                scrap_mask = np.zeros((frame_h, frame_w), dtype=np.uint8)
                cv2.fillPoly(scrap_mask, [np.array(scrap_polygon, dtype=np.int32)], 1)
            detections_scrap = filter_by_roi(detections, scrap_polygon, frame.shape, roi_mask=scrap_mask)
        else:
            detections_scrap = detections

        # FEEDING ROI filtering (precompute polygon/mask on size change)
        if ROI_ENABLED and feed_roi_cfg:
            if feed_polygon is None:
                feed_polygon = build_pixel_roi(feed_roi_cfg, frame_w, frame_h)
                feed_mask = np.zeros((frame_h, frame_w), dtype=np.uint8)
                cv2.fillPoly(feed_mask, [np.array(feed_polygon, dtype=np.int32)], 1)
            detections_feed = filter_by_roi(detections, feed_polygon, frame.shape, roi_mask=feed_mask)
        else:
            detections_feed = detections

        frame_processing_time = time.time() - t0
        total_processing_time += frame_processing_time
        
        # Use appropriate timestamp based on stream type
        # Live streams: wall-clock time (critical for real-time motion detection)
        # Recorded : video timestamp (prevents velocity miscalculation when processing faster than real-time)
        if is_live:
            ts = time.time()
        else:
            ts = video_timestamp(frame_count)

        if detections and processed_frame_count % 50 == 0:
            classes = sorted({d["class"] for d in detections})
            print(
                f"[CAMERA {camera_id}] Frame {processed_frame_count:5d} | "
                f"Time: {ts:6.2f}s | "
                f"Classes: [{', '.join(classes)}] | "
                f"Process: {frame_processing_time*1000:.1f}ms"
            )

        now = time.time()

        # ------------------------------------------------------------------
        # SCRAPPING - State Machine (using ROI-filtered detections)
        # ------------------------------------------------------------------
        scrapping = detect_scrapping(detections_scrap, person_classes, tool_classes, max_dist)
        sig = smoothers["SCRAPPING"].update(detections_scrap if scrapping else [])
        state = activity_state["SCRAPPING"]

        # 1. START transition: INACTIVE -> ACTIVE
        if zone_scrapping and sig == "START" and state["state"] == "INACTIVE":
            state["state"] = "ACTIVE"
            state["session_id"] = str(uuid4())
            state["last_frame_emit"] = 0.0

            payload = build_event_payload("SCRAPPING", "START_CANDIDATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[CAMERA {camera_id}][SCRAPPING] START_CANDIDATE emitted | session_id={state['session_id'][:8]}...")
            except Exception as e:
                print(
                    f"[EDGE][WARNING] Event queue full. "
                    f"Event dropped. Error: {str(e)[:100]}"
                )

        # 2. END transition: ACTIVE -> INACTIVE
        elif zone_scrapping and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("SCRAPPING", "END_CANDIDATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[CAMERA {camera_id}][SCRAPPING] END_CANDIDATE emitted | session_id={session_id_short}...")
            except Exception as e:
                print(
                    f"[EDGE][WARNING] Event queue full. "
                    f"Event dropped. Error: {str(e)[:100]}"
                )

            state["state"] = "INACTIVE"
            state["session_id"] = None
            state["last_frame_emit"] = 0.0  # Reset for next session

        # 3. FRAME_AGGREGATE: Only when ACTIVE and interval elapsed (AFTER START/END checks)
        elif state["state"] == "ACTIVE":
            if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                if zone_scrapping:
                    payload = build_event_payload("SCRAPPING", "FRAME_AGGREGATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
                    try:
                        EVENT_QUEUE.put(payload, block=False)
                        if processed_frame_count % 50 == 0:  # Print every 50 frames to avoid spam
                            print(f"[CAMERA {camera_id}][SCRAPPING] FRAME_AGGREGATE emitted | session_id={state['session_id'][:8]}...")
                    except Exception as e:
                        print(
                            f"[EDGE][WARNING] Event queue full. "
                            f"Event dropped. Error: {str(e)[:100]}"
                        )
                state["last_frame_emit"] = now

        # ------------------------------------------------------------------
        # FEEDING - State Machine (using ROI-filtered detections)
        # ------------------------------------------------------------------
        feeding = False
        active_ids = set()

        for det in detections_feed:
            cls = det["class"].lower()

            if cls in tmr_classes or cls in tractor_classes:
                object_id = det.get("track_id", cls)
                active_ids.add(object_id)

                if motion_detector.update(object_id, det["bbox"], ts):
                    feeding = True

        motion_detector.cleanup(active_ids)

        sig = smoothers["FEEDING"].update(detections_feed if feeding else [])
        state = activity_state["FEEDING"]

        # 1. START transition: INACTIVE -> ACTIVE
        if zone_feeding and sig == "START" and state["state"] == "INACTIVE":
            state["state"] = "ACTIVE"
            state["session_id"] = str(uuid4())
            state["last_frame_emit"] = 0.0

            payload = build_event_payload("FEEDING", "START_CANDIDATE", camera_id, zone_feeding, detections_feed, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[CAMERA {camera_id}][FEEDING] START_CANDIDATE emitted | session_id={state['session_id'][:8]}...")
            except Exception as e:
                print(
                    f"[EDGE][WARNING] Event queue full. "
                    f"Event dropped. Error: {str(e)[:100]}"
                )

        # 2. END transition: ACTIVE -> INACTIVE
        elif zone_feeding and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("FEEDING", "END_CANDIDATE", camera_id, zone_feeding, detections_feed, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                print(f"[CAMERA {camera_id}][FEEDING] END_CANDIDATE emitted | session_id={session_id_short}...")
            except Exception as e:
                print(
                    f"[EDGE][WARNING] Event queue full. "
                    f"Event dropped. Error: {str(e)[:100]}"
                )

            state["state"] = "INACTIVE"
            state["session_id"] = None
            state["last_frame_emit"] = 0.0  # Reset for next session

        # 3. FRAME_AGGREGATE: Only when ACTIVE and interval elapsed (AFTER START/END checks)
        elif state["state"] == "ACTIVE":
            if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                if zone_feeding:
                    payload = build_event_payload("FEEDING", "FRAME_AGGREGATE", camera_id, zone_feeding, detections_feed, state["session_id"])
                    try:
                        EVENT_QUEUE.put(payload, block=False)
                        if processed_frame_count % 50 == 0:  # Print every 50 frames to avoid spam
                            print(f"[CAMERA {camera_id}][FEEDING] FRAME_AGGREGATE emitted | session_id={state['session_id'][:8]}...")
                    except Exception as e:
                        print(
                            f"[EDGE][WARNING] Event queue full. "
                            f"Event dropped. Error: {str(e)[:100]}"
                        )
                state["last_frame_emit"] = now

        # MILKING disabled - model doesn't support it yet

    stream.release()

    # Summary
    video_duration = video_timestamp(frame_count)  # Use actual video frame count for duration
    total_elapsed = time.time() - start_time
    print(f"\n{'='*80}")
    print(f"[CAMERA {camera_id}] Video Processing Finished")
    print(f"{'='*80}")
    print(f"Total Video Frames: {frame_count}")
    print(f"Processed Frames: {processed_frame_count}")
    print(f"Video Duration: {video_duration:.2f}s (at {video_fps:.2f} FPS)")
    print(f"Total Processing Time: {total_processing_time:.2f}s")
    print(f"Average Processing Speed: {processed_frame_count / total_processing_time:.2f} FPS" if total_processing_time > 0 else "N/A")
    print(f"{'='*80}\n")


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    cfg = load_config()

    class_map = cfg["ml_model_version"]["class_map"]
    person_classes = {c.lower() for c in class_map["PERSON"]}
    tool_classes = {c.lower() for c in class_map["SCRAPPING_TOOL"]}
    tmr_classes = {c.lower() for c in class_map["TMR"]}
    tractor_classes = {c.lower() for c in class_map["TRACTOR"]}

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

    # Create shared ModelRunner (once per device, NOT per camera)
    shared_runner = ModelRunner(model_path, device=device)
    print(f"✅ Model loaded once: {model_path}")

    # Start async event sender (daemon thread)
    Thread(target=event_sender, daemon=True).start()

    # Print input video information
    print("\n" + "="*80)
    print("📹 INPUT VIDEO CONFIGURATION")
    print("="*80)
    for camera in cfg.get("cameras", []):
        camera_id = camera["camera_id"]
        camera_code = camera.get("code", "N/A")
        stream_type = camera.get("stream_type", "AUTO")
        video_path = camera.get("video_file_path", "N/A")
        rtsp_url = camera.get("rtsp_url", "N/A")
        resolution = camera.get("resolution", "N/A")
        
        print(f"\n[CAMERA] {camera_code}")
        # print(f"  ID: {camera_id}")
        # print(f"  Stream Type: {stream_type}")
        print(f"  Resolution: {resolution}")
        if stream_type == "FILE":
            print(f"  Video File: {video_path}")
        elif stream_type == "RTSP":
            print(f"  RTSP URL: {rtsp_url}")
        else:
            print(f"  Video Path/URL: {video_path if video_path != 'N/A' else rtsp_url}")
    print("="*80 + "\n")

    # Start camera threads with supervisor (auto-restart on crash)
    camera_threads = []
    for camera in cfg.get("cameras", []):
        t = Thread(
            target=camera_supervisor,
            args=(
                camera,
                shared_runner,
                person_classes,
                tool_classes,
                tmr_classes,
                tractor_classes,
                max_dist,
            ),
            daemon=False,
        )
        t.start()
        camera_threads.append(t)

    for t in camera_threads:
        t.join()


if __name__ == "__main__":
    main()
