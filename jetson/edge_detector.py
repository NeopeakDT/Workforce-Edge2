#!/usr/bin/env python3
"""
jetson/edge_detector.py
EDGE DETECTOR — FINAL (FRAME_AGGREGATE ENABLED)

Edge responsibility:
- Detect activities
- Emit START_CANDIDATE / FRAME_AGGREGATE / END_CANDIDATE
- NEVER decide lifecycle

# Run backend with the below command-
uvicorn app:app --log-level warning

"""

import cv2
import time
import math
import os
import json
import sqlite3
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
import numpy as np
import logging
from logging.handlers import QueueHandler, QueueListener
import queue
from collections import defaultdict
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
    # Lazy log: will be captured when we setup logger
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
# HTTP Session (Connection Pooling)
# ------------------------------------------------------------------
SESSION = requests.Session()

# HTTP connection pool tuning for burst performance
adapter = HTTPAdapter(
    pool_connections=10,
    pool_maxsize=10,
    max_retries=Retry(total=2, backoff_factor=0.1),
)
SESSION.mount("http://", adapter)
SESSION.mount("https://", adapter)

# ------------------------------------------------------------------
# Buffered Logging Configuration (Non-blocking, QueueHandler)
# ------------------------------------------------------------------
LOG_LEVEL = os.getenv("EDGE_LOG_LEVEL", "INFO").upper()
log_queue = queue.Queue(maxsize=10000)  # Bounded queue to prevent memory leak
queue_handler = QueueHandler(log_queue)

console_handler = logging.StreamHandler()
console_handler.setFormatter(
    logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
)
console_handler.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))

logger = logging.getLogger("edge")
logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
logger.addHandler(queue_handler)
logger.propagate = False  # Prevent duplicate logging

# Start listener thread (daemon, non-blocking writes)
log_listener = QueueListener(log_queue, console_handler, respect_handler_level=True)
log_listener.start()

# ------------------------------------------------------------------
# GPU Inference Pipeline (Queue-based, removes INFER_LOCK)
# ------------------------------------------------------------------
FRAME_QUEUE = Queue(maxsize=100)

# ------------------------------------------------------------------
# Async event queue (CRITICAL for performance)
# ------------------------------------------------------------------
EVENT_QUEUE = Queue(maxsize=10000)

# ------------------------------------------------------------------
# Performance Monitoring
# ------------------------------------------------------------------
STATS = {
    "frames_processed": 0,
    "events_sent": 0,
}
CAMERA_STATS = defaultdict(lambda: {"frames": 0})
STATS_LOCK = threading.Lock()

# ------------------------------------------------------------------
# Video Configuration
# ------------------------------------------------------------------
PROCESSING_FPS = float(os.getenv("EDGE_PROCESSING_FPS", "5.0"))

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
                    r = SESSION.post(
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
# GPU Inference Worker — Dedicated Thread for TensorRT
# ------------------------------------------------------------------
def inference_worker(runner):
    """
    Dedicated GPU worker thread.
    - Pops frames from FRAME_QUEUE
    - Runs inference (no lock needed)
    - Returns detections via callback
    - Keeps GPU fully utilized
    """
    while True:
        try:
            item = FRAME_QUEUE.get()
            if item is None:  # Sentinel value to stop
                break

            camera_id, frame, callback = item

            try:
                detections = runner.infer(frame)
                callback(detections)
            except Exception as e:
                logger.error("[GPU_WORKER] Inference error for camera %s: %s", camera_id, str(e))
                callback([])
            finally:
                FRAME_QUEUE.task_done()
        except Exception as e:
            logger.critical("[GPU_WORKER] Critical error: %s", str(e))
            FRAME_QUEUE.task_done()


# ------------------------------------------------------------------
# FIX 3: Thread Supervisor with Auto-Restart (Production Grade)
# ------------------------------------------------------------------
def camera_supervisor(camera, runner, person_classes, tool_classes, tmr_classes, tractor_classes, max_dist):
    """Supervisor wrapper: restarts camera on crash (self-healing system)."""
    camera_code = camera.get("code", "UNKNOWN")
    restart_count = 0
    
    while True:
        try:
            logger.info("[SUPERVISOR] Starting camera: %s", camera_code)
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
            logger.info("[SUPERVISOR] Camera %s finished normally", camera_code)
            break
        except Exception as e:
            restart_count += 1
            logger.warning("[SUPERVISOR] Camera %s crashed (attempt #%d): %s", camera_code, restart_count, str(e)[:150])
            logger.info("[SUPERVISOR] Restarting in 5 seconds...")
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
            # Queue backlog monitoring
            if EVENT_QUEUE.qsize() > 2000:
                logger.warning("[EDGE] Queue backlog: %d", EVENT_QUEUE.qsize())

            resend_failed_events()

            payload = EVENT_QUEUE.get()
            try:
                r = SESSION.post(
                    f"{API_BASE.rstrip('/')}/ingest/event",
                    json=payload,
                    headers=HEADERS,
                    timeout=3,
                )

                if r.status_code != 200:
                    raise Exception(f"Non-200: {r.status_code}")
                
                # Track successful event sends
                with STATS_LOCK:
                    STATS["events_sent"] += 1

            except Exception as e:
                logger.warning("[EDGE] Event send failed, saving locally: %s", str(e)[:200])
                save_failed_event(payload)

            finally:
                EVENT_QUEUE.task_done()

        except Exception as e:
            logger.critical("[EDGE] Event sender critical loop error: %s", str(e))
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
        logger.critical("[CAMERA %s] Critical error in camera thread: %s", camera['camera_id'], str(e)[:200])
        import traceback
        logger.debug(traceback.format_exc())
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
    logger.info("[CAMERA %s] started", camera.get('code', camera_id))
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("  Stream Type: %s", stream_type)
        logger.debug("  Source: %s", video_source)
    
    # Get actual frame resolution from video
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    actual_resolution = f"{frame_w}x{frame_h}" if frame_w > 0 and frame_h > 0 else "Unknown"
    config_resolution = camera.get("resolution", "N/A")
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("  Config Resolution: %s", config_resolution)
        logger.debug("  Actual Resolution: %s", actual_resolution)

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

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("[CAMERA %s] Starting video processing: %d frames @ %.2f FPS", camera_id, total_frames, video_fps)
        logger.debug("[CAMERA %s] Processing FPS: %.2f, Frame skip: %d", camera_id, processing_fps, frame_skip_ratio)

    frame_count = 0
    processed_frame_count = 0
    start_time = time.time()
    total_processing_time = 0.0

    # Log cooldowns to prevent spam
    last_timeout_log = 0
    last_thermal_log = 0

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

            logger.warning("[CAMERA %s] Stream lost. Reconnect attempt #%d (waiting %ds)...", camera_id, reconnect_attempt, backoff_delay)
            try:
                time.sleep(backoff_delay)
                stream.release()
                stream = open_stream(camera)
                reconnect_attempt = 0  # reset after success
                continue
            except Exception as e:
                logger.warning("[CAMERA %s] Reconnect failed: %s", camera_id, str(e)[:200])
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
        with STATS_LOCK:
            STATS["frames_processed"] += 1
            CAMERA_STATS[camera_id]["frames"] += 1
        
        t0 = time.time()

        frame = cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)
        
        # -------------------------------
        # Thermal Protection
        # -------------------------------
        if collect_telemetry:
            telemetry = collect_telemetry()
            gpu_temp = telemetry.get("gpu_temp_c")

            if gpu_temp and gpu_temp > 90:
                now_ts = time.time()
                if now_ts - last_thermal_log > 10:
                    logger.critical("[THERMAL] CRITICAL GPU TEMP %sC — throttling 2s", gpu_temp)
                    last_thermal_log = now_ts
                time.sleep(2)
            elif gpu_temp and gpu_temp > 85:
                now_ts = time.time()
                if now_ts - last_thermal_log > 10:
                    logger.warning("[THERMAL] High GPU TEMP %sC — slowing down", gpu_temp)
                    last_thermal_log = now_ts
                time.sleep(0.5)

        # -------------------------------
        # Inference (Queue-based Pipeline)
        # -------------------------------
        # Non-blocking: push frame to GPU worker, continue processing
        result = {}
        done_event = threading.Event()

        def set_detections(dets):
            result["detections"] = dets
            done_event.set()

        try:
            FRAME_QUEUE.put((camera_id, frame, set_detections), block=False)
        except:
            # Drop frame if GPU overloaded (acceptable in real-time)
            detections = []
        else:
            # Wait for inference result with proper blocking (no busy-wait)
            if not done_event.wait(timeout=5.0):
                now_ts = time.time()
                if now_ts - last_timeout_log > 10:
                    logger.warning("[CAMERA %s] Inference timeout, skipping frame", camera_id)
                    last_timeout_log = now_ts
                detections = []
            else:
                detections = result.get("detections", [])

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
                logger.info("[CAMERA %s][SCRAPPING] START_CANDIDATE emitted | session_id=%s...", camera_id, state['session_id'][:8])
            except Exception as e:
                logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

        # 2. END transition: ACTIVE -> INACTIVE
        elif zone_scrapping and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("SCRAPPING", "END_CANDIDATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                logger.info("[CAMERA %s][SCRAPPING] END_CANDIDATE emitted | session_id=%s...", camera_id, session_id_short)
            except Exception as e:
                logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

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
                        if processed_frame_count % 50 == 0:  # Log every 50 frames to avoid spam
                            logger.debug("[CAMERA %s][SCRAPPING] FRAME_AGGREGATE emitted | session_id=%s...", camera_id, state['session_id'][:8])
                    except Exception as e:
                        logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
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
                logger.info("[CAMERA %s][FEEDING] START_CANDIDATE emitted | session_id=%s...", camera_id, state['session_id'][:8])
            except Exception as e:
                logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

        # 2. END transition: ACTIVE -> INACTIVE
        elif zone_feeding and sig == "END" and state["state"] == "ACTIVE":
            session_id_short = state["session_id"][:8] if state["session_id"] else "None"
            payload = build_event_payload("FEEDING", "END_CANDIDATE", camera_id, zone_feeding, detections_feed, state["session_id"])
            try:
                EVENT_QUEUE.put(payload, block=False)
                logger.info("[CAMERA %s][FEEDING] END_CANDIDATE emitted | session_id=%s...", camera_id, session_id_short)
            except Exception as e:
                logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

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
                        if processed_frame_count % 50 == 0:  # Log every 50 frames to avoid spam
                            logger.debug("[CAMERA %s][FEEDING] FRAME_AGGREGATE emitted | session_id=%s...", camera_id, state['session_id'][:8])
                    except Exception as e:
                        logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                state["last_frame_emit"] = now

        # MILKING disabled - model doesn't support it yet

    stream.release()

    # Summary
    video_duration = video_timestamp(frame_count)  # Use actual video frame count for duration
    total_elapsed = time.time() - start_time
    logger.info("="*80)
    logger.info("[CAMERA %s] Video Processing Finished", camera_id)
    logger.info("="*80)
    logger.info("Total Video Frames: %d", frame_count)
    logger.info("Processed Frames: %d", processed_frame_count)
    logger.info("Video Duration: %.2fs (at %.2f FPS)", video_duration, video_fps)
    logger.info("Total Processing Time: %.2fs", total_processing_time)
    avg_speed = processed_frame_count / total_processing_time if total_processing_time > 0 else 0.0
    logger.info("Average Processing Speed: %.2f FPS", avg_speed)
    logger.info("="*80)


# ------------------------------------------------------------------
# Performance Monitor
# ------------------------------------------------------------------
def performance_monitor():
    """
    Lightweight monitoring thread that logs system health every 5 seconds.
    Tracks: GPU throughput (FPS), event send rate, queue health.
    """
    last_frames = 0
    last_time = time.time()

    while True:
        time.sleep(5)

        now = time.time()
        elapsed = now - last_time

        with STATS_LOCK:
            total_frames = STATS["frames_processed"]
            events = STATS["events_sent"]

        delta_frames = total_frames - last_frames
        fps = delta_frames / elapsed if elapsed > 0 else 0

        logger.info(
            "[MONITOR] Total FPS: %.2f | Events Sent: %d | FrameQ: %d | EventQ: %d",
            fps,
            events,
            FRAME_QUEUE.qsize(),
            EVENT_QUEUE.qsize(),
        )

        last_frames = total_frames
        last_time = now


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

    logger.info("Edge Detector: Using device: %s", device)

    model_path = os.path.join(PROJECT_ROOT, cfg["ml_model_version"]["model_path"])

    # Create shared ModelRunner (once per device, NOT per camera)
    shared_runner = ModelRunner(model_path, device=device)
    logger.info("Model loaded once: %s", model_path)

    # Warmup TensorRT engine (avoids first-frame latency spike)
    logger.info("Warming up TensorRT engine...")
    dummy = np.zeros((640, 640, 3), dtype=np.uint8)
    for _ in range(5):
        try:
            shared_runner.infer(dummy)
        except:
            pass
    logger.info("TensorRT engine warmed up")

    # Start dedicated GPU inference worker (removes INFER_LOCK bottleneck)
    Thread(target=inference_worker, args=(shared_runner,), daemon=True).start()
    logger.info("Started GPU inference worker (queue-based pipeline)")
    logger.info("   Tip: For dairy cameras, consider imgsz=512 (30-40 percent faster than 640)")
    logger.info("   To enable: edit backend/runtime/model_loader.py, change 'imgsz': 416 to 512")

    # Start multiple async event sender threads (parallel HTTP sends)
    SENDER_THREADS = int(os.getenv("EDGE_SENDER_THREADS", "3"))
    for i in range(SENDER_THREADS):
        Thread(target=event_sender, daemon=True).start()
    logger.info("Started %d event sender threads", SENDER_THREADS)

    # Start performance monitoring thread
    Thread(target=performance_monitor, daemon=True).start()
    logger.info("Started performance monitor thread")

    # Print input video information
    logger.info("=" * 80)
    logger.info("INPUT VIDEO CONFIGURATION")
    logger.info("=" * 80)
    for camera in cfg.get("cameras", []):
        camera_id = camera["camera_id"]
        camera_code = camera.get("code", "N/A")
        stream_type = camera.get("stream_type", "AUTO")
        video_path = camera.get("video_file_path", "N/A")
        rtsp_url = camera.get("rtsp_url", "N/A")
        resolution = camera.get("resolution", "N/A")
        
        logger.info("[CAMERA] %s", camera_code)
        # logger.debug("  ID: %s", camera_id)
        # logger.debug("  Stream Type: %s", stream_type)
        logger.info("  Resolution: %s", resolution)
        if stream_type == "FILE":
            logger.info("  Video File: %s", video_path)
        elif stream_type == "RTSP":
            logger.info("  RTSP URL: %s", rtsp_url)
        else:
            logger.info("  Video Path/URL: %s", video_path if video_path != 'N/A' else rtsp_url)
    logger.info("=" * 80)

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
