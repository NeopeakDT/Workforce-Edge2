#!/usr/bin/env python3
"""
Edge2 device
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
from collections import defaultdict, deque
from datetime import datetime, timezone
from queue import Queue
from threading import Thread
import threading
import subprocess
from uuid import uuid4, UUID, uuid5

from dotenv import load_dotenv
from config.local_cache import load_config
from config.model_classes import WF_CLASSES, MILKING_CLASSES
from utils.camera_routing import is_milking_camera
from runtime.model_loader import create_edge_runners, batch_size_for
from runtime.temporal_smoother import TemporalSmoother
from runtime.video_stream import open_stream, _validate_gstreamer
from posture.posture_detector import PostureDetector
from posture.posture_scheduler import PostureScheduler
from posture.milking_activity import set_milking_camera_active
from posture.posture_db import PostureDB

# GLOBAL RTSP START LOCK (prevents NVR overload)
RTSP_START_LOCK = threading.Lock()

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
SCRAP_ACTIVE_BUFFER_SEC = float(os.getenv("SCRAP_ACTIVE_BUFFER_SEC", "20"))
FEED_ACTIVE_BUFFER_SEC = float(os.getenv("FEED_ACTIVE_BUFFER_SEC", "25"))
# For long tools, use the lower segment as proxy for the tool head.
# 0.85 means "point at 85% bbox height from top" (near bottom tip).
SCRAP_TOOL_HEAD_Y_RATIO = float(os.getenv("SCRAP_TOOL_HEAD_Y_RATIO", "0.7"))

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

# Route posture package logs (posture.*) through the same handler.
# INFO keeps only [POSTURE][1MIN], [POSTURE][10MIN], [POSTURE][DB];
# per-detection ROI/RAW/SCHEDULER/SNAPSHOT stay at DEBUG.
posture_logger = logging.getLogger("posture")
posture_logger.setLevel(getattr(logging, LOG_LEVEL, logging.INFO))
posture_logger.addHandler(queue_handler)
posture_logger.propagate = False

# Start listener thread (daemon, non-blocking writes)
log_listener = QueueListener(log_queue, console_handler, respect_handler_level=True)
log_listener.start()

# ------------------------------------------------------------------
# GPU Inference Pipeline (Queue-based, removes INFER_LOCK)
# ------------------------------------------------------------------
FRAME_QUEUE_MAXSIZE = int(os.getenv("EDGE_FRAME_QUEUE_MAXSIZE", "10"))
FRAME_QUEUE = Queue(maxsize=FRAME_QUEUE_MAXSIZE)
INFERENCE_FRAME_SIZE = (640, 640)

# ------------------------------------------------------------------
# Async event queue (CRITICAL for performance)
# ------------------------------------------------------------------
EVENT_QUEUE_MAXSIZE = int(os.getenv("EDGE_EVENT_QUEUE_MAXSIZE", "10000"))
EVENT_QUEUE = Queue(maxsize=EVENT_QUEUE_MAXSIZE)

# ------------------------------------------------------------------
# Performance Monitoring
# ------------------------------------------------------------------
STATS = {
    "frames_processed": 0,
    "events_sent": 0,
}
CAMERA_STATS = defaultdict(
    lambda: {
        "frames": 0,
        "last_frames": 0,
        "last_seen": None,
        "fps": 0.0,
    }
)
STATS_LOCK = threading.Lock()

# ------------------------------------------------------------------
# Video Configuration
# ------------------------------------------------------------------
PROCESSING_FPS = float(os.getenv("EDGE_PROCESSING_FPS", "5.0"))
EDGE_MODE = os.getenv("EDGE_MODE", "LIVE")  # LIVE or BATCH
WATCHDOG_FILE_PATH = os.getenv("EDGE_WATCHDOG_FILE", "/tmp/workforce_edge_alive")
PROCESS_STARTED_AT = time.time()
MILKING_MODEL_ENABLED = True
MILKING_MODEL_PATH = "models/WF_Milking_v1.1_best.pt"
POSTURE_MODEL_ENABLED = True
POSTURE_MODEL_PATH = "models/cow_posture_v1.1_best.pt"
WF_MIN_OVERLAP_RATIO = 0.2
MILKING_MIN_OVERLAP_RATIO = 0.01
MILKING_CLUSTER_MEMORY_SEC = 20
MILKING_INTERSECT_PAD = 10
SESSION_BUCKET_SEC = 300
# UUID namespaces: stable 5-min bucket keys encoded as RFC-4122 UUIDs (ingest requires UUID).
WF_SESSION_NAMESPACE = UUID("a3f2c8e1-7b4d-4e9f-8c2a-1d5e6f7a8b9c")
MILKING_SESSION_NAMESPACE = UUID("b4e3d9f2-8c5e-41a0-9d3b-2e6f7a8b9c0d")
MILKING_CAMERA_STATE = {}

# ------------------------------------------------------------------
# Utils
# ------------------------------------------------------------------
def update_watchdog_heartbeat(extra=None):
    try:
        payload = {
            "ts": time.time(),
            "pid": os.getpid(),
        }
        if extra:
            payload.update(extra)

        tmp_path = f"{WATCHDOG_FILE_PATH}.tmp"
        with open(tmp_path, "w", encoding="utf-8") as handle:
            json.dump(payload, handle)
        os.replace(tmp_path, WATCHDOG_FILE_PATH)
    except Exception:
        pass


def build_watchdog_payload(state="running", camera_id=None, last_frame=None):
    with STATS_LOCK:
        total_frames = STATS["frames_processed"]
        camera_last_seen = {
            cam_id: stats.get("last_seen")
            for cam_id, stats in CAMERA_STATS.items()
        }

    payload = {
        "state": state,
        "process_started_at": PROCESS_STARTED_AT,
        "frame_queue": FRAME_QUEUE.qsize(),
        "event_queue": EVENT_QUEUE.qsize(),
        "total_frames": total_frames,
        "camera_last_seen": camera_last_seen,
    }

    if camera_id is not None:
        payload["camera_id"] = camera_id
    if last_frame is not None:
        payload["last_frame"] = last_frame

    return payload


def drop_oldest_frame_for_camera(camera_id):
    """Drop the oldest queued frame for this camera without disturbing others."""
    with FRAME_QUEUE.mutex:
        for idx, item in enumerate(FRAME_QUEUE.queue):
            if item and item[0] == camera_id:
                del FRAME_QUEUE.queue[idx]
                FRAME_QUEUE.unfinished_tasks = max(0, FRAME_QUEUE.unfinished_tasks - 1)
                if FRAME_QUEUE.unfinished_tasks == 0:
                    FRAME_QUEUE.all_tasks_done.notify_all()
                FRAME_QUEUE.not_full.notify()
                return True
    return False


def get_fps_ffprobe(path):
    """Robust FPS extraction using ffprobe (handles CCTV/VFR better)."""
    try:
        result = subprocess.run(
            [
                "ffprobe", "-v", "error",
                "-select_streams", "v:0",
                "-show_entries", "stream=avg_frame_rate,r_frame_rate",
                "-of", "json",
                path,
            ],
            capture_output=True,
            text=True,
            timeout=5,
        )

        data = json.loads(result.stdout)
        stream = data["streams"][0]

        def parse(rate):
            num, den = map(float, rate.split("/"))
            return num / den if den != 0 else 0

        # PRIORITY: avg_frame_rate (critical for DVR videos)
        if "avg_frame_rate" in stream:
            fps = parse(stream["avg_frame_rate"])
            if fps > 1:
                return fps

        # fallback
        if "r_frame_rate" in stream:
            fps = parse(stream["r_frame_rate"])
            if fps > 1:
                return fps
    except Exception:
        pass
    return None


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

    # SCRAPPING sessions often have empty ROI-filtered detections while still ACTIVE; avoid
    # all-empty `objects` on FRAME ticks so ingest/analytics keep continuity with the session.
    metadata = None
    if (
        activity == "SCRAPPING"
        and event_type == "FRAME_AGGREGATE"
        and not objects_dict
    ):
        metadata = {"scrapping_sparse_frame": True}
        objects_dict["edge_continuity_tick"] = [{"id": "scrapping_active_sparse"}]

    if event_type == "FRAME_AGGREGATE":
        confidence = sum(confidences) / len(confidences) if confidences else 0.5
    elif event_type == "END_CANDIDATE":
        confidence = 0.7
    elif event_type == "START_CANDIDATE":
        confidence = sum(confidences) / len(confidences) if confidences else 0.8
    else:
        confidence = 0.7

    now_utc = datetime.now(timezone.utc)
    event_time = now_utc.isoformat().replace("+00:00", "Z")

    # Generate unique event_id and use it as idempotency_key
    event_id = str(uuid4())

    out = {
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
    if metadata is not None:
        out["metadata"] = metadata
    return out


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
def inference_worker(runners: dict):
    """
    Batched GPU worker for better utilization.
    Routes by model_type through the runners registry (WF→WORKFORCE, MILKING, …).
    """
    deferred_items = deque()
    while True:
        batch = []
        callbacks = []
        model_type = None
        processed_items_count = 0
        actual_batch = 0

        try:
            if deferred_items:
                item = deferred_items.popleft()
            else:
                item = FRAME_QUEUE.get(timeout=1.0)
        except queue.Empty:
            update_watchdog_heartbeat(
                {
                    "state": "idle",
                    "frame_queue": FRAME_QUEUE.qsize(),
                }
            )
            continue
        except Exception as e:
            logger.error("[GPU_WORKER] Queue wait error: %s", str(e))
            update_watchdog_heartbeat({"state": "queue_error"})
            continue

        try:
            if item is None:
                FRAME_QUEUE.task_done()
                break

            camera_id, frame, model_type, callback = item
            batch.append(frame)
            callbacks.append(callback)
            processed_items_count += 1

            runner_key = "WORKFORCE" if model_type == "WF" else model_type
            required_batch = batch_size_for(runner_key)

            # Allow micro-wait window to fill batch
            batch_deadline = time.time() + 0.003  # 3ms accumulation window

            while len(batch) < required_batch:
                try:
                    timeout = max(0, batch_deadline - time.time())
                    if timeout <= 0:
                        break

                    next_item = FRAME_QUEUE.get(timeout=timeout)
                except Exception:
                    break

                if next_item is None:
                    deferred_items.append(next_item)
                    break

                cid, frm, next_model_type, cb = next_item
                if next_model_type != model_type:
                    deferred_items.append(next_item)
                    break

                batch.append(frm)
                callbacks.append(cb)
                processed_items_count += 1

        except Exception as e:
            logger.error("[GPU_WORKER] Batch build error: %s", str(e))
            continue

        try:
            actual_batch = len(batch)

            # TensorRT static batch handling (WF=2, milking=1).
            runner_key = "WORKFORCE" if model_type == "WF" else model_type
            required_batch = batch_size_for(runner_key)
            if actual_batch < required_batch:
                while len(batch) < required_batch:
                    batch.append(batch[-1])
                    callbacks.append(None)

            runner = runners.get(runner_key)
            if runner is None:
                raise RuntimeError(
                    f"{model_type} inference requested but {runner_key} runner not loaded"
                )
            results = runner.infer(batch)

            for dets, cb in zip(results[:actual_batch], callbacks[:actual_batch]):
                if cb:
                    cb(dets)

        except Exception as e:
            logger.error("[GPU_WORKER] Batch inference error: %s", str(e))
            for cb in callbacks[:actual_batch]:
                if cb:
                    cb([])

        finally:
            update_watchdog_heartbeat(
                {
                    "state": "running",
                    "batch_size": actual_batch,
                    "model_type": model_type,
                    "frame_queue": FRAME_QUEUE.qsize(),
                }
            )
            for _ in range(processed_items_count):
                FRAME_QUEUE.task_done()


# ------------------------------------------------------------------
# FIX 3: Thread Supervisor with Auto-Restart (Production Grade)
# ------------------------------------------------------------------
def log_stream_resolution(camera, cap, frame, camera_name):
    """Log configured vs OpenCV-decoded resolution; warn on mismatch."""
    frame_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    frame_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    if (frame_w <= 0 or frame_h <= 0) and frame is not None:
        frame_h, frame_w = frame.shape[:2]
    actual_resolution = f"{frame_w} x {frame_h}" if frame_w > 0 and frame_h > 0 else "Unknown"
    config_resolution = camera.get("resolution", "N/A")
    logger.info("  Configured resolution: %s", config_resolution)
    logger.info("  Actual decoded resolution: %s", actual_resolution)
    config_norm = str(config_resolution).lower().replace(" ", "").replace("×", "x")
    actual_norm = f"{frame_w}x{frame_h}" if frame_w > 0 and frame_h > 0 else ""
    if config_norm not in ("n/a", "") and actual_norm and config_norm != actual_norm:
        logger.warning(
            "[CAMERA %s] Resolution mismatch — configured=%s decoded=%s",
            camera_name,
            config_resolution,
            actual_resolution,
        )


def camera_supervisor(
    camera,
    person_classes,
    tool_classes,
    tmr_classes,
    tractor_classes,
    max_dist,
    milking_model_enabled,
    posture_scheduler=None,
):
    """Supervisor wrapper: restarts camera on crash (self-healing system)."""
    camera_code = camera.get("code", "UNKNOWN")
    restart_count = 0
    
    while True:
        try:
            logger.info("[SUPERVISOR] Starting camera: %s", camera_code)
            process_camera(
                camera,
                person_classes,
                tool_classes,
                tmr_classes,
                tractor_classes,
                max_dist,
                milking_model_enabled,
                posture_scheduler,
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
    last_retry = 0.0

    while True:
        try:
            # Queue backlog monitoring
            if EVENT_QUEUE.qsize() > 2000:
                logger.warning("[EDGE] Queue backlog: %d", EVENT_QUEUE.qsize())

            now_ts = time.time()
            if now_ts - last_retry > 30:
                resend_failed_events()
                last_retry = now_ts

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


def bbox_intersects(boxA, boxB, pad=10):
    xA = max(boxA[0], boxB[0] - pad)
    yA = max(boxA[1], boxB[1] - pad)
    xB = min(boxA[2], boxB[2] + pad)
    yB = min(boxA[3], boxB[3] + pad)
    return (xB > xA) and (yB > yA)


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


def bucket_session_id(
    pipeline: str,
    camera_id: str,
    activity_type: str,
    activity_start_ts: float,
) -> str:
    """
    Camera + activity + 5-minute bucket → stable session UUID (reconnect/replay safe).
    """
    bucket = int(activity_start_ts // SESSION_BUCKET_SEC)
    if pipeline == "milking":
        key = f"milking_{camera_id}_{bucket}"
        namespace = MILKING_SESSION_NAMESPACE
    else:
        key = f"wf_{camera_id}_{activity_type}_{bucket}"
        namespace = WF_SESSION_NAMESPACE
    return str(uuid5(namespace, key))


def detections_to_objects_all(detections):
    """Group detections by class name for camera-level milking logic."""
    objects_all = defaultdict(list)
    for det in detections or []:
        cls = str(det.get("class", "")).lower()
        bbox = det.get("bbox")
        if bbox is not None:
            objects_all[cls].append(bbox)
    return objects_all


def detect_milking_camera(camera_id, objects_all, ts):
    """
    Camera-level milking detection (cluster ∩ udder, cluster memory, udder-only fallback).
    No per-track IDs — activity continuity is per camera session.
    """
    state = MILKING_CAMERA_STATE.setdefault(
        camera_id,
        {
            "last_cluster_seen_ts": None,
            "active": False,
        },
    )

    clusters = objects_all.get("cluster_attached", [])
    udders = objects_all.get("cow_leg_udder", [])
    persons = objects_all.get("person", [])

    valid_cluster = False
    for c in clusters:
        for u in udders:
            if bbox_intersects(c, u, pad=MILKING_INTERSECT_PAD):
                valid_cluster = True
                break
        if valid_cluster:
            break

    has_udder = len(udders) > 0
    has_person = len(persons) > 0

    # -------------------------------------------------
    # Cluster memory
    # -------------------------------------------------
    if valid_cluster:
        state["last_cluster_seen_ts"] = ts

    cluster_recent = (
        state["last_cluster_seen_ts"] is not None
        and (ts - state["last_cluster_seen_ts"]) < MILKING_CLUSTER_MEMORY_SEC
    )

    # -------------------------------------------------
    # Sustained udder fallback
    # Real parlour visibility often misses cluster box
    # -------------------------------------------------
    udder_only_detected = has_udder and not has_person

    milking_detected = (
        valid_cluster
        or (has_udder and cluster_recent)
        or udder_only_detected
    )

    # logger.warning(
    #     "[MILKING GATE] "
    #     "clusters=%d udders=%d persons=%d "
    #     "valid_cluster=%s cluster_recent=%s detected=%s",
    #     len(clusters),
    #     len(udders),
    #     len(persons),
    #     valid_cluster,
    #     cluster_recent,
    #     milking_detected,
    # )

    # Temporal persistence is handled by TemporalSmoother (no second active buffer).
    state["active"] = milking_detected
    return milking_detected


def filter_detections_by_classes(detections, allowed_classes):
    allowed = {c.lower() for c in allowed_classes}
    return [
        d for d in (detections or [])
        if str(d.get("class", "")).lower() in allowed
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


def detect_cluster_udder_intersection(detections):
    clusters = [
        d for d in detections
        if d["class"].lower() == "cluster_attached"
    ]
    udders = [
        d for d in detections
        if d["class"].lower() == "cow_leg_udder"
    ]

    if not clusters or not udders:
        return False

    for cluster in clusters:
        for udder in udders:
            if bbox_intersects(cluster["bbox"], udder["bbox"], pad=MILKING_INTERSECT_PAD):
                return True
    return False




# ------------------------------------------------------------------
# Per-camera processing
# ------------------------------------------------------------------
def process_camera(
    camera,
    person_classes,
    tool_classes,
    tmr_classes,
    tractor_classes,
    max_dist,
    milking_model_enabled,
    posture_scheduler=None,
):
    # FIX 2: Wrap entire camera loop in crash guard (auto-restart safe)
    try:
        _process_camera_impl(
            camera,
            person_classes,
            tool_classes,
            tmr_classes,
            tractor_classes,
            max_dist,
            milking_model_enabled,
            posture_scheduler,
        )
    except Exception as e:
        logger.critical("[CAMERA %s] Critical error in camera thread: %s", camera['camera_id'], str(e)[:200])
        import traceback
        logger.debug(traceback.format_exc())
        # Let supervisor handle restart
        raise


def _process_camera_impl(
    camera,
    person_classes,
    tool_classes,
    tmr_classes,
    tractor_classes,
    max_dist,
    milking_model_enabled,
    posture_scheduler=None,
):
    camera_id = camera["camera_id"]
    camera_name = camera.get("code", camera_id)
    with STATS_LOCK:
        _ = CAMERA_STATS[camera_id]  # ensure camera stats entry exists

    # Per-camera motion memory for FEEDING detection
    CLASS_MOTION_MEMORY = {
        "tractor": None,
        "tmr_machine": None,
    }
    last_seen_scrap_ts = None
    last_seen_feed_ts = None

    milking_camera = is_milking_camera(camera)
    zone_scrapping = resolve_zone_id(camera, "SCRAPPING")
    zone_feeding = resolve_zone_id(camera, "FEEDING")
    zone_milking = resolve_zone_id(camera, "MILKING")

    run_wf_pipeline = not milking_camera
    run_milking_pipeline = milking_camera and milking_model_enabled and zone_milking

    if milking_camera:
        milking_smoother = TemporalSmoother(start_sec=5, end_sec=15)
        smoothers = {"MILKING": milking_smoother}
        activity_state = {
            "MILKING": {
                "state": "INACTIVE",
                "session_id": None,
                "last_frame_emit": 0.0,
            },
        }
        if not milking_model_enabled:
            logger.warning(
                "[CAMERA %s] Milking camera but milking model not loaded; MILKING disabled",
                camera_id,
            )
        elif not zone_milking:
            logger.warning(
                "[CAMERA %s] Milking camera but MILKING zone missing; MILKING disabled",
                camera_id,
            )
    else:
        wf_smoothers = {
            "SCRAPPING": TemporalSmoother(start_sec=10, end_sec=30),
            "FEEDING": TemporalSmoother(start_sec=10, end_sec=30),
        }
        smoothers = wf_smoothers
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

    logger.info(
        "[CAMERA %s] pipeline=%s wf=%s milking=%s",
        camera_id,
        "MILKING" if milking_camera else "WORKFORCE",
        run_wf_pipeline,
        run_milking_pipeline,
    )

    # --------------------------------------------------
    # Open video stream (FILE / RTSP / NVR_CHANNEL)
    # --------------------------------------------------
    logger.info("[CAMERA INIT] Starting %s", camera_name)
    stream = None
    attempt = 0
    while True:
        attempt += 1
        retry_delay = min(30, 2 ** attempt)
        try:
            # Serialize RTSP negotiation to avoid NVR burst failures.
            with RTSP_START_LOCK:
                logger.info("[CAMERA %s] Acquired RTSP start lock", camera_name)
                stream = open_stream(camera)
                time.sleep(1.5)  # allow pipeline settle
            if stream and stream.cap and stream.cap.isOpened():
                break
            logger.warning(
                "[CAMERA %s] Open stream attempt %d returned invalid handle; retrying...",
                camera_name,
                attempt,
            )
        except Exception as e:
            logger.warning(
                "[CAMERA %s] Open stream attempt %d failed: %s",
                camera_name,
                attempt,
                str(e)[:200],
            )
        logger.info("[CAMERA %s] Retry open in %ds", camera_name, retry_delay)
        time.sleep(retry_delay)

    # warmup decoder
    for _ in range(5):
        stream.read()

    # START DELAY (lets stream settle + reduces synchronized GPU burst)
    time.sleep(3)

    cap = stream.cap  # required for FPS / metadata only
    # CAP_PROP_BUFFERSIZE is not supported on OpenCV-GStreamer for Jetson.
    # Calling it can trigger "GStreamer: unhandled property" and stream churn.
    try:
        if cap.getBackendName() != "GStreamer":
            cap.set(cv2.CAP_PROP_BUFFERSIZE, 1)
    except Exception:
        pass

    ret, frame = stream.read()
    if not ret:
        logger.error(f"[CAMERA {camera_name}] STREAM OPEN FAILED")
        stream.release()
        raise RuntimeError(f"[CAMERA {camera_name}] Stream failed to open after handle creation")

    # Log video source information
    stream_type = (camera.get("stream_type") or "AUTO").upper()
    video_source = camera.get("video_file_path") or camera.get("rtsp_url") or "Unknown"
    logger.info("[CAMERA %s] started", camera_name)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("  Stream Type: %s", stream_type)
        logger.debug("  Source: %s", video_source)
    
    # Compare configured resolution (local_cache.json) vs OpenCV decoded frame size.
    log_stream_resolution(camera, cap, frame, camera_name)

    # Determine if stream is live (RTSP/NVR) or recorded (FILE)
    # Smart detection: explicit stream_type OR infer from config
    if stream_type == "FILE":
        is_live = False
    elif stream_type == "RTSP":
        is_live = True
    elif stream_type in ["NVR", "NVR_CHANNEL"]:
        is_live = True
    else:
        # AUTO: Detect from config - live if has rtsp_url or nvr_channel
        is_live = bool(camera.get("rtsp_url") or camera.get("nvr_channel"))

    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"[CAMERA {camera_id}] Total frames (OpenCV): {total_frames}")

    if is_live:
        # For live streams, FPS is irrelevant to throttling — use time-based only.
        video_fps = PROCESSING_FPS  # kept for logging only
        frame_skip_ratio = None
        processing_fps = PROCESSING_FPS
    else:
        file_path = camera.get("video_file_path")
        probed = get_fps_ffprobe(file_path) if file_path else None
        if probed and 5 <= probed <= 60:
            video_fps = probed
        else:
            fallback_fps = cap.get(cv2.CAP_PROP_FPS)
            if 5 <= fallback_fps <= 60:
                video_fps = fallback_fps
            else:
                logger.warning(f"[CAMERA {camera_id}] FPS unreliable => using default {PROCESSING_FPS}")
                video_fps = PROCESSING_FPS
        frame_skip_ratio = (
            max(1, int(video_fps / PROCESSING_FPS))
            if PROCESSING_FPS and video_fps > PROCESSING_FPS
            else 1
        )
        processing_fps = PROCESSING_FPS if frame_skip_ratio > 1 else video_fps

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug(f"[CAMERA {camera_id}] FPS detected: {video_fps:.2f} | is_live={is_live}")
    if video_fps < 5 or video_fps > 60:
        logger.warning("[CAMERA %s] FPS abnormal: %.2f", camera_id, video_fps)

    if logger.isEnabledFor(logging.DEBUG):
        logger.debug("[CAMERA %s] Starting video processing: %d frames @ %.2f FPS", camera_id, total_frames, video_fps)
        logger.debug("[CAMERA %s] Processing FPS: %.2f, Frame skip: %s", camera_id, processing_fps, "time-based (live)" if is_live else str(frame_skip_ratio))

    frame_count = 0
    PROCESS_EVERY_N_FRAMES = (
        max(1, int(video_fps / PROCESSING_FPS))
        if PROCESSING_FPS > 0 and video_fps > 0
        else 1
    )
    processed_frame_count = 0
    start_time = time.time()
    total_processing_time = 0.0

    # Log cooldowns to prevent spam
    last_timeout_log = 0
    last_thermal_log = 0

    scrap_roi_cfg = camera.get("activity_zones", {}).get("SCRAPPING", {}).get("roi")
    feed_roi_cfg = camera.get("activity_zones", {}).get("FEEDING", {}).get("roi")
    milking_roi_cfg = camera.get("activity_zones", {}).get("MILKING", {}).get("roi")
    inference_w, inference_h = INFERENCE_FRAME_SIZE
    scrap_polygon = None
    feed_polygon = None
    milking_polygon = None
    scrap_mask = None
    feed_mask = None
    milking_mask = None

    if ROI_ENABLED and scrap_roi_cfg:
        scrap_polygon = build_pixel_roi(scrap_roi_cfg, inference_w, inference_h)
        scrap_mask = np.zeros((inference_h, inference_w), dtype=np.uint8)
        cv2.fillPoly(scrap_mask, [np.array(scrap_polygon, dtype=np.int32)], 1)

    if ROI_ENABLED and feed_roi_cfg:
        feed_polygon = build_pixel_roi(feed_roi_cfg, inference_w, inference_h)
        feed_mask = np.zeros((inference_h, inference_w), dtype=np.uint8)
        cv2.fillPoly(feed_mask, [np.array(feed_polygon, dtype=np.int32)], 1)

    if ROI_ENABLED and milking_roi_cfg:
        milking_polygon = build_pixel_roi(milking_roi_cfg, inference_w, inference_h)
        milking_mask = np.zeros((inference_h, inference_w), dtype=np.uint8)
        cv2.fillPoly(milking_mask, [np.array(milking_polygon, dtype=np.int32)], 1)
    else:
        milking_polygon = None
        milking_mask = None
    milking_roi_missing_warned = False

    def video_timestamp(frame_idx):
        """Calculate actual video timestamp based on source frame index."""
        if video_fps <= 0:
            return frame_idx * (1.0 / PROCESSING_FPS)
        return frame_idx / video_fps

    # Hybrid throttling variables
    last_processed_time = 0.0
    frame_interval = 1.0 / PROCESSING_FPS if PROCESSING_FPS > 0 else 0

    # Reconnect backoff control
    reconnect_attempt = 0
    MAX_BACKOFF_SEC = 60
    thermal_check_counter = 0
    first_frame = frame

    while True:
        if first_frame is not None:
            ret, frame = True, first_frame
            first_frame = None
        else:
            try:
                ret, frame = stream.read()
            except Exception as e:
                logger.warning("[CAMERA %s] Stream read exception: %s", camera_id, str(e)[:200])
                ret, frame = False, None

        if not ret:
            # FILE streams should stop cleanly at EOF (no reconnect loop)
            if not is_live:
                logger.info("[CAMERA %s] End of file reached. Stopping FILE stream.", camera_id)
                break

            # LIVE streams: always attempt reconnect with bounded backoff.
            reconnect_attempt += 1
            backoff_delay = min(30, 2 ** reconnect_attempt)
            logger.warning(
                "[CAMERA %s] Stream lost. Reconnect attempt #%d (waiting %ds)...",
                camera_id,
                reconnect_attempt,
                backoff_delay,
            )
            try:
                time.sleep(backoff_delay)
                try:
                    if stream:
                        stream.release()
                        time.sleep(2.0)  # allow NVDEC cleanup
                except Exception:
                    pass

                with RTSP_START_LOCK:
                    stream = open_stream(camera)
                cap = stream.cap
                ret, reconnect_frame = stream.read()
                if ret:
                    log_stream_resolution(camera, cap, reconnect_frame, camera_name)
                logger.info("[CAMERA %s] Stream reconnected successfully", camera_id)
                reconnect_attempt = 0  # reset after success
                first_frame = reconnect_frame if ret else None
                continue
            except Exception as e:
                logger.warning("[CAMERA %s] Reconnect failed: %s", camera_id, str(e)[:200])
                continue

        frame_seen_at = time.time()
        update_watchdog_heartbeat(
            build_watchdog_payload(
                state="running",
                camera_id=camera_id,
                last_frame=frame_seen_at,
            )
        )

        frame_count += 1

        if frame_count % PROCESS_EVERY_N_FRAMES != 0:
            continue

        # Hard FPS cap to stabilize GPU load (in addition to deterministic frame gating).
        now_time = time.time()
        if now_time - last_processed_time < frame_interval:
            continue
        last_processed_time = now_time

        posture_frame = None
        if (
            posture_scheduler is not None
            and "POSTURE" in camera.get("activity_zones", {})
        ):
            # Posture ROIs are calibrated on native stream resolution (e.g. 1280x720).
            # Workforce activities use the 640x640 inference frame below.
            posture_frame = frame

        frame = cv2.resize(frame, INFERENCE_FRAME_SIZE)

        # Deterministic GPU control is handled by:
        # 1) frame_count % PROCESS_EVERY_N_FRAMES
        # 2) hard FPS cap (frame_interval) above

        processed_frame_count += 1
        with STATS_LOCK:
            STATS["frames_processed"] += 1
            CAMERA_STATS[camera_id]["frames"] += 1
            CAMERA_STATS[camera_id]["last_seen"] = frame_seen_at
        
        # Start full frame timing (include inference)
        t0 = time.time()
        
        # -------------------------------
        # Thermal Protection (throttled)
        # -------------------------------
        thermal_check_counter += 1

        if collect_telemetry and thermal_check_counter % 30 == 0:
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
        activity_result = {}
        milking_result = {}
        activity_done = threading.Event()
        milking_done = threading.Event()

        def set_activity_detections(dets):
            activity_result["detections"] = dets
            activity_done.set()

        def set_milking_detections(dets):
            milking_result["detections"] = dets
            milking_done.set()

        def enqueue_inference(model_type, callback):
            if FRAME_QUEUE.full():
                dropped = drop_oldest_frame_for_camera(camera_id)
                if dropped:
                    logger.debug(
                        "[CAMERA %s] GPU queue full — dropped oldest frame for camera",
                        camera_id,
                    )
                else:
                    logger.debug(
                        "[CAMERA %s] GPU queue full — no evictable frame for camera",
                        camera_id,
                    )
                    return False
            try:
                FRAME_QUEUE.put((camera_id, frame, model_type, callback), block=False)
                return True
            except queue.Full:
                logger.debug("[CAMERA %s] GPU queue full — dropping frame", camera_id)
                return False

        detections_wf = []
        detections_milking_only = []

        if run_wf_pipeline:
            wf_queued = enqueue_inference("WF", set_activity_detections)
            if wf_queued:
                if not activity_done.wait(timeout=0.4):
                    now_ts = time.time()
                    if now_ts - last_timeout_log > 10:
                        logger.warning(
                            "[CAMERA %s] WF inference timeout, skipping frame",
                            camera_id,
                        )
                        last_timeout_log = now_ts
                else:
                    detections_wf = filter_detections_by_classes(
                        activity_result.get("detections", []),
                        WF_CLASSES,
                    )

        if run_milking_pipeline:
            milking_queued = enqueue_inference("MILKING", set_milking_detections)
            if milking_queued:
                if not milking_done.wait(timeout=0.4):
                    now_ts = time.time()
                    if now_ts - last_timeout_log > 10:
                        logger.warning(
                            "[CAMERA %s] Milking inference timeout, skipping frame",
                            camera_id,
                        )
                        last_timeout_log = now_ts
                else:
                    raw_milking = milking_result.get("detections", [])
                    # logger.warning(
                    #     "[MILKING RAW] %s",
                    #     [
                    #         (d.get("class"), round(d.get("confidence", 0), 2))
                    #         for d in raw_milking
                    #     ],
                    # )
                    for det in raw_milking:
                        det["class"] = str(det.get("class", "")).lower()
                    detections_milking_only = filter_detections_by_classes(
                        raw_milking,
                        MILKING_CLASSES,
                    )

        detections_scrap = []
        detections_feed = []
        detections_milking = []

        if run_wf_pipeline:
            if ROI_ENABLED and scrap_roi_cfg:
                detections_scrap = filter_by_roi(
                    detections_wf,
                    scrap_polygon,
                    frame.shape,
                    min_overlap_ratio=WF_MIN_OVERLAP_RATIO,
                    roi_mask=scrap_mask,
                )
            else:
                detections_scrap = detections_wf

            if ROI_ENABLED and feed_roi_cfg:
                detections_feed = filter_by_roi(
                    detections_wf,
                    feed_polygon,
                    frame.shape,
                    min_overlap_ratio=WF_MIN_OVERLAP_RATIO,
                    roi_mask=feed_mask,
                )
            else:
                detections_feed = detections_wf

        if run_milking_pipeline:
            if ROI_ENABLED and milking_polygon is not None:
                detections_milking = filter_by_roi(
                    detections_milking_only,
                    milking_polygon,
                    frame.shape,
                    min_overlap_ratio=MILKING_MIN_OVERLAP_RATIO,
                    roi_mask=milking_mask,
                )
            elif zone_milking and milking_polygon is None:
                if not milking_roi_missing_warned:
                    logger.warning(
                        "[CAMERA %s] MILKING zone exists but ROI missing — disabling milking",
                        camera_id,
                    )
                    milking_roi_missing_warned = True
            else:
                detections_milking = detections_milking_only

        frame_processing_time = time.time() - t0
        total_processing_time += frame_processing_time

        # -------------------------------------------------
        # POSTURE scheduling
        # -------------------------------------------------

        if (
            posture_scheduler is not None
            and "POSTURE" in camera.get("activity_zones", {})
        ):
            try:
                posture_scheduler.process_frame(
                    frame=posture_frame,
                    camera=camera,
                )
            except Exception:
                logger.exception(
                    "[CAMERA %s] Posture scheduler failed",
                    camera_id,
                )
        
        # Use appropriate timestamp based on stream type
        # Live streams: wall-clock time (critical for real-time motion detection)
        # Recorded : video timestamp (prevents velocity miscalculation when processing faster than real-time)
        if is_live:
            ts = time.time()
        else:
            ts = video_timestamp(frame_count)

        now = time.time()

        # ------------------------------------------------------------------
        # SCRAPPING / FEEDING — workforce pipeline only (isolated from milking)
        # ------------------------------------------------------------------
        if run_wf_pipeline:
            scrapping_detected = detect_scrapping(
                detections_scrap, person_classes, tool_classes, max_dist
            )
            if scrapping_detected:
                last_seen_scrap_ts = ts
            scrapping = (
                last_seen_scrap_ts is not None
                and (ts - last_seen_scrap_ts) < SCRAP_ACTIVE_BUFFER_SEC
            )
            sig = smoothers["SCRAPPING"].update(scrapping, ts)
            state = activity_state["SCRAPPING"]

            # 1. START transition: INACTIVE -> ACTIVE
            if zone_scrapping and sig == "START" and state["state"] == "INACTIVE":
                state["state"] = "ACTIVE"
                activity_start_ts = ts
                state["activity_start_ts"] = activity_start_ts
                state["session_id"] = bucket_session_id(
                    "wf", camera_id, "SCRAPPING", activity_start_ts
                )
                state["last_frame_emit"] = 0.0

                payload = build_event_payload("SCRAPPING", "START_CANDIDATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
                try:
                    if payload["event_type"] in ("START_CANDIDATE", "END_CANDIDATE"):
                        logger.info(
                            "[EVENT] %s | %s | cam=%s | conf=%.2f",
                            payload["activity_type"],
                            payload["event_type"],
                            payload["camera_id"],
                            payload["confidence"],
                        )
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

            # 2. END transition: ACTIVE -> INACTIVE
            elif zone_scrapping and sig == "END" and state["state"] == "ACTIVE":
                payload = build_event_payload("SCRAPPING", "END_CANDIDATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
                try:
                    if payload["event_type"] in ("START_CANDIDATE", "END_CANDIDATE"):
                        logger.info(
                            "[EVENT] %s | %s | cam=%s | conf=%.2f",
                            payload["activity_type"],
                            payload["event_type"],
                            payload["camera_id"],
                            payload["confidence"],
                        )
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

                state["state"] = "INACTIVE"
                state["session_id"] = None
                state["last_frame_emit"] = 0.0

            # 3. FRAME_AGGREGATE
            elif state["state"] == "ACTIVE":
                if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                    if zone_scrapping:
                        payload = build_event_payload("SCRAPPING", "FRAME_AGGREGATE", camera_id, zone_scrapping, detections_scrap, state["session_id"])
                        try:
                            if EVENT_QUEUE.full():
                                logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                            else:
                                EVENT_QUEUE.put(payload, block=False)
                                state["last_frame_emit"] = now
                        except Exception as e:
                            logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                    else:
                        state["last_frame_emit"] = now

            # FEEDING — motion-based detection
            feeding_detected = False

            for cls in ["tractor", "tmr_machine"]:
                boxes = [
                    d["bbox"]
                    for d in detections_feed
                    if d["class"].lower() == cls
                ]

                if not boxes:
                    CLASS_MOTION_MEMORY[cls] = None
                    continue

                box = max(boxes, key=lambda b: (b[2]-b[0])*(b[3]-b[1]))
                cx = (box[0] + box[2]) / 2
                cy = (box[1] + box[3]) / 2
                area = (box[2] - box[0]) * (box[3] - box[1])
                mem = CLASS_MOTION_MEMORY[cls]

                if mem is None:
                    CLASS_MOTION_MEMORY[cls] = {
                        "prev_center": (cx, cy),
                        "prev_area": area,
                        "prev_ts": ts,
                        "moving_since": None,
                    }
                    continue

                dt = min(max(ts - mem["prev_ts"], 1e-6), 1.0)
                dx = cx - mem["prev_center"][0]
                dy = cy - mem["prev_center"][1]
                velocity = (dx*dx + dy*dy)**0.5 / dt
                area_delta = abs(area - mem["prev_area"])
                area_velocity = area_delta / dt
                mem["prev_center"] = (cx, cy)
                mem["prev_area"] = area
                mem["prev_ts"] = ts
                translation_motion = velocity > 3
                area_motion = area_velocity > 1000

                if translation_motion or area_motion:
                    if mem["moving_since"] is None:
                        mem["moving_since"] = ts
                    elif ts - mem["moving_since"] >= 3:
                        feeding_detected = True
                else:
                    mem["moving_since"] = None

            if feeding_detected:
                last_seen_feed_ts = ts
            feeding = (
                last_seen_feed_ts is not None
                and (ts - last_seen_feed_ts) < FEED_ACTIVE_BUFFER_SEC
            )

            sig = smoothers["FEEDING"].update(feeding, ts)
            state = activity_state["FEEDING"]

            if zone_feeding and sig == "START" and state["state"] == "INACTIVE":
                state["state"] = "ACTIVE"
                activity_start_ts = ts
                state["activity_start_ts"] = activity_start_ts
                state["session_id"] = bucket_session_id(
                    "wf", camera_id, "FEEDING", activity_start_ts
                )
                state["last_frame_emit"] = 0.0
                payload = build_event_payload("FEEDING", "START_CANDIDATE", camera_id, zone_feeding, detections_feed, state["session_id"])
                try:
                    if payload["event_type"] in ("START_CANDIDATE", "END_CANDIDATE"):
                        logger.info(
                            "[EVENT] %s | %s | cam=%s | conf=%.2f",
                            payload["activity_type"],
                            payload["event_type"],
                            payload["camera_id"],
                            payload["confidence"],
                        )
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

            elif zone_feeding and sig == "END" and state["state"] == "ACTIVE":
                payload = build_event_payload("FEEDING", "END_CANDIDATE", camera_id, zone_feeding, detections_feed, state["session_id"])
                try:
                    if payload["event_type"] in ("START_CANDIDATE", "END_CANDIDATE"):
                        logger.info(
                            "[EVENT] %s | %s | cam=%s | conf=%.2f",
                            payload["activity_type"],
                            payload["event_type"],
                            payload["camera_id"],
                            payload["confidence"],
                        )
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                state["state"] = "INACTIVE"
                state["session_id"] = None
                state["last_frame_emit"] = 0.0

            elif state["state"] == "ACTIVE":
                if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                    if zone_feeding:
                        payload = build_event_payload("FEEDING", "FRAME_AGGREGATE", camera_id, zone_feeding, detections_feed, state["session_id"])
                        try:
                            if EVENT_QUEUE.full():
                                logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                            else:
                                EVENT_QUEUE.put(payload, block=False)
                        except Exception as e:
                            logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                    state["last_frame_emit"] = now

        # ------------------------------------------------------------------
        # MILKING — milking pipeline only (isolated from workforce)
        # ------------------------------------------------------------------
        if run_milking_pipeline:
            objects_all = detections_to_objects_all(detections_milking)
            milking_signal = detect_milking_camera(camera_id, objects_all, ts)
            sig = smoothers["MILKING"].update(milking_signal, ts)
            state = activity_state["MILKING"]

            if zone_milking and sig == "START" and state["state"] == "INACTIVE":
                state["state"] = "ACTIVE"
                set_milking_camera_active(camera_id, True)
                activity_start_ts = ts
                state["activity_start_ts"] = activity_start_ts
                state["session_id"] = bucket_session_id(
                    "milking", camera_id, "MILKING", activity_start_ts
                )
                state["last_frame_emit"] = 0.0
                payload = build_event_payload(
                    "MILKING",
                    "START_CANDIDATE",
                    camera_id,
                    zone_milking,
                    detections_milking,
                    state["session_id"],
                )
                try:
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])

            elif zone_milking and sig == "END" and state["state"] == "ACTIVE":
                payload = build_event_payload(
                    "MILKING",
                    "END_CANDIDATE",
                    camera_id,
                    zone_milking,
                    detections_milking,
                    state["session_id"],
                )
                try:
                    if EVENT_QUEUE.full():
                        logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                    else:
                        EVENT_QUEUE.put(payload, block=False)
                except Exception as e:
                    logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                state["state"] = "INACTIVE"
                set_milking_camera_active(camera_id, False)
                state["session_id"] = None
                state["last_frame_emit"] = 0.0

            elif state["state"] == "ACTIVE":
                if now - state["last_frame_emit"] >= FRAME_AGGREGATE_INTERVAL_SEC:
                    set_milking_camera_active(camera_id, True)
                    payload = build_event_payload(
                        "MILKING",
                        "FRAME_AGGREGATE",
                        camera_id,
                        zone_milking,
                        detections_milking,
                        state["session_id"],
                    )
                    try:
                        if EVENT_QUEUE.full():
                            logger.warning("[EDGE] EVENT_QUEUE FULL — dropping event")
                        else:
                            EVENT_QUEUE.put(payload, block=False)
                    except Exception as e:
                        logger.warning("[EDGE] Event queue full. Event dropped: %s", str(e)[:100])
                    state["last_frame_emit"] = now

    stream.release()

    # Summary
    if not is_live and video_fps > 0:
        video_duration = total_frames / video_fps
    else:
        video_duration = 0
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
    Lightweight monitoring thread that logs system health every 15 seconds (production filter).
    Tracks: GPU throughput (FPS), event send rate, queue health.
    """
    last_frames = 0
    last_time = time.time()

    while True:
        time.sleep(15)

        now = time.time()
        elapsed = now - last_time

        with STATS_LOCK:
            total_frames = STATS["frames_processed"]
            events = STATS["events_sent"]
            camera_snapshot = []
            for cam_id, stats in CAMERA_STATS.items():
                frames = stats.get("frames", 0)
                last_frames_cam = stats.get("last_frames", 0)
                cam_fps = (frames - last_frames_cam) / elapsed if elapsed > 0 else 0.0
                stats["fps"] = cam_fps
                stats["last_frames"] = frames
                camera_snapshot.append(
                    (cam_id, cam_fps, stats.get("last_seen"))
                )

        delta_frames = total_frames - last_frames
        fps = delta_frames / elapsed if elapsed > 0 else 0
        frame_qsize = FRAME_QUEUE.qsize()
        event_qsize = EVENT_QUEUE.qsize()

        logger.info(
            "[MONITOR] Total FPS: %.2f | Events Sent: %d | FrameQ: %d/%d | EventQ: %d/%d",
            fps,
            events,
            frame_qsize,
            FRAME_QUEUE_MAXSIZE,
            event_qsize,
            EVENT_QUEUE_MAXSIZE,
        )

        if frame_qsize >= max(1, int(FRAME_QUEUE_MAXSIZE * 0.8)):
            logger.warning(
                "[MONITOR] Frame queue pressure high: %d/%d",
                frame_qsize,
                FRAME_QUEUE_MAXSIZE,
            )

        for cam_id, cam_fps, last_seen in camera_snapshot:
            if last_seen is None:
                last_seen_ago = -1.0
            else:
                last_seen_ago = max(0.0, now - last_seen)
            logger.info(
                "[CAM_HEALTH] %s | FPS=%.2f | last_seen=%.1fs",
                cam_id,
                cam_fps,
                last_seen_ago,
            )

        last_frames = total_frames
        last_time = now


# ------------------------------------------------------------------
# Main
# ------------------------------------------------------------------
def main():
    cfg = load_config()

    # Validate critical dependencies once (avoids repeated checks per camera)
    _validate_gstreamer()

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

    wf_model_path = os.path.join(PROJECT_ROOT, cfg["ml_model_version"]["model_path"])
    milking_model_path = os.path.join(PROJECT_ROOT, MILKING_MODEL_PATH)
    posture_model_path = os.path.join(
        PROJECT_ROOT,
        POSTURE_MODEL_PATH,
    )
    if not os.path.exists(wf_model_path):
        raise FileNotFoundError(
            f"Workforce model not found: {wf_model_path} "
            f"(check ml_model_version.model_path in local_cache.json)"
        )
    if not os.path.exists(milking_model_path):
        fallback_milking = os.path.join(PROJECT_ROOT, "models/WF_Milking_v1.1_best.pt")
        if os.path.exists(fallback_milking):
            milking_model_path = fallback_milking
            logger.info("Using fallback milking engine: %s", milking_model_path)

    milking_cameras = [c for c in cfg.get("cameras", []) if is_milking_camera(c)]

    posture_cameras = [
        c
        for c in cfg.get("cameras", [])
        if "POSTURE" in c.get("activity_zones", {})
    ]

    optional_models = {}
    if MILKING_MODEL_ENABLED and milking_cameras:
        optional_models["MILKING"] = milking_model_path

    if POSTURE_MODEL_ENABLED and posture_cameras:

        if os.path.exists(posture_model_path):

            optional_models["POSTURE"] = posture_model_path

        else:

            logger.warning(
                "Posture model not found: %s",
                posture_model_path,
            )

    runners = create_edge_runners(
        wf_model_path,
        device=device,
        optional_models=optional_models,
    )
    logger.info("Workforce model loaded: %s", wf_model_path)
    logger.info("Edge runners loaded: %s", list(runners.keys()))

    milking_model_enabled = "MILKING" in runners
    if MILKING_MODEL_ENABLED and milking_cameras and not milking_model_enabled:
        logger.warning(
            "MILKING model not found at %s; MILKING inference disabled",
            milking_model_path,
        )
    elif milking_model_enabled:
        logger.info(
            "Milking model loaded: %s (%d milking camera(s))",
            milking_model_path,
            len(milking_cameras),
        )

    posture_model_enabled = "POSTURE" in runners

    if POSTURE_MODEL_ENABLED and posture_cameras:

        if posture_model_enabled:

            logger.info(
                "Posture model loaded: %s (%d posture camera(s))",
                posture_model_path,
                len(posture_cameras),
            )

        else:

            logger.warning(
                "POSTURE model not loaded."
            )

    # -------------------------------------------------
    # Posture detector
    # -------------------------------------------------

    posture_detector = None

    if posture_model_enabled:

        posture_detector = PostureDetector(
            model_runner=runners["POSTURE"],
            herd_size=33,          # TODO: load from farm config later
        )

    # -------------------------------------------------
    # Posture scheduler
    # -------------------------------------------------

    posture_scheduler = None

    if posture_detector:

        posture_scheduler = PostureScheduler(
            runtime_config=cfg,
            detector=posture_detector,
            db=PostureDB(),
        )

        logger.info(
            "[POSTURE] Enabled (%d posture cameras, sample=%ds, flush=%dmin)",
            len(posture_cameras),
            posture_scheduler.SAMPLE_INTERVAL_SECONDS,
            posture_scheduler.DB_WRITE_INTERVAL_SECONDS // 60,
        )

    # Warmup TensorRT engine (avoids first-frame latency spike)
    logger.info("Warming up TensorRT engine...")
    dummy = np.zeros((512, 512, 3), dtype=np.uint8)
    for _ in range(5):
        try:
            runners["WORKFORCE"].infer([dummy, dummy])
        except:
            pass
    if milking_model_enabled:
        for _ in range(3):
            try:
                runners["MILKING"].infer([dummy, dummy])
            except:
                pass
    if posture_model_enabled:

        for _ in range(3):

            try:

                runners["POSTURE"].infer(dummy)

            except Exception:
                pass
    logger.info("TensorRT engine warmed up")

    # Start dedicated single GPU inference worker with multi-model routing
    Thread(
        target=inference_worker,
        args=(runners,),
        daemon=True,
    ).start()
    logger.info("Started GPU inference worker (multi-model queue routing)")
    logger.info("Tip: For dairy cameras, consider imgsz=512 (30-40 percent faster than 640)")
    logger.info("To enable: edit backend/runtime/model_loader.py, change 'imgsz': 416 to 512")

    # Start multiple async event sender threads (parallel HTTP sends)
    SENDER_THREADS = int(os.getenv("EDGE_SENDER_THREADS", "3"))
    for i in range(SENDER_THREADS):
        Thread(target=event_sender, daemon=True).start()
    logger.info("Started %d event sender threads", SENDER_THREADS)

    # Start performance monitoring thread
    Thread(target=performance_monitor, daemon=True).start()
    logger.info("Started performance monitor thread")

    if posture_scheduler:

        posture_scheduler.start()

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
        logger.info("  Configured resolution: %s", resolution)
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
                person_classes,
                tool_classes,
                tmr_classes,
                tractor_classes,
                max_dist,
                milking_model_enabled,
                posture_scheduler,
            ),
            daemon=False,
        )
        t.start()
        camera_threads.append(t)

        # Prevent RTSP/NVDEC burst when many streams start at once.
        time.sleep(2.0)

    for t in camera_threads:
        t.join()

    if posture_scheduler:

        posture_scheduler.stop()


if __name__ == "__main__":
    main()
