#!/usr/bin/env python3
#jetson/
"""
jetson/test_activity_motion_detection.py
Jetson Activity Logic Tester
---------------------------------
- Loads YOLO model
- Applies ROI filtering
- Motion-based feeding detection (velocity px/sec)
- Scrapping spatial detection
- Annotates output video
- Displays SCRAPPING / FEEDING status

Standalone. No backend dependency.
"""

import cv2
import math
import time
from pathlib import Path
import numpy as np
import threading
from collections import defaultdict
from ultralytics import YOLO

# ============================================================
# CONFIG
# ============================================================

# Use RTSP stream or video file
VIDEO_SOURCE = "rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/1702"  # Live RTSP
#VIDEO_SOURCE = "/home/neopeak/Desktop/projects/Rahuri farm videos/GRP_1_Front_center(5-10-25)/GRP_1_Front_center(5-10-25).mp4"  # Or use file
ACTIVITY_MODEL_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/models/WF_V1.4.1_best.engine"  # supports .pt/.engine
# Set separate milking model path if needed. Keep None to reuse ACTIVITY_MODEL_PATH.
MILKING_MODEL_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/models/Milking_best.pt"  # e.g. "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/models/milking_best.pt"

DEVICE = "cuda"  # Jetson
CONF_THRES = 0.5
MILKING_CONF_THRES = 0.5
IMG_SIZE = 512
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720
STREAM_TIMEOUT = 10  # seconds to wait for stream to open


# ---- Activity Parameters ----
SCRAP_DIST_PX = 120
# ---- MILKING CONFIG ----
CLUSTER_MEMORY_SEC = 300        # 5 min (camera blind spots)
MILKING_BUFFER_SEC = 60         # short occlusion buffer

MILKING_STATE = {
    "last_cluster_seen_ts": None,
    "last_milking_seen_ts": None,
    "active": False
}
# ---- ROI (Normalized 0–1 coordinates) ----
"""
Your current video resolution is: 2560 × 1440

But your ROI normalized values were calculated from: 1600 × 720.
"""
FEEDING_ROI = [
            {
              "x": 0.014,
              "y": 0.186
            },
            {
              "x": 0.178,
              "y": 0.992
            },
            {
              "x": 0.994,
              "y": 0.992
            },
            {
              "x": 0.098,
              "y": 0.122
            }
          ]

SCRAPPING_ROI = [
            {
              "x": 0.103,
              "y": 0.139
            },
            {
              "x": 0.997,
              "y": 0.903
            },
            {
              "x": 0.995,
              "y": 0.342
            },
            {
              "x": 0.225,
              "y": 0.044
            },
            {
              "x": 0.1,
              "y": 0.136
            }
          ]

# Example:  
# FEEDING_ROI = [
#     {"x": 0.1, "y": 0.4},
#     {"x": 0.9, "y": 0.4},
#     {"x": 0.9, "y": 0.9},
#     {"x": 0.1, "y": 0.9},
# ]

# ============================================================
# Motion Memory
# ============================================================

CLASS_MOTION_MEMORY = {
    "tractor": None,
    "tmr_machine": None
}

# ============================================================
# Scrapping State Hysteresis
# ============================================================

SCRAPPING_STATE = {
    "counter": 0,
    "active": False
}

# ============================================================
# Helpers
# ============================================================

def load_model(model_path, label):
    if not model_path:
        return None

    suffix = Path(model_path).suffix.lower()
    if suffix not in {".pt", ".engine"}:
        raise ValueError(
            f"{label} model must be .pt or .engine, got: {model_path}"
        )

    print(f"[MODEL] Loading {label} model: {model_path}")
    model = YOLO(model_path)
    print(f"[MODEL] {label} class names: {model.names}\n")
    return model


def build_gstreamer_pipeline(rtsp_url, codec):
    """Build RTSP pipeline aligned with production stability settings."""
    if codec == "h264":
        depay_parse = "rtph264depay ! h264parse"
    else:
        depay_parse = "rtph265depay ! h265parse"

    return (
        f"rtspsrc location={rtsp_url} latency=100 protocols=tcp "
        "drop-on-latency=true timeout=5000000 ! "
        "queue ! "
        f"{depay_parse} ! "
        "nvv4l2decoder ! nvvidconv ! "
        "video/x-raw,width=1280,height=720,format=BGRx ! "
        "videoconvert ! video/x-raw,format=BGR ! "
        "appsink drop=true max-buffers=1 sync=false"
    )


def open_rtsp_stream_with_timeout(rtsp_url, timeout_sec=STREAM_TIMEOUT):
    """Open RTSP with codec fallback + bounded retries."""
    for codec in ("h264", "h265"):
        pipeline_str = build_gstreamer_pipeline(rtsp_url, codec)
        print(f"[STREAM] Connecting ({codec}) to {pipeline_str[:70]}...")

        for attempt in range(1, 4):
            cap = None
            stream_ready = []

            def open_stream():
                nonlocal cap
                cap = cv2.VideoCapture(pipeline_str, cv2.CAP_GSTREAMER)
                stream_ready.append(cap.isOpened())

            thread = threading.Thread(target=open_stream, daemon=True)
            thread.start()
            thread.join(timeout=timeout_sec)

            if not stream_ready or not stream_ready[0]:
                if cap is not None:
                    cap.release()
                print(f"[STREAM] Open failed ({codec}) attempt {attempt}/3")
                time.sleep(2)
                continue

            if cap is None or not cap.isOpened():
                print(f"[STREAM] Invalid handle ({codec}) attempt {attempt}/3")
                time.sleep(2)
                continue

            time.sleep(0.5)
            ret, frame = cap.read()
            if ret:
                print(f"[STREAM] ✓ RTSP connected successfully ({codec})")
                return cap

            cap.release()
            print(f"[STREAM] Opened but frame read failed ({codec}) attempt {attempt}/3")
            time.sleep(2)

    raise RuntimeError(
        f"Failed to open RTSP stream within retry budget. "
        f"Check URL/codec/network. Timeout per attempt={timeout_sec}s."
    )

def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

def build_pixel_roi(normalized_roi, w, h):
    return [(int(p["x"] * w), int(p["y"] * h)) for p in normalized_roi]

def point_in_polygon(point, polygon):
    return cv2.pointPolygonTest(polygon, point, False) >= 0

def bbox_overlap_ratio(box, roi_polygon):
    """Calculate intersection area ratio for bbox-ROI overlap.
    Returns ratio of intersection area to bbox area.
    """
    x1, y1, x2, y2 = box

    bbox_poly = np.array([
        [x1, y1],
        [x2, y1],
        [x2, y2],
        [x1, y2]
    ], dtype=np.float32)

    inter_area, _ = cv2.intersectConvexConvex(
        roi_polygon,
        bbox_poly
    )

    bbox_area = (x2 - x1) * (y2 - y1)

    if bbox_area <= 0:
        return 0.0

    return inter_area / bbox_area

def bbox_roi_overlap(box, roi_polygon, min_overlap_ratio=0.04):
    """Check if bbox overlaps with ROI above threshold (deprecated, kept for scrapping)."""
    return bbox_overlap_ratio(box, roi_polygon) >= min_overlap_ratio

# ============================================================
# Scrapping Logic
# ============================================================

def detect_scrapping(objects):
    persons = objects.get("person", [])
    # Try both common class names for shovels
    tools = objects.get("shovel", []) or objects.get("scrapping_tool", [])

    scrapping_candidate = False
    for p in persons:
        pc = bbox_center(p)
        for t in tools:
            if euclidean(pc, bbox_center(t)) <= SCRAP_DIST_PX:
                scrapping_candidate = True
                break
        if scrapping_candidate:
            break

    # Apply hysteresis to prevent flicker
    if scrapping_candidate:
        SCRAPPING_STATE["counter"] += 1
    else:
        SCRAPPING_STATE["counter"] -= 1

    SCRAPPING_STATE["counter"] = max(0, min(20, SCRAPPING_STATE["counter"]))

    if SCRAPPING_STATE["counter"] > 10:
        SCRAPPING_STATE["active"] = True
    elif SCRAPPING_STATE["counter"] < 3:
        SCRAPPING_STATE["active"] = False

    return SCRAPPING_STATE["active"]

# ============================================================
# Feeding Motion Logic (Class-based)
# ============================================================

def detect_feeding_motion(objects_feed, current_ts):
    feeding = False

    for cls in ["tractor", "tmr_machine"]:
        boxes = objects_feed.get(cls, [])
        if not boxes:
            CLASS_MOTION_MEMORY[cls] = None
            continue

        box = max(boxes, key=lambda b: (b[2]-b[0])*(b[3]-b[1]))
        cx, cy = bbox_center(box)
        area = (box[2] - box[0]) * (box[3] - box[1])

        mem = CLASS_MOTION_MEMORY[cls]

        if mem is None:
            CLASS_MOTION_MEMORY[cls] = {
                "prev_center": (cx, cy),
                "prev_area": area,
                "prev_ts": current_ts,
                "moving_since": None
            }
            continue

        dt = max(current_ts - mem["prev_ts"], 1e-6)
        displacement = euclidean(mem["prev_center"], (cx, cy))
        velocity = displacement / dt

        area_delta = abs(area - mem["prev_area"])
        area_velocity = area_delta / dt

        mem["prev_center"] = (cx, cy)
        mem["prev_area"] = area
        mem["prev_ts"] = current_ts

        translation_motion = velocity > 3
        area_motion = area_velocity > 1000

        if translation_motion or area_motion:
            if mem["moving_since"] is None:
                mem["moving_since"] = current_ts
            elif current_ts - mem["moving_since"] >= 3:
                feeding = True
        else:
            mem["moving_since"] = None

    return feeding


def detect_milking(objects_all, ts):
    has_cluster_attached = len(objects_all.get("cluster_attached", [])) > 0
    has_udder = len(objects_all.get("cow_leg_udder", [])) > 0

    # --- Update cluster memory ---
    if has_cluster_attached:
        MILKING_STATE["last_cluster_seen_ts"] = ts

    # --- Check cluster recent ---
    cluster_recent = (
        MILKING_STATE["last_cluster_seen_ts"] is not None and
        (ts - MILKING_STATE["last_cluster_seen_ts"]) < CLUSTER_MEMORY_SEC
    )

    # --- Final detection ---
    milking_detected = (
        has_cluster_attached
        or (has_udder and cluster_recent)
    )

    # --- Buffer (like merge-gap lite on edge) ---
    if milking_detected:
        MILKING_STATE["last_milking_seen_ts"] = ts

    milking_active = (
        MILKING_STATE["last_milking_seen_ts"] is not None and
        (ts - MILKING_STATE["last_milking_seen_ts"]) < MILKING_BUFFER_SEC
    )

    return milking_active, has_cluster_attached, has_udder, cluster_recent

# ============================================================
# Main
# ============================================================

def main():
    activity_model = load_model(ACTIVITY_MODEL_PATH, "ACTIVITY")
    if activity_model is None:
        raise RuntimeError("ACTIVITY_MODEL_PATH is required.")

    if MILKING_MODEL_PATH:
        milking_model = load_model(MILKING_MODEL_PATH, "MILKING")
    else:
        milking_model = activity_model
        print("[MODEL] MILKING model not set; reusing ACTIVITY model\n")

    # Open stream (RTSP or file)
    if VIDEO_SOURCE.startswith("rtsp://"):
        print("[STREAM] Detected RTSP source")
        cap = open_rtsp_stream_with_timeout(VIDEO_SOURCE, STREAM_TIMEOUT)
    else:
        print("[STREAM] Detected file source")
        cap = cv2.VideoCapture(VIDEO_SOURCE)
        if not cap.isOpened():
            raise RuntimeError(f"Failed to open file: {VIDEO_SOURCE}")

    fps = cap.get(cv2.CAP_PROP_FPS)
    src_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    src_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    print("Video resolution:", src_w, src_h)
    print("Processing resolution:", TARGET_WIDTH, TARGET_HEIGHT)
    print("\nLive stream started. Press 'q' to quit.")
    print("-" * 50)

    names = activity_model.names
    milking_names = milking_model.names

    # ---- ROI Setup (use original frame dimensions for detection) ----
    feeding_roi_poly = None
    scrapping_roi_poly = None
    
    # Build ROI polygons for original resolution (detection)
    if FEEDING_ROI:
        feeding_roi_poly = build_pixel_roi(FEEDING_ROI, src_w, src_h)
        feeding_roi_poly = np.array(feeding_roi_poly, dtype=np.float32)
        feeding_roi_poly = cv2.convexHull(feeding_roi_poly)

    if SCRAPPING_ROI:
        scrapping_roi_poly = build_pixel_roi(SCRAPPING_ROI, src_w, src_h)
        scrapping_roi_poly = np.array(scrapping_roi_poly, dtype=np.float32)
        scrapping_roi_poly = cv2.convexHull(scrapping_roi_poly)

    frame_index = 0
    
    # Start timing for performance measurement
    start_time = time.time()

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        # Keep original for inference, only resize for display
        orig_frame = frame.copy()

        frame_index += 1

        # Run detection on original resolution (single resize in model)
        result = activity_model.predict(
            orig_frame,
            imgsz=IMG_SIZE,
            conf=CONF_THRES,
            device=DEVICE,
            verbose=False,
            half=True
        )[0]

        if milking_model is activity_model:
            milking_result = result
        else:
            milking_result = milking_model.predict(
                orig_frame,
                imgsz=IMG_SIZE,
                conf=MILKING_CONF_THRES,
                device=DEVICE,
                verbose=False,
                half=True
            )[0]
        
        # Resize only for display/output
        frame = cv2.resize(orig_frame, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_AREA)

        objects_all = defaultdict(list)
        objects_scrap = defaultdict(list)
        objects_feed = defaultdict(list)

        if result.boxes:
            for b in result.boxes:
                cls = names[int(b.cls[0])]
                box = list(map(int, b.xyxy[0]))

                objects_all[cls].append(box)

                # ---- ROI Filtering ----
                # Scrapping: use bbox overlap ratio (0.03 threshold)
                if scrapping_roi_poly is not None:
                    if bbox_roi_overlap(box, scrapping_roi_poly, 0.03):
                        objects_scrap[cls].append(box)
                else:
                    objects_scrap[cls].append(box)

                # Feeding: use bbox overlap ratio
                if feeding_roi_poly is not None:
                    overlap = bbox_overlap_ratio(box, feeding_roi_poly)
                    if overlap >= 0.02:  # 2% overlap is enough for large objects
                        objects_feed[cls].append(box)
                else:
                    objects_feed[cls].append(box)

                # Scale bbox coordinates to display resolution for drawing
                scale_x = TARGET_WIDTH / src_w
                scale_y = TARGET_HEIGHT / src_h
                scaled_box = [
                    int(box[0] * scale_x),
                    int(box[1] * scale_y),
                    int(box[2] * scale_x),
                    int(box[3] * scale_y)
                ]
                
                # Draw bounding box
                cv2.rectangle(frame, scaled_box[:2], scaled_box[2:], (0, 255, 0), 2)

                cv2.putText(
                    frame,
                    cls,
                    (scaled_box[0], scaled_box[1] - 5),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    (0, 255, 0),
                    1
                )

        # Add milking-model detections into objects_all (for milking logic only).
        # If both activities share one model, avoid duplicate inserts.
        if milking_model is not activity_model and milking_result.boxes:
            for b in milking_result.boxes:
                cls = milking_names[int(b.cls[0])]
                box = list(map(int, b.xyxy[0]))
                objects_all[cls].append(box)

        scrapping = detect_scrapping(objects_scrap)
        ts = frame_index / fps if fps > 0 else float(frame_index)
        feeding = detect_feeding_motion(objects_feed, ts)
        milking, has_cluster, has_udder, cluster_recent = detect_milking(objects_all, ts)

        if frame_index % 30 == 0:
            print(
                f"[MILKING DEBUG] frame={frame_index} "
                f"cluster={has_cluster} udder={has_udder} "
                f"cluster_recent={cluster_recent} "
                f"milking={milking}"
            )

        # ---- Status Overlay ----
        def draw_status(label, value, y):
            text = f"{label}: {'YES' if value else 'NO'}"
            color = (0, 255, 0) if value else (0, 0, 255)
            
            # Get text size for background
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.8
            thickness = 2
            (text_w, text_h), baseline = cv2.getTextSize(text, font, font_scale, thickness)
            
            # Draw black background
            cv2.rectangle(frame, (15, y - text_h - 5), (25 + text_w, y + baseline), (0, 0, 0), -1)
            
            # Draw text
            cv2.putText(
                frame,
                text,
                (20, y),
                font,
                font_scale,
                color,
                thickness
            )

        draw_status("SCRAPPING", scrapping, 30)
        draw_status("FEEDING", feeding, 60)
        draw_status("MILKING", milking, 90)
        draw_status("CLUSTER", has_cluster, 120)
        draw_status("UDDER", has_udder, 150)
        draw_status("CLUSTER_RECENT", cluster_recent, 180)

        # ---- Draw ROI On Frame (scaled to display resolution) ----
        scale_x = TARGET_WIDTH / src_w
        scale_y = TARGET_HEIGHT / src_h
        
        if feeding_roi_poly is not None:
            scaled_feeding_roi = (feeding_roi_poly * np.array([scale_x, scale_y])).astype(np.int32)
            cv2.polylines(frame, [scaled_feeding_roi], True, (255, 0, 0), 2)

        if scrapping_roi_poly is not None:
            scaled_scrapping_roi = (scrapping_roi_poly * np.array([scale_x, scale_y])).astype(np.int32)
            cv2.polylines(frame, [scaled_scrapping_roi], True, (0, 0, 255), 2)

        # Display live annotated frame
        cv2.imshow("Activity Detection Live", frame)
        
        # Press 'q' to quit
        if cv2.waitKey(1) & 0xFF == ord('q'):
            print("\nQuitting...")
            break

    cap.release()
    cv2.destroyAllWindows()
    
    # End timing and report performance
    end_time = time.time()
    print(f"Processed {frame_index} frames in {(end_time - start_time):.2f} seconds")
    print(f"Average FPS: {frame_index / (end_time - start_time):.2f}")

if __name__ == "__main__":
    main()
