#!/usr/bin/env python3
#jetson/
"""
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
import numpy as np
from collections import defaultdict
from pathlib import Path
from ultralytics import YOLO

# ============================================================
# CONFIG
# ============================================================

VIDEO_PATH = "test_data/Full video (17-2-26)/GRP_1_Front_left_17-2-26.mp4"
MODEL_PATH = "models/WF_V1.4_best.engine" # trained and exported with imgsz=512
OUTPUT_PATH = "test_data/Full video (17-2-26)/GRP_1_Front_left_17-2-26_output-1.1.mp4"

DEVICE = "cuda"  # Jetson
CONF_THRES = 0.5
IMG_SIZE = 512
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720


# ---- Activity Parameters ----
SCRAP_DIST_PX = 120
# ---- Feeding Motion Parameters (Frame-based) ----
MIN_VELOCITY = 0.005        # normalized: relative to bbox diagonal (perspective-invariant)
VELOCITY_WINDOW = 2         # sliding window
MIN_ACTIVE_FRAMES = 2       # sustain
CENTROID_SMOOTH_ALPHA = 0.6 # centroid smoothing factor
# ---- ROI (Normalized 0–1 coordinates) ----
"""
Your current video resolution is: 2560 × 1440

But your ROI normalized values were calculated from: 1600 × 720.
"""
FEEDING_ROI =[
    {
        "x": 0.0,
        "y": 0.397
    },
    {
        "x": 0.139,
        "y": 0.991
    },
    {
        "x": 0.752,
        "y": 0.992
    },
    {
        "x": 0.794,
        "y": 0.838
    },
    {
        "x": 0.006,
        "y": 0.156
    },
    {
        "x": 0.002,
        "y": 0.395
    }
]

SCRAPPING_ROI = [
    {
        "x": 0.074,
        "y": 0.122
    },
    {
        "x": 0.867,
        "y": 0.476
    },
    {
        "x": 0.794,
        "y": 0.833
    },
    {
        "x": 0.025,
        "y": 0.147
    },
    {
        "x": 0.073,
        "y": 0.122
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
    "tractor": {
        "prev_center": None,
        "velocity_buffer": [],
        "motion_frames": 0
    },
    "tmr_machine": {
        "prev_center": None,
        "velocity_buffer": [],
        "motion_frames": 0
    }
}

# ============================================================
# Feeding State Hysteresis
# ============================================================

FEEDING_STATE = {
    "counter": 0,
    "active": False
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

def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2, (y1 + y2) / 2)

def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)

def build_pixel_roi(normalized_roi, w, h):
    return [(int(p["x"] * w), int(p["y"] * h)) for p in normalized_roi]

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

def bbox_roi_overlap(box, roi_polygon, min_overlap_ratio=0.05):
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

def detect_feeding_motion(objects_feed):
    feeding_candidate = False

    for cls in ["tractor", "tmr_machine"]:
        boxes = objects_feed.get(cls, [])
        if not boxes:
            # decay motion instead of reset
            mem = CLASS_MOTION_MEMORY[cls]
            mem["motion_frames"] = max(0, mem["motion_frames"] - 1)
            # Clear velocity buffer when object leaves ROI
            mem["velocity_buffer"].clear()
            mem["prev_center"] = None
            continue

        box = max(boxes, key=lambda b: (b[2]-b[0])*(b[3]-b[1]))
        raw_cx, raw_cy = bbox_center(box)

        mem = CLASS_MOTION_MEMORY[cls]

        # Apply centroid smoothing
        if mem["prev_center"] is not None:
            cx = CENTROID_SMOOTH_ALPHA * raw_cx + (1 - CENTROID_SMOOTH_ALPHA) * mem["prev_center"][0]
            cy = CENTROID_SMOOTH_ALPHA * raw_cy + (1 - CENTROID_SMOOTH_ALPHA) * mem["prev_center"][1]
        else:
            cx, cy = raw_cx, raw_cy

        if mem["prev_center"] is None:
            mem["prev_center"] = (cx, cy)
            continue

        px, py = mem["prev_center"]
        dx = cx - px
        dy = cy - py

        # Normalize velocity by bbox diagonal (perspective-invariant)
        bbox_w = box[2] - box[0]
        bbox_h = box[3] - box[1]
        bbox_diag = math.sqrt(bbox_w*bbox_w + bbox_h*bbox_h)
        bbox_diag = max(bbox_diag, 1)
        
        velocity = math.sqrt(dx*dx + dy*dy) / bbox_diag

        mem["velocity_buffer"].append(velocity)

        if len(mem["velocity_buffer"]) > VELOCITY_WINDOW:
            mem["velocity_buffer"].pop(0)

        avg_velocity = sum(mem["velocity_buffer"]) / len(mem["velocity_buffer"])

        # Direction constraint: relaxed to allow diagonal motion
        horizontal_ratio = abs(dx) / (abs(dy) + 1e-6)

        if avg_velocity > MIN_VELOCITY and horizontal_ratio > 0.8:
            mem["motion_frames"] += 1
        else:
            mem["motion_frames"] = max(0, mem["motion_frames"] - 1)

        mem["prev_center"] = (cx, cy)

        if mem["motion_frames"] >= MIN_ACTIVE_FRAMES:
            feeding_candidate = True

    # Apply hysteresis to prevent flicker
    if feeding_candidate:
        FEEDING_STATE["counter"] += 1
    else:
        FEEDING_STATE["counter"] -= 1

    FEEDING_STATE["counter"] = max(0, min(20, FEEDING_STATE["counter"]))

    if FEEDING_STATE["counter"] > 10:
        FEEDING_STATE["active"] = True
    elif FEEDING_STATE["counter"] < 3:
        FEEDING_STATE["active"] = False

    return FEEDING_STATE["active"]

# ============================================================
# Main
# ============================================================

def main():
    model = YOLO(MODEL_PATH)
    print("\nModel class names:")
    print(model.names)
    print()

    cap = cv2.VideoCapture(VIDEO_PATH)
    if not cap.isOpened():
        raise FileNotFoundError(VIDEO_PATH)

    fps = cap.get(cv2.CAP_PROP_FPS)
    src_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    src_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    print("Video resolution:", src_w, src_h)
    print("Processing resolution:", TARGET_WIDTH, TARGET_HEIGHT)
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT))
    next_log_percent = 10
    if total_frames > 0:
        print(f"Processing video... total frames: {total_frames}")
    else:
        print("Processing video... total frames unknown")

    writer = cv2.VideoWriter(
        OUTPUT_PATH,
        cv2.VideoWriter_fourcc(*"mp4v"),
        fps,
        (TARGET_WIDTH, TARGET_HEIGHT)
    )

    names = model.names

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
        if total_frames > 0:
            progress = (frame_index / total_frames) * 100
            if progress >= next_log_percent:
                print(f"Progress: {next_log_percent}%")
                next_log_percent += 10

        # Run detection on original resolution (single resize in model)
        result = model.track(
            orig_frame,
            imgsz=IMG_SIZE,
            conf=CONF_THRES,
            device=DEVICE,
            persist=True,
            tracker="bytetrack.yaml",
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

                # Feeding: use intersection area ratio (0.08 threshold - sensitive for distant vehicles)
                if feeding_roi_poly is not None:
                    overlap = bbox_overlap_ratio(box, feeding_roi_poly)
                    if overlap >= 0.08:
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

        scrapping = detect_scrapping(objects_scrap)
        feeding = detect_feeding_motion(objects_feed)

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

        # ---- Motion Debug Overlay ----
        y_offset = 100
        for cls, mem in CLASS_MOTION_MEMORY.items():
            debug_text = f"{cls}: MF:{mem['motion_frames']}"
            
            # Get text size for background
            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.6
            thickness = 2
            (text_w, text_h), baseline = cv2.getTextSize(debug_text, font, font_scale, thickness)
            
            # Draw black background
            cv2.rectangle(frame, (15, y_offset - text_h - 5), (25 + text_w, y_offset + baseline), (0, 0, 0), -1)
            
            # Draw text in white for better visibility
            cv2.putText(
                frame,
                debug_text,
                (20, y_offset),
                font,
                font_scale,
                (255, 255, 255),  # White color
                thickness
            )
            
            y_offset += 30  # Move down for next entry

        # ---- Draw ROI On Frame (scaled to display resolution) ----
        scale_x = TARGET_WIDTH / src_w
        scale_y = TARGET_HEIGHT / src_h
        
        if feeding_roi_poly is not None:
            scaled_feeding_roi = (feeding_roi_poly * np.array([scale_x, scale_y])).astype(np.int32)
            cv2.polylines(frame, [scaled_feeding_roi], True, (255, 0, 0), 2)

        if scrapping_roi_poly is not None:
            scaled_scrapping_roi = (scrapping_roi_poly * np.array([scale_x, scale_y])).astype(np.int32)
            cv2.polylines(frame, [scaled_scrapping_roi], True, (0, 0, 255), 2)

        writer.write(frame)

    cap.release()
    writer.release()
    
    # End timing and report performance
    end_time = time.time()
    print(f"\nTotal processing time (min): {(end_time - start_time)/60:.2f}")
    print("Saved to:", OUTPUT_PATH)

if __name__ == "__main__":
    main()
