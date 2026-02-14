#!/usr/bin/env python3
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

VIDEO_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/test_data/rahuri_video-4.mp4"
MODEL_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/models/WF_V1.3_best.pt"
OUTPUT_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/test_data/outputs/rahuri_video-4.1_motion_test.mp4"

DEVICE = "cuda"  # Jetson
CONF_THRES = 0.25
IMG_SIZE = 416

# ---- Activity Parameters ----
SCRAP_DIST_PX = 120
MOTION_THRESHOLD_PX_PER_SEC = 5
MIN_MOTION_DURATION_SEC = 3
AREA_THRESHOLD_PX2_PER_SEC = 1200
# ---- ROI (Normalized 0–1 coordinates) ----
"""
Your current video resolution is: 2560 × 1440

But your ROI normalized values were calculated from: 1600 × 720.
"""
FEEDING_ROI = [
    {"x": 0.63125, "y": 0.05277777777777778},
    {"x": 0.14625, "y": 0.9944444444444445},
    {"x": 0.61375, "y": 0.9958333333333333},
    {"x": 0.6775, "y": 0.06944444444444445},
    {"x": 0.6325, "y": 0.04722222222222222},
]
SCRAPPING_ROI = [
    {"x": 0.7025, "y": 0.08055555555555556},
    {"x": 0.65875, "y": 0.9888888888888889},
    {"x": 0.9325, "y": 0.9972222222222222},
    {"x": 0.926875, "y": 0.48055555555555557},
    {"x": 0.748125, "y": 0.08472222222222223},
    {"x": 0.7025, "y": 0.07777777777777778},
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

MOTION_MEMORY = {}

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

def bbox_roi_overlap(box, roi_polygon, frame_shape, min_overlap_ratio=0.2):
    h, w = frame_shape[:2]
    x1, y1, x2, y2 = map(int, box)

    x1 = max(0, min(w - 1, x1))
    y1 = max(0, min(h - 1, y1))
    x2 = max(0, min(w, x2))
    y2 = max(0, min(h, y2))

    bbox_w = x2 - x1
    bbox_h = y2 - y1
    if bbox_w <= 0 or bbox_h <= 0:
        return False

    roi_mask = np.zeros((h, w), dtype=np.uint8)
    cv2.fillPoly(roi_mask, [roi_polygon], 1)

    bbox_mask = np.zeros_like(roi_mask)
    bbox_mask[y1:y2, x1:x2] = 1

    intersection = np.logical_and(roi_mask, bbox_mask).sum()
    bbox_area = bbox_w * bbox_h
    overlap_ratio = intersection / bbox_area
    return overlap_ratio >= min_overlap_ratio

# ============================================================
# Scrapping Logic
# ============================================================

def detect_scrapping(objects):
    persons = objects.get("person", [])
    tools = objects.get("scrapping_tool", [])

    for p in persons:
        pc = bbox_center(p)
        for t in tools:
            if euclidean(pc, bbox_center(t)) <= SCRAP_DIST_PX:
                return True
    return False

# ============================================================
# Feeding Motion Logic (Velocity-based)
# ============================================================

def detect_feeding_motion(objects, current_ts):
    global MOTION_MEMORY

    feeding = False

    for cls in ["tmr_machine", "tractor"]:
        for box in objects.get(cls, []):

            cx, cy = bbox_center(box)
            current_area = (box[2] - box[0]) * (box[3] - box[1])

            mem = MOTION_MEMORY.get(cls)

            if mem is None:
                MOTION_MEMORY[cls] = {
                    "prev_centroid": (cx, cy),
                    "prev_area": current_area,
                    "prev_ts": current_ts,
                    "moving_since": None
                }
                continue

            # ---- TIME ----
            delta_t = max(current_ts - mem["prev_ts"], 1e-6)

            # ---- TRANSLATION MOTION ----
            displacement = euclidean(mem["prev_centroid"], (cx, cy))
            velocity = displacement / delta_t

            # ---- AREA MOTION ----
            area_delta = abs(current_area - mem["prev_area"])
            area_velocity = area_delta / delta_t

            # ---- UPDATE MEMORY ----
            mem["prev_centroid"] = (cx, cy)
            mem["prev_area"] = current_area
            mem["prev_ts"] = current_ts
            mem["last_velocity"] = velocity
            mem["last_area_velocity"] = area_velocity

            translation_motion = velocity >= MOTION_THRESHOLD_PX_PER_SEC
            area_motion = area_velocity >= AREA_THRESHOLD_PX2_PER_SEC

            if translation_motion or area_motion:
                if mem["moving_since"] is None:
                    mem["moving_since"] = current_ts
                elif current_ts - mem["moving_since"] >= MIN_MOTION_DURATION_SEC:
                    feeding = True
            else:
                mem["moving_since"] = None

    return feeding

# ============================================================
# Main
# ============================================================

def main():
    model = YOLO(MODEL_PATH)
    
    # Fuse model and convert to FP16 (match production ModelRunner)
    model.fuse()
    model.to(DEVICE)
    if DEVICE == "cuda":
        model.model.half()

    cap = cv2.VideoCapture(VIDEO_PATH)
    if not cap.isOpened():
        raise FileNotFoundError(VIDEO_PATH)

    fps = cap.get(cv2.CAP_PROP_FPS)
    w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    print("Video resolution:", w, h)
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
        (w, h)
    )

    names = model.names

    # ---- ROI Setup ----
    feeding_roi_poly = None
    scrapping_roi_poly = None

    if FEEDING_ROI:
        feeding_roi_poly = build_pixel_roi(FEEDING_ROI, w, h)
        feeding_roi_poly = cv2.convexHull(
            np.array(feeding_roi_poly, dtype=np.int32)
        )

    if SCRAPPING_ROI:
        scrapping_roi_poly = build_pixel_roi(SCRAPPING_ROI, w, h)
        scrapping_roi_poly = cv2.convexHull(
            np.array(scrapping_roi_poly, dtype=np.int32)
        )

    frame_index = 0

    while True:
        ret, frame = cap.read()
        if not ret:
            break

        frame_index += 1
        ts = frame_index / fps
        if total_frames > 0:
            progress = (frame_index / total_frames) * 100
            if progress >= next_log_percent:
                print(f"Progress: {next_log_percent}%")
                next_log_percent += 10

        result = model(
            frame,
            imgsz=IMG_SIZE,
            conf=CONF_THRES,
            device=DEVICE,
            half=(DEVICE == "cuda"),
            stream=False,
            verbose=False
        )[0]

        objects_all = defaultdict(list)
        objects_scrap = defaultdict(list)
        objects_feed = defaultdict(list)

        if result.boxes:
            for b in result.boxes:
                cls = names[int(b.cls[0])]
                box = list(map(int, b.xyxy[0]))

                objects_all[cls].append(box)

                # ---- ROI Filtering (BBox overlap based) ----
                if scrapping_roi_poly is not None:
                    if bbox_roi_overlap(box, scrapping_roi_poly, frame.shape, 0.15):
                        objects_scrap[cls].append(box)
                else:
                    objects_scrap[cls].append(box)

                if feeding_roi_poly is not None:
                    if bbox_roi_overlap(box, feeding_roi_poly, frame.shape, 0.15):
                        objects_feed[cls].append(box)
                else:
                    objects_feed[cls].append(box)

                # Draw bounding box
                cv2.rectangle(frame, box[:2], box[2:], (0, 255, 0), 2)

                cv2.putText(
                    frame,
                    cls,
                    (box[0], box[1] - 5),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    (0, 255, 0),
                    1
                )

        scrapping = detect_scrapping(objects_scrap)
        feeding = detect_feeding_motion(objects_feed, ts)

        # Clean stale memory if object disappears
        active_classes = set(objects_feed.keys())
        for cls in list(MOTION_MEMORY.keys()):
            if cls not in active_classes:
                MOTION_MEMORY.pop(cls)

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
        for cls, mem in MOTION_MEMORY.items():
            vel = mem.get("last_velocity", 0)
            area_vel = mem.get("last_area_velocity", 0)

            debug_text = f"{cls} V:{vel:.1f} A:{area_vel:.0f}"
            
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

        # ---- Draw ROI On Frame ----
        if feeding_roi_poly is not None:
            cv2.polylines(frame, [feeding_roi_poly], True, (255, 0, 0), 2)

        if scrapping_roi_poly is not None:
            cv2.polylines(frame, [scrapping_roi_poly], True, (0, 0, 255), 2)

        writer.write(frame)

    cap.release()
    writer.release()

    print("Saved to:", OUTPUT_PATH)

if __name__ == "__main__":
    main()
