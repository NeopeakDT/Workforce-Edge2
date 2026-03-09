#!/usr/bin/env python3
"""
jetson/test_activity_motion_detection_cpu.py
CPU-compatible Activity Logic Tester for normal PCs.

- Loads YOLO .pt model (PyTorch)
- Applies ROI filtering
- Motion-based feeding detection (velocity px/sec)
- Scrapping spatial detection
- Displays live annotated stream with SCRAPPING / FEEDING status

Standalone. No backend dependency.
"""

import os
import cv2
import math
import time
import numpy as np
from collections import defaultdict
from ultralytics import YOLO

# ============================================================
# CONFIG
# ============================================================

# Use RTSP stream or video file
VIDEO_SOURCE = "rtsp://admin:ADMIN123@192.168.0.64:554/Streaming/Channels/101"
# VIDEO_SOURCE = "test_data/Full video (17-2-26)/GRP_1_Front_center_17-2-26.mp4"

# IMPORTANT: CPU script must use .pt model, not TensorRT .engine
MODEL_PATH = "/home/neopeak/Desktop/WF-project/WF/Workforce-Detection/models/WF_V1.4.1_best.pt"

DEVICE = "cpu"
CONF_THRES = 0.5
IMG_SIZE = 512
TARGET_WIDTH = 1280
TARGET_HEIGHT = 720

# ---- Activity Parameters ----
SCRAP_DIST_PX = 120

# ---- ROI (Normalized 0-1 coordinates) ----
FEEDING_ROI = None

SCRAPPING_ROI = [
    {"x": 0.619, "y": 0.101},
    {"x": 0.933, "y": 0.224},
    {"x": 0.831, "y": 0.997},
    {"x": 0.004, "y": 0.981},
    {"x": 0.611, "y": 0.101},
]

# ============================================================
# Motion Memory
# ============================================================

CLASS_MOTION_MEMORY = {
    "tractor": None,
    "tmr_machine": None,
}

# ============================================================
# Scrapping State Hysteresis
# ============================================================

SCRAPPING_STATE = {
    "counter": 0,
    "active": False,
}

# ============================================================
# Helpers
# ============================================================

def open_stream_pc(source):
    """Open RTSP/file stream on normal PC using OpenCV backends."""
    if source.startswith("rtsp://"):
        # Help FFmpeg-based OpenCV prefer TCP for RTSP stability.
        os.environ["OPENCV_FFMPEG_CAPTURE_OPTIONS"] = "rtsp_transport;tcp"

        cap = cv2.VideoCapture(source, cv2.CAP_FFMPEG)
        if not cap.isOpened():
            cap.release()
            cap = cv2.VideoCapture(source)
    else:
        cap = cv2.VideoCapture(source)

    if not cap.isOpened():
        raise RuntimeError(f"Failed to open stream: {source}")

    # Warm-up read so first displayed frame is valid.
    for _ in range(3):
        ret, _ = cap.read()
        if ret:
            break
        time.sleep(0.1)

    return cap


def bbox_center(b):
    x1, y1, x2, y2 = b
    return ((x1 + x2) / 2, (y1 + y2) / 2)


def euclidean(a, b):
    return math.sqrt((a[0] - b[0]) ** 2 + (a[1] - b[1]) ** 2)


def build_pixel_roi(normalized_roi, w, h):
    return [(int(p["x"] * w), int(p["y"] * h)) for p in normalized_roi]


def bbox_overlap_ratio(box, roi_polygon):
    """Calculate intersection area ratio for bbox-ROI overlap."""
    x1, y1, x2, y2 = box

    bbox_poly = np.array(
        [[x1, y1], [x2, y1], [x2, y2], [x1, y2]], dtype=np.float32
    )

    inter_area, _ = cv2.intersectConvexConvex(roi_polygon, bbox_poly)
    bbox_area = (x2 - x1) * (y2 - y1)

    if bbox_area <= 0:
        return 0.0

    return inter_area / bbox_area


def bbox_roi_overlap(box, roi_polygon, min_overlap_ratio=0.04):
    return bbox_overlap_ratio(box, roi_polygon) >= min_overlap_ratio


# ============================================================
# Scrapping Logic
# ============================================================

def detect_scrapping(objects):
    persons = objects.get("person", [])
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

        box = max(boxes, key=lambda b: (b[2] - b[0]) * (b[3] - b[1]))
        cx, cy = bbox_center(box)
        area = (box[2] - box[0]) * (box[3] - box[1])

        mem = CLASS_MOTION_MEMORY[cls]

        if mem is None:
            CLASS_MOTION_MEMORY[cls] = {
                "prev_center": (cx, cy),
                "prev_area": area,
                "prev_ts": current_ts,
                "moving_since": None,
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


# ============================================================
# Main
# ============================================================

def main():
    model = YOLO(MODEL_PATH)
    print("\nModel class names:")
    print(model.names)
    print()

    cap = open_stream_pc(VIDEO_SOURCE)

    fps = cap.get(cv2.CAP_PROP_FPS)
    src_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    src_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))

    if src_w <= 0 or src_h <= 0:
        raise RuntimeError("Could not read valid stream dimensions")

    print("Video resolution:", src_w, src_h)
    print("Processing resolution:", TARGET_WIDTH, TARGET_HEIGHT)
    print("\nLive stream started. Press 'q' to quit.")
    print("-" * 50)

    names = model.names

    feeding_roi_poly = None
    scrapping_roi_poly = None

    if FEEDING_ROI:
        feeding_roi_poly = build_pixel_roi(FEEDING_ROI, src_w, src_h)
        feeding_roi_poly = np.array(feeding_roi_poly, dtype=np.float32)
        feeding_roi_poly = cv2.convexHull(feeding_roi_poly)

    if SCRAPPING_ROI:
        scrapping_roi_poly = build_pixel_roi(SCRAPPING_ROI, src_w, src_h)
        scrapping_roi_poly = np.array(scrapping_roi_poly, dtype=np.float32)
        scrapping_roi_poly = cv2.convexHull(scrapping_roi_poly)

    frame_index = 0
    start_time = time.time()

    # Initialize status so logging is safe from first iteration.
    scrapping = False
    feeding = False

    while True:
        ret, frame = cap.read()
        if not ret:
            print("[STREAM] Frame read failed or stream ended")
            break

        orig_frame = frame.copy()
        frame_index += 1

        # Run detection on original resolution
        result = model.predict(
            orig_frame,
            imgsz=IMG_SIZE,
            conf=CONF_THRES,
            device=DEVICE,
            verbose=False,
            half=False,
        )[0]

        frame = cv2.resize(
            orig_frame, (TARGET_WIDTH, TARGET_HEIGHT), interpolation=cv2.INTER_AREA
        )

        objects_scrap = defaultdict(list)
        objects_feed = defaultdict(list)

        if result.boxes:
            for b in result.boxes:
                cls = names[int(b.cls[0])]
                box = list(map(int, b.xyxy[0]))

                if scrapping_roi_poly is not None:
                    if bbox_roi_overlap(box, scrapping_roi_poly, 0.03):
                        objects_scrap[cls].append(box)
                else:
                    objects_scrap[cls].append(box)

                if feeding_roi_poly is not None:
                    overlap = bbox_overlap_ratio(box, feeding_roi_poly)
                    if overlap >= 0.02:
                        objects_feed[cls].append(box)
                else:
                    objects_feed[cls].append(box)

                scale_x = TARGET_WIDTH / src_w
                scale_y = TARGET_HEIGHT / src_h
                scaled_box = [
                    int(box[0] * scale_x),
                    int(box[1] * scale_y),
                    int(box[2] * scale_x),
                    int(box[3] * scale_y),
                ]

                cv2.rectangle(frame, scaled_box[:2], scaled_box[2:], (0, 255, 0), 2)
                cv2.putText(
                    frame,
                    cls,
                    (scaled_box[0], scaled_box[1] - 5),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.5,
                    (0, 255, 0),
                    1,
                )

        scrapping = detect_scrapping(objects_scrap)
        ts = frame_index / fps if fps > 0 else float(frame_index)
        feeding = detect_feeding_motion(objects_feed, ts)

        if frame_index % 30 == 0:
            print(f"Frame: {frame_index} | SCRAPPING: {scrapping} | FEEDING: {feeding}")

        def draw_status(label, value, y):
            text = f"{label}: {'YES' if value else 'NO'}"
            color = (0, 255, 0) if value else (0, 0, 255)

            font = cv2.FONT_HERSHEY_SIMPLEX
            font_scale = 0.8
            thickness = 2
            (text_w, text_h), baseline = cv2.getTextSize(text, font, font_scale, thickness)

            cv2.rectangle(
                frame,
                (15, y - text_h - 5),
                (25 + text_w, y + baseline),
                (0, 0, 0),
                -1,
            )
            cv2.putText(frame, text, (20, y), font, font_scale, color, thickness)

        draw_status("SCRAPPING", scrapping, 30)
        draw_status("FEEDING", feeding, 60)

        scale_x = TARGET_WIDTH / src_w
        scale_y = TARGET_HEIGHT / src_h

        if feeding_roi_poly is not None:
            scaled_feeding_roi = (
                feeding_roi_poly * np.array([scale_x, scale_y])
            ).astype(np.int32)
            cv2.polylines(frame, [scaled_feeding_roi], True, (255, 0, 0), 2)

        if scrapping_roi_poly is not None:
            scaled_scrapping_roi = (
                scrapping_roi_poly * np.array([scale_x, scale_y])
            ).astype(np.int32)
            cv2.polylines(frame, [scaled_scrapping_roi], True, (0, 0, 255), 2)

        cv2.imshow("Activity Detection Live (CPU)", frame)

        if cv2.waitKey(1) & 0xFF == ord("q"):
            print("\nQuitting...")
            break

    cap.release()
    cv2.destroyAllWindows()

    end_time = time.time()
    elapsed = max(end_time - start_time, 1e-6)
    print(f"Processed {frame_index} frames in {elapsed:.2f} seconds")
    print(f"Average FPS: {frame_index / elapsed:.2f}")


if __name__ == "__main__":
    main()
