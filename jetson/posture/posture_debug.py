#!/usr/bin/env python3

"""
Live Posture Debug Tool

- Uses live RTSP
- Reads POSTURE ROIs from local_cache.json
- Shows annotated live stream


GRP2-TMR_WAY        → 501 (main) / 502 (sub)
GRP2-FRONT_RIGHT    → 1901 (main) / 1902 (sub)
GRP1-FRONT_LEFT     → 2301 (main) / 1802 (sub)
GRP1-FRONT_RIGHT    → 2201 (main) / 1302 (sub)
GRP1-FRONT_CENTER   → 2401 (main) / 1702 (sub)
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import cv2
import numpy as np
from ultralytics import YOLO

REPO = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(REPO / "jetson"))

from posture.posture_utils import (  # noqa: E402
    assign_detection_to_zone,
    polygon_overlap_ratio,
)
from runtime.video_stream import open_stream  # noqa: E402

##############################################################
# CONFIG
##############################################################

VIDEO_SOURCE = "rtsp://admin:OMSAI%2312@192.168.31.157:554/Streaming/Channels/2301"
CAMERA_CODE = "grp1-front-left"

MODEL_PATH = REPO / "models" / "cow_posture_v1.1_best.pt"

LOCAL_CACHE = REPO / "jetson" / "config" / "local_cache.json"

DEVICE = "cuda"

CONF_THRES = 0.50

IMG_SIZE = 640

TARGET_WIDTH = 1280
TARGET_HEIGHT = 720

DECODE_MODE = "GPU"


def polygon_to_pixels(roi, width, height):
    pts = []

    for p in roi:
        pts.append([
            int(p["x"] * width),
            int(p["y"] * height),
        ])

    return np.array(pts, dtype=np.int32)


def draw_roi(frame, polygon, color, label):
    cv2.polylines(
        frame,
        [polygon],
        True,
        color,
        2,
    )

    x = polygon[:, 0].min()
    y = polygon[:, 1].min() - 10

    cv2.putText(
        frame,
        label,
        (x, y),
        cv2.FONT_HERSHEY_SIMPLEX,
        0.55,
        color,
        1,
    )


def draw_stats_panel(
    frame,
    *,
    camera_code,
    standing_rest,
    standing_feeding,
    lying,
    outside_roi,
):
    lines = [
        f"Camera : {camera_code}",
        "",
        f"Standing (REST)    : {standing_rest}",
        f"Standing (FEEDING): {standing_feeding}",
        f"Outside ROI        : {outside_roi}",
        f"Lying              : {lying}",
    ]

    x = 15
    y = 28

    line_height = 20
    font = 0.52
    thickness = 1

    panel_width = 285
    panel_height = 15 + line_height * len(lines)

    overlay = frame.copy()
    cv2.rectangle(
        overlay,
        (x - 8, y - 20),
        (x - 8 + panel_width, y - 20 + panel_height),
        (0, 0, 0),
        -1,
    )
    # darker background
    cv2.addWeighted(
        overlay,
        0.75,
        frame,
        0.25,
        0,
        frame,
    )

    for idx, line in enumerate(lines):
        cv2.putText(
            frame,
            line,
            (x, y + idx * line_height),
            cv2.FONT_HERSHEY_SIMPLEX,
            font,
            (255, 255, 255),
            thickness,
            cv2.LINE_AA,
        )

def main():
    with open(LOCAL_CACHE, "r") as f:
        cache = json.load(f)

    camera_cfg = next(
        (c for c in cache["cameras"] if c["code"] == CAMERA_CODE),
        None,
    )

    if camera_cfg is None:
        raise RuntimeError(
            "Camera not found in local_cache.json"
        )

    print()
    print("Camera :", camera_cfg["code"])
    if camera_cfg["rtsp_url"] != VIDEO_SOURCE:
        print("[WARNING] VIDEO_SOURCE does not match local_cache RTSP.")

    posture_cfg = camera_cfg["activity_zones"].get("POSTURE")

    if posture_cfg is None:
        raise RuntimeError(
            "POSTURE config missing."
        )

    zones = posture_cfg["zones"]

    rest_zone = None
    feeding_zone = None

    for zone in zones:
        if zone["zone_type"] == "REST":
            rest_zone = zone

        elif zone["zone_type"] == "FEEDING":
            feeding_zone = zone

    if rest_zone is None:
        raise RuntimeError("REST ROI missing.")

    if feeding_zone is None:
        raise RuntimeError("FEEDING ROI missing.")

    print()
    print("[MODEL] Loading posture model...")

    model = YOLO(str(MODEL_PATH))

    print("[MODEL] Loaded.")

    stream = open_stream(
        {
            "code": "posture-debug",
            "stream_type": "RTSP",
            "rtsp_url": VIDEO_SOURCE,
            "decode_mode": DECODE_MODE,
        }
    )
    cap = stream.cap

    src_w = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    src_h = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    stream_fps = cap.get(cv2.CAP_PROP_FPS)

    print()
    print(f"Resolution : {src_w} x {src_h}")
    print(f"FPS : {stream_fps}")

    rest_polygon = polygon_to_pixels(
        rest_zone["roi"],
        src_w,
        src_h,
    )

    feeding_polygon = polygon_to_pixels(
        feeding_zone["roi"],
        src_w,
        src_h,
    )

    # Same zone assignment path as PostureDetector (precomputed pixel polygons).
    posture_zones = {
        "zones": [
            {
                **rest_zone,
                "polygon": rest_polygon,
            },
            {
                **feeding_zone,
                "polygon": feeding_polygon,
            },
        ],
    }

    print()
    print("[DEBUG] REST ROI loaded.")
    print("[DEBUG] FEEDING ROI loaded.")

    print()
    print("Press Q to quit.")
    print("-" * 60)

    while True:
        ret, frame = stream.read()

        if not ret:
            print("[STREAM] Lost frame.")
            break

        result = model.predict(
            frame,
            imgsz=IMG_SIZE,
            conf=CONF_THRES,
            device=DEVICE,
            verbose=False,
        )[0]

        display = cv2.resize(
            frame,
            (TARGET_WIDTH, TARGET_HEIGHT),
            interpolation=cv2.INTER_CUBIC,
        )

        sx = TARGET_WIDTH / src_w
        sy = TARGET_HEIGHT / src_h

        rest_display = rest_polygon.astype(np.float32).copy()
        rest_display[:, 0] *= sx
        rest_display[:, 1] *= sy

        feeding_display = feeding_polygon.astype(np.float32).copy()
        feeding_display[:, 0] *= sx
        feeding_display[:, 1] *= sy

        rest_display = rest_display.astype(np.int32)
        feeding_display = feeding_display.astype(np.int32)

        draw_roi(display, rest_display, (0, 255, 0), "REST")
        draw_roi(display, feeding_display, (255, 0, 0), "FEEDING")

        standing_rest = 0
        standing_feeding = 0
        lying = 0
        outside_roi = 0

        if result.boxes:
            for box in result.boxes:
                cls_id = int(box.cls[0])
                cls_name = model.names[cls_id]
                conf = float(box.conf[0])
                x1, y1, x2, y2 = map(int, box.xyxy[0])

                zone = assign_detection_to_zone(
                    {"bbox": (x1, y1, x2, y2)},
                    posture_zones,
                )

                zone_type = None
                if zone is not None:
                    zone_type = zone.get("zone_type")

                feeding_overlap = polygon_overlap_ratio(
                    (x1, y1, x2, y2),
                    feeding_polygon,
                )

                rest_overlap = polygon_overlap_ratio(
                    (x1, y1, x2, y2),
                    rest_polygon,
                )

                color = (180, 180, 180)

                if cls_name == "cow_standing":
                    if zone_type == "REST":
                        standing_rest += 1
                        color = (0, 255, 0)
                    elif zone_type == "FEEDING":
                        standing_feeding += 1
                        color = (0, 255, 255)
                    else:
                        outside_roi += 1
                        color = (0, 0, 255)

                elif cls_name == "cow_lying":
                    lying += 1
                    color = (255, 0, 0)

                label_lines = [
                    f"{zone_type}",
                    f"F={feeding_overlap:.2f}",
                    f"R={rest_overlap:.2f}",
                ]

                dx1 = int(x1 * sx)
                dy1 = int(y1 * sy)
                dx2 = int(x2 * sx)
                dy2 = int(y2 * sy)

                cv2.rectangle(
                    display,
                    (dx1, dy1),
                    (dx2, dy2),
                    color,
                    3 if zone_type is None else 2,
                )

                for i, txt in enumerate(label_lines):
                    cv2.putText(
                        display,
                        txt,
                        (
                            dx1,
                            dy1 - 5 - i * 14,
                        ),
                        cv2.FONT_HERSHEY_SIMPLEX,
                        0.42,
                        color,
                        1,
                        cv2.LINE_AA,
                    )
        draw_stats_panel(
            display,
            camera_code=camera_cfg["code"],
            standing_rest=standing_rest,
            standing_feeding=standing_feeding,
            lying=lying,
            outside_roi=outside_roi,
        )

        cv2.imshow(
            "Posture Debug",
            display,
        )

        key = cv2.waitKey(1)

        if key & 0xFF == ord("q"):
            break

    stream.release()
    cv2.destroyAllWindows()


if __name__ == "__main__":
    main()
