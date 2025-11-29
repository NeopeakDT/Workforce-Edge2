#!/usr/bin/env python3
"""
detect_milking_windows_updated.py

CPU inference using Ultralytics YOLOv8 best.pt.

Changes:
 - Lowered Milking status (top-left moved down slightly) with +10% font
 - Black background and white text for status
 - Different bbox colors per class via CONFIG["CLASS_COLORS"]
 - JSON log uses CCTV timestamp (OCR) / interpolation; activity instances use time strings and duration_sec
"""

import time
import json
from pathlib import Path
from collections import deque, Counter
import re
from datetime import datetime

import cv2
import numpy as np
from ultralytics import YOLO
from tqdm import tqdm

# Optional dependencies
try:
    from dateutil import parser as dateparser
except Exception:
    dateparser = None

try:
    import pytesseract
    from PIL import Image
    # Check if Tesseract binary is available
    try:
        pytesseract.get_tesseract_version()
    except Exception:
        # Tesseract binary not found, disable OCR
        pytesseract = None
except Exception:
    pytesseract = None

# -------------------------- CONFIG --------------------------
CONFIG = {
    # I/O
    "SOURCE_VIDEO": r"C:\Users\offic\OneDrive\Desktop\Workforce Videos\Milking\Rahuri milking video 7.mp4",
    "WEIGHTS": r"C:\Users\offic\OneDrive\Desktop\Workforce-Detection\Activities\01-Milking\best.pt",
    "OUT_VIDEO": r"C:\Users\offic\OneDrive\Desktop\Workforce Videos\Milking\Output\Rahuri milking video 7.mp4",
    "OUT_JSON": r"C:\Users\offic\OneDrive\Desktop\Workforce Videos\Milking\Output\Rahuri milking video 7.json",

    # detection thresholds
    "CONF_THRESH": 0.5,
    "IOU_THRESH": 0.45,

    # activity logic (frames threshold)
    "ATTACH_FRAMES_THRESHOLD": 5,

    # dev/test limiter: 0 => whole video / unlimited
    "MAX_FRAMES": 0,

    # class names must match model training order
    "CLASS_NAMES": ["cluster_attached", "cluster_detached", "cow_leg_udder"],

    # whether to store full per-frame detections
    "LOG_FULL_DETECTIONS": True,

    # ROI for CCTV timestamp (None => auto-guess bottom-right)
    "TIMESTAMP_ROI": None,

    # tesseract config (single-line numeric); add '.' or letters if needed
    "OCR_CONF": "--psm 7 -c tessedit_char_whitelist=0123456789/:. -l eng",

    # Per-class BGR colors (B,G,R) - tweak as needed
    "CLASS_COLORS": {
        "cluster_attached": (0, 200, 0),    # green
        "cluster_detached": (0, 0, 255),    # red
        "cow_leg_udder": (0, 165, 255)      # orange
    },

    # Status font scale base (we'll increase 10% relative to previous script value 0.6)
    "STATUS_FONT_SCALE": 0.6 * 1.1,  # 0.66

    # Status bar height (pixels)
    "STATUS_BAR_H": 36
}
# -------------------------------------------------------------

DEFAULT_CONF = CONFIG["CONF_THRESH"]
DEFAULT_IOU = CONFIG["IOU_THRESH"]
DEFAULT_ATTACH_THRESHOLD = CONFIG["ATTACH_FRAMES_THRESHOLD"]
DEFAULT_DET_CLASSES = CONFIG["CLASS_NAMES"]

def xyxy_to_int(bbox):
    return [int(round(x)) for x in bbox]

def pretty_time_from_unix(unix_ts):
    return datetime.fromtimestamp(unix_ts).strftime("%Y-%m-%d %H:%M:%S.%f")[:-3]

# timestamp parsing helpers
def parse_timestamp_str(s):
    if not s or not s.strip():
        return None
    s = s.strip()
    s = re.sub(r"[^\x20-\x7E]", "", s)
    patterns = [
        "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S", "%d/%m/%Y %H:%M:%S",
        "%d-%m-%Y %H:%M:%S", "%d.%m.%Y %H:%M:%S", "%m/%d/%Y %H:%M:%S",
        "%H:%M:%S %d/%m/%Y", "%H:%M:%S", "%H:%M:%S.%f", "%d-%b-%Y %H:%M:%S",
    ]
    s_clean = s.replace("l", "1").replace("I", "1").replace("O", "0").replace("|", ":")
    s_clean = re.sub(r"\s+", " ", s_clean)
    for p in patterns:
        try:
            dt = datetime.strptime(s_clean, p)
            return dt
        except Exception:
            pass
    m = re.search(r"(\d{1,2}:\d{2}:\d{2}(?:\.\d{1,3})?)", s_clean)
    if m:
        tpart = m.group(1)
        try:
            dt_time = datetime.strptime(tpart, "%H:%M:%S.%f")
            dt = datetime.now().replace(hour=dt_time.hour, minute=dt_time.minute, second=dt_time.second, microsecond=dt_time.microsecond)
            return dt
        except Exception:
            try:
                dt_time = datetime.strptime(tpart, "%H:%M:%S")
                dt = datetime.now().replace(hour=dt_time.hour, minute=dt_time.minute, second=dt_time.second, microsecond=0)
                return dt
            except Exception:
                pass
    if dateparser is not None:
        try:
            dt = dateparser.parse(s_clean, fuzzy=True)
            return dt
        except Exception:
            pass
    return None

def ocr_read_timestamp(frame, roi=None, ocr_conf=None):
    if pytesseract is None:
        return None, None
    h, w = frame.shape[:2]
    if roi is None:
        rw, rh = min(320, w), min(48, h)
        rx = max(0, w - rw - 10)
        ry = max(0, h - rh - 10)
        roi = (rx, ry, rw, rh)
    x, y, rw, rh = roi
    x = max(0, int(x)); y = max(0, int(y))
    rw = int(min(rw, w - x)); rh = int(min(rh, h - y))
    crop = frame[y:y+rh, x:x+rw]
    if crop.size == 0:
        return None, None
    gray = cv2.cvtColor(crop, cv2.COLOR_BGR2GRAY)
    try:
        _, th = cv2.threshold(gray, 0, 255, cv2.THRESH_BINARY + cv2.THRESH_OTSU)
    except Exception:
        th = gray
    pil_img = Image.fromarray(th)
    try:
        raw = pytesseract.image_to_string(pil_img, config=ocr_conf or CONFIG["OCR_CONF"])
    except Exception:
        try:
            raw = pytesseract.image_to_string(pil_img)
        except Exception:
            # Tesseract not available or failed, return None
            return None, None
    if raw:
        raw = raw.strip()
    dt = parse_timestamp_str(raw)
    return raw, dt

def create_video_writer(out_path, fps, width, height):
    out_path = Path(out_path)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    ext = out_path.suffix.lower()
    candidates = []
    if ext in [".mp4", ".m4v"]:
        candidates = [("mp4v", ".mp4"), ("XVID", ".mp4"), ("MJPG", ".mp4")]
    elif ext in [".avi"]:
        candidates = [("XVID", ".avi"), ("MJPG", ".avi"), ("DIVX", ".avi")]
    else:
        candidates = [("mp4v", ".mp4"), ("XVID", ".avi"), ("MJPG", ".avi")]
    for fourcc_str, out_ext in candidates:
        fourcc = cv2.VideoWriter_fourcc(*fourcc_str)
        trial_path = str(out_path) if Path(out_path).suffix.lower() == out_ext else str(out_path.with_suffix(out_ext))
        writer = cv2.VideoWriter(trial_path, fourcc, fps, (int(width), int(height)))
        if writer.isOpened():
            print(f"[VideoWriter] success: codec='{fourcc_str}' -> writing to '{trial_path}'")
            return writer, trial_path, fourcc_str
        else:
            try:
                writer.release()
            except Exception:
                pass
            print(f"[VideoWriter] failed with codec '{fourcc_str}' for path '{trial_path}'")
    raise RuntimeError("Failed to open VideoWriter with tested codecs. Try installing OpenCV with ffmpeg support or change OUT_VIDEO to .avi.")

def run_detection(cfg):
    source = str(cfg["SOURCE_VIDEO"])
    weights = str(cfg["WEIGHTS"])
    out_video = str(cfg["OUT_VIDEO"])
    out_json = str(cfg["OUT_JSON"])
    conf_threshold = cfg["CONF_THRESH"]
    iou_threshold = cfg["IOU_THRESH"]
    attach_frames_threshold = cfg["ATTACH_FRAMES_THRESHOLD"]
    max_frames = cfg["MAX_FRAMES"] if cfg["MAX_FRAMES"] and cfg["MAX_FRAMES"] > 0 else None
    class_names = cfg.get("CLASS_NAMES", DEFAULT_DET_CLASSES)
    log_full = cfg.get("LOG_FULL_DETECTIONS", True)
    ts_roi_cfg = cfg.get("TIMESTAMP_ROI", None)
    ocr_conf = cfg.get("OCR_CONF", None)
    class_colors = cfg.get("CLASS_COLORS", {})
    status_font_scale = float(cfg.get("STATUS_FONT_SCALE", 0.66))
    status_bar_h = int(cfg.get("STATUS_BAR_H", 36))

    if pytesseract is None:
        print("WARNING: pytesseract not available -> timestamp OCR disabled; script will interpolate or use system time.")

    model = YOLO(weights)
    try:
        model.predictor.device = "cpu"
    except Exception:
        pass

    cap = cv2.VideoCapture(source)
    if not cap.isOpened():
        raise RuntimeError(f"Cannot open input video '{source}'")

    fps = cap.get(cv2.CAP_PROP_FPS) or 25.0
    width = int(cap.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(cap.get(cv2.CAP_PROP_FRAME_HEIGHT))
    total_frames = int(cap.get(cv2.CAP_PROP_FRAME_COUNT) or 0)
    if max_frames:
        total_frames = min(total_frames, max_frames)

    # compute ROI
    if ts_roi_cfg is None:
        rw = int(min(320, width * 0.35))
        rh = int(min(48, height * 0.06))
        rx = max(0, width - rw - 8)
        ry = max(0, height - rh - 8)
        ts_roi = (rx, ry, rw, rh)
    else:
        if isinstance(ts_roi_cfg, (list, tuple)) and len(ts_roi_cfg) == 4 and ts_roi_cfg[0] < 0:
            rw = int(ts_roi_cfg[2]); rh = int(ts_roi_cfg[3])
            rx = max(0, width + int(ts_roi_cfg[0])); ry = max(0, height + int(ts_roi_cfg[1]))
            ts_roi = (rx, ry, rw, rh)
        else:
            ts_roi = tuple(map(int, ts_roi_cfg))

    out_writer, actual_out_path, used_codec = create_video_writer(out_video, fps, width, height)

    json_log = {
        "source": source,
        "weights": weights,
        "frame_count_expected": total_frames,
        "fps": fps,
        "width": width,
        "height": height,
        "timestamp_roi": {"x": int(ts_roi[0]), "y": int(ts_roi[1]), "w": int(ts_roi[2]), "h": int(ts_roi[3])},
        "detections": [],
        "activity_instances": []
    }

    attach_history = deque(maxlen=attach_frames_threshold)
    current_activity = None
    # when activity open, we will keep counts per class
    frame_idx = 0
    pbar_total = total_frames if total_frames > 0 else None
    pbar = tqdm(total=pbar_total, desc="Processing", unit="fr")
    prev_valid_unix = None

    try:
        while True:
            ret, frame = cap.read()
            if not ret:
                break
            frame_idx += 1
            if max_frames and frame_idx > max_frames:
                break

            # OCR timestamp
            raw_ts, dt_obj = (None, None)
            if pytesseract is not None:
                raw_ts, dt_obj = ocr_read_timestamp(frame, roi=ts_roi, ocr_conf=ocr_conf)
            if dt_obj is not None:
                unix_ts = dt_obj.timestamp()
                prev_valid_unix = unix_ts
                time_source = "ocr"
            else:
                if prev_valid_unix is not None:
                    unix_ts = prev_valid_unix + (1.0 / fps)
                    prev_valid_unix = unix_ts
                    time_source = "interp"
                else:
                    unix_ts = time.time()
                    prev_valid_unix = unix_ts
                    time_source = "system"

            # inference
            try:
                results = model(frame, device="cpu", conf=conf_threshold, iou=iou_threshold, verbose=False)
            except TypeError:
                results = model(frame, conf=conf_threshold, iou=iou_threshold)
            res = results[0]

            # iterate detections
            detections = []
            # get raw class indices for presence flags
            cls_idxs_all = []
            if hasattr(res, "boxes") and res.boxes is not None and len(res.boxes) > 0:
                boxes = res.boxes.xyxy.cpu().numpy()
                scores = res.boxes.conf.cpu().numpy()
                cls_idxs = res.boxes.cls.cpu().numpy().astype(int)
                cls_idxs_all = cls_idxs.tolist()

                for bbox, score, cls_idx in zip(boxes, scores, cls_idxs):
                    name = class_names[cls_idx] if cls_idx < len(class_names) else str(cls_idx)
                    x1, y1, x2, y2 = xyxy_to_int(bbox)
                    det = {"class_id": int(cls_idx), "class_name": name, "score": float(score), "bbox": [x1, y1, x2, y2]}
                    if log_full:
                        detections.append(det)
                    # draw box in class color, fallback to cyan
                    color = class_colors.get(name, (255, 200, 0))
                    cv2.rectangle(frame, (x1, y1), (x2, y2), color, 2)
                    label = f"{name} {score:.2f}"
                    (tw, th), _ = cv2.getTextSize(label, cv2.FONT_HERSHEY_SIMPLEX, 0.5, 1)
                    cv2.rectangle(frame, (x1, y1 - 18), (x1 + tw + 4, y1), color, -1)
                    cv2.putText(frame, label, (x1 + 2, y1 - 4), cv2.FONT_HERSHEY_SIMPLEX, 0.5, (0,0,0), 1, cv2.LINE_AA)

            # presence flags
            present_names = set()
            for cls_idx in cls_idxs_all:
                if cls_idx < len(class_names):
                    present_names.add(class_names[int(cls_idx)])
                else:
                    present_names.add(str(int(cls_idx)))
            has_attached = "cluster_attached" in present_names
            has_detached = "cluster_detached" in present_names
            attach_history.append(1 if has_attached else 0)

            # ACTIVITY FSM using unix_ts
            if current_activity is None:
                if sum(attach_history) >= attach_frames_threshold:
                    start_time_unix = float(unix_ts)
                    current_activity = {
                        "start_time_unix": start_time_unix,
                        "start_time_str": raw_ts if raw_ts else pretty_time_from_unix(start_time_unix),
                        "end_time_unix": None,
                        "end_time_str": None,
                        "duration_sec": None,
                        "reason": "cluster_attached",
                        "milking": "yes",
                        "frames": [],
                        "class_counts": Counter()
                    }
            else:
                if has_detached:
                    end_time_unix = float(unix_ts)
                    current_activity["end_time_unix"] = end_time_unix
                    current_activity["end_time_str"] = raw_ts if raw_ts else pretty_time_from_unix(end_time_unix)
                    current_activity["duration_sec"] = current_activity["end_time_unix"] - current_activity["start_time_unix"]
                    # flatten Counter to normal dict for JSON
                    current_activity["class_counts"] = dict(current_activity["class_counts"])
                    json_log["activity_instances"].append(current_activity)
                    current_activity = None

            # if activity open, append per-frame summary and accumulate class_counts
            if current_activity is not None:
                # small summary per frame (time-based)
                frame_summary = {
                    "time_unix": float(unix_ts),
                    "time_str": raw_ts if raw_ts else pretty_time_from_unix(unix_ts),
                    "detections_count": int(len(cls_idxs_all))
                }
                # accumulate class counts from this frame's detections
                for cls_idx in cls_idxs_all:
                    name = class_names[int(cls_idx)] if cls_idx < len(class_names) else str(int(cls_idx))
                    current_activity["class_counts"][name] += 1
                current_activity["frames"].append(frame_summary)

            # per-frame milking flag (yes/no)
            if current_activity is not None:
                frame_milking_flag = "yes"
            else:
                frame_milking_flag = "yes" if has_attached and sum(attach_history) >= attach_frames_threshold else "no"

            # ------------------ STATUS BANNER (top-left lowered, black bg, white text) ------------------
            status_text = f"Milking: {'yes' if frame_milking_flag == 'yes' else 'no'}"
            # position: move down from top (so it doesn't overlap timestamp)
            x_text = 8
            y_text = 8 + int(status_font_scale * 40)  # lowered slightly from top
            # compute text size and draw background rectangle
            (text_w, text_h), _ = cv2.getTextSize(status_text, cv2.FONT_HERSHEY_SIMPLEX, status_font_scale, 2)
            pad = 8
            rect_x1, rect_y1 = 0, max(0, y_text - text_h - 6)
            rect_x2, rect_y2 = rect_x1 + text_w + pad * 2, rect_y1 + text_h + pad
            # ensure not to go below top of frame (clamp)
            rect_y1 = max(0, rect_y1)
            rect_y2 = min(height, rect_y2)
            cv2.rectangle(frame, (rect_x1, rect_y1), (rect_x2, rect_y2), (0, 0, 0), -1)  # black bg
            cv2.putText(frame, status_text, (rect_x1 + pad, rect_y2 - 8), cv2.FONT_HERSHEY_SIMPLEX, status_font_scale, (255, 255, 255), 2, cv2.LINE_AA)
            # --------------------------------------------------------------------------------------------

            # overlay the timestamp ROI and display OCR read/time for clarity (bottom-right)
            x_ro, y_ro, rw, rh = ts_roi
            cv2.rectangle(frame, (x_ro, y_ro), (x_ro + rw, y_ro + rh), (0, 0, 0), 1)
            ts_display = raw_ts if raw_ts else pretty_time_from_unix(unix_ts)
            cv2.putText(frame, ts_display, (x_ro + 4, y_ro + max(16, int(rh * 0.7))), cv2.FONT_HERSHEY_SIMPLEX, 0.45, (255, 255, 255), 1, cv2.LINE_AA)

            # frame-level JSON record (time-based, not frame-based)
            dt_unix = float(unix_ts)
            frame_log = {
                "time_str": raw_ts if raw_ts else pretty_time_from_unix(dt_unix),
                "time_unix": float(dt_unix),
                "time_only_sec": int((datetime.fromtimestamp(dt_unix).hour * 3600 +
                                      datetime.fromtimestamp(dt_unix).minute * 60 +
                                      datetime.fromtimestamp(dt_unix).second)),
                "time_source": time_source,
                "milking": frame_milking_flag,
                "detections_count": int(len(cls_idxs_all))
            }
            if log_full:
                frame_log["detections"] = detections
            json_log["detections"].append(frame_log)

            # ensure frame valid and write to video
            if frame is None:
                print(f"[WARN] frame is None at idx {frame_idx}; skipping write")
            else:
                if len(frame.shape) == 2:
                    frame = cv2.cvtColor(frame, cv2.COLOR_GRAY2BGR)
                if frame.dtype != np.uint8:
                    frame = (np.clip(frame, 0, 255)).astype(np.uint8)
                h_f, w_f = frame.shape[:2]
                if (w_f, h_f) != (width, height):
                    frame = cv2.resize(frame, (width, height))
                out_writer.write(frame)

            pbar.update(1)

    finally:
        pbar.close()
        cap.release()
        out_writer.release()

    # if activity still open at end, close it using last known unix_ts
    if current_activity is not None:
        last_unix = float(prev_valid_unix) if prev_valid_unix is not None else time.time()
        current_activity["end_time_unix"] = float(last_unix)
        current_activity["end_time_str"] = pretty_time_from_unix(last_unix)
        current_activity["duration_sec"] = current_activity["end_time_unix"] - current_activity["start_time_unix"]
        current_activity["class_counts"] = dict(current_activity["class_counts"])
        json_log["activity_instances"].append(current_activity)
        current_activity = None

    # finalize metadata and write JSON
    json_log["frame_count_processed"] = int(frame_idx)
    json_log["completed_at"] = time.strftime("%Y-%m-%dT%H:%M:%S", time.localtime())
    Path(out_json).parent.mkdir(parents=True, exist_ok=True)
    with open(out_json, "w", encoding="utf-8") as f:
        json.dump(json_log, f, indent=2)

    print(f"Done. annotated video written to: {actual_out_path}")
    print(f"JSON log: {out_json}")
    return actual_out_path, out_json

if __name__ == "__main__":
    run_detection(CONFIG)
