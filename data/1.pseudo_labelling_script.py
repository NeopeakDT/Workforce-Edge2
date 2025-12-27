#!/usr/bin/env python3
"""
pseudo_labelling_script.py - YOLO-based pseudo-labeling for unlabeled CCTV frames.

PURPOSE:
    Generate high-confidence pseudo-labels for unlabeled CCTV frames using a trained YOLO model.

WHAT THIS SCRIPT DOES:
    - Runs single-frame YOLO inference on unlabeled images (no tracking).
    - Applies class-specific confidence thresholds.
    - Filters small / noisy detections using minimum area.
    - Preserves camera folder structure.
    - Copies images and writes YOLO-format labels.

WHAT THIS SCRIPT DOES NOT DO:
    - Does NOT modify original unlabeled data.
    - Does NOT perform QA or human review.
    - Does NOT create the final training dataset.

PIPELINE STAGE:
    Phase A – Step 2 (Pseudo-Label Generation)

Classes:
0: cow
1: person
2: scrapping_tool
3: tmr_machine
4: tractor
"""

from ultralytics import YOLO
from pathlib import Path
import shutil

# -------------------------------------------------------------------
# CONFIG
# -------------------------------------------------------------------

MODEL = YOLO("../WF_V1.2_best.pt")

IMG_DIR = Path("unlabeled")
OUT_DIR = Path("1.pseudo_labeled")

OUT_IMG = OUT_DIR / "images"
OUT_LBL = OUT_DIR / "labels"

# Confidence thresholds
CONF_DEFAULT = 0.5        # cow, person
CONF_SCRAPPING_TOOL = 0.45   # class 2
CONF_TOOLS = 0.6         # tmr_machine, tractor

# Area thresholds (normalized YOLO format)
MIN_AREA_DEFAULT = 0.002
MIN_AREA_BY_CLASS = {
    2: 0.001,  # scrapping_tool (thin object)
}

# Class IDs
SCRAPPING_TOOL_CLASS_ID = 2
TOOL_CLASS_IDS = {3, 4}

# -------------------------------------------------------------------

OUT_IMG.mkdir(parents=True, exist_ok=True)
OUT_LBL.mkdir(parents=True, exist_ok=True)

# -------------------------------------------------------------------
# MAIN LOOP
# -------------------------------------------------------------------

total_images = 0
processed_images = 0
skipped_no_detections = 0
skipped_no_valid_boxes = 0

print(f"🔍 Scanning images in: {IMG_DIR}")
print(f"📁 Output directory: {OUT_DIR}\n")

for img_path in IMG_DIR.rglob("*"):
    if img_path.suffix.lower() not in {".jpg", ".jpeg", ".png"}:
        continue

    total_images += 1
    rel_path = img_path.relative_to(IMG_DIR)
    cam_dir = rel_path.parent

    # IMPORTANT: conf MUST be <= lowest class threshold (0.5)
    results = MODEL(
        img_path,
        conf=0.4,      # DO NOT increase
        iou=0.5,
        verbose=False
    )

    if not results or len(results[0].boxes) == 0:
        skipped_no_detections += 1
        continue

    (OUT_IMG / cam_dir).mkdir(parents=True, exist_ok=True)
    (OUT_LBL / cam_dir).mkdir(parents=True, exist_ok=True)

    valid_boxes = []

    for box in results[0].boxes:
        cls = int(box.cls)
        conf = float(box.conf)
        x, y, w, h = box.xywhn[0].tolist()

        # ------------------ AREA FILTER ------------------
        min_area = MIN_AREA_BY_CLASS.get(cls, MIN_AREA_DEFAULT)
        if (w * h) < min_area:
            continue

        # ---------------- CONFIDENCE FILTER --------------
        if cls == SCRAPPING_TOOL_CLASS_ID:
            if conf < CONF_SCRAPPING_TOOL:
                continue

        elif cls in TOOL_CLASS_IDS:
            if conf < CONF_TOOLS:
                continue

        else:
            if conf < CONF_DEFAULT:
                continue

        valid_boxes.append((cls, x, y, w, h))

    if not valid_boxes:
        skipped_no_valid_boxes += 1
        continue

    # ---------------- COPY IMAGE ------------------------
    shutil.copy2(img_path, OUT_IMG / cam_dir / img_path.name)

    # ---------------- WRITE LABEL -----------------------
    label_path = OUT_LBL / cam_dir / f"{img_path.stem}.txt"
    with open(label_path, "w") as f:
        for cls, x, y, w, h in valid_boxes:
            f.write(f"{cls} {x:.6f} {y:.6f} {w:.6f} {h:.6f}\n")

    processed_images += 1
    if processed_images % 50 == 0:
        print(f"✅ Processed {processed_images} images", end="\r")

# -------------------------------------------------------------------
# SUMMARY
# -------------------------------------------------------------------

print("\n\n📊 Processing Summary")
print(f"   Total images scanned        : {total_images}")
print(f"   ✅ Images saved              : {processed_images}")
print(f"   🚫 Skipped (no detections)   : {skipped_no_detections}")
print(f"   🚫 Skipped (filtered boxes)  : {skipped_no_valid_boxes}")

print("\n📁 Output locations")
print(f"   Images → {OUT_IMG}")
print(f"   Labels → {OUT_LBL}")

# -------------------------------------------------------------------
# END
# -------------------------------------------------------------------
