"""
pseudo_labelling_script.py - YOLO-based pseudo-labeling for unlabeled CCTV frames.
PURPOSE:
    Generate high-confidence pseudo-labels for unlabeled CCTV frames using a trained YOLO model (WF_V1.1_best.pt).

WHAT THIS SCRIPT DOES:
    - Runs single-frame YOLO inference on unlabeled images (no tracking).
    - Filters detections by confidence, class type, and minimum box area.
    - Preserves camera folder structure.
    - Copies images and writes YOLO-format labels to a temporary pseudo-labeled dataset.

WHAT THIS SCRIPT DOES NOT DO:
    - Does NOT modify original unlabeled data.
    - Does NOT perform QA or human review.
    - Does NOT create the final training dataset.

OUTPUT:
    data/pseudo_labeled/images/
    data/pseudo_labeled/labels/

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

MODEL = YOLO("../WF_V1.1_best.pt")

IMG_DIR = Path("unlabeled")
OUT_DIR = Path("1.pseudo_labeled")

OUT_IMG = OUT_DIR / "images"
OUT_LBL = OUT_DIR / "labels"

CONF_DEFAULT = 0.6
CONF_TOOLS = 0.7
MIN_AREA = 0.002  # normalized (w*h)

OUT_IMG.mkdir(parents=True, exist_ok=True)
OUT_LBL.mkdir(parents=True, exist_ok=True)

# OPTIONAL: map class IDs that are tools
TOOL_CLASS_IDS = {
    3,  # TMR
    4,  # tractor
    5,  # wheelbarrow
}

for img_path in IMG_DIR.rglob("*.jpg"):
    rel_path = img_path.relative_to(IMG_DIR)
    cam_dir = rel_path.parent

    results = MODEL(img_path, conf=CONF_DEFAULT, iou=0.5, verbose=False)

    if not results or len(results[0].boxes) == 0:
        continue

    # Prepare output dirs (preserve camera structure)
    (OUT_IMG / cam_dir).mkdir(parents=True, exist_ok=True)
    (OUT_LBL / cam_dir).mkdir(parents=True, exist_ok=True)

    valid_boxes = []

    for box in results[0].boxes:
        cls = int(box.cls)
        conf = float(box.conf)
        x, y, w, h = box.xywhn[0].tolist()

         # 🚫 SKIP scrapping_tool entirely
        if cls == 2:  # scrapping_tool
            continue
        
        if w * h < MIN_AREA:
            continue

        if cls in TOOL_CLASS_IDS and conf < CONF_TOOLS:
            continue

        if conf < CONF_DEFAULT:
            continue

        valid_boxes.append((cls, x, y, w, h))

    if not valid_boxes:
        continue

    # copy image (DO NOT MOVE)
    shutil.copy2(img_path, OUT_IMG / cam_dir / img_path.name)

    # write label
    label_path = (OUT_LBL / cam_dir / f"{img_path.stem}.txt")
    with open(label_path, "w") as f:
        for cls, x, y, w, h in valid_boxes:
            f.write(f"{cls} {x:.6f} {y:.6f} {w:.6f} {h:.6f}\n")

# --------------------------------------------------------------------------------------------------------

# Below code is used to check the classes in the model-

# from ultralytics import YOLO
# model = YOLO("WF_V1.1_best.pt")
# print(model.names)
