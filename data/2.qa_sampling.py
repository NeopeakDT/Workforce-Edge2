"""
PURPOSE:
    Select a small, representative subset of pseudo-labeled images for manual QA and correction.

WHAT THIS SCRIPT DOES:
    - Reads existing pseudo-labeled images and labels.
    - Samples ~5–10% of per camera.
    - Prioritizes frames with multiple objects (higher activity context).
    - Copies selected images and labels into a QA-only dataset for CVAT review.

WHAT THIS SCRIPT DOES NOT DO:
    - Does NOT run model inference.
    - Does NOT change or generate labels.
    - Does NOT affect the full pseudo-labeled dataset.

OUTPUT:
    qa_sample/images/
    qa_sample/labels/

PIPELINE STAGE:
    Phase A – Step 3 (QA Sampling before Final Retraining)
"""
import random
import shutil
from pathlib import Path
from collections import defaultdict

PSEUDO_DIR = Path("1.pseudo_labeled")
OUT_DIR = Path("2.qa_sample")

SAMPLE_RATIO = 1   # 100%
MIN_BOXES = 1         # prioritize images with >= 1 objects

IMG_DIR = PSEUDO_DIR / "images"
LBL_DIR = PSEUDO_DIR / "labels"

OUT_IMG = OUT_DIR / "images"
OUT_LBL = OUT_DIR / "labels"

OUT_IMG.mkdir(parents=True, exist_ok=True)
OUT_LBL.mkdir(parents=True, exist_ok=True)

# Collect per-camera candidates
candidates = defaultdict(list)

for lbl_path in LBL_DIR.rglob("*.txt"):
    cam = lbl_path.parent.relative_to(LBL_DIR)
    with open(lbl_path) as f:
        num_boxes = sum(1 for _ in f)

    if num_boxes >= MIN_BOXES:
        candidates[cam].append(lbl_path.stem)

# Sample per camera
for cam, stems in candidates.items():
    if not stems:
        continue

    k = max(1, int(len(stems) * SAMPLE_RATIO))
    sampled = random.sample(stems, k)

    (OUT_IMG / cam).mkdir(parents=True, exist_ok=True)
    (OUT_LBL / cam).mkdir(parents=True, exist_ok=True)

    for stem in sampled:
        shutil.copy2(IMG_DIR / cam / f"{stem}.jpg",
                     OUT_IMG / cam / f"{stem}.jpg")
        shutil.copy2(LBL_DIR / cam / f"{stem}.txt",
                     OUT_LBL / cam / f"{stem}.txt")

print("QA sampling completed.")
