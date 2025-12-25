"""
Dataset merge and preparation script for YOLO training.

This script:
- Merges multiple Roboflow-exported YOLO datasets (train-only) into a single dataset
- Prevents filename collisions by prefixing source dataset names
- Creates a validation split from the merged training data (configurable ratio)
- Preserves image–label pairing and YOLO annotation format
- Automatically generates a canonical data.yaml file for Ultralytics YOLO

Output:
- workforce_merged/images/{train, valid}
- workforce_merged/labels/{train, valid}
- workforce_merged/data.yaml

Intended for object-detection datasets with identical class definitions.
"""


from pathlib import Path
import shutil
import random
import yaml   # pip install pyyaml if missing

# ---------------- CONFIG ----------------
DATASETS = [
    Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal\WF_zip_files\Workforce Detection-2.v5i.yolov8"),   # contains train/images, train/labels
    Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal\WF_zip_files\Workforce_management-1.v2i.yolov8")
]

OUT = Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal")
IMG_EXTS = {".jpg", ".jpeg", ".png"}
VAL_RATIO = 0.2
SEED = 42

# Canonical class list (SINGLE SOURCE OF TRUTH)
CLASS_NAMES = [
    "cow",
    "person",
    "scrapping_tool",
    "tmr_machine",
    "tractor"
]
# --------------------------------------

random.seed(SEED)

# Create output folders
for split in ["train", "valid"]:
    (OUT / "images" / split).mkdir(parents=True, exist_ok=True)
    (OUT / "labels" / split).mkdir(parents=True, exist_ok=True)

# -------- Step 1: Merge all TRAIN data --------
merged_images = []

for ds in DATASETS:
    img_dir = ds / "train" / "images"
    lbl_dir = ds / "train" / "labels"

    if not img_dir.exists():
        raise FileNotFoundError(f"Missing {img_dir}")

    for img in img_dir.iterdir():
        if img.suffix.lower() not in IMG_EXTS:
            continue

        new_name = f"{ds.name}_{img.name}"
        out_img = OUT / "images" / "train" / new_name
        out_lbl = OUT / "labels" / "train" / new_name.replace(img.suffix, ".txt")

        shutil.copy2(img, out_img)

        lbl = lbl_dir / img.with_suffix(".txt").name
        if lbl.exists():
            shutil.copy2(lbl, out_lbl)
        else:
            out_lbl.touch()

        merged_images.append(out_img)

print(f"[INFO] Merged {len(merged_images)} images into train/")

# -------- Step 2: Create VALID split --------
random.shuffle(merged_images)
val_count = int(len(merged_images) * VAL_RATIO)

for img_path in merged_images[:val_count]:
    lbl_path = OUT / "labels" / "train" / img_path.name.replace(img_path.suffix, ".txt")

    shutil.move(img_path, OUT / "images" / "valid" / img_path.name)

    if lbl_path.exists():
        shutil.move(lbl_path, OUT / "labels" / "valid" / lbl_path.name)

print(f"[INFO] Moved {val_count} images to valid/")

# -------- Step 3: Auto-generate data.yaml --------
data_yaml = {
    "path": str(OUT),
    "train": "images/train",
    "val": "images/valid",
    "names": {i: name for i, name in enumerate(CLASS_NAMES)}
}

yaml_path = OUT / "data.yaml"
with open(yaml_path, "w") as f:
    yaml.safe_dump(data_yaml, f, sort_keys=False)

print(f"[INFO] data.yaml written to {yaml_path}")
