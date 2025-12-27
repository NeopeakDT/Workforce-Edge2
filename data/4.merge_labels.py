from pathlib import Path
import shutil
import random
import yaml

# ---------------- CONFIG ----------------
DATASETS = [
    Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal\WF_zip_files\Workforce Detection-2.v5i.yolov8"),
    Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal\WF_zip_files\Workforce_management-1.v2i.yolov8"),
]

OUT = Path(r"C:\Users\offic\OneDrive\Desktop\Pranjal\workforce_merged")
IMG_EXTS = {".jpg", ".jpeg", ".png"}
VAL_RATIO = 0.2
SEED = 42

# Canonical class list (single source of truth)
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

merged_images = []

# -------- Step 1: Merge TRAIN data safely --------
for ds in DATASETS:
    yaml_path = ds / "data.yaml"
    if not yaml_path.exists():
        raise FileNotFoundError(f"Missing data.yaml in {ds}")

    with open(yaml_path) as f:
        ds_yaml = yaml.safe_load(f)

    ds_names = ds_yaml["names"]
    ds_class_map = {name: idx for idx, name in ds_names.items()}

    # Validate class names
    for name in CLASS_NAMES:
        if name not in ds_class_map:
            raise ValueError(f"Class '{name}' missing in dataset {ds}")

    img_dir = ds / "train" / "images"
    lbl_dir = ds / "train" / "labels"

    for img in img_dir.iterdir():
        if img.suffix.lower() not in IMG_EXTS:
            continue

        new_name = f"{ds.name}_{img.name}"
        out_img = OUT / "images" / "train" / new_name
        out_lbl = OUT / "labels" / "train" / new_name.replace(img.suffix, ".txt")

        shutil.copy2(img, out_img)

        lbl = lbl_dir / img.with_suffix(".txt").name
        if not lbl.exists():
            out_lbl.touch()
            merged_images.append(out_img)
            continue

        with open(lbl) as f:
            lines = f.readlines()

        with open(out_lbl, "w") as f:
            for line in lines:
                parts = line.strip().split()
                old_cls = int(parts[0])
                cls_name = ds_names[old_cls]
                new_cls = CLASS_NAMES.index(cls_name)
                f.write(" ".join([str(new_cls)] + parts[1:]) + "\n")

        merged_images.append(out_img)

print(f"[INFO] Merged {len(merged_images)} images")

# -------- Step 2: Create VALID split --------
random.shuffle(merged_images)
val_count = int(len(merged_images) * VAL_RATIO)

for img_path in merged_images[:val_count]:
    lbl_path = OUT / "labels" / "train" / img_path.name.replace(img_path.suffix, ".txt")

    shutil.move(img_path, OUT / "images" / "valid" / img_path.name)
    if lbl_path.exists():
        shutil.move(lbl_path, OUT / "labels" / "valid" / lbl_path.name)

print(f"[INFO] Validation samples: {val_count}")

# -------- Step 3: data.yaml --------
data_yaml = {
    "path": str(OUT),
    "train": "images/train",
    "val": "images/valid",
    "names": {i: n for i, n in enumerate(CLASS_NAMES)},
}

with open(OUT / "data.yaml", "w") as f:
    yaml.safe_dump(data_yaml, f, sort_keys=False)

print("[INFO] data.yaml written")
