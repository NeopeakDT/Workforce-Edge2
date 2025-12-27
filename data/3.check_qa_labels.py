#!/usr/bin/env python3

import cv2
from pathlib import Path

QA_DIR = Path("1.pseudo_labeled")
IMG_DIR = QA_DIR / "images"
LBL_DIR = QA_DIR / "labels"

CLASS_NAMES = {
    0: "cow",
    1: "person",
    2: "scrapping_tool",
    3: "tmr_machine",
    4: "tractor",
}

COLORS = {
    0: (0, 255, 0),
    1: (255, 0, 0),
    2: (0, 255, 255),
    3: (255, 255, 0),
    4: (0, 0, 255),
}

OUT_DIR = Path("3.qa_preview")
OUT_DIR.mkdir(parents=True, exist_ok=True)

print(f"🔍 QA preview from: {IMG_DIR}")
print(f"📁 Output to: {OUT_DIR}\n")

for img_path in IMG_DIR.rglob("*"):
    if img_path.suffix.lower() not in {".jpg", ".jpeg", ".png"}:
        continue

    rel = img_path.relative_to(IMG_DIR)
    lbl_path = LBL_DIR / rel.with_suffix(".txt")

    img = cv2.imread(str(img_path))
    if img is None:
        print(f"⚠ Failed to read image: {rel}")
        continue

    h, w = img.shape[:2]

    if lbl_path.exists():
        with open(lbl_path) as f:
            for line in f:
                cls, xc, yc, bw, bh = map(float, line.split())
                cls = int(cls)

                # Skip unknown classes safely
                if cls not in CLASS_NAMES:
                    continue

                x1 = int((xc - bw / 2) * w)
                y1 = int((yc - bh / 2) * h)
                x2 = int((xc + bw / 2) * w)
                y2 = int((yc + bh / 2) * h)

                # Clamp to image bounds
                x1 = max(0, min(x1, w - 1))
                y1 = max(0, min(y1, h - 1))
                x2 = max(0, min(x2, w - 1))
                y2 = max(0, min(y2, h - 1))

                color = COLORS.get(cls, (255, 255, 255))
                label = CLASS_NAMES.get(cls, f"class_{cls}")

                cv2.rectangle(img, (x1, y1), (x2, y2), color, 2)
                cv2.putText(
                    img,
                    label,
                    (x1, max(15, y1 - 6)),
                    cv2.FONT_HERSHEY_SIMPLEX,
                    0.6,
                    color,
                    2,
                )

    # Preserve camera folder structure
    out_path = OUT_DIR / rel
    out_path.parent.mkdir(parents=True, exist_ok=True)
    cv2.imwrite(str(out_path), img)

    print(f"✅ Saved: {rel}")

print(f"\n📁 All QA preview images saved to: {OUT_DIR}")
